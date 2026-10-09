package service

import (
	"context"
	"encoding/json"
	"errors"
	"mime/multipart"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type autoCompletionUnavailableQueue struct{ calls int }

func (q *autoCompletionUnavailableQueue) Enqueue(*asynq.Task, ...asynq.Option) (*asynq.TaskInfo, error) {
	q.calls++
	return nil, errors.New("isolated enqueue unavailable")
}

func autoCompletionFixture(t *testing.T, pg bool) *machinePipelineFixture {
	t.Helper()
	m := newMachinePipelineFixture(t, pg)
	require.NoError(t, m.f.db.AutoMigrate(&types.TaskPendingOp{}))
	sql, err := m.f.db.DB()
	require.NoError(t, err)
	sql.SetMaxOpenConns(1)
	sql.SetMaxIdleConns(1)
	var kb types.KnowledgeBase
	require.NoError(t, m.f.db.Where("id='kb'").Take(&kb).Error)
	kb.QuestionGenerationConfig = &types.QuestionGenerationConfig{Enabled: true, QuestionCount: 2}
	require.NoError(t, m.f.store.UpdateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(m.f.ctx), &kb))
	m.f.model.detectQuestions = true
	m.f.model.response = `{"summary":"A complete generated summary","gist":"Generated original profile title","topics":["actual topic"],"doc_type":"guide","typical_question":"What is this actual topic?"}`
	return m
}

func autoCompletionRunUntilPending(t *testing.T, m *machinePipelineFixture) (*types.Knowledge, types.TaskPendingOp) {
	t.Helper()
	row := m.ingest(t)
	for i := 0; i < len(m.f.queue.tasks); i++ {
		require.NoError(t, m.run(t, m.f.queue.tasks[i]), m.f.queue.tasks[i].Type())
	}
	require.Equal(t, 2, m.f.model.calls, "Summary and QA must finish before sealing Auto")
	require.Zero(t, m.model.calls)
	require.NoError(t, m.f.db.Where("id=?", row.ID).Take(row).Error)
	require.Equal(t, types.ParseStatusCompleted, row.ParseStatus)
	require.NotNil(t, row.Profile)
	var op types.TaskPendingOp
	require.NoError(t, m.f.db.Where("task_type=? AND scope_id=?", types.TypeKnowledgeAutoTag, row.ID).Take(&op).Error)
	require.Equal(t, "auto_tag_completion", op.Op)
	var p types.KnowledgeAutoTagPayload
	require.NoError(t, json.Unmarshal(op.Payload, &p))
	require.NotNil(t, p.TaskInput)
	require.NotEmpty(t, p.CompletionInput)
	require.NoError(t, m.f.store.VerifyKnowledgeAutoTagTaskInput(context.Background(), p))
	var receipt struct{ ActualInputs string }
	require.NoError(t, m.f.db.Table("knowledge_auto_tag_input_manifests").Select("actual_inputs").Where("manifest_id=?", p.CompletionInput).Take(&receipt).Error)
	require.Contains(t, receipt.ActualInputs, "Generated original profile title")
	require.Contains(t, receipt.ActualInputs, "questions")
	return row, op
}

func autoCompletionResume(t *testing.T, m *machinePipelineFixture, op types.TaskPendingOp) *asynq.Task {
	t.Helper()
	// A new producer object rehydrates only persisted SQL bytes after a
	// process-local queue has disappeared. It does not recapture current body.
	queue := &knowledgeInputQueue{}
	count, err := RecoverKnowledgeAutoTagCompletions(context.Background(), m.f.store, queue, op.ScopeID)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Len(t, queue.tasks, 1)
	require.JSONEq(t, string(op.Payload), string(queue.tasks[0].Payload()))
	require.NoError(t, m.run(t, queue.tasks[0]))
	require.Equal(t, 1, m.model.calls)
	var saved types.TaskPendingOp
	require.NoError(t, m.f.db.Where("id=?", op.ID).Take(&saved).Error)
	require.Equal(t, "auto_tag_done", saved.Op)
	require.NoError(t, m.run(t, queue.tasks[0]), "a duplicate terminal delivery is idempotent")
	require.Equal(t, 1, m.model.calls, "a duplicate cannot repeat model or tags")
	var attached int64
	require.NoError(t, m.f.db.Model(&types.KnowledgeTagRelation{}).Where("knowledge_id=? AND tag_id='machine-topic'", op.ScopeID).Count(&attached).Error)
	require.EqualValues(t, 1, attached)
	count, err = RecoverKnowledgeAutoTagCompletions(context.Background(), m.f.store, queue, op.ScopeID)
	require.NoError(t, err)
	require.Zero(t, count, "a terminal intent cannot be awakened by restart")
	return queue.tasks[0]
}

func runAutoCompletionDurability(t *testing.T, pg bool) {
	for _, failure := range []string{"enqueue_failure", "exit_before_enqueue", "consumer_process_exit"} {
		t.Run(failure, func(t *testing.T) {
			m := autoCompletionFixture(t, pg)
			if failure == "exit_before_enqueue" || failure == "consumer_process_exit" {
				// This hook is wake-up only. The transactional handoff remains installed.
				repository.ConfigureKnowledgeCompletionObserver(m.f.s.repo, nil)
			} else {
				bad := &autoCompletionUnavailableQueue{}
				m.f.post.taskEnqueuer = bad
				// The postprocess fan-out needs its normal queue; only completion wakeup
				// is unavailable. Reconfigure its observer with an actual failed sender.
				repository.ConfigureKnowledgeCompletionObserver(m.f.s.repo, func(ctx context.Context, id string) error {
					_, err := RecoverKnowledgeAutoTagCompletions(ctx, m.f.store, bad, id)
					require.Error(t, err)
					return nil
				})
				m.f.post.taskEnqueuer = m.f.queue
			}
			row, op := autoCompletionRunUntilPending(t, m)
			if failure == "consumer_process_exit" {
				actualCompletionChildExit(t, m, op, pg)
			}
			autoCompletionResume(t, m, op)
			require.NoError(t, m.f.db.Where("id=?", row.ID).Take(row).Error)
			require.Equal(t, "enabled", row.EnableStatus)
			guard, err := NewKnowledgeOutputAuthorizer(m.f.store, m.f.guard).Begin(m.f.ctx, []*types.Knowledge{row})
			require.NoError(t, err)
			require.NoError(t, guard.Check(m.f.ctx, []*types.Knowledge{row}))
		})
	}
}
func TestKnowledgeAutoCompletionDurabilitySQLitePoolOne(t *testing.T) {
	runAutoCompletionDurability(t, false)
}
func TestPostgresKnowledgeAutoCompletionDurabilityPoolOne(t *testing.T) {
	runAutoCompletionDurability(t, true)
}

func actualCompletionChildExit(t *testing.T, m *machinePipelineFixture, op types.TaskPendingOp, pg bool) {
	t.Helper()
	payload := filepath.Join(t.TempDir(), "actual-auto-payload.json")
	require.NoError(t, os.WriteFile(payload, op.Payload, 0600))
	dsn := ""
	dialect := "sqlite"
	if pg {
		dialect = "postgres"
		var schema string
		require.NoError(t, m.f.db.Raw("SELECT current_schema()").Scan(&schema).Error)
		dsn = os.Getenv("WEKNORA_AUTO_TAG_TEST_POSTGRES_DSN") + " search_path=" + schema
	} else {
		var databases []struct{ File string }
		require.NoError(t, m.f.db.Raw("PRAGMA database_list").Scan(&databases).Error)
		require.Len(t, databases, 1)
		dsn = databases[0].File
	}
	binary, err := os.Executable()
	require.NoError(t, err)
	command := exec.Command(binary, "-test.run=^TestKnowledgeAutoCompletionCrashChild$", "-test.v")
	command.Env = append(os.Environ(), "WEKNORA_AUTO_COMPLETION_CRASH_PAYLOAD="+payload, "WEKNORA_AUTO_COMPLETION_CRASH_DIALECT="+dialect, "WEKNORA_AUTO_COMPLETION_CRASH_DSN="+dsn)
	output, err := command.CombinedOutput()
	var exited *exec.ExitError
	require.ErrorAs(t, err, &exited, string(output))
	require.Equal(t, 73, exited.ExitCode(), string(output))
	var saved types.TaskPendingOp
	require.NoError(t, m.f.db.Where("id=?", op.ID).Take(&saved).Error)
	require.Equal(t, "auto_tag_running", saved.Op, "actual child exited after SQL claim commit")
	require.NotNil(t, saved.ClaimedAt)
	// Advance the durable claim's clock past the documented recovery grace.
	// The child exit is real; waiting three minutes adds no different boundary.
	stale := time.Now().UTC().Add(-4 * time.Minute)
	require.NoError(t, m.f.db.Model(&types.TaskPendingOp{}).Where("id=?", op.ID).Update("claimed_at", stale).Error)
}
func TestKnowledgeAutoCompletionCrashChild(t *testing.T) {
	path := os.Getenv("WEKNORA_AUTO_COMPLETION_CRASH_PAYLOAD")
	if path == "" {
		return
	}
	var dialector gorm.Dialector = sqlite.Open(os.Getenv("WEKNORA_AUTO_COMPLETION_CRASH_DSN"))
	if os.Getenv("WEKNORA_AUTO_COMPLETION_CRASH_DIALECT") == "postgres" {
		dialector = postgres.Open(os.Getenv("WEKNORA_AUTO_COMPLETION_CRASH_DSN"))
	}
	db, err := gorm.Open(dialector, &gorm.Config{})
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var p types.KnowledgeAutoTagPayload
	require.NoError(t, json.Unmarshal(data, &p))
	store := repository.NewKnowledgeInputProvenanceStore(db)
	require.NoError(t, store.VerifyKnowledgeAutoTagTaskInput(context.Background(), p))
	claim, err := store.ClaimKnowledgeAutoTagCompletion(context.Background(), p)
	require.NoError(t, err)
	require.NotNil(t, claim)
	os.Exit(73)
}

func runAutoCompletionWithdraw(t *testing.T, pg bool) {
	for _, mutation := range []string{"source_withdraw", "actor_withdraw", "model_ABA", "tenant_ABA", "body_ABA", "publication_etag_ABA", "publication_updated_at_ABA", "publication_metadata_ABA", "queue_forge"} {
		t.Run(mutation, func(t *testing.T) {
			m := autoCompletionFixture(t, pg)
			m.authenticated = true
			repository.ConfigureKnowledgeCompletionObserver(m.f.s.repo, nil)
			_, op := autoCompletionRunUntilPending(t, m)
			task := asynq.NewTask(types.TypeKnowledgeAutoTag, op.Payload)
			switch mutation {
			case "source_withdraw":
				m.allow.Store(false)
			case "actor_withdraw":
				require.NoError(t, m.f.db.Model(&types.User{}).Where("id='enrich-human'").Update("is_active", false).Error)
			case "model_ABA":
				require.NoError(t, m.f.db.Model(&types.Model{}).Where("id='auto-model'").Update("name", "withdrawn").Error)
				require.NoError(t, m.f.db.Model(&types.Model{}).Where("id='auto-model'").Update("name", "actual local auto mock").Error)
			case "tenant_ABA":
				require.NoError(t, m.f.db.Model(&types.Tenant{}).Where("id=7").Update("status", "withdrawn").Error)
				require.NoError(t, m.f.db.Model(&types.Tenant{}).Where("id=7").Update("status", "").Error)
			case "body_ABA":
				var c types.Chunk
				require.NoError(t, m.f.db.Where("knowledge_id=? AND chunk_type=?", op.ScopeID, types.ChunkTypeText).Take(&c).Error)
				require.NoError(t, m.f.db.Model(&types.Chunk{}).Where("id=?", c.ID).Update("content", "withdrawn").Error)
				require.NoError(t, m.f.db.Model(&types.Chunk{}).Where("id=?", c.ID).Update("content", c.Content).Error)
			case "publication_etag_ABA", "publication_updated_at_ABA", "publication_metadata_ABA":
				var k types.Knowledge
				require.NoError(t, m.f.db.Where("id=?", op.ScopeID).Take(&k).Error)
				if mutation == "publication_updated_at_ABA" {
					require.NoError(t, m.f.db.Model(&types.Knowledge{}).Where("id=?", k.ID).Update("updated_at", k.UpdatedAt.Add(time.Second)).Error)
					require.NoError(t, m.f.db.Model(&types.Knowledge{}).Where("id=?", k.ID).Update("updated_at", k.UpdatedAt).Error)
				} else {
					metadata := k.GetMetadata()
					if mutation == "publication_etag_ABA" {
						metadata["nextcloud_etag"] = "external-forged"
					} else {
						metadata["external_bookkeeping"] = "external-forged"
					}
					data, err := json.Marshal(metadata)
					require.NoError(t, err)
					require.NoError(t, m.f.db.Model(&types.Knowledge{}).Where("id=?", k.ID).Update("metadata", types.JSON(data)).Error)
					require.NoError(t, m.f.db.Model(&types.Knowledge{}).Where("id=?", k.ID).Update("metadata", k.Metadata).Error)
				}
			case "queue_forge":
				var p types.KnowledgeAutoTagPayload
				require.NoError(t, json.Unmarshal(op.Payload, &p))
				p.Language = "forged-current"
				task = enrichmentTask(t, types.TypeKnowledgeAutoTag, p)
			}
			require.Error(t, m.run(t, task))
			require.Zero(t, m.model.calls)
			var added int64
			require.NoError(t, m.f.db.Model(&types.KnowledgeTagRelation{}).Where("knowledge_id=? AND tag_id='machine-topic'", op.ScopeID).Count(&added).Error)
			require.Zero(t, added)
			if mutation != "queue_forge" {
				var saved types.TaskPendingOp
				require.NoError(t, m.f.db.Where("id=?", op.ID).Take(&saved).Error)
				require.Equal(t, "auto_tag_rejected", saved.Op)
				m.allow.Store(true)
				require.NoError(t, m.run(t, asynq.NewTask(types.TypeKnowledgeAutoTag, op.Payload)))
				require.Zero(t, m.model.calls, "restored source cannot revive a rejected old task")
			}
		})
	}
}
func TestKnowledgeAutoCompletionWithdrawSQLitePoolOne(t *testing.T) {
	runAutoCompletionWithdraw(t, false)
}
func TestPostgresKnowledgeAutoCompletionWithdrawPoolOne(t *testing.T) {
	runAutoCompletionWithdraw(t, true)
}

func runAutoCompletionActualRedis(t *testing.T, pg bool) {
	addr := os.Getenv("WEKNORA_AUTO_COMPLETION_TEST_REDIS_ADDR")
	require.NotEmpty(t, addr, "isolated Redis address is mandatory for this test")
	m := autoCompletionFixture(t, pg)
	// A real Asynq sender reaches an unavailable loopback port. The permanent
	// SQL intent must survive the actual network/enqueue error.
	offline := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond, MaxRetries: -1})
	defer offline.Close()
	unavailable := asynq.NewClientFromRedisClient(offline)
	repository.ConfigureKnowledgeCompletionObserver(m.f.s.repo, func(ctx context.Context, id string) error {
		_, err := RecoverKnowledgeAutoTagCompletions(ctx, m.f.store, unavailable, id)
		require.Error(t, err)
		return nil
	})
	_, op := autoCompletionRunUntilPending(t, m)
	restored := asynq.NewClient(asynq.RedisClientOpt{Addr: addr, DB: 9})
	defer restored.Close()
	count, err := RecoverKnowledgeAutoTagCompletions(context.Background(), m.f.store, restored, op.ScopeID)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: addr, DB: 9})
	defer inspector.Close()
	pending, err := inspector.ListPendingTasks(types.QueueSummary)
	require.NoError(t, err)
	var delivered *asynq.TaskInfo
	for _, p := range pending {
		if string(p.Payload) == string(op.Payload) {
			delivered = p
			break
		}
	}
	require.NotNil(t, delivered, "actual Redis contains the exact SQL-sealed payload")
	require.NoError(t, m.run(t, asynq.NewTask(delivered.Type, delivered.Payload)))
	require.Equal(t, 1, m.model.calls)
	require.NoError(t, inspector.DeleteTask(delivered.Queue, delivered.ID))
}
func TestKnowledgeAutoCompletionActualRedisSQLite(t *testing.T) {
	runAutoCompletionActualRedis(t, false)
}
func TestPostgresKnowledgeAutoCompletionActualRedis(t *testing.T) {
	runAutoCompletionActualRedis(t, true)
}

var _ interfaces.TaskEnqueuer = (*autoCompletionUnavailableQueue)(nil)

func runAutoCompletionProducerSQLFailure(t *testing.T, pg bool) {
	m := autoCompletionFixture(t, pg)
	repository.ConfigureKnowledgeCompletionObserver(m.f.s.repo, nil)
	row := m.ingest(t)
	var last *asynq.Task
	for i := 0; i < len(m.f.queue.tasks); i++ {
		task := m.f.queue.tasks[i]
		if task.Type() == types.TypeQuestionGeneration {
			last = task
			break
		}
		require.NoError(t, m.run(t, task), task.Type())
	}
	require.NotNil(t, last)
	require.NoError(t, m.f.db.Exec("ALTER TABLE task_pending_ops RENAME TO auto_completion_unavailable").Error)
	require.Error(t, m.run(t, last), "failed SQL production must reach the actual task's returned error")
	require.NoError(t, m.f.db.Where("id=?", row.ID).Take(row).Error)
	require.Equal(t, types.ParseStatusFinalizing, row.ParseStatus)
	require.Equal(t, 1, row.PendingSubtasksCount)
	var receipts int64
	require.NoError(t, m.f.db.Table("knowledge_auto_tag_input_manifests").Count(&receipts).Error)
	require.Zero(t, receipts, "completion, candidate/model receipt and intent roll back together")
	require.NoError(t, m.f.db.Exec("ALTER TABLE auto_completion_unavailable RENAME TO task_pending_ops").Error)
	require.NoError(t, m.run(t, last), "actual task redelivery retries its rolled-back handoff")
	var op types.TaskPendingOp
	require.NoError(t, m.f.db.Where("scope_id=? AND task_type=?", row.ID, types.TypeKnowledgeAutoTag).Take(&op).Error)
	autoCompletionResume(t, m, op)
}

func TestKnowledgeAutoCompletionProducerSQLFailureSQLitePoolOne(t *testing.T) {
	runAutoCompletionProducerSQLFailure(t, false)
}
func TestPostgresKnowledgeAutoCompletionProducerSQLFailurePoolOne(t *testing.T) {
	runAutoCompletionProducerSQLFailure(t, true)
}

func runAutoCompletionPublicationOutage(t *testing.T, pg bool) {
	for _, probe := range []int64{1, 2} {
		t.Run(map[int64]string{1: "before_publish_commit", 2: "after_publish_commit"}[probe], func(t *testing.T) {
			m := autoCompletionFixture(t, pg)
			repository.ConfigureKnowledgeCompletionObserver(m.f.s.repo, nil)
			row := m.ingest(t)
			m.publicationFailureProbe.Store(probe)
			m.publicationUnavailableAfterCompletion.Store(true)
			var lastErr error
			for i := 0; i < len(m.f.queue.tasks); i++ {
				lastErr = m.run(t, m.f.queue.tasks[i])
				if lastErr != nil {
					break
				}
			}
			require.Error(t, lastErr, "actual signed publication transport returns HTTP503 after completion commits")
			require.NoError(t, m.f.db.Where("id=?", row.ID).Take(row).Error)
			require.Equal(t, types.ParseStatusCompleted, row.ParseStatus)
			require.Empty(t, row.GetMetadata()["nextcloud_etag"], "source remains unpublished during unavailable verification")
			var op types.TaskPendingOp
			require.NoError(t, m.f.db.Where("scope_id=? AND task_type=?", row.ID, types.TypeKnowledgeAutoTag).Take(&op).Error)
			require.Equal(t, "auto_tag_completion", op.Op)
			m.publicationUnavailableAfterCompletion.Store(false)
			autoCompletionResume(t, m, op)
			require.NoError(t, m.f.db.Where("id=?", row.ID).Take(row).Error)
			require.Equal(t, "actual-etag", row.GetMetadata()["nextcloud_etag"])
			var transition int64
			require.NoError(t, m.f.db.Table("knowledge_auto_publication_transitions").Where("knowledge_id=?", row.ID).Count(&transition).Error)
			require.EqualValues(t, 2*probe-1, transition, "recovery uses only contiguous real publish/unpublish SQL receipts")
		})
	}
}
func TestKnowledgeAutoCompletionPublicationOutageSQLitePoolOne(t *testing.T) {
	runAutoCompletionPublicationOutage(t, false)
}
func TestPostgresKnowledgeAutoCompletionPublicationOutagePoolOne(t *testing.T) {
	runAutoCompletionPublicationOutage(t, true)
}

// Forward every operation to the actual KnowledgeService. The sole choice is
// an explicit original per-upload process configuration: Summary/QA are off.
// The normal signed sync/file admission seals these controls before parsing.
type autoCompletionZeroEnrichmentSource struct{ interfaces.KnowledgeService }

func (s autoCompletionZeroEnrichmentSource) CreateKnowledgeFromFile(ctx context.Context, kb string, file *multipart.FileHeader, metadata map[string]string, multimodal *bool, name string, tags []string, channel string, _ *types.KnowledgeProcessOverrides) (*types.Knowledge, error) {
	disabled := false
	return s.KnowledgeService.CreateKnowledgeFromFile(ctx, kb, file, metadata, multimodal, name, tags, channel, &types.KnowledgeProcessOverrides{SummaryEnabled: &disabled, QuestionGenerationConfig: &types.QuestionGenerationConfig{Enabled: false}})
}
func runAutoCompletionZeroSubtasks(t *testing.T, pg bool) {
	m := autoCompletionFixture(t, pg)
	m.source.knowledgeService = autoCompletionZeroEnrichmentSource{m.f.s}
	repository.ConfigureKnowledgeCompletionObserver(m.f.s.repo, nil)
	row := m.ingest(t)
	for i := 0; i < len(m.f.queue.tasks); i++ {
		require.NoError(t, m.run(t, m.f.queue.tasks[i]))
	}
	require.Zero(t, m.f.model.calls)
	require.NoError(t, m.f.db.Where("id=?", row.ID).Take(row).Error)
	require.Equal(t, types.ParseStatusCompleted, row.ParseStatus)
	require.Zero(t, row.PendingSubtasksCount)
	var op types.TaskPendingOp
	require.NoError(t, m.f.db.Where("scope_id=? AND task_type=?", row.ID, types.TypeKnowledgeAutoTag).Take(&op).Error)
	autoCompletionResume(t, m, op)
}
func TestKnowledgeAutoCompletionZeroSubtasksSQLitePoolOne(t *testing.T) {
	runAutoCompletionZeroSubtasks(t, false)
}
func TestPostgresKnowledgeAutoCompletionZeroSubtasksPoolOne(t *testing.T) {
	runAutoCompletionZeroSubtasks(t, true)
}

func runAutoCompletionClaimFence(t *testing.T, pg bool) {
	m := autoCompletionFixture(t, pg)
	repository.ConfigureKnowledgeCompletionObserver(m.f.s.repo, nil)
	_, op := autoCompletionRunUntilPending(t, m)
	var p types.KnowledgeAutoTagPayload
	require.NoError(t, json.Unmarshal(op.Payload, &p))
	var replacement *time.Time
	m.model.beforeReturn = func() {
		require.NoError(t, m.f.db.Model(&types.TaskPendingOp{}).Where("id=?", op.ID).Update("claimed_at", time.Now().UTC().Add(-4*time.Minute)).Error)
		var err error
		replacement, err = m.f.store.ClaimKnowledgeAutoTagCompletion(context.Background(), p)
		require.NoError(t, err)
		require.NotNil(t, replacement)
	}
	require.Error(t, m.run(t, asynq.NewTask(types.TypeKnowledgeAutoTag, op.Payload)), "a consumer whose claim expired cannot atomically publish tag relations")
	var added int64
	require.NoError(t, m.f.db.Model(&types.KnowledgeTagRelation{}).Where("knowledge_id=? AND tag_id='machine-topic'", op.ScopeID).Count(&added).Error)
	require.Zero(t, added, "the losing claim rolls back tags and their producer receipt")
	var current types.TaskPendingOp
	require.NoError(t, m.f.db.Where("id=?", op.ID).Take(&current).Error)
	require.Equal(t, "auto_tag_running", current.Op, "the losing worker cannot reset another worker's claim")
	require.True(t, replacement.Equal(*current.ClaimedAt))
	require.NoError(t, m.f.store.FinishKnowledgeAutoTagCompletion(context.Background(), p, *replacement, false, false))
	m.model.beforeReturn = nil
	require.NoError(t, m.run(t, asynq.NewTask(types.TypeKnowledgeAutoTag, op.Payload)))
	require.Equal(t, 2, m.model.calls, "only the replacement delivery may persist its model result")
	require.NoError(t, m.f.db.Model(&types.KnowledgeTagRelation{}).Where("knowledge_id=? AND tag_id='machine-topic'", op.ScopeID).Count(&added).Error)
	require.EqualValues(t, 1, added)
}
func TestKnowledgeAutoCompletionClaimFenceSQLitePoolOne(t *testing.T) {
	runAutoCompletionClaimFence(t, false)
}
func TestPostgresKnowledgeAutoCompletionClaimFencePoolOne(t *testing.T) {
	runAutoCompletionClaimFence(t, true)
}
