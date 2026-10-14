package router

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type syncLifecycleKnowledgeRepo struct {
	interfaces.KnowledgeRepository
	row      *types.Knowledge
	failures int
}

func (r *syncLifecycleKnowledgeRepo) GetKnowledgeByID(context.Context, uint64, string) (*types.Knowledge, error) {
	return r.row, nil
}

func (r *syncLifecycleKnowledgeRepo) FailKnowledgeAuthorization(context.Context, *types.Knowledge, bool) error {
	r.failures++
	return nil
}

type syncLifecycleChunkRepo struct{ interfaces.ChunkRepository }

func (syncLifecycleChunkRepo) GetChunkByID(context.Context, uint64, string) (*types.Chunk, error) {
	return &types.Chunk{ID: "chunk", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "doc"}, nil
}

type syncLifecycleTracker struct {
	service.SpanTracker
	finalized int
}

func (*syncLifecycleTracker) LatestAttempt(context.Context, string) int { return 2 }

func (s *syncLifecycleTracker) FinalizeAttempt(_ context.Context, knowledgeID string, attempt int, status string,
	_ types.JSONMap, code, _ string,
) {
	if knowledgeID == "doc" && attempt == 2 && status == types.SpanStatusFailed && code == "AUTHORIZATION_REVOKED" {
		s.finalized++
	}
}

type syncLifecycleTaskHandler struct {
	handle func(context.Context, *asynq.Task) error
}

func (s syncLifecycleTaskHandler) Handle(ctx context.Context, task *asynq.Task) error {
	return s.handle(ctx, task)
}

type syncLifecycleKnowledgeService struct {
	interfaces.KnowledgeService
	syncLifecycleTaskHandler
}

func (s syncLifecycleKnowledgeService) ProcessDocument(ctx context.Context, task *asynq.Task) error {
	return s.Handle(ctx, task)
}
func (s syncLifecycleKnowledgeService) ProcessManualUpdate(ctx context.Context, task *asynq.Task) error {
	return s.Handle(ctx, task)
}
func (s syncLifecycleKnowledgeService) ProcessFAQImport(ctx context.Context, task *asynq.Task) error {
	return s.Handle(ctx, task)
}
func (s syncLifecycleKnowledgeService) ProcessQuestionGeneration(ctx context.Context, task *asynq.Task) error {
	return s.Handle(ctx, task)
}
func (s syncLifecycleKnowledgeService) ProcessSummaryGeneration(ctx context.Context, task *asynq.Task) error {
	return s.Handle(ctx, task)
}
func (s syncLifecycleKnowledgeService) ProcessKBClone(ctx context.Context, task *asynq.Task) error {
	return s.Handle(ctx, task)
}
func (s syncLifecycleKnowledgeService) ProcessKnowledgeMove(ctx context.Context, task *asynq.Task) error {
	return s.Handle(ctx, task)
}
func (s syncLifecycleKnowledgeService) ProcessKnowledgeListDelete(ctx context.Context, task *asynq.Task) error {
	return s.Handle(ctx, task)
}
func (s syncLifecycleKnowledgeService) ProcessKnowledgeListReparse(ctx context.Context, task *asynq.Task) error {
	return s.Handle(ctx, task)
}

type syncLifecycleKBService struct {
	interfaces.KnowledgeBaseService
}

func (syncLifecycleKBService) GetKnowledgeBaseByID(context.Context, string) (*types.KnowledgeBase, error) {
	return &types.KnowledgeBase{ID: "kb", TenantID: 7}, nil
}
func (syncLifecycleKBService) ProcessKBDelete(context.Context, *asynq.Task) error { return nil }

type syncLifecycleTagService struct{ interfaces.KnowledgeTagService }

func (syncLifecycleTagService) ProcessIndexDelete(context.Context, *asynq.Task) error { return nil }

type syncLifecycleDataSourceService struct{ interfaces.DataSourceService }

func (syncLifecycleDataSourceService) ProcessSync(context.Context, *asynq.Task) error { return nil }

type syncLifecycleTemporaryDocument struct {
	interfaces.TemporaryDocumentService
}

func (syncLifecycleTemporaryDocument) Process(context.Context, *asynq.Task) error { return nil }

type syncLifecycleMemory struct{ interfaces.MemoryService }

func (syncLifecycleMemory) Handle(context.Context, *asynq.Task) error { return nil }

// Exercise the actual Lite registrations: losing either middleware while
// resolving a merge must fail this test, even if each wrapper passes alone.
func TestRegisterSyncHandlersCombinesAuthorizationAndNextcloudBuildLease(t *testing.T) {
	for _, stage := range []string{
		types.TypeChunkExtract, types.TypeDataTableSummary, types.TypeImageMultimodal,
		types.TypeKnowledgePostProcess, types.TypeQuestionGeneration, types.TypeSummaryGeneration,
		types.TypeKnowledgeAutoTag, types.TypeKnowledgeBaseProfile,
	} {
		t.Run(stage, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "leases.db")), &gorm.Config{})
			require.NoError(t, err)
			sqlDB, err := db.DB()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
			for _, migration := range []string{
				"000031_nextcloud_source_versions.up.sql", "000046_nextcloud_content_leases.up.sql",
			} {
				script, err := os.ReadFile(filepath.Join("../../migrations/sqlite", migration))
				require.NoError(t, err)
				require.NoError(t, db.Exec(string(script)).Error)
			}
			require.NoError(t, db.Exec(`INSERT INTO nextcloud_source_versions
				(tenant_id, knowledge_base_id, datasource_id, external_id, desired_etag, candidate_knowledge_id, state)
				VALUES (7, 'kb', 'source', 'nextcloud:instance:file', 'etag-1', 'doc', 'staging')`).Error)
			leases := repository.NewNextcloudContentLeaseStore(db)
			repo := &syncLifecycleKnowledgeRepo{row: &types.Knowledge{
				ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", Channel: types.ConnectorTypeNextcloud,
				Metadata: []byte(`{"datasource_id":"source","external_id":"nextcloud:instance:file",` +
					`"nextcloud_target_etag":"etag-1"}`),
			}}
			access := &enrichmentAccessProbe{allow: true}
			tracker := &syncLifecycleTracker{}
			calls := 0
			leaseExpected := stage != types.TypeKnowledgeBaseProfile
			initiator := types.TaskInitiator{UserID: "user-1", CallerTenantID: 8, Role: types.TenantRoleContributor}
			expectedInitiator := initiator
			expectedPrincipal := types.Principal{Type: types.PrincipalWebUser, ID: "user-1"}
			expectedCaller := types.Caller{TenantID: 8, UserID: "user-1", Role: types.TenantRoleContributor}
			countLeases := func(where string) int64 {
				var count int64
				require.NoError(t, db.Table("nextcloud_content_leases").Where(where).Count(&count).Error)
				return count
			}
			handler := syncLifecycleTaskHandler{handle: func(ctx context.Context, _ *asynq.Task) error {
				calls++
				principal, ok := types.PrincipalFromContext(ctx)
				require.True(t, ok)
				require.Equal(t, expectedPrincipal, principal)
				require.Equal(t, expectedInitiator, types.TaskInitiatorFromContext(ctx))
				require.Equal(t, expectedCaller, types.CallerFromContext(ctx))
				executionTenant, ok := types.TenantIDFromContext(ctx)
				require.True(t, ok)
				require.Equal(t, uint64(7), executionTenant)
				require.True(t, types.IsBackgroundTask(ctx))
				require.Equal(t, leaseExpected, repository.NextcloudBuildLeasePresent(ctx, 7, "kb", "doc"))
				if leaseExpected {
					require.EqualValues(t, 1, countLeases("released_at_ms IS NULL"))
				}
				return nil
			}}
			executor := NewSyncTaskExecutor()
			RegisterSyncHandlers(SyncTaskParams{
				Executor: executor, KnowledgeService: syncLifecycleKnowledgeService{syncLifecycleTaskHandler: handler},
				KnowledgeRepo: repo, ChunkRepo: syncLifecycleChunkRepo{}, ContentLeases: leases,
				KnowledgeBaseService: syncLifecycleKBService{}, GroupAccess: access, SpanTracker: tracker,
				TagService: syncLifecycleTagService{}, DataSourceService: syncLifecycleDataSourceService{},
				ChunkExtractor: handler, DataTableSummary: handler, ImageMultimodal: handler,
				KnowledgePostProcess: handler, KnowledgeAutoTag: handler, KnowledgeBaseProfile: handler,
				WikiIngest: handler, TemporaryDocument: syncLifecycleTemporaryDocument{},
				MemoryService: syncLifecycleMemory{},
			})
			payload := map[string]any{
				"tenant_id": 7, "knowledge_base_id": "kb", "attempt": 2,
				"initiator": initiator,
			}
			if leaseExpected {
				payload["knowledge_id"] = "doc"
				payload["chunk_id"] = "chunk"
			}
			raw, err := json.Marshal(payload)
			require.NoError(t, err)
			task := asynq.NewTask(stage, raw)
			registered := executor.handlers[stage]
			require.NoError(t, registered(context.Background(), task))
			require.Equal(t, 1, calls)
			require.Zero(t, countLeases("released_at_ms IS NULL"), "worker exit must release its build lease")
			if leaseExpected {
				require.EqualValues(t, 1, countLeases("released_at_ms IS NOT NULL"))
			}

			// Revoke after registration: the next queued stage must use the
			// current grant, finish the denied attempt, and never acquire a lease.
			access.allow = false
			err = registered(context.Background(), task)
			require.ErrorIs(t, err, asynq.SkipRetry)
			require.Equal(t, 1, calls)
			require.Zero(t, countLeases("released_at_ms IS NULL"))
			if leaseExpected {
				require.Equal(t, 1, repo.failures)
				require.Equal(t, 1, tracker.finalized)
				require.EqualValues(t, 1, countLeases("1 = 1"), "authorization must run before lease admission")
			}

			// The lease wrapper must also preserve machine identity. Attributed
			// user fields on an API task never become an authenticated human.
			access.allow = true
			payload["initiator"] = types.TaskInitiator{UserID: "user-1", APIKeyID: 3, APIKeyName: "connector"}
			expectedPrincipal = types.Principal{Type: types.PrincipalAPITenant, ID: "task"}
			expectedCaller = types.Caller{Role: types.TenantRoleViewer}
			expectedInitiator = types.TaskInitiator{APIKeyID: 3, APIKeyName: "connector"}
			raw, err = json.Marshal(payload)
			require.NoError(t, err)
			require.NoError(t, registered(context.Background(), asynq.NewTask(stage, raw)))
			require.Equal(t, 2, calls)
			require.Zero(t, countLeases("released_at_ms IS NULL"))

			if leaseExpected {
				// A valid human grant cannot bypass a retired Nextcloud source.
				require.EqualValues(t, 2, countLeases("released_at_ms IS NOT NULL"))
				scope, err := nextcloudBuildScope(repo.row)
				require.NoError(t, err)
				require.NoError(t, leases.RetireKnowledge(context.Background(), scope))
				require.ErrorIs(t, registered(context.Background(), task), repository.ErrNextcloudContentLeaseDenied)
				require.Equal(t, 2, calls)
			}
		})
	}
}
