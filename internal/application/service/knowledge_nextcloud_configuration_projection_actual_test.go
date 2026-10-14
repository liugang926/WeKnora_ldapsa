package service

import (
	"context"
	"mime/multipart"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/logger"
)

// Keep only column-level outcomes. Do not export SQL arguments, credentials,
// directory GUIDs, model material or body text to the test output.
type workerConfigurationQueryAudit struct {
	logger.Interface
	bodyReads      atomic.Int64
	unrelatedReads atomic.Int64
}

func (a *workerConfigurationQueryAudit) LogMode(level logger.LogLevel) logger.Interface { return a }
func (a *workerConfigurationQueryAudit) Trace(ctx context.Context, begin time.Time, query func() (string, int64), err error) {
	sql, _ := query()
	lower := strings.ReplaceAll(strings.ToLower(sql), "`", "\"")
	if !strings.HasPrefix(strings.TrimSpace(lower), "select") || !(strings.Contains(lower, `from "knowledge_bases"`) || strings.Contains(lower, "from knowledge_bases")) {
		return
	}
	projection := strings.SplitN(lower, " from ", 2)[0]
	if strings.Contains(projection, "count(") {
		return
	}
	if strings.Contains(projection, "*") || strings.Contains(projection, "\"name\"") || strings.Contains(projection, "\"description\"") || strings.Contains(projection, "generated_profile") {
		a.unrelatedReads.Add(1)
	}
	if strings.Contains(projection, "chunking_config") || strings.Contains(projection, "enrichment_templates") || strings.Contains(projection, "*") {
		a.bodyReads.Add(1)
	}
}

func TestNextcloudWorkerConfigurationProjectionActualSQLite(t *testing.T) {
	m := newMachinePipelineFixture(t, false)
	ctx, close, err := m.f.store.BeginNextcloudOriginalFetchConfigurationRead(context.Background(), m.ds)
	require.NoError(t, err)
	defer close()
	audit := &workerConfigurationQueryAudit{Interface: m.f.db.Logger}
	previous := m.f.db.Logger
	m.f.db.Logger = audit
	defer func() { m.f.db.Logger = previous }()
	bases := repository.NewKnowledgeBaseRepository(m.f.db)
	first, err := bases.GetKnowledgeBaseByID(ctx, m.ds.KnowledgeBaseID)
	require.NoError(t, err)
	second, err := bases.GetKnowledgeBaseByIDAndTenant(ctx, m.ds.KnowledgeBaseID, 7)
	require.NoError(t, err)
	require.NotSame(t, first, second, "each getter decodes an independent actual config value")
	require.Empty(t, first.Name)
	require.Empty(t, first.Description)
	require.Nil(t, first.GeneratedProfile)
	require.NotEmpty(t, first.SummaryModelID)
	require.Positive(t, audit.bodyReads.Load())
	require.Zero(t, audit.unrelatedReads.Load(), "worker configuration queries never hydrate unrelated KB body columns")
	_, err = bases.GetKnowledgeBaseByIDAndTenant(ctx, first.ID, 8)
	require.Error(t, err)
	require.ErrorIs(t, bases.UpdateKnowledgeBase(ctx, first), repository.ErrKnowledgeInputChanged, "partial config projection cannot clear unrelated fields through Save")
	require.ErrorIs(t, m.f.store.UpdateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(ctx), first), repository.ErrKnowledgeInputChanged)
	first.SummaryModelID = "mutated-primary-sync-control"
	require.ErrorIs(t, repository.CheckScopedOnboardingConfigurationReads(ctx), repository.ErrKnowledgeInputChanged)
	require.NotEqual(t, first.SummaryModelID, second.SummaryModelID)
}

func TestNextcloudSourceBoundConfigurationProjectionDenyBeforeBodyActualSQLite(t *testing.T) {
	for _, mode := range []string{"denied", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			decision := &atomic.Int32{}
			m, _ := newScopedConfigurationWorkerFixture(t, decision)
			ctx, close, err := m.f.store.BeginNextcloudOriginalFetchConfigurationRead(context.Background(), m.ds)
			require.NoError(t, err)
			defer close()
			audit := &workerConfigurationQueryAudit{Interface: m.f.db.Logger}
			previous := m.f.db.Logger
			m.f.db.Logger = audit
			defer func() { m.f.db.Logger = previous }()
			if mode == "denied" {
				decision.Store(1)
			} else {
				decision.Store(2)
			}
			row, err := repository.NewKnowledgeBaseRepository(m.f.db).GetKnowledgeBaseByID(ctx, m.ds.KnowledgeBaseID)
			require.Error(t, err)
			require.Nil(t, row)
			require.Zero(t, audit.bodyReads.Load(), "current source deny/unknown is checked before target config body SELECT")
			require.Zero(t, audit.unrelatedReads.Load())
		})
	}
}

type workerRememberingBases struct {
	interfaces.KnowledgeBaseService
	mu     sync.Mutex
	last   *types.KnowledgeBase
	mutate atomic.Bool
}

func (b *workerRememberingBases) GetKnowledgeBaseByID(ctx context.Context, id string) (*types.KnowledgeBase, error) {
	row, err := b.KnowledgeBaseService.GetKnowledgeBaseByID(ctx, id)
	if err == nil && row != nil {
		if b.mutate.Load() {
			row.SummaryModelID = "unknown-local-perfile-config"
		}
		b.mu.Lock()
		b.last = row
		b.mu.Unlock()
	}
	return row, err
}
func (b *workerRememberingBases) GetKnowledgeBaseByIDOnly(ctx context.Context, id string) (*types.KnowledgeBase, error) {
	return b.GetKnowledgeBaseByID(ctx, id)
}
func (b *workerRememberingBases) lastProjection() *types.KnowledgeBase {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.last
}

type workerPerFileMutationService struct {
	interfaces.KnowledgeService
	bases    *workerRememberingBases
	observed error
}

func (s *workerPerFileMutationService) CreateKnowledgeFromFile(ctx context.Context, kb string, file *multipart.FileHeader, metadata map[string]string, multimodal *bool, name string, tags []string, channel string, overrides *types.KnowledgeProcessOverrides) (*types.Knowledge, error) {
	s.bases.mutate.Store(true)
	defer s.bases.mutate.Store(false)
	row, err := s.KnowledgeService.CreateKnowledgeFromFile(ctx, kb, file, metadata, multimodal, name, tags, channel, overrides)
	s.observed = err
	return row, err
}

func TestNextcloudWorkerPerFileProjectionMutationActualSQLite(t *testing.T) {
	m := newMachinePipelineFixture(t, false)
	bases := &workerRememberingBases{KnowledgeBaseService: m.f.s.kbService}
	m.f.s.kbService = bases
	mutating := &workerPerFileMutationService{KnowledgeService: m.f.s, bases: bases}
	m.source.knowledgeService = mutating
	// The first actual ProcessSync projection remains intact. Only the later
	// real CreateKnowledgeFromFile getter is changed. The production original
	// fetched artifact, fields and immutable receipt/writer still run unchanged.
	log := &types.SyncLog{ID: uuid.NewString(), DataSourceID: m.ds.ID, TenantID: 7, Status: types.SyncLogStatusRunning, StartedAt: time.Now().UTC()}
	require.NoError(t, m.source.syncLogRepo.Create(context.Background(), log))
	_ = m.source.ProcessSync(context.Background(), enrichmentTask(t, types.TypeDataSourceSync, types.DataSourceSyncPayload{DataSourceID: m.ds.ID, TenantID: 7, SyncLogID: log.ID, ForceFull: true}))
	require.ErrorIs(t, mutating.observed, repository.ErrKnowledgeInputChanged, "later per-file pointers need their actual config digest, not only the first sync pointer")
	var count int64
	require.NoError(t, m.f.db.Model(&types.Knowledge{}).Where("knowledge_base_id=?", m.ds.KnowledgeBaseID).Count(&count).Error)
	require.Zero(t, count)
	require.Zero(t, m.f.model.calls)
}

func TestNextcloudWorkerModelProjectionMutationActualSQLite(t *testing.T) {
	m := newMachinePipelineFixture(t, false)
	bases := &workerRememberingBases{KnowledgeBaseService: m.f.s.kbService}
	m.f.s.kbService = bases
	row := m.ingest(t)
	changed := false
	m.f.model.beforeReturn = func() {
		projected := bases.lastProjection()
		require.NotNil(t, projected)
		projected.SummaryModelID = "changed-after-model-start"
		changed = true
	}
	var taskErr error
	for i := 0; i < len(m.f.queue.tasks); i++ {
		taskErr = m.run(t, m.f.queue.tasks[i])
		if taskErr != nil {
			break
		}
	}
	require.True(t, changed)
	require.ErrorIs(t, taskErr, repository.ErrKnowledgeInputChanged)
	require.NoError(t, m.f.db.Where("id=?", row.ID).Take(row).Error)
	require.Empty(t, row.Description, "a changed actual model-consumed config projection cannot commit output")
}
