package service

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestOrdinaryConnectorScheduleIngestsWithLiveWorkspaceEditor(t *testing.T) {
	h, db, _, queue := datasourceReplacementFixture(t)
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
	require.NoError(t, repository.InstallConnectorScheduleSchema(db))
	sources := repository.NewDataSourceRepository(db)
	factory := func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
		store := repository.NewKnowledgeInputProvenanceStore(tx)
		groups := NewGroupAccessService(repository.NewGroupAccessRepository(tx))
		repository.ConfigureKnowledgeInputActorGroupAccess(store, groups)
		return store
	}
	repository.ConfigureDataSourceNameInputWriters(sources, nil, factory)
	logs := repository.NewSyncLogRepository(db)
	schedulerLogs := repository.ConnectorScheduleLogs{SyncLogRepository: logs, DB: db}
	registry := datasource.NewConnectorRegistry()
	fetched := types.FetchedItem{
		ExternalID: "doc-1", FileName: "doc.md", Title: "scheduled file", Content: []byte("content from cron"),
	}
	queue.fetched = fetched.Content
	require.NoError(t, registry.Register(singleFileConnector{kind: h.source.Type, item: fetched}))
	core := &DataSourceService{
		dsRepo: sources, syncLogRepo: logs, connectorRegistry: registry, knowledgeService: h.svc,
		kbService: h.svc.kbService, tagService: &processSyncTagService{},
		tenantRepo: &processSyncTenantRepo{tenant: h.ctx.Value(types.TenantInfoContextKey).(*types.Tenant)},
	}
	svc := WithConnectorScheduleIdentity(core)
	ConfigureDataSourceOriginalInputSources(svc, queue.inputs)
	cron := func() (*types.SyncLog, types.DataSourceSyncPayload, context.Context) {
		t.Helper()
		log := &types.SyncLog{
			DataSourceID: h.source.ID, TenantID: h.source.TenantID,
			StartedAt: time.Now().UTC(), Status: types.SyncLogStatusRunning,
		}
		require.NoError(t, schedulerLogs.Create(context.Background(), log))
		p := types.DataSourceSyncPayload{
			Trigger: "schedule", DataSourceID: h.source.ID,
			TenantID: h.source.TenantID, SyncLogID: log.ID,
		}
		return log, p, types.WithTaskExecutionID(context.Background(), log.ScheduledMinuteTaskID())
	}
	process := func(ctx context.Context, p types.DataSourceSyncPayload) error {
		t.Helper()
		return svc.ProcessSync(ctx, enrichmentTask(t, types.TypeDataSourceSync, p))
	}
	save := func() {
		t.Helper()
		h.source.SyncSchedule = "0 15 3 * * *"
		require.NoError(t, core.dsRepo.PatchSyncSettings(h.ctx, h.source, nil, []string{"sync_schedule"}))
	}
	// Pre-upgrade configuration lacks evidence of who delegated a recurring write.
	require.NoError(t, db.Model(h.source).UpdateColumn("sync_schedule", "0 15 3 * * *").Error)
	_, legacy, worker := cron()
	require.ErrorIs(t, process(worker, legacy), repository.ErrConnectorScheduleGrantRequired)
	save()
	log, payload, worker := cron()
	forged := payload
	forged.Initiator = types.TaskInitiatorFromContext(h.ctx)
	require.ErrorIs(t, process(worker, forged), asynq.SkipRetry)
	forged.Trigger = "manual"
	require.ErrorIs(t, process(types.WithTaskExecutionID(worker, "manual-task"), forged), asynq.SkipRetry)
	forged = payload
	forged.TenantID++
	require.ErrorIs(t, process(worker, forged), asynq.SkipRetry)
	require.ErrorIs(t, process(types.WithTaskExecutionID(worker, "unrelated-task"), payload), asynq.SkipRetry)
	require.Zero(t, queue.queued)
	require.NoError(t, process(worker, payload))
	stored, err := logs.FindByID(h.ctx, log.ID)
	require.NoError(t, err)
	require.Equal(t, types.SyncLogStatusSuccess, stored.Status, stored.ErrorMessage)
	require.Equal(t, 1, stored.ItemsUpdated)
	require.Equal(t, 1, queue.queued)
	require.ErrorIs(t, process(worker, payload), asynq.SkipRetry)
	require.Equal(t, 1, queue.queued)
	_, stale, worker := cron()
	save()
	require.ErrorIs(t, process(worker, stale), repository.ErrConnectorScheduleGrantRequired)
	_, revoked, worker := cron()
	require.NoError(t, db.Model(&types.User{}).Where("id = ?", types.CallerFromContext(h.ctx).UserID).
		UpdateColumn("is_active", false).Error)
	require.ErrorIs(t, process(worker, revoked), repository.ErrConnectorScheduleGrantRevoked)
	require.Equal(t, 1, queue.queued)
}
