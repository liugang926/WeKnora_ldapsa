package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type retryingScheduleConnector struct {
	singleFileConnector
	fail        error
	calls       int
	duringFetch func()
}

func (c *retryingScheduleConnector) FetchAll(ctx context.Context, cfg *types.DataSourceConfig,
	resourceIDs []string,
) ([]types.FetchedItem, error) {
	c.calls++
	if c.duringFetch != nil {
		c.duringFetch()
	}
	if c.fail != nil {
		return nil, c.fail
	}
	return c.singleFileConnector.FetchAll(ctx, cfg, resourceIDs)
}

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
	connector := &retryingScheduleConnector{
		singleFileConnector: singleFileConnector{kind: h.source.Type, item: fetched},
	}
	require.NoError(t, registry.Register(connector))
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
	// A real fetch failure is recorded as failed. Only a later queue attempt may
	// reclaim that same run, and only after the original invocation has returned.
	transient := errors.New("remote source temporarily unavailable")
	connector.fail = transient
	first := types.WithTaskRetryMetadata(worker, 0, 5)
	retry := types.WithTaskRetryMetadata(worker, 1, 5)
	connector.duringFetch = func() {
		require.ErrorIs(t, process(first, payload), asynq.SkipRetry)
		require.ErrorIs(t, process(retry, payload), asynq.SkipRetry)
	}
	require.ErrorIs(t, process(first, payload), transient)
	require.ErrorIs(t, process(first, payload), asynq.SkipRetry)
	require.Equal(t, 1, connector.calls)
	require.Zero(t, queue.queued)
	// The queue catches handler panics. A panic must release this attempt too.
	connector.duringFetch = func() { panic("connector panic") }
	require.PanicsWithValue(t, "connector panic", func() { _ = process(retry, payload) })
	connector.duringFetch = nil
	connector.fail = nil
	require.NoError(t, process(types.WithTaskRetryMetadata(worker, 2, 5), payload))
	require.Equal(t, 3, connector.calls)
	stored, err := logs.FindByID(h.ctx, log.ID)
	require.NoError(t, err)
	require.Equal(t, types.SyncLogStatusSuccess, stored.Status, stored.ErrorMessage)
	require.Empty(t, stored.ErrorMessage)
	require.Equal(t, 1, stored.ItemsUpdated)
	require.Equal(t, 1, queue.queued)
	require.ErrorIs(t, process(worker, payload), asynq.SkipRetry)
	require.Equal(t, 1, queue.queued)
	// A later scheduler window takes precedence over an older pending retry.
	_, superseded, oldWorker := cron()
	connector.fail = transient
	require.ErrorIs(t, process(types.WithTaskRetryMetadata(oldWorker, 0, 5), superseded), transient)
	_, stale, worker := cron()
	require.ErrorIs(t, process(types.WithTaskRetryMetadata(oldWorker, 1, 5), superseded), asynq.SkipRetry)
	require.Equal(t, 4, connector.calls)
	connector.fail = nil
	save()
	require.ErrorIs(t, process(worker, stale), repository.ErrConnectorScheduleGrantRequired)
	// A killed worker cannot run a defer. Leave its real claim unfinished,
	// then advance its persisted deadline after checking the active run guard.
	orphan, lost, orphanWorker := cron()
	claim, err := queue.inputs.RestoreConnectorSchedule(orphanWorker, lost, orphan.ScheduledMinuteTaskID(), 0)
	require.NoError(t, err)
	require.NotNil(t, claim)
	blocked, err := schedulerLogs.HasRunningSync(h.ctx, h.source.ID)
	require.NoError(t, err)
	require.True(t, blocked)
	require.NoError(t, db.Exec("UPDATE ext_connector_schedule_attempts SET deadline_ms = ? WHERE log_id = ?",
		time.Now().Add(-time.Minute).UnixMilli(), orphan.ID).Error)
	blocked, err = schedulerLogs.HasRunningSync(h.ctx, h.source.ID)
	require.NoError(t, err)
	require.False(t, blocked)
	stored, err = logs.FindByID(h.ctx, orphan.ID)
	require.NoError(t, err)
	require.Equal(t, types.SyncLogStatusCanceled, stored.Status)
	require.ErrorIs(t, process(types.WithTaskRetryMetadata(orphanWorker, 1, 5), lost), asynq.SkipRetry)
	_, revoked, worker := cron()
	require.NoError(t, db.Model(&types.User{}).Where("id = ?", types.CallerFromContext(h.ctx).UserID).
		UpdateColumn("is_active", false).Error)
	require.ErrorIs(t, process(worker, revoked), repository.ErrConnectorScheduleGrantRevoked)
	require.Equal(t, 1, queue.queued)
}
