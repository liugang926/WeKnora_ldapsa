package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestDingTalkScheduledActorGrantLifecycle(t *testing.T) {
	store, db, editor, _, _, _ := displayTestFixture(t, false)
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &types.SyncLog{},
		&dingTalkRunClaim{}, &dingTalkScheduleAuthorization{}))
	require.NoError(t, db.Exec(`CREATE TABLE dingtalk_source_input_revisions (
 datasource_id TEXT PRIMARY KEY, revision INTEGER NOT NULL)`).Error)
	ds := &types.DataSource{
		ID: "finance", TenantID: 7, KnowledgeBaseID: "kb", Type: types.ConnectorTypeDingTalk,
		SyncSchedule: "0 30 4 * * *", Status: types.DataSourceStatusActive,
	}
	require.NoError(t, db.Create(ds).Error)
	require.NoError(t, db.Exec("INSERT INTO dingtalk_source_input_revisions VALUES (?, 1)", ds.ID).Error)
	save := func(ctx context.Context) {
		t.Helper()
		require.NoError(t, saveDingTalkScheduleAuthorization(ctx, db, ds, store.displayAuthorityFactory))
	}
	logs := NewSyncLogRepository(db)
	newRun := func(start time.Time) (*types.SyncLog, context.Context) {
		t.Helper()
		log := &types.SyncLog{
			DataSourceID: ds.ID, TenantID: 7, Status: types.SyncLogStatusRunning, StartedAt: start,
			RecoveryTrigger: "schedule", QueueTaskID: "dssync:finance:" + start.UTC().Format("200601021504"),
		}
		worker := types.WithTaskAuthorization(context.Background(), 7, types.TaskInitiator{})
		require.NoError(t, logs.Create(worker, log))
		ctx, err := store.ClaimDingTalkRun(worker, ds, log.ID, log.QueueTaskID, 0, time.Minute)
		require.NoError(t, err)
		return log, ctx
	}
	start := time.Now().UTC()
	save(editor) // A normal/manual worker cannot delegate its identity to future schedules.
	legacy, ctx := newRun(start)
	require.Empty(t, legacy.DingTalkScheduleGrant)
	_, err := store.RestoreDingTalkScheduledActor(ctx)
	require.ErrorIs(t, err, ErrDingTalkScheduleAuthorizationRequired)
	require.NoError(t, store.FinalizeDingTalkRun(ctx, types.SyncLogStatusFailed, &types.SyncResult{}, nil, err.Error()))

	save(WithDingTalkScheduleEdit(editor))
	log, ctx := newRun(start.Add(time.Minute))
	require.NotEmpty(t, log.DingTalkScheduleGrant)
	ctx, err = store.RestoreDingTalkScheduledActor(ctx)
	require.NoError(t, err)
	selector, err := captureKnowledgeActorSelector(ctx, "")
	require.NoError(t, err)
	require.Equal(t, "display-user", selector.UserID)
	require.NoError(t, store.RenewDingTalkRun(ctx, time.Minute))
	require.NoError(t, store.FinalizeDingTalkRun(ctx, types.SyncLogStatusSuccess,
		&types.SyncResult{Skipped: 1}, nil, ""))

	repo := &DataSourceRepository{db: db, nameAuthorityFactory: store.displayAuthorityFactory}
	patch := &types.DataSource{ID: ds.ID, TenantID: ds.TenantID, KnowledgeBaseID: ds.KnowledgeBaseID, Name: "Renamed"}
	require.NoError(t, repo.Update(editor, patch))
	require.Equal(t, ds.SyncSchedule, patch.SyncSchedule)
	require.Equal(t, types.DataSourceStatusActive, patch.Status)
	require.NoError(t, repo.Update(WithDingTalkScheduleResume(editor), patch)) // Already active.
	later, ctx := newRun(start.Add(2 * time.Minute))
	require.Equal(t, log.DingTalkScheduleGrant, later.DingTalkScheduleGrant)
	ctx, err = store.RestoreDingTalkScheduledActor(ctx)
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.User{}).Where("id = ?", selector.UserID).Update("is_active", false).Error)
	require.ErrorIs(t, store.RenewDingTalkRun(ctx, time.Minute), ErrDingTalkScheduleAuthorizationRevoked)
	require.ErrorIs(t, store.FinalizeDingTalkRun(ctx, types.SyncLogStatusSuccess, &types.SyncResult{}, nil, ""),
		ErrDingTalkScheduleAuthorizationRevoked)
	require.NoError(t, store.FinalizeDingTalkRun(ctx, types.SyncLogStatusFailed, &types.SyncResult{}, nil,
		ErrDingTalkScheduleAuthorizationRevoked.Error()))
	require.NoError(t, db.Model(&types.User{}).Where("id = ?", selector.UserID).Update("is_active", true).Error)
	_, ctx = newRun(start.Add(3 * time.Minute))
	save(WithDingTalkScheduleEdit(editor)) // Reauthorization cannot transfer a queued run to a new grant.
	_, err = store.RestoreDingTalkScheduledActor(ctx)
	require.ErrorIs(t, err, ErrDingTalkScheduleAuthorizationRequired)

	ds.Status = types.DataSourceStatusPaused
	require.NoError(t, repo.Update(editor, ds))
	var count int64
	require.NoError(t, db.Model(&dingTalkScheduleAuthorization{}).Count(&count).Error)
	require.Zero(t, count)
	ds.Status = types.DataSourceStatusActive
	require.NoError(t, repo.Update(WithDingTalkScheduleResume(editor), ds))
	grant, err := findDingTalkScheduleAuthorization(db, ds.ID, ds.TenantID)
	require.NoError(t, err)
	require.NotEmpty(t, grant.GrantID)
	require.NotEqual(t, log.DingTalkScheduleGrant, grant.GrantID)

	// Generic writes may change the cron but cannot restore a superseded authorization.
	originalCron := ds.SyncSchedule
	ds.SyncSchedule = "0 0 8 * * *"
	require.NoError(t, repo.Update(editor, ds))
	ds.SyncSchedule = originalCron
	require.NoError(t, repo.Update(editor, ds))
	grant, err = findDingTalkScheduleAuthorization(db, ds.ID, ds.TenantID)
	require.NoError(t, err)
	require.Empty(t, grant.GrantID)
}
