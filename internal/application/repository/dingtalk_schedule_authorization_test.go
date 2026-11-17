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

func TestDingTalkScheduleStatusReadsCurrentDelegation(t *testing.T) {
	store, db, editor, _, _, _ := displayTestFixture(t, false)
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &dingTalkScheduleAuthorization{}))
	require.NoError(t, db.Exec(`CREATE TABLE dingtalk_source_input_revisions (
		datasource_id TEXT PRIMARY KEY, revision INTEGER NOT NULL)`).Error)
	ds := &types.DataSource{
		ID: "schedule-status", TenantID: 7, KnowledgeBaseID: "kb", Type: types.ConnectorTypeDingTalk,
		SyncSchedule: "0 30 4 * * *", Status: types.DataSourceStatusError,
		ErrorMessage: ErrDingTalkScheduleAuthorizationRequired.Error(),
	}
	require.NoError(t, db.Create(ds).Error)
	require.NoError(t, db.Exec("INSERT INTO dingtalk_source_input_revisions VALUES (?, 1)", ds.ID).Error)
	check := func(want string) {
		t.Helper()
		status, err := store.DingTalkScheduleAuthorizationStatus(editor, 7, ds.ID)
		require.NoError(t, err)
		require.Equal(t, want, status)
	}
	check("required")
	require.NoError(t, saveDingTalkScheduleAuthorization(
		WithDingTalkScheduleEdit(editor), db, ds, store.displayAuthorityFactory))
	check("authorized")
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
	logs := ConnectorScheduleLogs{SyncLogRepository: NewSyncLogRepository(db), DB: db, ScheduleAuthority: store}
	for i := 0; i < 2; i++ {
		run := &types.SyncLog{
			TenantID: ds.TenantID, DataSourceID: ds.ID, Status: types.SyncLogStatusFailed,
			StartedAt: time.Now().Add(time.Duration(i) * time.Minute), RecoveryTrigger: "schedule",
			ErrorMessage: ErrDingTalkScheduleAuthorizationRequired.Error(),
		}
		run.QueueTaskID = run.ScheduledMinuteTaskID()
		require.NoError(t, db.Create(run).Error)
	}
	failures, _, err := logs.ConsecutiveFailures(editor, ds.ID)
	require.NoError(t, err)
	require.Zero(t, failures)
	latest, err := logs.FindLatest(editor, ds.ID)
	require.NoError(t, err)
	grant, err := findDingTalkScheduleAuthorization(db, ds.ID, ds.TenantID)
	require.NoError(t, err)
	require.NoError(t, db.Table("sync_logs").Where("id = ?", latest.ID).
		UpdateColumn("dingtalk_schedule_grant", grant.GrantID).Error)
	failures, _, err = logs.ConsecutiveFailures(editor, ds.ID)
	require.NoError(t, err)
	require.Equal(t, 2, failures)
	var restored types.DataSource
	require.NoError(t, db.First(&restored, "id = ?", ds.ID).Error)
	require.Equal(t, types.DataSourceStatusActive, restored.Status)
	require.Empty(t, restored.ErrorMessage)
	_, err = store.DingTalkScheduleAuthorizationStatus(editor, 8, ds.ID)
	require.Error(t, err)
	// The viewer remains active; only the saved editor changes.
	require.NoError(t, db.Model(&dingTalkScheduleAuthorization{}).Where("data_source_id = ?", ds.ID).
		Update("user_id", "removed-editor").Error)
	check("revoked")
	require.NoError(t, db.Exec(
		"UPDATE dingtalk_source_input_revisions SET revision = 2 WHERE datasource_id = ?", ds.ID).Error)
	check("required")
}
