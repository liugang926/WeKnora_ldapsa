package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func nextcloudSyncRecoveryDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	raw, err := db.DB()
	require.NoError(t, err)
	raw.SetMaxOpenConns(1)
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
	require.NoError(t, db.Exec("CREATE TABLE data_sources (id TEXT PRIMARY KEY, type TEXT)").Error)
	require.NoError(t, db.Exec("INSERT INTO data_sources (id, type) VALUES ('ds', 'nextcloud')").Error)
	return db
}

func TestNextcloudSyncRecoveryClaimAndReleaseRace(t *testing.T) {
	ctx := context.Background()
	db := nextcloudSyncRecoveryDB(t)
	logs := &SyncLogRepository{db: db}
	store := NewNextcloudSyncRecoveryStore(db)
	now := time.Now().UTC()
	admitted := &types.SyncLog{
		ID: "new-log", DataSourceID: "ds", TenantID: 7,
		Status: types.SyncLogStatusRunning, StartedAt: now.Add(-10 * time.Minute),
		RecoveryVersion: 1, RecoveryTrigger: "manual",
	}
	require.NoError(t, logs.Create(ctx, admitted))
	require.Equal(t, "dssync:new-log", admitted.QueueTaskID)
	candidates, err := store.UnstartedBefore(ctx, now.Add(-5*time.Minute), 100)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	for _, tc := range []struct{ trigger, taskID string }{
		{"schedule", "dssync:new-log"}, {"manual", "dssync:other"}, {"manual", ""},
	} {
		token, err := logs.ClaimNextcloudSyncStart(ctx, admitted.ID, "ds", 7, tc.trigger, tc.taskID, 0)
		require.NoError(t, err)
		require.Empty(t, token)
	}
	token, err := logs.ClaimNextcloudSyncStart(ctx, admitted.ID, "ds", 7, "manual", admitted.QueueTaskID, 0)
	require.NoError(t, err)
	require.NotEmpty(t, token)
	marked, err := logs.MarkNextcloudEnqueueUncertain(ctx, admitted.ID, "ds", 7, "manual", admitted.QueueTaskID)
	require.NoError(t, err)
	require.False(t, marked, "a lost enqueue reply cannot overwrite a started worker")
	released, err := store.FailMissing(ctx, candidates[0], now.Add(-5*time.Minute), now)
	require.NoError(t, err)
	require.False(t, released, "worker claim must defeat a stale absence proof")
	duplicate, err := logs.ClaimNextcloudSyncStart(ctx, admitted.ID, "ds", 7, "manual", admitted.QueueTaskID, 0)
	require.NoError(t, err)
	require.Empty(t, duplicate)
}

func TestNextcloudSyncRecoveryReleaseFencesLateWorkerAndOldRows(t *testing.T) {
	ctx := context.Background()
	db := nextcloudSyncRecoveryDB(t)
	logs := &SyncLogRepository{db: db}
	store := NewNextcloudSyncRecoveryStore(db)
	now := time.Now().UTC()
	old := &types.SyncLog{
		ID: "old-log", DataSourceID: "ds", TenantID: 7,
		Status: types.SyncLogStatusRunning, StartedAt: now.Add(-time.Hour),
	}
	current := &types.SyncLog{
		ID: "current-log", DataSourceID: "ds", TenantID: 7,
		Status: types.SyncLogStatusRunning, StartedAt: now.Add(-time.Hour),
		RecoveryVersion: 1, RecoveryTrigger: "schedule",
	}
	// The live trigger permits one running log per source; use separate
	// records without the production admission trigger in this focused DB.
	require.NoError(t, logs.Create(ctx, old))
	require.NoError(t, logs.Create(ctx, current))
	candidates, err := store.UnstartedBefore(ctx, now.Add(-5*time.Minute), 100)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, current.ID, candidates[0].ID)
	released, err := store.FailMissing(ctx, *old, now.Add(-5*time.Minute), now)
	require.NoError(t, err)
	require.False(t, released)
	released, err = store.FailMissing(ctx, candidates[0], now.Add(-5*time.Minute), now)
	require.NoError(t, err)
	require.True(t, released)
	token, err := logs.ClaimNextcloudSyncStart(ctx, current.ID, "ds", 7, "schedule", current.QueueTaskID, 0)
	require.NoError(t, err)
	require.Empty(t, token, "late queue acceptance must be fenced")
	marked, err := logs.MarkNextcloudEnqueueUncertain(ctx, current.ID, "ds", 7, "schedule", current.QueueTaskID)
	require.NoError(t, err)
	require.False(t, marked, "late enqueue reply cannot reopen a recovered log")
	oldAfter, err := logs.FindByID(ctx, old.ID)
	require.NoError(t, err)
	require.Equal(t, types.SyncLogStatusRunning, oldAfter.Status)
}

func TestNextcloudSyncRetryReclaimsOnlyReleasedExactTask(t *testing.T) {
	ctx := context.Background()
	db := nextcloudSyncRecoveryDB(t)
	logs := &SyncLogRepository{db: db}
	log := &types.SyncLog{
		ID: "retry-log", DataSourceID: "ds", TenantID: 7,
		Status: types.SyncLogStatusRunning, RecoveryVersion: 1, RecoveryTrigger: "manual",
	}
	require.NoError(t, logs.Create(ctx, log))
	first, err := logs.ClaimNextcloudSyncStart(ctx, log.ID, "ds", 7, "manual", log.QueueTaskID, 0)
	require.NoError(t, err)
	require.NotEmpty(t, first)
	blocked, err := logs.ClaimNextcloudSyncStart(ctx, log.ID, "ds", 7, "manual", log.QueueTaskID, 1)
	require.NoError(t, err)
	require.Empty(t, blocked, "concurrent duplicate cannot enter while first worker is active")
	released, err := logs.FinishNextcloudSyncAttempt(ctx, log.ID, "ds", 7,
		"manual", log.QueueTaskID, first, false, "retry pending")
	require.NoError(t, err)
	require.True(t, released)
	second, err := logs.ClaimNextcloudSyncStart(ctx, log.ID, "ds", 7, "manual", log.QueueTaskID, 1)
	require.NoError(t, err)
	require.NotEmpty(t, second)
	require.NotEqual(t, first, second)
	stale, err := logs.FinishNextcloudSyncAttempt(ctx, log.ID, "ds", 7,
		"manual", log.QueueTaskID, first, true, "stale")
	require.NoError(t, err)
	require.False(t, stale)
	finished, err := logs.FinishNextcloudSyncAttempt(ctx, log.ID, "ds", 7,
		"manual", log.QueueTaskID, second, true, "attempt exhausted")
	require.NoError(t, err)
	require.True(t, finished)
	late, err := logs.ClaimNextcloudSyncStart(ctx, log.ID, "ds", 7, "manual", log.QueueTaskID, 2)
	require.NoError(t, err)
	require.Empty(t, late)
	after, err := logs.FindByID(ctx, log.ID)
	require.NoError(t, err)
	require.Equal(t, types.SyncLogStatusFailed, after.Status)
	require.NotNil(t, after.WorkerStartedAt)
}

func TestNextcloudSyncRecoveryMalformedIDsCannotStarveValidAdmission(t *testing.T) {
	ctx := context.Background()
	db := nextcloudSyncRecoveryDB(t)
	logs := &SyncLogRepository{db: db}
	store := NewNextcloudSyncRecoveryStore(db)
	now := time.Now().UTC()
	for i := 0; i < 101; i++ {
		id := fmt.Sprintf("bad-%03d", i)
		log := &types.SyncLog{
			ID: id, DataSourceID: "ds", TenantID: 7,
			Status: types.SyncLogStatusRunning, StartedAt: now.Add(-10 * time.Minute),
			RecoveryVersion: 1, RecoveryTrigger: "manual",
		}
		require.NoError(t, logs.Create(ctx, log))
		require.NoError(t, db.Model(&types.SyncLog{}).Where("id = ?", id).
			Update("queue_task_id", "malformed").Error)
	}
	valid := &types.SyncLog{
		ID: "valid-after-bad", DataSourceID: "ds", TenantID: 7,
		Status: types.SyncLogStatusRunning, StartedAt: now.Add(-9 * time.Minute),
		RecoveryVersion: 1, RecoveryTrigger: "schedule",
	}
	require.NoError(t, logs.Create(ctx, valid))
	candidates, err := store.UnstartedBefore(ctx, now.Add(-5*time.Minute), 100)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, valid.ID, candidates[0].ID)
}
