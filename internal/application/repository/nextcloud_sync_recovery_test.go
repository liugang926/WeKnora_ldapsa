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
	candidates, err := store.RecoverableBefore(ctx, now.Add(-5*time.Minute), 100)
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
	candidates, err := store.RecoverableBefore(ctx, now.Add(-5*time.Minute), 100)
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
	candidates, err := store.RecoverableBefore(ctx, now.Add(-5*time.Minute), 100)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, valid.ID, candidates[0].ID)
}

func TestNextcloudSyncRecoveryRecoversReleasedStartedAttemptAfterGrace(t *testing.T) {
	ctx := context.Background()
	db := nextcloudSyncRecoveryDB(t)
	logs := &SyncLogRepository{db: db}
	store := NewNextcloudSyncRecoveryStore(db)
	now := time.Now().UTC()
	started := now.Add(-20 * time.Minute)
	log := &types.SyncLog{
		ID: "started-log", DataSourceID: "ds", TenantID: 7,
		Status: types.SyncLogStatusRunning, StartedAt: started,
		RecoveryVersion: 1, RecoveryTrigger: "manual",
		WorkerStartedAt: &started, WorkerActive: false,
	}
	require.NoError(t, logs.Create(ctx, log))
	require.NoError(t, db.Model(&types.SyncLog{}).Where("id = ?", log.ID).
		UpdateColumn("updated_at", now.Add(-10*time.Minute)).Error)

	candidates, err := store.RecoverableBefore(ctx, now.Add(-5*time.Minute), 100)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, log.ID, candidates[0].ID)

	released, err := store.FailMissing(ctx, candidates[0], now.Add(-5*time.Minute), now)
	require.NoError(t, err)
	require.True(t, released)
	after, err := logs.FindByID(ctx, log.ID)
	require.NoError(t, err)
	require.Equal(t, types.SyncLogStatusFailed, after.Status)
	require.Equal(t, "sync_task_absent_after_worker_start", after.ErrorMessage)
	require.NotNil(t, after.FinishedAt)

	replayed, err := store.FailMissing(ctx, candidates[0], now.Add(-5*time.Minute), now)
	require.NoError(t, err)
	require.False(t, replayed, "a recovered row cannot be recovered twice")
}

func TestNextcloudSyncRecoveryKeepsActiveAndRecentStartedAttempts(t *testing.T) {
	ctx := context.Background()
	db := nextcloudSyncRecoveryDB(t)
	logs := &SyncLogRepository{db: db}
	store := NewNextcloudSyncRecoveryStore(db)
	now := time.Now().UTC()
	started := now.Add(-20 * time.Minute)

	for _, tc := range []struct {
		id            string
		active        bool
		updatedAt     time.Time
		workerStarted bool
	}{
		{id: "active-worker", active: true, updatedAt: now.Add(-10 * time.Minute), workerStarted: true},
		{id: "retry-backoff", active: false, updatedAt: now.Add(-time.Minute), workerStarted: true},
	} {
		log := &types.SyncLog{
			ID: tc.id, DataSourceID: "ds", TenantID: 7,
			Status: types.SyncLogStatusRunning, StartedAt: started,
			RecoveryVersion: 1, RecoveryTrigger: "schedule", WorkerActive: tc.active,
		}
		if tc.workerStarted {
			log.WorkerStartedAt = &started
		}
		require.NoError(t, logs.Create(ctx, log))
		require.NoError(t, db.Model(&types.SyncLog{}).Where("id = ?", tc.id).
			UpdateColumn("updated_at", tc.updatedAt).Error)
	}

	candidates, err := store.RecoverableBefore(ctx, now.Add(-5*time.Minute), 100)
	require.NoError(t, err)
	require.Empty(t, candidates)
}

func TestNextcloudSyncRecoveryStartedAttemptClaimDefeatsStaleRelease(t *testing.T) {
	ctx := context.Background()
	db := nextcloudSyncRecoveryDB(t)
	logs := &SyncLogRepository{db: db}
	store := NewNextcloudSyncRecoveryStore(db)
	now := time.Now().UTC()
	started := now.Add(-20 * time.Minute)
	log := &types.SyncLog{
		ID: "started-race", DataSourceID: "ds", TenantID: 7,
		Status: types.SyncLogStatusRunning, StartedAt: started,
		RecoveryVersion: 1, RecoveryTrigger: "manual",
		WorkerStartedAt: &started, WorkerActive: false,
	}
	require.NoError(t, logs.Create(ctx, log))
	require.NoError(t, db.Model(&types.SyncLog{}).Where("id = ?", log.ID).
		UpdateColumn("updated_at", now.Add(-10*time.Minute)).Error)
	candidates, err := store.RecoverableBefore(ctx, now.Add(-5*time.Minute), 100)
	require.NoError(t, err)
	require.Len(t, candidates, 1)

	token, err := logs.ClaimNextcloudSyncStart(ctx, log.ID, "ds", 7,
		"manual", log.QueueTaskID, 1)
	require.NoError(t, err)
	require.NotEmpty(t, token)
	released, err := store.FailMissing(ctx, candidates[0], now.Add(-5*time.Minute), now)
	require.NoError(t, err)
	require.False(t, released, "an exact worker claim must defeat a stale absence proof")
	after, err := logs.FindByID(ctx, log.ID)
	require.NoError(t, err)
	require.Equal(t, types.SyncLogStatusRunning, after.Status)
	require.True(t, after.WorkerActive)
}

func TestNextcloudQueuedEventSyncBeforeFindsOnlyStaleRunningQueuedDispatch(t *testing.T) {
	ctx := context.Background()
	db := nextcloudSyncRecoveryDB(t)
	require.NoError(t, db.Exec(`CREATE TABLE nextcloud_event_connections (
		connection_id TEXT PRIMARY KEY, status TEXT NOT NULL)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE nextcloud_event_dispatch (
		connection_id TEXT PRIMARY KEY, state TEXT NOT NULL,
		last_sync_log_id TEXT)`).Error)
	now := time.Now().UTC()
	for _, tc := range []struct {
		connection, logID, connectionStatus, dispatchState, logStatus string
		startedAt, updatedAt                                          time.Time
		workerActive                                                  bool
		want                                                          bool
	}{
		{
			connection: "stale-connection", logID: "stale-event-log",
			connectionStatus: "active", dispatchState: "queued",
			logStatus: types.SyncLogStatusRunning,
			startedAt: now.Add(-20 * time.Minute), updatedAt: now.Add(-10 * time.Minute), want: true,
		},
		{
			connection: "recent-connection", logID: "recent-event-log",
			connectionStatus: "active", dispatchState: "queued",
			logStatus: types.SyncLogStatusRunning,
			startedAt: now.Add(-20 * time.Minute), updatedAt: now.Add(-time.Minute),
		},
		{
			connection: "active-worker-connection", logID: "active-worker-event-log",
			connectionStatus: "active", dispatchState: "queued",
			logStatus: types.SyncLogStatusRunning, workerActive: true,
			startedAt: now.Add(-20 * time.Minute), updatedAt: now.Add(-10 * time.Minute),
		},
		{
			connection: "success-connection", logID: "success-event-log",
			connectionStatus: "active", dispatchState: "queued",
			logStatus: types.SyncLogStatusSuccess,
			startedAt: now.Add(-20 * time.Minute), updatedAt: now.Add(-10 * time.Minute),
		},
		{
			connection: "idle-connection", logID: "idle-event-log",
			connectionStatus: "active", dispatchState: "idle",
			logStatus: types.SyncLogStatusRunning,
			startedAt: now.Add(-20 * time.Minute), updatedAt: now.Add(-10 * time.Minute),
		},
		{
			connection: "revoked-connection", logID: "revoked-event-log",
			connectionStatus: "revoked", dispatchState: "queued",
			logStatus: types.SyncLogStatusRunning,
			startedAt: now.Add(-20 * time.Minute), updatedAt: now.Add(-10 * time.Minute),
		},
	} {
		require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_connections
			(connection_id, status) VALUES (?, ?)`, tc.connection, tc.connectionStatus).Error)
		require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_dispatch
			(connection_id, state, last_sync_log_id) VALUES (?, ?, ?)`,
			tc.connection, tc.dispatchState, tc.logID).Error)
		log := &types.SyncLog{
			ID: tc.logID, DataSourceID: "ds", TenantID: 7,
			Status: tc.logStatus, StartedAt: tc.startedAt, WorkerActive: tc.workerActive,
		}
		require.NoError(t, db.Create(log).Error)
		require.NoError(t, db.Model(&types.SyncLog{}).Where("id = ?", tc.logID).
			UpdateColumn("updated_at", tc.updatedAt).Error)
	}

	candidates, err := NewNextcloudSyncRecoveryStore(db).
		QueuedEventSyncBefore(ctx, now.Add(-5*time.Minute), 100)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, "stale-connection", candidates[0].ConnectionID)
	require.Equal(t, "stale-event-log", candidates[0].SyncLogID)
}

func TestRecoverQueuedEventSyncRejectsActiveWorker(t *testing.T) {
	db, inbox, now, _ := newNextcloudQueuedPublicationTest(t)
	require.NoError(t, db.Model(&types.SyncLog{}).Where("id = ?", "event-log").
		Updates(map[string]any{
			"worker_active": true,
			"updated_at":    now.Add(-10 * time.Minute),
		}).Error)

	recovered, err := inbox.RecoverQueuedEventSync(
		context.Background(), "event-connection", "event-log", now,
	)
	require.ErrorIs(t, err, ErrNextcloudEventConflict)
	require.False(t, recovered)

	var log types.SyncLog
	require.NoError(t, db.Where("id = ?", "event-log").Take(&log).Error)
	require.Equal(t, types.SyncLogStatusRunning, log.Status)
	require.True(t, log.WorkerActive)
}

func TestRecoverQueuedEventSyncRejectsWorkerClaimedAfterCandidateRead(t *testing.T) {
	db, inbox, now, _ := newNextcloudQueuedPublicationTest(t)
	require.NoError(t, db.Model(&types.SyncLog{}).Where("id = ?", "event-log").
		Updates(map[string]any{
			"started_at": now.Add(-20 * time.Minute),
			"updated_at": now.Add(-10 * time.Minute),
		}).Error)

	candidates, err := NewNextcloudSyncRecoveryStore(db).
		QueuedEventSyncBefore(context.Background(), now.Add(-5*time.Minute), 100)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, "event-log", candidates[0].SyncLogID)

	require.NoError(t, db.Model(&types.SyncLog{}).Where("id = ?", "event-log").
		Update("worker_active", true).Error)
	recovered, err := inbox.RecoverQueuedEventSync(
		context.Background(), candidates[0].ConnectionID, candidates[0].SyncLogID, now,
	)
	require.ErrorIs(t, err, ErrNextcloudEventConflict)
	require.False(t, recovered)

	var log types.SyncLog
	require.NoError(t, db.Where("id = ?", "event-log").Take(&log).Error)
	require.Equal(t, types.SyncLogStatusRunning, log.Status)
	require.True(t, log.WorkerActive)
}

func TestNextcloudQueuedEventSyncRecoveryUsesExactDispatchFence(t *testing.T) {
	ctx := context.Background()
	db, inbox, now, _ := newNextcloudQueuedPublicationTest(t)
	started := now.Add(-20 * time.Minute)
	require.NoError(t, db.Model(&types.SyncLog{}).Where("id = ?", "event-log").
		Updates(map[string]any{"started_at": started, "updated_at": now.Add(-10 * time.Minute)}).Error)

	candidates, err := NewNextcloudSyncRecoveryStore(db).
		QueuedEventSyncBefore(ctx, now.Add(-5*time.Minute), 100)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, "event-connection", candidates[0].ConnectionID)
	require.Equal(t, "event-log", candidates[0].SyncLogID)

	recovered, err := inbox.RecoverQueuedEventSync(ctx,
		candidates[0].ConnectionID, candidates[0].SyncLogID, now)
	require.NoError(t, err)
	require.True(t, recovered)
	var log types.SyncLog
	require.NoError(t, db.Where("id = ?", "event-log").Take(&log).Error)
	require.Equal(t, types.SyncLogStatusFailed, log.Status)
	require.Equal(t, "sync_worker_terminated", log.ErrorMessage)
	row := publicationDispatchRow(t, db)
	require.Equal(t, "retry", row.State)
	require.Equal(t, "sync_worker_terminated", row.LastErrorCode)
	require.Zero(t, row.AttemptCount)
}
