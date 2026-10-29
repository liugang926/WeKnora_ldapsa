package container

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type nextcloudRecoveryProbe struct {
	err        error
	exists     bool
	existsByID map[string]bool
	id         string
}

type nextcloudEventRecoveryProbe struct {
	calls     []string
	recovered bool
	err       error
}

func (r *nextcloudEventRecoveryProbe) RecoverQueuedEventSync(
	_ context.Context, connectionID, syncLogID string, _ time.Time,
) (bool, error) {
	r.calls = append(r.calls, connectionID+":"+syncLogID)
	return r.recovered, r.err
}

func (p *nextcloudRecoveryProbe) GetTaskInfo(queue, id string) (*asynq.TaskInfo, error) {
	p.id = id
	if queue != types.QueueSync {
		return nil, errors.New("wrong queue")
	}
	if p.err != nil {
		return nil, p.err
	}
	if p.exists || p.existsByID[id] {
		return &asynq.TaskInfo{ID: id, Queue: queue}, nil
	}
	return nil, asynq.ErrTaskNotFound
}

func TestNextcloudSyncAdmissionRecoveryProofs(t *testing.T) {
	for _, tc := range []struct {
		name        string
		distributed bool
		probeErr    error
		exists      bool
		wantFailed  bool
		wantErr     bool
	}{
		{name: "redis_task_present", distributed: true, exists: true},
		{name: "redis_timeout", distributed: true, probeErr: errors.New("timeout"), wantErr: true},
		{name: "redis_absent", distributed: true, probeErr: asynq.ErrTaskNotFound, wantFailed: true},
		{
			name: "redis_queue_absent_is_not_exact_proof", distributed: true, probeErr: asynq.ErrQueueNotFound,
			wantErr: true,
		},
		{name: "lite_lost_queue", wantFailed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
			require.NoError(t, err)
			raw, err := db.DB()
			require.NoError(t, err)
			raw.SetMaxOpenConns(1)
			require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
			require.NoError(t, db.Exec("CREATE TABLE data_sources (id TEXT PRIMARY KEY, type TEXT)").Error)
			require.NoError(t, db.Exec("INSERT INTO data_sources (id, type) VALUES ('ds', 'nextcloud')").Error)
			now := time.Now().UTC()
			log := &types.SyncLog{
				ID: "admission", DataSourceID: "ds", TenantID: 7,
				Status: types.SyncLogStatusRunning, StartedAt: now.Add(-10 * time.Minute),
				RecoveryVersion: 1, RecoveryTrigger: "manual",
			}
			require.NoError(t, db.Create(log).Error)
			probe := &nextcloudRecoveryProbe{err: tc.probeErr, exists: tc.exists}
			count, err := recoverNextcloudSyncAdmissionsOnce(context.Background(),
				repository.NewNextcloudSyncRecoveryStore(db), probe, nil, tc.distributed, now)
			require.Equal(t, tc.wantErr, err != nil)
			if tc.wantFailed {
				require.Equal(t, 1, count)
			} else {
				require.Zero(t, count)
			}
			var after types.SyncLog
			require.NoError(t, db.First(&after, "id = ?", log.ID).Error)
			if tc.wantFailed {
				require.Equal(t, types.SyncLogStatusFailed, after.Status)
			} else {
				require.Equal(t, types.SyncLogStatusRunning, after.Status)
			}
			if tc.distributed {
				require.Equal(t, "dssync:admission", probe.id)
			}
		})
	}
}

func TestNextcloudSyncAdmissionRecoveryRotatesPastExistingTasks(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	raw, err := db.DB()
	require.NoError(t, err)
	raw.SetMaxOpenConns(1)
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
	require.NoError(t, db.Exec("CREATE TABLE data_sources (id TEXT PRIMARY KEY, type TEXT)").Error)
	require.NoError(t, db.Exec("INSERT INTO data_sources (id, type) VALUES ('ds', 'nextcloud')").Error)
	now := time.Now().UTC()
	probe := &nextcloudRecoveryProbe{existsByID: make(map[string]bool)}
	for i := 0; i < 101; i++ {
		id := fmt.Sprintf("present-%03d", i)
		log := &types.SyncLog{
			ID: id, DataSourceID: "ds", TenantID: 7,
			Status: types.SyncLogStatusRunning, StartedAt: now.Add(-10 * time.Minute),
			RecoveryVersion: 1, RecoveryTrigger: "manual",
		}
		require.NoError(t, db.Create(log).Error)
		probe.existsByID["dssync:"+id] = true
	}
	missing := &types.SyncLog{
		ID: "missing-after-present", DataSourceID: "ds", TenantID: 7,
		Status: types.SyncLogStatusRunning, StartedAt: now.Add(-9 * time.Minute),
		RecoveryVersion: 1, RecoveryTrigger: "schedule",
	}
	require.NoError(t, db.Create(missing).Error)
	store := repository.NewNextcloudSyncRecoveryStore(db)
	for run := 0; run < 2; run++ {
		count, err := recoverNextcloudSyncAdmissionsOnce(context.Background(), store, probe, nil, true, now)
		require.NoError(t, err)
		if run == 0 {
			require.Zero(t, count)
		} else {
			require.Equal(t, 1, count)
		}
	}
	var after types.SyncLog
	require.NoError(t, db.First(&after, "id = ?", missing.ID).Error)
	require.Equal(t, types.SyncLogStatusFailed, after.Status)
	require.Equal(t, "dssync:"+missing.ID, probe.id)
	var present types.SyncLog
	require.NoError(t, db.First(&present, "id = ?", "present-000").Error)
	require.Equal(t, types.SyncLogStatusRunning, present.Status)
}

func TestNextcloudSyncAdmissionRecoveryRequiresMissingExactStartedTask(t *testing.T) {
	for _, tc := range []struct {
		name       string
		probeErr   error
		exists     bool
		workerLive bool
		recent     bool
		wantFailed bool
		wantErr    bool
	}{
		{name: "missing after released worker", probeErr: asynq.ErrTaskNotFound, wantFailed: true},
		{name: "exact retry task exists", exists: true},
		{name: "queue lookup unavailable", probeErr: errors.New("redis timeout"), wantErr: true},
		{name: "worker still active", probeErr: asynq.ErrTaskNotFound, workerLive: true},
		{name: "released worker is still in grace", probeErr: asynq.ErrTaskNotFound, recent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
			require.NoError(t, err)
			raw, err := db.DB()
			require.NoError(t, err)
			raw.SetMaxOpenConns(1)
			require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
			require.NoError(t, db.Exec("CREATE TABLE data_sources (id TEXT PRIMARY KEY, type TEXT)").Error)
			require.NoError(t, db.Exec("INSERT INTO data_sources (id, type) VALUES ('ds', 'nextcloud')").Error)
			now := time.Now().UTC()
			started := now.Add(-20 * time.Minute)
			log := &types.SyncLog{
				ID: "started", DataSourceID: "ds", TenantID: 7,
				Status: types.SyncLogStatusRunning, StartedAt: started,
				RecoveryVersion: 1, RecoveryTrigger: "manual",
				WorkerStartedAt: &started, WorkerActive: tc.workerLive,
			}
			require.NoError(t, db.Create(log).Error)
			updatedAt := now.Add(-10 * time.Minute)
			if tc.recent {
				updatedAt = now.Add(-time.Minute)
			}
			require.NoError(t, db.Model(&types.SyncLog{}).Where("id = ?", log.ID).
				UpdateColumn("updated_at", updatedAt).Error)

			probe := &nextcloudRecoveryProbe{err: tc.probeErr, exists: tc.exists}
			count, err := recoverNextcloudSyncAdmissionsOnce(context.Background(),
				repository.NewNextcloudSyncRecoveryStore(db), probe, nil, true, now)
			require.Equal(t, tc.wantErr, err != nil)
			if tc.wantFailed {
				require.Equal(t, 1, count)
			} else {
				require.Zero(t, count)
			}

			var after types.SyncLog
			require.NoError(t, db.First(&after, "id = ?", log.ID).Error)
			if tc.wantFailed {
				require.Equal(t, types.SyncLogStatusFailed, after.Status)
				require.Equal(t, "sync_task_absent_after_worker_start", after.ErrorMessage)
				require.NotNil(t, after.FinishedAt)
			} else {
				require.Equal(t, types.SyncLogStatusRunning, after.Status)
			}
		})
	}
}

func TestNextcloudSyncAdmissionRecoveryRequiresMissingExactQueuedEventTask(t *testing.T) {
	for _, tc := range []struct {
		name       string
		probeErr   error
		exists     bool
		recovered  bool
		wantCalls  int
		wantErr    bool
		wantOutput int
	}{
		{
			name: "task absent", probeErr: asynq.ErrTaskNotFound, recovered: true,
			wantCalls: 1, wantOutput: 1,
		},
		{name: "task present", exists: true},
		{name: "queue unavailable", probeErr: errors.New("redis timeout"), wantErr: true},
		{name: "state changed during recovery", probeErr: asynq.ErrTaskNotFound, wantCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
			require.NoError(t, err)
			raw, err := db.DB()
			require.NoError(t, err)
			raw.SetMaxOpenConns(1)
			require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
			require.NoError(t, db.Exec("CREATE TABLE data_sources (id TEXT PRIMARY KEY, type TEXT)").Error)
			require.NoError(t, db.Exec(`CREATE TABLE nextcloud_event_connections (
				connection_id TEXT PRIMARY KEY, status TEXT NOT NULL)`).Error)
			require.NoError(t, db.Exec(`CREATE TABLE nextcloud_event_dispatch (
				connection_id TEXT PRIMARY KEY, state TEXT NOT NULL,
				last_sync_log_id TEXT)`).Error)
			now := time.Now().UTC()
			log := &types.SyncLog{
				ID: "event-log", DataSourceID: "ds", TenantID: 7,
				Status: types.SyncLogStatusRunning, StartedAt: now.Add(-20 * time.Minute),
			}
			require.NoError(t, db.Create(log).Error)
			require.NoError(t, db.Model(&types.SyncLog{}).Where("id = ?", log.ID).
				UpdateColumn("updated_at", now.Add(-10*time.Minute)).Error)
			require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_connections
				(connection_id, status) VALUES ('event-connection', 'active')`).Error)
			require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_dispatch
				(connection_id, state, last_sync_log_id)
				VALUES ('event-connection', 'queued', ?)`, log.ID).Error)

			probe := &nextcloudRecoveryProbe{err: tc.probeErr, exists: tc.exists}
			events := &nextcloudEventRecoveryProbe{recovered: tc.recovered}
			count, err := recoverNextcloudSyncAdmissionsOnce(context.Background(),
				repository.NewNextcloudSyncRecoveryStore(db), probe, events, true, now)
			require.Equal(t, tc.wantErr, err != nil)
			require.Equal(t, tc.wantOutput, count)
			require.Len(t, events.calls, tc.wantCalls)
			if tc.wantCalls == 1 {
				require.Equal(t, "event-connection:event-log", events.calls[0])
				require.Equal(t, "ncevent:event-connection:event-log", probe.id)
			}
		})
	}
}

func TestNextcloudSyncAdmissionRecoveryRequiresReleasedEventWorkerWithoutQueueProof(t *testing.T) {
	for _, tc := range []struct {
		name       string
		workerLive bool
		wantCalls  int
		wantOutput int
	}{
		{name: "released worker", wantCalls: 1, wantOutput: 1},
		{name: "active worker", workerLive: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
			require.NoError(t, err)
			raw, err := db.DB()
			require.NoError(t, err)
			raw.SetMaxOpenConns(1)
			require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
			require.NoError(t, db.Exec("CREATE TABLE data_sources (id TEXT PRIMARY KEY, type TEXT)").Error)
			require.NoError(t, db.Exec("INSERT INTO data_sources (id, type) VALUES ('ds', 'nextcloud')").Error)
			require.NoError(t, db.Exec(`CREATE TABLE nextcloud_event_connections (
				connection_id TEXT PRIMARY KEY, status TEXT NOT NULL)`).Error)
			require.NoError(t, db.Exec(`CREATE TABLE nextcloud_event_dispatch (
				connection_id TEXT PRIMARY KEY, state TEXT NOT NULL,
				last_sync_log_id TEXT)`).Error)
			now := time.Now().UTC()
			started := now.Add(-20 * time.Minute)
			log := &types.SyncLog{
				ID: "event-log", DataSourceID: "ds", TenantID: 7,
				Status: types.SyncLogStatusRunning, StartedAt: started,
				WorkerStartedAt: &started, WorkerActive: tc.workerLive,
			}
			require.NoError(t, db.Create(log).Error)
			require.NoError(t, db.Model(&types.SyncLog{}).Where("id = ?", log.ID).
				UpdateColumn("updated_at", now.Add(-10*time.Minute)).Error)
			require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_connections
				(connection_id, status) VALUES ('event-connection', 'active')`).Error)
			require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_dispatch
				(connection_id, state, last_sync_log_id)
				VALUES ('event-connection', 'queued', ?)`, log.ID).Error)

			events := &nextcloudEventRecoveryProbe{recovered: true}
			count, err := recoverNextcloudSyncAdmissionsOnce(context.Background(),
				repository.NewNextcloudSyncRecoveryStore(db), nil, events, false, now)
			require.NoError(t, err)
			require.Equal(t, tc.wantOutput, count)
			require.Len(t, events.calls, tc.wantCalls)

			if tc.workerLive {
				var after types.SyncLog
				require.NoError(t, db.First(&after, "id = ?", log.ID).Error)
				require.Equal(t, types.SyncLogStatusRunning, after.Status)
			}
		})
	}
}
