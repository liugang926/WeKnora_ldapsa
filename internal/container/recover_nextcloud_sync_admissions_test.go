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
				repository.NewNextcloudSyncRecoveryStore(db), probe, tc.distributed, now)
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
		count, err := recoverNextcloudSyncAdmissionsOnce(context.Background(), store, probe, true, now)
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
