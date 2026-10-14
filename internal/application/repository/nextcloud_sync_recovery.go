package repository

import (
	"context"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// NextcloudSyncRecoveryStore only considers versioned manual/cron intents.
// Old running rows, and rows reached by any worker, remain operator-owned.
type NextcloudSyncRecoveryStore struct {
	db            *gorm.DB
	mu            sync.Mutex
	lastStartedAt time.Time
	lastID        string
}

// NewNextcloudSyncRecoveryStore returns the durable source-sync recovery store.
func NewNextcloudSyncRecoveryStore(db *gorm.DB) *NextcloudSyncRecoveryStore {
	return &NextcloudSyncRecoveryStore{db: db}
}

// UnstartedBefore rotates across eligible logs in stable (started_at, id)
// order. Existing Redis tasks remain running, so repeatedly reading only the
// oldest 100 would starve later missing tasks forever.
func (s *NextcloudSyncRecoveryStore) UnstartedBefore(ctx context.Context, cutoff time.Time, limit int) (
	[]types.SyncLog,
	error,
) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	load := func(after bool) ([]types.SyncLog, error) {
		query := s.db.WithContext(ctx).Model(&types.SyncLog{}).
			Where(("status = ? AND recovery_version = 1 AND recovery_trigge"+
				"r IN ?"), types.SyncLogStatusRunning, []string{"manual", "schedule"}).
			Where(("queue_task_id = 'dssync:' || sync_logs.id AND worker_st"+
				"arted_at IS NULL AND worker_active = ? AND started_at <"+
				"= ?"), false, cutoff).
			Where(("EXISTS (SELECT 1 FROM data_sources ds WHERE ds.id = syn" +
				"c_logs.data_source_id AND ds.type = ?)"), types.ConnectorTypeNextcloud)
		if after {
			query = query.Where(
				"(started_at > ? OR (started_at = ? AND id > ?))",
				s.lastStartedAt,
				s.lastStartedAt,
				s.lastID,
			)
		}
		var logs []types.SyncLog
		err := query.Order("started_at, id").Limit(limit).Find(&logs).Error
		return logs, err
	}
	logs, err := load(s.lastID != "")
	if err != nil {
		return nil, err
	}
	if len(logs) == 0 && s.lastID != "" {
		logs, err = load(false)
		if err != nil {
			return nil, err
		}
	}
	if len(logs) == 0 {
		s.lastStartedAt = time.Time{}
		s.lastID = ""
	} else {
		last := logs[len(logs)-1]
		s.lastStartedAt = last.StartedAt
		s.lastID = last.ID
	}
	return logs, nil
}

// FailMissing releases one slot only after the caller proved the exact task is
// absent. The worker's ClaimNextcloudSyncStart races this statement on the
// same row: one wins, so no worker can read the source after release.
func (s *NextcloudSyncRecoveryStore) FailMissing(ctx context.Context, log types.SyncLog, cutoff, now time.Time) (
	bool,
	error,
) {
	if log.ID == "" || log.DataSourceID == "" || log.TenantID == 0 ||
		(log.RecoveryTrigger != "manual" && log.RecoveryTrigger != "schedule") ||
		log.QueueTaskID != "dssync:"+log.ID {
		return false, nil
	}
	result := s.db.WithContext(ctx).Model(&types.SyncLog{}).
		Where("id = ? AND data_source_id = ? AND tenant_id = ?", log.ID, log.DataSourceID, log.TenantID).
		Where(("status = ? AND recovery_version = 1 AND recovery_trigge"+
			"r = ?"), types.SyncLogStatusRunning, log.RecoveryTrigger).
		Where(("queue_task_id = ? AND worker_started_at IS NULL AND wor" +
			"ker_active = ? AND started_at <= ?"), log.QueueTaskID, false, cutoff).
		Updates(map[string]any{
			"status":      types.SyncLogStatusFailed,
			"finished_at": now, "error_message": "sync_task_absent_before_worker_start",
		})
	return result.RowsAffected == 1, result.Error
}
