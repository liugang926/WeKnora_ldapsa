package repository

import (
	"context"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// NextcloudSyncRecoveryStore only considers versioned manual/cron intents.
// Version-zero rows, active workers, and recent released workers remain
// operator-owned until the caller proves that their exact queue task is absent.
type NextcloudSyncRecoveryStore struct {
	db            *gorm.DB
	mu            sync.Mutex
	lastStartedAt time.Time
	lastID        string

	eventLastStartedAt  time.Time
	eventLastConnection string
	eventLastSyncLogID  string
}

func NewNextcloudSyncRecoveryStore(db *gorm.DB) *NextcloudSyncRecoveryStore {
	return &NextcloudSyncRecoveryStore{db: db}
}

// RecoverableBefore rotates across eligible logs in stable (started_at, id)
// order. A started row is only eligible after its worker released the attempt
// and the row has remained untouched for the grace period. Existing Redis
// tasks remain running, so repeatedly reading only the oldest 100 would starve
// later missing tasks forever.
func (s *NextcloudSyncRecoveryStore) RecoverableBefore(
	ctx context.Context,
	cutoff time.Time,
	limit int,
) ([]types.SyncLog, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	load := func(after bool) ([]types.SyncLog, error) {
		query := s.db.WithContext(ctx).Model(&types.SyncLog{}).
			Where("status = ? AND recovery_version = 1 AND recovery_trigger IN ?", types.SyncLogStatusRunning, []string{
				"manual",
				"schedule",
			}).
			Where(("queue_task_id = 'dssync:' || sync_logs.id AND worker_active = ? AND "+
				"started_at <= ? AND (worker_started_at IS NULL OR updated_at <= ?)"),
				false, cutoff, cutoff).
			Where(("EXISTS (SELECT 1 FROM data_sources ds WHERE ds.id = sync_logs.data_source_" +
				"id AND ds.type = ?)"), types.ConnectorTypeNextcloud)
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

// NextcloudEventSyncRecoveryCandidate is one exact queued event dispatch whose
// worker-owned sync log has stopped changing. Queue absence is still required
// before recovery; this candidate list alone is not proof of a dead worker.
type NextcloudEventSyncRecoveryCandidate struct {
	ConnectionID string
	SyncLogID    string
	StartedAt    time.Time
	UpdatedAt    time.Time
}

// QueuedEventSyncBefore lists stale queued event runs with stable rotation so
// long-lived active tasks cannot starve later orphaned tasks.
func (s *NextcloudSyncRecoveryStore) QueuedEventSyncBefore(
	ctx context.Context,
	cutoff time.Time,
	limit int,
) ([]NextcloudEventSyncRecoveryCandidate, error) {
	if !s.db.Migrator().HasTable("nextcloud_event_dispatch") ||
		!s.db.Migrator().HasTable("nextcloud_event_connections") {
		return nil, nil
	}
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	load := func(after bool) ([]NextcloudEventSyncRecoveryCandidate, error) {
		query := s.db.WithContext(ctx).Table("nextcloud_event_dispatch AS d").
			Select("d.connection_id, d.last_sync_log_id AS sync_log_id, "+
				"s.started_at, s.updated_at").
			Joins("JOIN nextcloud_event_connections AS c ON c.connection_id = d.connection_id").
			Joins("JOIN sync_logs AS s ON s.id = d.last_sync_log_id").
			Where("c.status = 'active' AND d.state = 'queued' AND s.status = ?",
				types.SyncLogStatusRunning).
			Where("s.worker_active = ? AND s.started_at <= ? AND s.updated_at <= ?",
				false, cutoff, cutoff)
		if after {
			query = query.Where(
				"(s.started_at > ? OR (s.started_at = ? AND "+
					"(d.connection_id > ? OR (d.connection_id = ? AND s.id > ?))))",
				s.eventLastStartedAt,
				s.eventLastStartedAt,
				s.eventLastConnection,
				s.eventLastConnection,
				s.eventLastSyncLogID,
			)
		}
		var events []NextcloudEventSyncRecoveryCandidate
		err := query.Order("s.started_at, d.connection_id, s.id").Limit(limit).
			Scan(&events).Error
		return events, err
	}
	events, err := load(s.eventLastSyncLogID != "")
	if err != nil {
		return nil, err
	}
	if len(events) == 0 && s.eventLastSyncLogID != "" {
		events, err = load(false)
		if err != nil {
			return nil, err
		}
	}
	if len(events) == 0 {
		s.eventLastStartedAt = time.Time{}
		s.eventLastConnection = ""
		s.eventLastSyncLogID = ""
	} else {
		last := events[len(events)-1]
		s.eventLastStartedAt = last.StartedAt
		s.eventLastConnection = last.ConnectionID
		s.eventLastSyncLogID = last.SyncLogID
	}
	return events, nil
}

// FailMissing releases one slot only after the caller proved the exact task is
// absent. A started row must also have been released and stale for the grace
// period. The worker's ClaimNextcloudSyncStart races this statement on the
// same row: one wins, so no worker can read the source after release.
func (s *NextcloudSyncRecoveryStore) FailMissing(
	ctx context.Context,
	log types.SyncLog,
	cutoff, now time.Time,
) (bool, error) {
	if log.ID == "" || log.DataSourceID == "" || log.TenantID == 0 ||
		(log.RecoveryTrigger != "manual" && log.RecoveryTrigger != "schedule") ||
		log.QueueTaskID != "dssync:"+log.ID {
		return false, nil
	}
	errorMessage := "sync_task_absent_before_worker_start"
	if log.WorkerStartedAt != nil {
		errorMessage = "sync_task_absent_after_worker_start"
	}
	result := s.db.WithContext(ctx).Model(&types.SyncLog{}).
		Where("id = ? AND data_source_id = ? AND tenant_id = ?", log.ID, log.DataSourceID, log.TenantID).
		Where(
			"status = ? AND recovery_version = 1 AND recovery_trigger = ?",
			types.SyncLogStatusRunning,
			log.RecoveryTrigger,
		).
		Where(("queue_task_id = ? AND worker_active = ? AND started_at <= ? AND " +
			"(worker_started_at IS NULL OR updated_at <= ?)"),
			log.QueueTaskID, false, cutoff, cutoff).
		Updates(map[string]any{
			"status":      types.SyncLogStatusFailed,
			"finished_at": now, "error_message": errorMessage,
		})
	return result.RowsAffected == 1, result.Error
}
