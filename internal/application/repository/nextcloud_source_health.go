package repository

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// NextcloudSourceHealth reports only database observations attributable to one
// paired source. None of these counters is a proof of per-file publication or
// a measure of the shared Asynq/Redis task queue.
type NextcloudSourceHealth struct {
	OperationID     string                       `json:"operation_id"`
	CheckedAt       time.Time                    `json:"checked_at"`
	SourceStatus    string                       `json:"source_status"`
	EventInbox      *NextcloudSourceEventHealth  `json:"event_inbox"`
	SyncLogs        NextcloudSourceSyncHealth    `json:"sync_logs"`
	CurrentVersions NextcloudSourceVersionHealth `json:"current_versions"`
}

// NextcloudSourceEventHealth contains the source-event health projection.
// EventInbox is nil when no event connection has ever been installed for the
// paired source. Its pending counts refer to durable event receipts only.
type NextcloudSourceEventHealth struct {
	ConnectionStatus          string `json:"connection_status"`
	ReceivedThroughEventID    string `json:"received_through_event_id"`
	DispatchedThroughEventID  string `json:"dispatched_through_event_id"`
	AppliedThroughEventID     string `json:"applied_through_event_id"`
	UnappliedCount            int64  `json:"unapplied_count"`
	UndispatchedCount         int64  `json:"undispatched_count"`
	OldestUnappliedAgeSeconds *int64 `json:"oldest_unapplied_age_seconds"`
	DispatchState             string `json:"dispatch_state"`
	LastErrorCode             string `json:"last_error_code"`
}

// NextcloudSourceSyncHealth contains the source-sync health projection.
// Failed and partial sync counts cover retained logs started in the previous
// 24 hours. Log retention means they are not a lifetime failure total.
type NextcloudSourceSyncHealth struct {
	RunningCount            int64   `json:"running_count"`
	OldestRunningAgeSeconds *int64  `json:"oldest_running_age_seconds"`
	FailedLast24Hours       int64   `json:"failed_last_24_hours"`
	PartialLast24Hours      int64   `json:"partial_last_24_hours"`
	LatestStatus            *string `json:"latest_status"`
}

// NextcloudSourceVersionHealth contains the source-version health projection.
// These are the current source-version rows, not historical revisions or all
// knowledge in the dedicated KB. A completed parse does not prove that the
// current ETag was published or that a question is authorized.
type NextcloudSourceVersionHealth struct {
	StagingCount               int64  `json:"staging_count"`
	PublishedCount             int64  `json:"published_count"`
	TombstoneCount             int64  `json:"tombstone_count"`
	MissingCandidateCount      int64  `json:"missing_candidate_count"`
	ParsePendingCount          int64  `json:"parse_pending_count"`
	ParseProcessingCount       int64  `json:"parse_processing_count"`
	ParseFinalizingCount       int64  `json:"parse_finalizing_count"`
	ParseFailedCount           int64  `json:"parse_failed_count"`
	ParseCompletedEnabledCount int64  `json:"parse_completed_enabled_count"`
	OtherCandidateStatusCount  int64  `json:"other_candidate_status_count"`
	OldestStagingAgeSeconds    *int64 `json:"oldest_staging_age_seconds"`
}

type nextcloudSyncHealthRow struct {
	RunningCount       int64 `gorm:"column:running_count"`
	FailedLast24Hours  int64 `gorm:"column:failed_last_24_hours"`
	PartialLast24Hours int64 `gorm:"column:partial_last_24_hours"`
}

type nextcloudVersionHealthRow struct {
	StagingCount               int64 `gorm:"column:staging_count"`
	PublishedCount             int64 `gorm:"column:published_count"`
	TombstoneCount             int64 `gorm:"column:tombstone_count"`
	MissingCandidateCount      int64 `gorm:"column:missing_candidate_count"`
	ParsePendingCount          int64 `gorm:"column:parse_pending_count"`
	ParseProcessingCount       int64 `gorm:"column:parse_processing_count"`
	ParseFinalizingCount       int64 `gorm:"column:parse_finalizing_count"`
	ParseFailedCount           int64 `gorm:"column:parse_failed_count"`
	ParseCompletedEnabledCount int64 `gorm:"column:parse_completed_enabled_count"`
	OtherCandidateStatusCount  int64 `gorm:"column:other_candidate_status_count"`
}

func nextcloudHealthAge(now time.Time, since *time.Time) *int64 {
	if since == nil {
		return nil
	}
	age := int64(now.Sub(*since).Seconds())
	if age < 0 {
		age = 0
	}
	return &age
}

func nextcloudHealthErrorCode(code string) string {
	switch code {
	case "", "source_changed", "cursor_missing_manual_review", "sync_stale_manual_review",
		"sync_not_successful", "deletion_or_cursor_pending", "publication_pending",
		"publication_unproven", "inbox_missing", "queue_unavailable", "source_paused",
		"source_unavailable", "dispatch_failed", "queue_uncertain":
		return code
	default:
		return "unknown"
	}
}

// SourceHealth requires a pairing already authorized by the HTTP handler.
// The queries repeat its tenant, KB and source identity to avoid counting any
// other source in the same tenant. They intentionally never select a URL,
// file path, error message, payload, key, or encrypted credential.
func (r *NextcloudSourcePairingRepository) SourceHealth(
	ctx context.Context, pair NextcloudSourcePairing, now time.Time,
) (NextcloudSourceHealth, error) {
	result := NextcloudSourceHealth{OperationID: pair.OperationID, CheckedAt: now.UTC()}
	if pair.OperationID == "" || pair.TenantID == 0 || pair.KnowledgeBaseID == "" ||
		pair.DataSourceID == "" || pair.InstanceID == "" || pair.BindingID == "" ||
		pair.State == "aborted" {
		return result, ErrNextcloudSourcePairingConflict
	}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var source struct {
			Status string `gorm:"column:status"`
		}
		if err := tx.Table("data_sources").Select("status").Where(
			"id = ? AND tenant_id = ? AND knowledge_base_id = ? AND type = ? AND deleted_at IS NULL",
			pair.DataSourceID, pair.TenantID, pair.KnowledgeBaseID, types.ConnectorTypeNextcloud,
		).Take(&source).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNextcloudSourcePairingConflict
			}
			return fmt.Errorf("read paired source health: %w", err)
		}
		result.SourceStatus = source.Status

		var sync nextcloudSyncHealthRow
		if err := tx.Raw(("SELECT\n" +
			"\t\t\tCOALESCE(SUM(CASE WHEN status = 'running' THEN 1 ELS" +
			"E 0 END), 0) AS running_count,\n" +
			"\t\t\tCOALESCE(SUM(CASE WHEN status = 'failed' AND started" +
			"_at >= ? THEN 1 ELSE 0 END), 0) AS failed_last_24_hours" +
			",\n" +
			"\t\t\tCOALESCE(SUM(CASE WHEN status = 'partial' AND starte" +
			"d_at >= ? THEN 1 ELSE 0 END), 0) AS partial_last_24_hou" +
			"rs\n" +
			"\t\t\tFROM sync_logs WHERE tenant_id = ? AND data_source_i" +
			"d = ?"),
			now.Add(-24*time.Hour), now.Add(-24*time.Hour), pair.TenantID, pair.DataSourceID,
		).Scan(&sync).Error; err != nil {
			return fmt.Errorf("read paired sync health: %w", err)
		}
		result.SyncLogs = NextcloudSourceSyncHealth{
			RunningCount:       sync.RunningCount,
			FailedLast24Hours:  sync.FailedLast24Hours,
			PartialLast24Hours: sync.PartialLast24Hours,
		}
		var oldestRunning struct {
			StartedAt time.Time `gorm:"column:started_at"`
		}
		runningQuery := tx.Table("sync_logs").Select("started_at").Where(
			"tenant_id = ? AND data_source_id = ? AND status = 'running'",
			pair.TenantID, pair.DataSourceID,
		).Order("started_at ASC, id ASC").Limit(1).Find(&oldestRunning)
		if runningQuery.Error != nil {
			return fmt.Errorf("read oldest paired sync: %w", runningQuery.Error)
		}
		if runningQuery.RowsAffected == 1 {
			result.SyncLogs.OldestRunningAgeSeconds = nextcloudHealthAge(now, &oldestRunning.StartedAt)
		}
		var latest struct {
			Status string `gorm:"column:status"`
		}
		latestQuery := tx.Table("sync_logs").Select("status").Where(
			"tenant_id = ? AND data_source_id = ?", pair.TenantID, pair.DataSourceID,
		).Order("started_at DESC, id DESC").Limit(1).Find(&latest)
		if latestQuery.Error != nil {
			return fmt.Errorf("read latest paired sync: %w", latestQuery.Error)
		}
		if latestQuery.RowsAffected == 1 {
			result.SyncLogs.LatestStatus = &latest.Status
		}

		var versions nextcloudVersionHealthRow
		if err := tx.Raw(("SELECT\n" +
			"\t\t\tCOALESCE(SUM(CASE WHEN v.state = 'staging' THEN 1 EL" +
			"SE 0 END), 0) AS staging_count,\n" +
			"\t\t\tCOALESCE(SUM(CASE WHEN v.state = 'published' THEN 1 " +
			"ELSE 0 END), 0) AS published_count,\n" +
			"\t\t\tCOALESCE(SUM(CASE WHEN v.state = 'tombstone' THEN 1 " +
			"ELSE 0 END), 0) AS tombstone_count,\n" +
			"\t\t\tCOALESCE(SUM(CASE WHEN v.state <> 'tombstone' AND k." +
			"id IS NULL THEN 1 ELSE 0 END), 0) AS missing_candidate_" +
			"count,\n" +
			"\t\t\tCOALESCE(SUM(CASE WHEN k.parse_status = 'pending' TH" +
			"EN 1 ELSE 0 END), 0) AS parse_pending_count,\n" +
			"\t\t\tCOALESCE(SUM(CASE WHEN k.parse_status = 'processing'" +
			" THEN 1 ELSE 0 END), 0) AS parse_processing_count,\n" +
			"\t\t\tCOALESCE(SUM(CASE WHEN k.parse_status = 'finalizing'" +
			" THEN 1 ELSE 0 END), 0) AS parse_finalizing_count,\n" +
			"\t\t\tCOALESCE(SUM(CASE WHEN k.parse_status = 'failed' THE" +
			"N 1 ELSE 0 END), 0) AS parse_failed_count,\n" +
			"\t\t\tCOALESCE(SUM(CASE WHEN k.parse_status = 'completed' " +
			"AND k.enable_status = 'enabled' THEN 1 ELSE 0 END), 0) " +
			"AS parse_completed_enabled_count,\n" +
			"\t\t\tCOALESCE(SUM(CASE WHEN k.id IS NOT NULL AND\n" +
			"\t\t\t\t(k.parse_status NOT IN ('pending', 'processing', 'f" +
			"inalizing', 'failed', 'completed') OR\n" +
			"\t\t\t\t(k.parse_status = 'completed' AND k.enable_status <" +
			"> 'enabled')) THEN 1 ELSE 0 END), 0) AS other_candidate" +
			"_status_count\n" +
			"\t\t\tFROM nextcloud_source_versions AS v\n" +
			"\t\t\tLEFT JOIN knowledges AS k ON k.id = v.candidate_know" +
			"ledge_id AND\n" +
			"\t\t\t\tk.tenant_id = v.tenant_id AND k.knowledge_base_id =" +
			" v.knowledge_base_id AND\n" +
			"\t\t\t\tk.deleted_at IS NULL AND v.state <> 'tombstone'\n" +
			"\t\t\tWHERE v.tenant_id = ? AND v.knowledge_base_id = ? AN" +
			"D v.datasource_id = ?"),
			pair.TenantID, pair.KnowledgeBaseID, pair.DataSourceID,
		).Scan(&versions).Error; err != nil {
			return fmt.Errorf("read paired version health: %w", err)
		}
		result.CurrentVersions = NextcloudSourceVersionHealth{
			StagingCount: versions.StagingCount, PublishedCount: versions.PublishedCount,
			TombstoneCount: versions.TombstoneCount, MissingCandidateCount: versions.MissingCandidateCount,
			ParsePendingCount: versions.ParsePendingCount, ParseProcessingCount: versions.ParseProcessingCount,
			ParseFinalizingCount: versions.ParseFinalizingCount, ParseFailedCount: versions.ParseFailedCount,
			ParseCompletedEnabledCount: versions.ParseCompletedEnabledCount,
			OtherCandidateStatusCount:  versions.OtherCandidateStatusCount,
		}
		var oldestStaging struct {
			UpdatedAt time.Time `gorm:"column:updated_at"`
		}
		stagingQuery := tx.Table("nextcloud_source_versions").Select("updated_at").Where(
			"tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND state = 'staging'",
			pair.TenantID, pair.KnowledgeBaseID, pair.DataSourceID,
		).Order("updated_at ASC, external_id ASC").Limit(1).Find(&oldestStaging)
		if stagingQuery.Error != nil {
			return fmt.Errorf("read oldest paired staging version: %w", stagingQuery.Error)
		}
		if stagingQuery.RowsAffected == 1 {
			result.CurrentVersions.OldestStagingAgeSeconds = nextcloudHealthAge(now, &oldestStaging.UpdatedAt)
		}
		var currentConnection struct {
			ConnectionID string `gorm:"column:connection_id"`
		}
		connectionQuery := tx.Table("nextcloud_event_connections").
			Select("connection_id").
			Where("tenant_id = ? AND datasource_id = ?", pair.TenantID, pair.DataSourceID).
			Order("created_at DESC").Limit(1).Find(&currentConnection)
		if connectionQuery.Error != nil {
			return fmt.Errorf("read paired event connection: %w", connectionQuery.Error)
		}
		if connectionQuery.RowsAffected == 0 {
			return nil
		}
		status, statusErr := (&NextcloudEventInboxRepository{db: tx}).ConnectionStatus(
			ctx, pair.TenantID, pair.DataSourceID)
		if statusErr != nil {
			return fmt.Errorf("read paired event health: %w", statusErr)
		}
		if status.NextcloudInstanceID != pair.InstanceID || status.BindingID != pair.BindingID {
			return ErrNextcloudSourcePairingConflict
		}
		appliedID, err := strconv.ParseInt(status.AppliedThroughEventID, 10, 64)
		if err != nil || appliedID < 0 {
			return ErrNextcloudEventConflict
		}
		var oldest struct {
			ReceivedAt time.Time `gorm:"column:received_at"`
		}
		oldestQuery := tx.Table("nextcloud_event_inbox").Select("received_at").Where(
			"connection_id = ? AND event_id > ?", status.ConnectionID, appliedID,
		).Order("event_id ASC").Limit(1).Find(&oldest)
		if oldestQuery.Error != nil {
			return fmt.Errorf("read oldest paired event: %w", oldestQuery.Error)
		}
		var oldestUnappliedAge *int64
		if oldestQuery.RowsAffected == 1 {
			oldestUnappliedAge = nextcloudHealthAge(now, &oldest.ReceivedAt)
		}
		result.EventInbox = &NextcloudSourceEventHealth{
			ConnectionStatus:          status.Status,
			ReceivedThroughEventID:    status.ReceivedThroughEventID,
			DispatchedThroughEventID:  status.DispatchedThroughEventID,
			AppliedThroughEventID:     status.AppliedThroughEventID,
			UnappliedCount:            status.BacklogCount,
			UndispatchedCount:         status.UndispatchedCount,
			OldestUnappliedAgeSeconds: oldestUnappliedAge,
			DispatchState:             status.DispatchState,
			LastErrorCode:             nextcloudHealthErrorCode(status.LastErrorCode),
		}
		return nil
	})
	return result, err
}
