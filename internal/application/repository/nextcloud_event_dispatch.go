package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type nextcloudEventDispatchRow struct {
	ConnectionID  string     `gorm:"column:connection_id"`
	DispatchedID  int64      `gorm:"column:dispatched_id"`
	AppliedID     int64      `gorm:"column:applied_id"`
	State         string     `gorm:"column:state"`
	LeaseToken    *string    `gorm:"column:lease_token"`
	LeaseUntil    *time.Time `gorm:"column:lease_until"`
	TargetEventID int64      `gorm:"column:target_event_id"`
	LastSyncLogID *string    `gorm:"column:last_sync_log_id"`
	AttemptCount  int        `gorm:"column:attempt_count"`
	NextAttemptAt time.Time  `gorm:"column:next_attempt_at"`
	LastErrorCode string     `gorm:"column:last_error_code"`
}

const (
	nextcloudQueuedPollInterval        = 5 * time.Second
	nextcloudPublicationFastPollWindow = time.Minute
	nextcloudPublicationRetryGrace     = 15 * time.Second
)

// NextcloudEventDispatchClaim is a short-lived, DB-fenced right to enqueue a
// source reconciliation. It is never an applied acknowledgement.
type NextcloudEventDispatchClaim struct {
	ConnectionID  string
	DatasourceID  string
	TenantID      uint64
	TargetEventID int64
	LeaseToken    string
	ConfigSHA256  string
}

// DispatchCandidates returns only connection IDs; ClaimDispatch rechecks all
// state under a transaction so multiple application instances can race safely.
func (r *NextcloudEventInboxRepository) DispatchCandidates(ctx context.Context, now time.Time) ([]string, error) {
	var ids []string
	err := r.db.WithContext(ctx).Table("nextcloud_event_dispatch AS d").
		Select("d.connection_id").
		Joins("JOIN nextcloud_event_connections AS c ON c.connection_id = d.connection_id").
		Joins("JOIN nextcloud_event_checkpoint AS p ON p.connection_id = d.connection_id").
		Where("c.status = 'active'").
		Where(`(d.state = 'idle' AND p.received_id > d.dispatched_id)
			OR (d.state IN ('queued', 'retry') AND d.next_attempt_at <= ?)
			OR (d.state = 'queued' AND d.last_error_code = ''
				AND p.received_id > d.dispatched_id
				AND EXISTS (SELECT 1 FROM sync_logs AS s
					WHERE s.id = d.last_sync_log_id
					AND s.data_source_id = c.datasource_id
					AND s.tenant_id = c.tenant_id
					AND s.status = 'success' AND s.finished_at IS NOT NULL))
			OR (d.state = 'queued' AND d.last_error_code = 'publication_pending'
				AND p.received_id > d.target_event_id AND d.updated_at <= ?
				AND EXISTS (SELECT 1 FROM sync_logs AS s
					WHERE s.id = d.last_sync_log_id
					AND s.data_source_id = c.datasource_id
					AND s.tenant_id = c.tenant_id
					AND s.status = 'success' AND s.finished_at IS NOT NULL))
			OR (d.state = 'retry' AND d.last_error_code = 'publication_unproven'
				AND p.received_id > d.target_event_id
				AND EXISTS (SELECT 1 FROM sync_logs AS s
					WHERE s.id = d.last_sync_log_id
					AND s.data_source_id = c.datasource_id
					AND s.tenant_id = c.tenant_id
					AND s.status = 'success' AND s.finished_at IS NOT NULL))
			OR (d.state = 'leased' AND d.lease_until <= ? AND d.next_attempt_at <= ?)`,
			now, now.Add(-nextcloudQueuedPollInterval), now, now).
		Order("d.next_attempt_at ASC, d.connection_id ASC").Limit(32).Pluck("d.connection_id", &ids).Error
	return ids, err
}

// ClaimDispatch consumes no hint and does no network I/O. Its target is the
// committed receipt watermark, so a later pushed event remains for a later
// complete manifest reconciliation. Retried work can coalesce newer hints.
func (
	r *NextcloudEventInboxRepository,
) ClaimDispatch(ctx context.Context, connectionID string, now time.Time) (*NextcloudEventDispatchClaim, error) {
	var claim *NextcloudEventDispatchClaim
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Name() == "sqlite" {
			lock := tx.Exec(`UPDATE nextcloud_event_connections SET status = status
				WHERE connection_id = ? AND status = 'active'`, connectionID)
			if lock.Error != nil {
				return fmt.Errorf("lock Nextcloud event dispatch: %w", lock.Error)
			}
			if lock.RowsAffected != 1 {
				return nil
			}
		}
		var connection nextcloudEventConnection
		if err := tx.Table("nextcloud_event_connections").Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("connection_id = ? AND status = 'active'", connectionID).Take(&connection).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return fmt.Errorf("read Nextcloud dispatch connection: %w", err)
		}
		var dispatch nextcloudEventDispatchRow
		if err := tx.Table("nextcloud_event_dispatch").Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("connection_id = ?", connectionID).Take(&dispatch).Error; err != nil {
			return fmt.Errorf("read Nextcloud event dispatch: %w", err)
		}
		if dispatch.State == "blocked" {
			return nil
		}
		if err := checkNextcloudEventDataSource(tx, connection); err != nil {
			if errors.Is(err, ErrNextcloudEventScope) {
				return setNextcloudDispatchState(
					tx,
					connectionID,
					"blocked",
					"source_changed",
					now,
					dispatch.AttemptCount,
				)
			}
			return err
		}
		baselineOK, err := nextcloudDispatchBaselineValid(tx, connection)
		if err != nil {
			return err
		}
		if !baselineOK {
			return setNextcloudDispatchState(tx, connectionID, "blocked",
				"cursor_missing_manual_review", now, dispatch.AttemptCount)
		}
		if dispatch.State == "leased" && ((dispatch.LeaseUntil != nil && now.Before(*dispatch.LeaseUntil)) ||
			now.Before(dispatch.NextAttemptAt)) {
			return nil
		}
		if dispatch.State == "queued" {
			coalescing := false
			status, startedAt, finishedAt, err := nextcloudDispatchSyncStatus(tx, dispatch.LastSyncLogID)
			if err != nil {
				return err
			}
			if status == "running" {
				if now.Sub(startedAt) < 150*time.Minute {
					return tx.Table("nextcloud_event_dispatch").Where("connection_id = ?", connectionID).
						Updates(map[string]any{"next_attempt_at": now.Add(
							nextcloudQueuedPollInterval,
						), "updated_at": now}).Error
				}
				return setNextcloudDispatchState(tx, connectionID, "blocked",
					"sync_stale_manual_review", now, dispatch.AttemptCount)
			}
			matching, err := nextcloudDispatchMatchingSync(tx, connection, dispatch.LastSyncLogID)
			if err != nil {
				return err
			}
			if status != "success" || finishedAt == nil || !matching {
				return setNextcloudDispatchState(
					tx,
					connectionID,
					"retry",
					"sync_not_successful",
					now.Add(
						nextcloudDispatchBackoff(
							dispatch.AttemptCount+1,
						)), dispatch.AttemptCount+1)
			}
			confirmed, err := nextcloudDispatchCursorComplete(tx, connection, startedAt)
			if err != nil {
				return err
			}
			if !confirmed {
				return setNextcloudDispatchState(tx, connectionID, "retry", "deletion_or_cursor_pending",
					now.Add(nextcloudDispatchBackoff(dispatch.AttemptCount+1)), dispatch.AttemptCount+1)
			}
			proof, err := nextcloudDispatchPublicationProof(tx, connection, dispatch.AppliedID,
				dispatch.TargetEventID, startedAt)
			if err != nil {
				return err
			}
			switch proof {
			case nextcloudApplyWaiting:
				// A newer signed version of the one file being parsed may replace
				// this scan before its old parser finishes. Limit this admission to
				// one outstanding parser generation for that file; Stage retires
				// the old build fence, and the replacement still needs its own
				// exact publication proof before any ACK advances.
				var latest nextcloudEventCheckpoint
				if err := tx.Table("nextcloud_event_checkpoint").Where("connection_id = ?", connectionID).
					Take(&latest).Error; err != nil {
					return err
				}
				if latest.ReceivedID > dispatch.TargetEventID {
					coalescing, err = nextcloudCanSupersedePendingParser(tx, connection,
						dispatch.TargetEventID, latest.ReceivedID)
					if err != nil {
						return err
					}
					if coalescing {
						break
					}
				}
				// Without a bounded replacement, keep polling the exact old run.
				return tx.Table("nextcloud_event_dispatch").Where("connection_id = ?", connectionID).
					Updates(map[string]any{
						"next_attempt_at": now.Add(nextcloudPublicationPollDelay(now, finishedAt)),
						"last_error_code": nextcloudPublicationPendingCode(
							dispatch.LastErrorCode,
						), "updated_at": now,
					}).Error
			case nextcloudApplyRetry:
				// A newer durable hint can supersede an old candidate that the
				// source no longer permits to publish (for example, an ETag
				// changed during asynchronous parsing). Reconcile the latest
				// manifest now, but leave the old range unapplied until that
				// newer run obtains its own complete publication proof.
				var latest nextcloudEventCheckpoint
				if err := tx.Table("nextcloud_event_checkpoint").Where("connection_id = ?", connectionID).
					Take(&latest).Error; err != nil {
					return err
				}
				if latest.ReceivedID > dispatch.TargetEventID {
					coalescing, err = nextcloudCanCoalesceUnproven(tx, connection,
						dispatch.TargetEventID, latest.ReceivedID)
					if err != nil {
						return err
					}
					if coalescing {
						break
					}
				}
				// Sync completion can precede asynchronous candidate registration.
				// Allow that short handoff to settle before requesting another scan;
				// never acknowledge without a complete publication proof.
				if finishedAt != nil && now.Before(finishedAt.Add(nextcloudPublicationRetryGrace)) {
					return tx.Table("nextcloud_event_dispatch").Where("connection_id = ?", connectionID).
						Updates(map[string]any{
							"next_attempt_at": now.Add(nextcloudQueuedPollInterval),
							"last_error_code": nextcloudPublicationPendingCode(
								dispatch.LastErrorCode,
							), "updated_at": now,
						}).Error
				}
				unprovable, err := nextcloudHintRangeUnprovable(tx, connectionID,
					dispatch.AppliedID, dispatch.TargetEventID)
				if err != nil {
					return err
				}
				if unprovable {
					return setNextcloudDispatchState(tx, connectionID, "blocked",
						"hint_unbound_manual_review", now, dispatch.AttemptCount)
				}
				return setNextcloudDispatchState(tx, connectionID, "retry", "publication_unproven",
					now.Add(nextcloudDispatchBackoff(dispatch.AttemptCount+1)), dispatch.AttemptCount+1)
			case nextcloudApplyComplete:
				if dispatch.TargetEventID <= dispatch.AppliedID || dispatch.TargetEventID > dispatch.DispatchedID {
					return ErrNextcloudEventConflict
				}
				broad, err := nextcloudHintRangeNeedsTwoScans(tx, connection,
					dispatch.AppliedID, dispatch.TargetEventID)
				if err != nil {
					return err
				}
				marker := nextcloudBroadConfirmationCode(dispatch.TargetEventID)
				if broad && dispatch.LastErrorCode != marker {
					// A subtree hint does not identify all affected files. Require
					// a second complete inventory and publication pass for this
					// exact receipt range before advancing applied_id.
					return setNextcloudDispatchState(tx, connectionID, "retry", marker,
						now.Add(nextcloudQueuedPollInterval), dispatch.AttemptCount)
				}
				if err := tx.Table("nextcloud_event_inbox").
					Where("connection_id = ? AND event_id <= ?", connectionID, dispatch.TargetEventID).
					Update("state", "applied").Error; err != nil {
					return err
				}
				if err := tx.Table("nextcloud_event_dispatch").Where("connection_id = ?", connectionID).
					Updates(map[string]any{
						"applied_id": dispatch.TargetEventID, "state": "idle",
						"lease_token": nil, "lease_until": nil, "attempt_count": 0,
						"next_attempt_at": now, "last_error_code": "", "updated_at": now,
					}).Error; err != nil {
					return err
				}
			default:
				return ErrNextcloudEventConflict
			}
			if !coalescing {
				dispatch.State = "idle"
				dispatch.AppliedID = dispatch.TargetEventID
			}
		}
		if dispatch.State == "retry" && now.Before(dispatch.NextAttemptAt) {
			if dispatch.LastErrorCode != "publication_unproven" {
				return nil
			}
			var latest nextcloudEventCheckpoint
			if err := tx.Table("nextcloud_event_checkpoint").Where("connection_id = ?", connectionID).
				Take(&latest).Error; err != nil {
				return err
			}
			if latest.ReceivedID <= dispatch.TargetEventID {
				return nil
			}
			status, startedAt, finishedAt, err := nextcloudDispatchSyncStatus(tx, dispatch.LastSyncLogID)
			if err != nil {
				return err
			}
			matching, err := nextcloudDispatchMatchingSync(tx, connection, dispatch.LastSyncLogID)
			if err != nil {
				return err
			}
			if status != "success" || finishedAt == nil || !matching {
				return nil
			}
			complete, err := nextcloudDispatchCursorComplete(tx, connection, startedAt)
			if err != nil || !complete {
				return err
			}
			coalescible, err := nextcloudCanCoalesceUnproven(tx, connection,
				dispatch.TargetEventID, latest.ReceivedID)
			if err != nil || !coalescible {
				return err
			}
		}
		if dispatch.State == "leased" && dispatch.LeaseUntil != nil && !now.Before(*dispatch.LeaseUntil) {
			// The log ID is persisted before the log and queue task are created.
			// Recover an uncertain enqueue by watching that exact event-owned log.
			status, startedAt, _, err := nextcloudDispatchSyncStatus(tx, dispatch.LastSyncLogID)
			if err != nil {
				return err
			}
			if status == "running" && now.Sub(startedAt) < 150*time.Minute {
				return tx.Table("nextcloud_event_dispatch").Where("connection_id = ?", connectionID).
					Updates(map[string]any{"next_attempt_at": now.Add(time.Minute), "updated_at": now}).Error
			}
			if status == "success" {
				if err := tx.Table("nextcloud_event_inbox").
					Where(("connection_id = ? AND event_id <= ? AND state = 'pendin"+
						"g'"), connectionID, dispatch.TargetEventID).
					Update("state", "dispatched").Error; err != nil {
					return err
				}
				return tx.Table("nextcloud_event_dispatch").Where("connection_id = ?", connectionID).
					Updates(map[string]any{
						"dispatched_id": dispatch.TargetEventID, "state": "queued",
						"lease_token": nil, "lease_until": nil, "next_attempt_at": now,
						"last_error_code": "", "updated_at": now,
					}).Error
			}
			if status == "running" {
				// Lite tasks have no enforced timeout. An old task might still
				// be applying items, so automatic replacement could run in parallel.
				return setNextcloudDispatchState(tx, connectionID, "blocked",
					"sync_stale_manual_review", now, dispatch.AttemptCount)
			}
			// Missing/terminal logs can be retried. A fresh attempt gets a fresh
			// task ID and fences off the old task via last_sync_log_id.
			dispatch.AttemptCount++
		}
		var checkpoint nextcloudEventCheckpoint
		if err := tx.Table("nextcloud_event_checkpoint").Where("connection_id = ?", connectionID).
			Take(&checkpoint).Error; err != nil {
			return err
		}
		if checkpoint.ReceivedID <= dispatch.DispatchedID && dispatch.State == "idle" {
			return nil
		}
		var count int64
		if err := tx.Table("nextcloud_event_inbox").Where("connection_id = ? AND event_id = ?",
			connectionID, checkpoint.ReceivedID).Count(&count).Error; err != nil {
			return err
		}
		if count == 0 {
			return setNextcloudDispatchState(tx, connectionID, "blocked", "inbox_missing", now, dispatch.AttemptCount)
		}
		token := uuid.NewString()
		leaseUntil := now.Add(3 * time.Minute)
		if err := tx.Table("nextcloud_event_dispatch").Where("connection_id = ?", connectionID).
			Updates(map[string]any{
				"state": "leased", "lease_token": token, "lease_until": leaseUntil,
				"target_event_id": checkpoint.ReceivedID, "attempt_count": dispatch.AttemptCount,
				"last_sync_log_id": nil, "next_attempt_at": leaseUntil, "updated_at": now,
			}).Error; err != nil {
			return err
		}
		claim = &NextcloudEventDispatchClaim{
			ConnectionID: connectionID, DatasourceID: connection.DatasourceID,
			TenantID: connection.TenantID, TargetEventID: checkpoint.ReceivedID,
			LeaseToken: token, ConfigSHA256: connection.DatasourceConfigSHA256,
		}
		return nil
	})
	return claim, err
}

// A missing cursor is valid for a first import only. Older installations may
// have imported knowledge without a source-version row, so inspect both.
func nextcloudDispatchBaselineValid(tx *gorm.DB, connection nextcloudEventConnection) (bool, error) {
	var source struct {
		LastSyncCursor types.JSON `gorm:"column:last_sync_cursor"`
	}
	if err := tx.Table("data_sources").Select("last_sync_cursor").
		Where("id = ?", connection.DatasourceID).Take(&source).Error; err != nil {
		return false, err
	}
	var cursor types.SyncCursor
	if len(source.LastSyncCursor) > 0 && string(source.LastSyncCursor) != "null" {
		if err := json.Unmarshal(source.LastSyncCursor, &cursor); err != nil {
			return false, nil
		}
		if cursor.ConnectorCursor != nil {
			return nextcloudDispatchInventoryValid(tx, cursor.ConnectorCursor, connection)
		}
	}
	return nextcloudSourceHasNoHistory(tx, connection.TenantID, connection.KnowledgeBaseID, connection.DatasourceID)
}

func nextcloudSourceHasNoHistory(tx *gorm.DB, tenantID uint64, kbID, dsID string) (bool, error) {
	var existing int64
	if err := tx.Table("nextcloud_source_versions").
		Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ?",
			tenantID, kbID, dsID).
		Count(&existing).Error; err != nil {
		return false, err
	}
	if existing != 0 {
		return false, nil
	}
	// Include soft-deleted knowledge. Old imports predate source_versions and
	// an empty inventory could otherwise silently lose their deletion history.
	if err := tx.Table("knowledges").
		Where("tenant_id = ? AND knowledge_base_id = ? AND channel = ?",
			tenantID, kbID, types.ConnectorTypeNextcloud).
		Count(&existing).Error; err != nil {
		return false, err
	}
	return existing == 0, nil
}

// Existing source rows must be represented in the cursor inventory. A cursor
// with the correct instance and an empty files map would otherwise let a
// deletion disappear without ever producing a tombstone.
func nextcloudDispatchInventoryValid(tx *gorm.DB, raw map[string]interface{}, connection nextcloudEventConnection) (
	bool,
	error,
) {
	return nextcloudSourceInventoryValid(tx, raw, connection.TenantID, connection.KnowledgeBaseID,
		connection.DatasourceID, connection.NextcloudInstanceID, []string{connection.BindingID})
}

func nextcloudSourceInventoryValid(tx *gorm.DB, raw map[string]interface{}, tenantID uint64,
	kbID, dsID, expectedInstanceID string, bindings []string,
) (bool, error) {
	encoded, err := json.Marshal(raw)
	if err != nil {
		return false, err
	}
	var state struct {
		InstanceID string                                `json:"instance_id"`
		Files      map[string]map[string]json.RawMessage `json:"files"`
		Tombstones map[string]map[string]json.RawMessage `json:"tombstones"`
	}
	if err := json.Unmarshal(encoded, &state); err != nil {
		return false, nil
	}
	if state.InstanceID ==
		"" ||
		(expectedInstanceID !=
			"" &&
			state.InstanceID !=
				expectedInstanceID) ||
		len(bindings) ==
			0 {
		return false, nil
	}
	for _, bindingID := range bindings {
		if bindingID == "" || state.Files[bindingID] == nil {
			return false, nil
		}
	}
	var externalIDs []string
	if err := tx.Table("nextcloud_source_versions").
		Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND state IN ?",
			tenantID, kbID, dsID,
			[]string{"staging", "published"}).Pluck("external_id", &externalIDs).Error; err != nil {
		return false, err
	}
	var oldRows []struct {
		Metadata types.JSON `gorm:"column:metadata"`
	}
	if err := tx.Table("knowledges").Select("metadata").
		Where("tenant_id = ? AND knowledge_base_id = ? AND channel = ?",
			tenantID, kbID, types.ConnectorTypeNextcloud).
		Where("metadata->>'datasource_id' = ? OR metadata->>'datasource_id' IS NULL", dsID).
		Find(&oldRows).Error; err != nil {
		return false, err
	}
	for _, row := range oldRows {
		var metadata struct {
			DatasourceID string `json:"datasource_id"`
			ExternalID   string `json:"external_id"`
		}
		if json.Unmarshal(row.Metadata, &metadata) != nil || metadata.DatasourceID == "" || metadata.ExternalID == "" {
			return false, nil
		}
		externalIDs = append(externalIDs, metadata.ExternalID)
	}
	prefix := "nextcloud:" + state.InstanceID + ":"
	for _, externalID := range externalIDs {
		if !strings.HasPrefix(externalID, prefix) {
			return false, nil
		}
		fileID := strings.TrimPrefix(externalID, prefix)
		parsed, err := strconv.ParseInt(fileID, 10, 64)
		if err != nil || parsed < 1 || strconv.FormatInt(parsed, 10) != fileID {
			return false, nil
		}
		covered := false
		for _, bindingID := range bindings {
			if len(state.Files[bindingID][fileID]) > 0 || len(state.Tombstones[bindingID][fileID]) > 0 {
				covered = true
				break
			}
		}
		if !covered {
			return false, nil
		}
	}
	return true, nil
}

// ValidateNextcloudSourceBaseline protects manual and scheduled syncs as well
// as event work. A lost or truncated cursor cannot be treated as first import
// when any older source knowledge or version still exists.
func (
	r *NextcloudEventInboxRepository,
) ValidateNextcloudSourceBaseline(ctx context.Context, source *types.DataSource) error {
	if source == nil || source.Type != types.ConnectorTypeNextcloud {
		return ErrNextcloudEventScope
	}
	cursor, err := source.ParseSyncCursor()
	if err != nil {
		return ErrNextcloudSourceCursor
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if cursor == nil || cursor.ConnectorCursor == nil {
			noHistory, err := nextcloudSourceHasNoHistory(tx, source.TenantID, source.KnowledgeBaseID, source.ID)
			if err != nil {
				return err
			}
			if !noHistory {
				return ErrNextcloudSourceCursor
			}
			return nil
		}
		config, err := source.ParseConfig()
		if err != nil || config == nil {
			return ErrNextcloudSourceCursor
		}
		valid, err := nextcloudSourceInventoryValid(tx, cursor.ConnectorCursor,
			source.TenantID, source.KnowledgeBaseID, source.ID, "", config.ResourceIDs)
		if err != nil {
			return err
		}
		if !valid {
			return ErrNextcloudSourceCursor
		}
		return nil
	})
}

// ErrNextcloudSourceCursor reports an invalid source event cursor.
var ErrNextcloudSourceCursor = apperrors.NewProtocolError(
	errors.New("nextcloud source cursor requires manual review"), "Nextcloud source cursor requires manual review",
)

// The connector deliberately requires two complete manifests before emitting
// a deletion. A successful first scan can therefore leave a non-empty Missing
// set. The cursor must also belong to this sync run: a failed cursor write must
// never let an older, clean snapshot acknowledge a newer event.
func nextcloudDispatchCursorComplete(tx *gorm.DB, connection nextcloudEventConnection, syncStartedAt time.Time) (
	bool,
	error,
) {
	var source types.DataSource
	if err := tx.Select("last_sync_cursor").Where("id = ?", connection.DatasourceID).Take(&source).Error; err != nil {
		return false, err
	}
	cursor, err := source.ParseSyncCursor()
	if err != nil || cursor == nil || cursor.ConnectorCursor == nil || cursor.LastSyncTime.Before(syncStartedAt) {
		return false, err
	}
	valid, err := nextcloudDispatchInventoryValid(tx, cursor.ConnectorCursor, connection)
	if err != nil || !valid {
		return false, err
	}
	raw, err := json.Marshal(cursor.ConnectorCursor)
	if err != nil {
		return false, err
	}
	var state struct {
		LastReconcileAt int64                      `json:"last_reconcile_at"`
		Missing         map[string]map[string]bool `json:"missing"`
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return false, err
	}
	if state.LastReconcileAt == 0 || time.Unix(state.LastReconcileAt, 0).Before(syncStartedAt.Add(-time.Second)) {
		return false, nil
	}
	for _, files := range state.Missing {
		for _, absent := range files {
			if absent {
				return false, nil
			}
		}
	}
	return true, nil
}

func nextcloudDispatchSyncStatus(tx *gorm.DB, syncLogID *string) (string, time.Time, *time.Time, error) {
	if syncLogID == nil || *syncLogID == "" {
		return "missing", time.Time{}, nil, nil
	}
	var row struct {
		Status     string     `gorm:"column:status"`
		StartedAt  time.Time  `gorm:"column:started_at"`
		FinishedAt *time.Time `gorm:"column:finished_at"`
	}
	if err := tx.Table(
		"sync_logs",
	).Select("status", "started_at", "finished_at").Where("id = ?", *syncLogID).Take(&row).Error; err !=
		nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "missing", time.Time{}, nil, nil
		}
		return "", time.Time{}, nil, err
	}
	return row.Status, row.StartedAt, row.FinishedAt, nil
}

func nextcloudDispatchMatchingSync(tx *gorm.DB, connection nextcloudEventConnection,
	syncLogID *string,
) (bool, error) {
	if syncLogID == nil || *syncLogID == "" {
		return false, nil
	}
	var count int64
	err := tx.Table("sync_logs").Where("id = ? AND data_source_id = ? AND tenant_id = ?",
		*syncLogID, connection.DatasourceID, connection.TenantID).Count(&count).Error
	return count == 1, err
}

// Only a small, signed, file-scoped receipt range can replace an old
// publication-unproven scan. The replacement still needs its own complete
// manifest and publication proof before any applied watermark advances.
func nextcloudCanCoalesceUnproven(tx *gorm.DB, connection nextcloudEventConnection,
	oldTargetID, newReceivedID int64,
) (bool, error) {
	if newReceivedID <= oldTargetID {
		return false, nil
	}
	var hints []nextcloudEventInboxRow
	if err := tx.Table("nextcloud_event_inbox").
		Where("connection_id = ? AND event_id > ? AND event_id <= ?",
			connection.ConnectionID, oldTargetID, newReceivedID).
		Order("event_id ASC").Limit(201).Find(&hints).Error; err != nil {
		return false, err
	}
	if len(hints) == 0 || len(hints) > 200 || hints[len(hints)-1].EventID != newReceivedID {
		return false, nil
	}
	var source types.DataSource
	if err := tx.Select("last_sync_cursor").Where("id = ?", connection.DatasourceID).
		Take(&source).Error; err != nil {
		return false, err
	}
	cursor, err := source.ParseSyncCursor()
	if err != nil || cursor == nil || cursor.ConnectorCursor == nil {
		return false, err
	}
	raw, err := json.Marshal(cursor.ConnectorCursor)
	if err != nil {
		return false, err
	}
	var snapshot nextcloudApplyCursor
	if json.Unmarshal(raw, &snapshot) != nil ||
		snapshot.InstanceID != connection.NextcloudInstanceID ||
		snapshot.Files[connection.BindingID] == nil {
		return false, nil
	}
	observedID, valid := nextcloudChangeCursorID(snapshot.Changes[connection.BindingID], connection.BindingID)
	if !valid || observedID < oldTargetID {
		return false, nil
	}
	var kb types.KnowledgeBase
	if err := tx.Where("id = ? AND tenant_id = ?", connection.KnowledgeBaseID,
		connection.TenantID).Take(&kb).Error; err != nil {
		return false, err
	}
	for _, hint := range hints {
		if hint.FileID == nil || *hint.FileID <= 0 {
			return false, nil
		}
		fileID := strconv.FormatInt(*hint.FileID, 10)
		switch hint.EventType {
		case "upsert":
			if hint.ETag == nil || strings.TrimSpace(*hint.ETag) == "" ||
				hint.RelativePath ==
					nil ||
				!nextcloud.ImportableFileName(path.Base(*hint.RelativePath), kb.IsMultimodalEnabled()) {
				return false, nil
			}
		case "delete":
			if _, present := snapshot.Files[connection.BindingID][fileID]; !present {
				return false, nil
			}
		default:
			return false, nil
		}
	}
	return true, nil
}

// A pending parser may be overtaken only by signed, importable upserts of the
// same file, with a changed ETag. The currently staged parser must be the only
// unfinished generation for that file. A second unfinished generation waits:
// repeatedly replacing a stalled parser would otherwise grow without bound.
// This is an admission check, never publication or applied-ACK evidence.
func nextcloudCanSupersedePendingParser(tx *gorm.DB, connection nextcloudEventConnection,
	oldTargetID, newReceivedID int64,
) (bool, error) {
	coalescible, err := nextcloudCanCoalesceUnproven(tx, connection, oldTargetID, newReceivedID)
	if err != nil || !coalescible {
		return false, err
	}
	var hints []nextcloudEventInboxRow
	if err := tx.Table("nextcloud_event_inbox").
		Where("connection_id = ? AND event_id > ? AND event_id <= ?",
			connection.ConnectionID, oldTargetID, newReceivedID).
		Order("event_id ASC").Limit(201).Find(&hints).Error; err != nil {
		return false, err
	}
	if len(hints) == 0 || len(hints) > 200 || hints[len(hints)-1].EventID != newReceivedID ||
		hints[0].FileID == nil || *hints[0].FileID <= 0 {
		return false, nil
	}
	fileID := *hints[0].FileID
	for _, hint := range hints {
		if hint.EventType != "upsert" || hint.FileID == nil || *hint.FileID != fileID ||
			hint.ETag == nil || strings.TrimSpace(*hint.ETag) == "" ||
			hint.RelativePath == nil || strings.TrimSpace(*hint.RelativePath) == "" {
			return false, nil
		}
	}
	externalID := "nextcloud:" + connection.NextcloudInstanceID + ":" + strconv.FormatInt(fileID, 10)
	var waiting []struct {
		ExternalID           string `gorm:"column:external_id"`
		DesiredETag          string `gorm:"column:desired_etag"`
		CandidateKnowledgeID string `gorm:"column:candidate_knowledge_id"`
	}
	if err := tx.Table("nextcloud_source_versions AS v").
		Select("v.external_id, v.desired_etag, v.candidate_knowledge_id").
		Joins(`JOIN knowledges AS k ON k.id = v.candidate_knowledge_id
			AND k.tenant_id = v.tenant_id AND k.knowledge_base_id = v.knowledge_base_id`).
		Where("v.tenant_id = ? AND v.knowledge_base_id = ? AND v.datasource_id = ? AND v.state = 'staging'",
			connection.TenantID, connection.KnowledgeBaseID, connection.DatasourceID).
		Where("k.deleted_at IS NULL AND k.parse_status IN ?", []string{
			types.ParseStatusPending, types.ParseStatusProcessing, types.ParseStatusFinalizing,
		}).Limit(2).Find(&waiting).Error; err != nil {
		return false, err
	}
	if len(waiting) != 1 || waiting[0].ExternalID != externalID ||
		waiting[0].DesiredETag == *hints[len(hints)-1].ETag {
		return false, nil
	}
	// Count unfinished rows and live build leases together. A housekeeping
	// failure status does not free the budget while its old worker holds a
	// build lease. Retired generations remain in the local history.
	nowMS, err := nextcloudLeaseNowMS(tx)
	if err != nil {
		return false, err
	}
	var unfinished int64
	if err := tx.Table("knowledges AS k").Distinct("k.id").
		Joins(`LEFT JOIN nextcloud_content_leases AS l ON l.tenant_id = k.tenant_id
			AND l.knowledge_base_id = k.knowledge_base_id AND l.knowledge_id = k.id
			AND l.kind = 'build' AND l.released_at_ms IS NULL AND l.expires_at_ms > ?`, nowMS).
		Where("k.tenant_id = ? AND k.knowledge_base_id = ? AND k.channel = ?",
			connection.TenantID, connection.KnowledgeBaseID, types.ConnectorTypeNextcloud).
		Where("k.metadata->>'datasource_id' = ? AND k.metadata->>'external_id' = ?",
			connection.DatasourceID, externalID).
		Where("k.parse_status IN ? OR l.lease_id IS NOT NULL", []string{
			types.ParseStatusPending, types.ParseStatusProcessing, types.ParseStatusFinalizing,
		}).Count(&unfinished).Error; err != nil {
		return false, err
	}
	return unfinished == 1, nil
}

func nextcloudBroadConfirmationCode(targetEventID int64) string {
	return "broad_confirm:" + strconv.FormatInt(targetEventID, 10)
}

func nextcloudBroadMarker(code string) string {
	if strings.HasPrefix(code, "broad_confirm:") {
		return code
	}
	return ""
}

func nextcloudPublicationPendingCode(code string) string {
	if marker := nextcloudBroadMarker(code); marker != "" {
		return marker
	}
	return "publication_pending"
}

func nextcloudPublicationPollDelay(now time.Time, finishedAt *time.Time) time.Duration {
	if finishedAt != nil && now.Before(finishedAt.Add(nextcloudPublicationFastPollWindow)) {
		return nextcloudQueuedPollInterval
	}
	return time.Minute
}

func nextcloudDispatchBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 8 {
		attempt = 8
	}
	return time.Duration(1<<uint(attempt-1)) * time.Minute
}

func setNextcloudDispatchState(tx *gorm.DB, connectionID, state, errorCode string, next time.Time, attempts int) error {
	return tx.Table("nextcloud_event_dispatch").Where("connection_id = ?", connectionID).
		Updates(map[string]any{
			"state": state, "lease_token": nil, "lease_until": nil,
			"next_attempt_at": next, "attempt_count": attempts, "last_error_code": errorCode,
			"updated_at": time.Now().UTC(),
		}).Error
}

// PrepareDispatch makes the attempt's sync log ID durable before creating the
// log or submitting the task. A crash after this point can be reconciled from
// the leased row without guessing which running log belongs to the event.
func (
	r *NextcloudEventInboxRepository,
) PrepareDispatch(ctx context.Context, claim NextcloudEventDispatchClaim, syncLogID string, now time.Time) error {
	if claim.LeaseToken == "" || syncLogID == "" {
		return ErrNextcloudEventConflict
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Name() == "sqlite" {
			lock := tx.Exec(`UPDATE nextcloud_event_connections SET status = status
				WHERE connection_id = ? AND status = 'active'`, claim.ConnectionID)
			if lock.Error != nil {
				return lock.Error
			}
			if lock.RowsAffected != 1 {
				return ErrNextcloudEventScope
			}
		}
		var connection nextcloudEventConnection
		if err := tx.Table("nextcloud_event_connections").Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("connection_id = ? AND status = 'active'", claim.ConnectionID).Take(&connection).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNextcloudEventScope
			}
			return err
		}
		if connection.DatasourceID != claim.DatasourceID || connection.TenantID != claim.TenantID ||
			connection.DatasourceConfigSHA256 != claim.ConfigSHA256 {
			return ErrNextcloudEventScope
		}
		if err := checkNextcloudEventDataSource(tx, connection); err != nil {
			return err
		}
		result := tx.Table("nextcloud_event_dispatch").
			Where(("connection_id = ? AND state = 'leased' AND lease_token " +
				"= ? AND target_event_id = ? AND last_sync_log_id IS NUL" +
				"L"),
				claim.ConnectionID, claim.LeaseToken, claim.TargetEventID).
			Updates(map[string]any{"last_sync_log_id": syncLogID, "updated_at": now})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrNextcloudEventConflict
		}
		return nil
	})
}

// FinishDispatch records only that a task was accepted by the queue. It does
// not advance applied_id; successful parsing/publication requires later proof.
func (
	r *NextcloudEventInboxRepository,
) FinishDispatch(ctx context.Context, claim NextcloudEventDispatchClaim, syncLogID string, now time.Time) error {
	if syncLogID == "" || claim.LeaseToken == "" {
		return ErrNextcloudEventConflict
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Name() == "sqlite" {
			lock := tx.Exec(("UPDATE nextcloud_event_connections SET status = status " +
				"WHERE connection_id = ? AND status = 'active'"), claim.ConnectionID)
			if lock.Error != nil {
				return lock.Error
			}
			if lock.RowsAffected != 1 {
				return ErrNextcloudEventScope
			}
		}
		var connection nextcloudEventConnection
		if err := tx.Table("nextcloud_event_connections").Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("connection_id = ? AND status = 'active'", claim.ConnectionID).Take(&connection).Error; err != nil {
			return ErrNextcloudEventScope
		}
		if connection.DatasourceID != claim.DatasourceID || connection.TenantID != claim.TenantID ||
			connection.DatasourceConfigSHA256 != claim.ConfigSHA256 {
			return ErrNextcloudEventScope
		}
		if err := checkNextcloudEventDataSource(tx, connection); err != nil {
			return err
		}
		var dispatch nextcloudEventDispatchRow
		if err := tx.Table("nextcloud_event_dispatch").Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("connection_id = ?", claim.ConnectionID).Take(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.State != "leased" || dispatch.LeaseToken == nil || *dispatch.LeaseToken != claim.LeaseToken ||
			dispatch.TargetEventID != claim.TargetEventID || dispatch.LastSyncLogID == nil ||
			*dispatch.LastSyncLogID != syncLogID {
			return ErrNextcloudEventConflict
		}
		if err := tx.Table("nextcloud_event_inbox").Where("connection_id = ? AND event_id <= ? AND state = 'pending'",
			claim.ConnectionID, claim.TargetEventID).Update("state", "dispatched").Error; err != nil {
			return err
		}
		return tx.Table("nextcloud_event_dispatch").Where("connection_id = ?", claim.ConnectionID).
			Updates(map[string]any{
				"dispatched_id": claim.TargetEventID, "state": "queued",
				"lease_token": nil, "lease_until": nil, "last_sync_log_id": syncLogID,
				"next_attempt_at": now.Add(nextcloudQueuedPollInterval),
				"last_error_code": nextcloudBroadMarker(dispatch.LastErrorCode), "updated_at": now,
			}).Error
	})
}

// FailDispatch leaves the range retryable. Error codes are static and contain
// no URLs, credentials, filenames, or upstream response bodies.
func (
	r *NextcloudEventInboxRepository,
) FailDispatch(ctx context.Context, claim NextcloudEventDispatchClaim, errorCode string, now time.Time) error {
	if claim.LeaseToken == "" {
		return ErrNextcloudEventConflict
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var dispatch nextcloudEventDispatchRow
		if err := tx.Table("nextcloud_event_dispatch").Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("connection_id = ?", claim.ConnectionID).Take(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.State != "leased" || dispatch.LeaseToken == nil || *dispatch.LeaseToken != claim.LeaseToken {
			return ErrNextcloudEventConflict
		}
		if errorCode != "queue_unavailable" && errorCode != "source_paused" && errorCode != "source_unavailable" {
			errorCode = "dispatch_failed"
		}
		return setNextcloudDispatchState(tx, claim.ConnectionID, "retry", errorCode,
			now.Add(nextcloudDispatchBackoff(dispatch.AttemptCount+1)), dispatch.AttemptCount+1)
	})
}

// UncertainDispatch preserves the lease and intended log ID when enqueue
// returns an error: a queue may have accepted the task but lost its response.
// Lease recovery observes the exact log before allowing another attempt.
func (
	r *NextcloudEventInboxRepository,
) UncertainDispatch(ctx context.Context, claim NextcloudEventDispatchClaim, now time.Time) error {
	if claim.LeaseToken == "" {
		return ErrNextcloudEventConflict
	}
	result := r.db.WithContext(ctx).Table("nextcloud_event_dispatch").
		Where("connection_id = ? AND state = 'leased' AND lease_token = ? AND last_sync_log_id IS NOT NULL",
			claim.ConnectionID, claim.LeaseToken).
		Updates(map[string]any{"last_error_code": "queue_uncertain", "updated_at": now})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrNextcloudEventConflict
	}
	return nil
}

// ValidateEventSyncTask prevents queued event work from starting after a
// connection was revoked or its pinned source changed.
func (
	r *NextcloudEventInboxRepository,
) ValidateEventSyncTask(
	ctx context.Context,
	connectionID, dsID string,
	tenantID uint64,
	configSHA, syncLogID string,
	observedInstanceID ...string,
) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var connection nextcloudEventConnection
		if err := tx.Table(
			"nextcloud_event_connections",
		).Where("connection_id = ? AND status = 'active'", connectionID).
			Take(&connection).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNextcloudEventScope
			}
			return err
		}
		if connection.DatasourceID != dsID || connection.TenantID != tenantID ||
			connection.DatasourceConfigSHA256 != configSHA {
			return ErrNextcloudEventScope
		}
		if len(observedInstanceID) > 0 && observedInstanceID[0] != connection.NextcloudInstanceID {
			return ErrNextcloudEventScope
		}
		var dispatch nextcloudEventDispatchRow
		if err := tx.Table("nextcloud_event_dispatch").Where("connection_id = ?", connectionID).
			Take(&dispatch).Error; err != nil {
			return err
		}
		if (dispatch.State != "leased" && dispatch.State != "queued") ||
			dispatch.LastSyncLogID == nil || *dispatch.LastSyncLogID != syncLogID || syncLogID == "" {
			return ErrNextcloudEventScope
		}
		return checkNextcloudEventDataSource(tx, connection)
	})
}
