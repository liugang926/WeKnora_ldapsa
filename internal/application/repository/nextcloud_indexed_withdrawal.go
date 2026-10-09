package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// NextcloudIndexedWithdrawal records a durable logical withdrawal. It never
// claims that every historical copy was found and is never a deletion ACK.
// Credentials and the paired source remain in place for recovery and GC.
type NextcloudIndexedWithdrawal struct {
	OperationID      string    `json:"operation_id" gorm:"column:operation_id;primaryKey"`
	PairOperationID  string    `json:"pair_operation_id" gorm:"column:pair_operation_id"`
	TenantID         uint64    `json:"-" gorm:"column:tenant_id"`
	KnowledgeBaseID  string    `json:"knowledge_base_id" gorm:"column:knowledge_base_id"`
	DataSourceID     string    `json:"data_source_id" gorm:"column:datasource_id"`
	InstanceID       string    `json:"instance_id" gorm:"column:nextcloud_instance_id"`
	BindingID        string    `json:"binding_id" gorm:"column:binding_id"`
	PublicationEpoch int64     `json:"publication_epoch" gorm:"column:publication_epoch"`
	KeyID            string    `json:"key_id" gorm:"column:key_id"`
	State            string    `json:"state" gorm:"column:state"`
	CreatedAt        time.Time `json:"created_at" gorm:"column:created_at"`
	UpdatedAt        time.Time `json:"updated_at" gorm:"column:updated_at"`
}

// TableName returns the indexed-withdrawal table name.
func (NextcloudIndexedWithdrawal) TableName() string { return "nextcloud_indexed_withdrawals" }

type nextcloudIndexedWithdrawalItem struct {
	OperationID string    `gorm:"column:operation_id;primaryKey"`
	Kind        string    `gorm:"column:kind;primaryKey"`
	ObjectRef   string    `gorm:"column:object_ref;primaryKey"`
	KnowledgeID string    `gorm:"column:knowledge_id"`
	ObservedAt  time.Time `gorm:"column:observed_at"`
}

func (nextcloudIndexedWithdrawalItem) TableName() string {
	return "nextcloud_indexed_withdrawal_items"
}

// NextcloudIndexedWithdrawalStatus contains the indexed-withdrawal status projection.
type NextcloudIndexedWithdrawalStatus struct {
	NextcloudIndexedWithdrawal
	ObservedItems     int64 `json:"observed_items"`
	InventoryComplete bool  `json:"inventory_complete"`
	LogicalWithdrawn  bool  `json:"logical_withdrawn"`
}

func sameIndexedWithdrawal(a, b NextcloudIndexedWithdrawal) bool {
	return a.OperationID == b.OperationID && a.PairOperationID == b.PairOperationID &&
		a.TenantID == b.TenantID && a.KnowledgeBaseID == b.KnowledgeBaseID &&
		a.DataSourceID == b.DataSourceID && a.InstanceID == b.InstanceID &&
		a.BindingID == b.BindingID && a.PublicationEpoch == b.PublicationEpoch && a.KeyID == b.KeyID
}

// The receiver and dispatcher lock the connection before the KB. Withdrawal
// owns the KB first, so NOWAIT turns a concurrent receipt into a retryable
// conflict instead of waiting in the opposite lock order. The revocation and
// dispatch block commit with the source pause and withdrawal tombstone.
func revokeIndexedEventConnection(tx *gorm.DB, pair NextcloudSourcePairing, now time.Time) error {
	var connection nextcloudEventConnection
	err := tx.Table("nextcloud_event_connections").
		Clauses(clause.Locking{Strength: "UPDATE", Options: "NOWAIT"}).
		Where("datasource_id = ? AND status = 'active'", pair.DataSourceID).
		Take(&connection).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return ErrNextcloudSourcePairingConflict
	}
	if connection.TenantID != pair.TenantID ||
		connection.KnowledgeBaseID != pair.KnowledgeBaseID ||
		connection.DatasourceID != pair.DataSourceID ||
		connection.NextcloudInstanceID != pair.InstanceID ||
		connection.BindingID != pair.BindingID ||
		connection.DatasourceBaseURL != pair.BaseURL ||
		connection.DatasourceConfigSHA256 != pair.ConfigSHA {
		return ErrNextcloudSourcePairingConflict
	}
	result := tx.Table("nextcloud_event_connections").
		Where("connection_id = ? AND tenant_id = ? AND datasource_id = ? AND status = 'active'",
			connection.ConnectionID, pair.TenantID, pair.DataSourceID).
		Updates(map[string]any{
			"status": "revoked", "previous_key_id": nil,
			"previous_secret_ciphertext": nil, "previous_valid_until": nil,
			"updated_at": now,
		})
	if result.Error != nil || result.RowsAffected != 1 {
		return ErrNextcloudSourcePairingConflict
	}
	result = tx.Table("nextcloud_event_dispatch").Where("connection_id = ?", connection.ConnectionID).
		Updates(map[string]any{
			"state": "blocked", "lease_token": nil,
			"lease_until": nil, "last_error_code": "source_withdrawn", "updated_at": now,
		})
	if result.Error != nil || result.RowsAffected != 1 {
		return ErrNextcloudSourcePairingConflict
	}
	return nil
}

// BeginIndexedSourceWithdrawal is allowed only after a separately verified,
// stopped Nextcloud intent. The repository fences the source and SQL writers
// in one transaction. It deliberately does not erase credentials, mark GC
// complete, or acknowledge the Nextcloud decommission operation.
func (r *NextcloudSourcePairingRepository) BeginIndexedSourceWithdrawal(
	ctx context.Context, pair NextcloudSourcePairing, proposed NextcloudIndexedWithdrawal,
) (NextcloudIndexedWithdrawal, error) {
	if r.db.Name() != "postgres" || proposed.OperationID == "" ||
		proposed.PairOperationID != pair.OperationID || proposed.TenantID != pair.TenantID ||
		proposed.KnowledgeBaseID != pair.KnowledgeBaseID || proposed.DataSourceID != pair.DataSourceID ||
		proposed.InstanceID != pair.InstanceID || proposed.BindingID != pair.BindingID ||
		proposed.KeyID != pair.KeyID || proposed.PublicationEpoch < 0 {
		return NextcloudIndexedWithdrawal{}, ErrNextcloudSourcePairingConflict
	}
	var result NextcloudIndexedWithdrawal
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		lock := tx.Exec(`UPDATE knowledge_bases SET ever_had_nextcloud_source = ever_had_nextcloud_source
			WHERE id = ? AND tenant_id = ? AND deleted_at IS NULL`, pair.KnowledgeBaseID, pair.TenantID)
		if lock.Error != nil || lock.RowsAffected != 1 {
			return ErrNextcloudSourcePairingConflict
		}
		var current NextcloudSourcePairing
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("operation_id = ? AND tenant_id = ? AND state = 'active'", pair.OperationID, pair.TenantID).
			Take(&current).Error; err != nil || current.DataSourceID != pair.DataSourceID ||
			current.ConfigSHA != pair.ConfigSHA || current.InstanceID != pair.InstanceID ||
			current.BindingID != pair.BindingID || current.KeyID != pair.KeyID ||
			current.BaseURL != pair.BaseURL {
			return ErrNextcloudSourcePairingConflict
		}
		var prior NextcloudIndexedWithdrawal
		err := tx.Where("pair_operation_id = ?", pair.OperationID).Take(&prior).Error
		if err == nil {
			if !sameIndexedWithdrawal(prior, proposed) {
				return ErrNextcloudSourcePairingConflict
			}
			if err := revokeIndexedEventConnection(tx, pair, time.Now().UTC()); err != nil {
				return err
			}
			result = prior
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		var liveRotation int64
		if err := tx.Table("nextcloud_source_rotations").
			Where("pair_operation_id = ? AND state IN ?", pair.OperationID,
				[]string{"pending", "committed", "switched"}).Count(&liveRotation).Error; err !=
			nil ||
			liveRotation !=
				0 {
			return ErrNextcloudSourcePairingConflict
		}
		var emptyDecommission int64
		if err := tx.Model(&NextcloudSourceDecommission{}).
			Where("pair_operation_id = ?", pair.OperationID).Count(&emptyDecommission).Error; err !=
			nil ||
			emptyDecommission !=
				0 {
			return ErrNextcloudSourcePairingConflict
		}
		var source types.DataSource
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND deleted_at IS NULL",
				pair.DataSourceID, pair.TenantID, pair.KnowledgeBaseID).Take(&source).Error; err != nil {
			return ErrNextcloudSourcePairingConflict
		}
		base, digest, binding, err := NextcloudEventDataSourceIdentity(source.Config)
		if err != nil || source.Type != types.ConnectorTypeNextcloud ||
			base != pair.BaseURL || digest != pair.ConfigSHA || binding != pair.BindingID ||
			(source.Status != types.DataSourceStatusActive && source.Status != types.DataSourceStatusPaused) {
			return ErrNextcloudSourcePairingConflict
		}
		config, err := source.ParseConfig()
		if err != nil || config == nil || config.Credentials["key_id"] != pair.KeyID {
			return ErrNextcloudSourcePairingConflict
		}
		// A source-level inventory can use the KB as its scope only when that
		// KB still belongs to this one paired source. Unknown co-tenants make
		// even the observed local copy attribution ambiguous.
		var otherSources, unattributed int64
		if err := tx.Table("data_sources").Where(
			"tenant_id = ? AND knowledge_base_id = ? AND id <> ?",
			pair.TenantID, pair.KnowledgeBaseID, pair.DataSourceID).Count(&otherSources).Error; err != nil {
			return err
		}
		if err := tx.Table("knowledges").Where("tenant_id = ? AND knowledge_base_id = ?",
			pair.TenantID, pair.KnowledgeBaseID).
			Where(`channel IS DISTINCT FROM 'nextcloud' AND NOT (
				COALESCE(jsonb_exists(metadata, 'nextcloud_instance_id'), FALSE)
				OR COALESCE(jsonb_exists(metadata, 'nextcloud_binding_id'), FALSE)
				OR COALESCE(jsonb_exists(metadata, 'nextcloud_file_id'), FALSE))`).
			Count(&unattributed).Error; err != nil {
			return err
		}
		if otherSources != 0 || unattributed != 0 {
			return ErrNextcloudSourcePairingConflict
		}
		var runningSyncs int64
		if err := tx.Table("sync_logs").Where("data_source_id = ? AND status = ?",
			pair.DataSourceID, types.SyncLogStatusRunning).Count(&runningSyncs).Error; err != nil || runningSyncs != 0 {
			return ErrNextcloudSourcePairingConflict
		}
		// A provider delete already claimed outside this transaction could
		// finish after withdrawal. Wait for that lease to resolve first.
		var deleting int64
		if err := tx.Table("nextcloud_gc_items AS i").Joins(
			"JOIN nextcloud_gc_jobs AS j ON j.id = i.job_id").
			Where("j.tenant_id = ? AND j.knowledge_base_id = ? AND j.datasource_id = ? AND i.state = 'deleting'",
				pair.TenantID, pair.KnowledgeBaseID, pair.DataSourceID).Count(&deleting).Error; err !=
			nil ||
			deleting !=
				0 {
			return ErrNextcloudSourcePairingConflict
		}
		if err := tx.Model(&types.DataSource{}).Where("id = ?", source.ID).
			Update("status", types.DataSourceStatusPaused).Error; err != nil {
			return err
		}
		if err := revokeIndexedEventConnection(tx, pair, time.Now().UTC()); err != nil {
			return err
		}
		// Hide already published rows in the same transaction as the durable
		// pause. The access guard also rechecks the source status on reads.
		if err := tx.Exec(`UPDATE knowledges
			SET enable_status = 'disabled',
			    metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb),
			        '{nextcloud_etag}', to_jsonb(''::text), true)
			WHERE tenant_id = ? AND knowledge_base_id = ?
			  AND (channel = 'nextcloud' OR jsonb_exists(metadata, 'nextcloud_instance_id')
			       OR jsonb_exists(metadata, 'nextcloud_binding_id')
			       OR jsonb_exists(metadata, 'nextcloud_file_id'))`,
			pair.TenantID, pair.KnowledgeBaseID).Error; err != nil {
			return err
		}
		proposed.State = "withdrawn_inventory_incomplete"
		proposed.CreatedAt = time.Now().UTC()
		proposed.UpdatedAt = proposed.CreatedAt
		if err := tx.Create(&proposed).Error; err != nil {
			return err
		}
		result = proposed
		return nil
	})
	if err != nil {
		return NextcloudIndexedWithdrawal{}, err
	}
	return result, nil
}

// RefreshIndexedWithdrawalInventory records every currently visible SQL copy
// under the dedicated source KB. It is append-only and retries after errors.
// It never sets InventoryComplete: historical deletes and external backends
// make a complete claim impossible with today's provenance model.
func (r *NextcloudSourcePairingRepository) RefreshIndexedWithdrawalInventory(
	ctx context.Context, row NextcloudIndexedWithdrawal,
) (NextcloudIndexedWithdrawalStatus, error) {
	status := NextcloudIndexedWithdrawalStatus{NextcloudIndexedWithdrawal: row, LogicalWithdrawn: true}
	if r.db.Name() != "postgres" {
		return status, ErrNextcloudSourcePairingConflict
	}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current NextcloudIndexedWithdrawal
		if err := tx.Where("operation_id = ? AND tenant_id = ?", row.OperationID, row.TenantID).
			Take(&current).Error; err != nil || !sameIndexedWithdrawal(current, row) ||
			current.State != "withdrawn_inventory_incomplete" {
			return ErrNextcloudSourcePairingConflict
		}
		var source types.DataSource
		if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND status = ?",
			row.DataSourceID, row.TenantID, row.KnowledgeBaseID, types.DataSourceStatusPaused).
			Take(&source).Error; err != nil {
			return ErrNextcloudSourcePairingConflict
		}
		// Each query uses persisted IDs. The GORM item table is append-only;
		// ON CONFLICT makes interruption/retry idempotent.
		queries := []struct {
			kind, sql string
			args      []any
		}{
			{"knowledge", ("SELECT id AS object_ref, id AS knowledge_id FROM knowle" +
				"dges WHERE tenant_id = ? AND knowledge_base_id = ?"), []any{row.TenantID, row.KnowledgeBaseID}},
			{"source_file", ("SELECT id || ':' || file_path AS object_ref, id AS know" +
				"ledge_id FROM knowledges WHERE tenant_id = ? AND knowle" +
				"dge_base_id = ? AND file_path <> ''"), []any{row.TenantID, row.KnowledgeBaseID}},
			{"chunk", ("SELECT id AS object_ref, knowledge_id FROM chunks WHERE" +
				" tenant_id = ? AND knowledge_base_id = ?"), []any{row.TenantID, row.KnowledgeBaseID}},
			{"gc_job", ("SELECT id AS object_ref, knowledge_id FROM nextcloud_gc" +
				"_jobs WHERE tenant_id = ? AND knowledge_base_id = ? AND" +
				" datasource_id = ?"), []any{row.TenantID, row.KnowledgeBaseID, row.DataSourceID}},
			{"gc_item", ("SELECT i.job_id || ':' || i.kind || ':' || i.object_ref" +
				" AS object_ref, j.knowledge_id FROM nextcloud_gc_items " +
				"i JOIN nextcloud_gc_jobs j ON j.id = i.job_id WHERE j.t" +
				"enant_id = ? AND j.knowledge_base_id = ? AND j.datasour" +
				"ce_id = ?"), []any{row.TenantID, row.KnowledgeBaseID, row.DataSourceID}},
			{"source_version", ("SELECT external_id AS object_ref, candidate_knowledge_i" +
				"d AS knowledge_id FROM nextcloud_source_versions WHERE " +
				"tenant_id = ? AND knowledge_base_id = ? AND datasource_" +
				"id = ?"), []any{row.TenantID, row.KnowledgeBaseID, row.DataSourceID}},
			// The mutable source-version row names only the latest candidate. Keep
			// every later observed revision, including one whose knowledge row
			// was already hard-deleted. This remains an incomplete copy inventory:
			// pre-ledger history and external backends are still unknown.
			{"source_revision", ("SELECT jsonb_build_array(external_id, revision)::text A" +
				"S object_ref, candidate_knowledge_id AS knowledge_id FR" +
				"OM nextcloud_source_revisions WHERE tenant_id = ? AND k" +
				"nowledge_base_id = ? AND datasource_id = ?"), []any{
				row.TenantID,
				row.KnowledgeBaseID,
				row.DataSourceID,
			}},
			{"sync_log", ("SELECT id AS object_ref, '' AS knowledge_id FROM sync_l" +
				"ogs WHERE data_source_id = ?"), []any{row.DataSourceID}},
		}
		for _, q := range queries {
			if err := tx.Exec(`INSERT INTO nextcloud_indexed_withdrawal_items
				(operation_id, kind, object_ref, knowledge_id)
				SELECT ?, ?, observed.object_ref, observed.knowledge_id
				FROM (`+q.sql+`) AS observed WHERE observed.object_ref <> ''
				ON CONFLICT (operation_id, kind, object_ref) DO NOTHING`,
				append([]any{row.OperationID, q.kind}, q.args...)...).Error; err != nil {
				return fmt.Errorf("inventory %s: %w", q.kind, err)
			}
		}
		blockers := []nextcloudIndexedWithdrawalItem{{
			OperationID: row.OperationID,
			Kind:        "unverified_external_index", ObjectRef: row.KnowledgeBaseID,
			ObservedAt: time.Now().UTC(),
		}}
		if tx.Migrator().HasTable("embeddings") {
			if err := tx.Exec(`INSERT INTO nextcloud_indexed_withdrawal_items
				(operation_id, kind, object_ref, knowledge_id)
				SELECT ?, 'postgres_embedding', CAST(id AS TEXT), COALESCE(knowledge_id, '')
				FROM embeddings WHERE knowledge_base_id = ?
				ON CONFLICT (operation_id, kind, object_ref) DO NOTHING`,
				row.OperationID, row.KnowledgeBaseID).Error; err != nil {
				return err
			}
		} else {
			blockers = append(blockers, nextcloudIndexedWithdrawalItem{
				OperationID: row.OperationID,
				Kind:        "missing_local_embedding_table", ObjectRef: row.KnowledgeBaseID,
				ObservedAt: time.Now().UTC(),
			})
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&blockers).Error; err != nil {
			return err
		}
		lastChunkID := ""
		for {
			var imageRows []struct {
				ID          string
				KnowledgeID string
				ImageInfo   string
			}
			if err := tx.Table("chunks").Select("id, knowledge_id, image_info").
				Where(("tenant_id = ? AND knowledge_base_id = ? AND id > ? AND " +
					"image_info IS NOT NULL AND image_info <> ''"),
					row.TenantID, row.KnowledgeBaseID, lastChunkID).
				Order("id ASC").Limit(500).Find(&imageRows).Error; err != nil {
				return err
			}
			if len(imageRows) == 0 {
				break
			}
			imageItems := make([]nextcloudIndexedWithdrawalItem, 0)
			observedAt := time.Now().UTC()
			for _, chunk := range imageRows {
				var images []types.ImageInfo
				if err := json.Unmarshal([]byte(chunk.ImageInfo), &images); err != nil {
					imageItems = append(imageItems, nextcloudIndexedWithdrawalItem{
						OperationID: row.OperationID, Kind: "invalid_image_inventory",
						ObjectRef: chunk.ID, KnowledgeID: chunk.KnowledgeID, ObservedAt: observedAt,
					})
					continue
				}
				for _, image := range images {
					if image.URL == "" {
						continue
					}
					imageItems = append(imageItems, nextcloudIndexedWithdrawalItem{
						OperationID: row.OperationID, Kind: "extracted_image",
						ObjectRef: chunk.ID + ":" + image.URL, KnowledgeID: chunk.KnowledgeID,
						ObservedAt: observedAt,
					})
				}
			}
			if len(imageItems) != 0 {
				if err := tx.Clauses(clause.OnConflict{DoNothing: true}).
					CreateInBatches(imageItems, 100).Error; err != nil {
					return err
				}
			}
			lastChunkID = imageRows[len(imageRows)-1].ID
		}
		return tx.Model(&nextcloudIndexedWithdrawalItem{}).
			Where("operation_id = ?", row.OperationID).Count(&status.ObservedItems).Error
	})
	return status, err
}

// IndexedWithdrawalStatus hides paths and IDs while reporting the durable
// logical state. No caller may infer an ACK from the observed item count.
func (r *NextcloudSourcePairingRepository) IndexedWithdrawalStatus(
	ctx context.Context, pair NextcloudSourcePairing,
) (NextcloudIndexedWithdrawalStatus, error) {
	var row NextcloudIndexedWithdrawal
	err := r.db.WithContext(ctx).Where("pair_operation_id = ? AND tenant_id = ?",
		pair.OperationID, pair.TenantID).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return NextcloudIndexedWithdrawalStatus{}, ErrNextcloudSourcePairingMissing
	}
	if err != nil || row.DataSourceID != pair.DataSourceID || row.KnowledgeBaseID != pair.KnowledgeBaseID ||
		row.InstanceID != pair.InstanceID || row.BindingID != pair.BindingID || row.KeyID != pair.KeyID {
		return NextcloudIndexedWithdrawalStatus{}, ErrNextcloudSourcePairingConflict
	}
	status := NextcloudIndexedWithdrawalStatus{NextcloudIndexedWithdrawal: row, LogicalWithdrawn: true}
	err = r.db.WithContext(ctx).Model(&nextcloudIndexedWithdrawalItem{}).
		Where("operation_id = ?", row.OperationID).Count(&status.ObservedItems).Error
	return status, err
}
