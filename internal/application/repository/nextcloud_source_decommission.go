package repository

import (
	"context"
	"errors"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// EmptyNextcloudInventorySHA256 is the digest of an empty source inventory.
const EmptyNextcloudInventorySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// NextcloudSourceDecommission records the durable source-decommission acknowledgement.
// A row is an immutable identity and a retry checkpoint, never a claim that
// general source GC has completed. This protocol handles virgin sources only.
type NextcloudSourceDecommission struct {
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
	InventorySHA256  string    `json:"inventory_sha256" gorm:"column:inventory_sha256"`
	CreatedAt        time.Time `json:"created_at" gorm:"column:created_at"`
	UpdatedAt        time.Time `json:"updated_at" gorm:"column:updated_at"`
}

// TableName returns the source-decommission table name.
func (NextcloudSourceDecommission) TableName() string { return "nextcloud_source_decommissions" }

type nextcloudSourceVirgin struct {
	PairOperationID string `gorm:"column:pair_operation_id;primaryKey"`
	EverTouched     bool   `gorm:"column:ever_touched"`
}

func (nextcloudSourceVirgin) TableName() string { return "nextcloud_source_virgin" }

func sameSourceDecommission(a, b NextcloudSourceDecommission) bool {
	return a.OperationID == b.OperationID && a.PairOperationID == b.PairOperationID &&
		a.TenantID == b.TenantID && a.KnowledgeBaseID == b.KnowledgeBaseID &&
		a.DataSourceID == b.DataSourceID && a.InstanceID == b.InstanceID &&
		a.BindingID == b.BindingID && a.PublicationEpoch == b.PublicationEpoch && a.KeyID == b.KeyID
}

// BeginEmptySourceDecommission is a compare-and-swap against the exact pair.
// The virgin marker is absent for pre-migration pairs and burns permanently on
// first sync/event/content admission. Thus a pruned log cannot fabricate an
// empty historical inventory. The database triggers serialize first writes
// against this transaction and reject writes after its row is committed.
func (r *NextcloudSourcePairingRepository) BeginEmptySourceDecommission(
	ctx context.Context, pair NextcloudSourcePairing, proposed NextcloudSourceDecommission,
) (NextcloudSourceDecommission, error) {
	var result NextcloudSourceDecommission
	if proposed.OperationID == "" || proposed.PairOperationID != pair.OperationID ||
		proposed.TenantID != pair.TenantID || proposed.KnowledgeBaseID != pair.KnowledgeBaseID ||
		proposed.DataSourceID != pair.DataSourceID || proposed.InstanceID != pair.InstanceID ||
		proposed.BindingID != pair.BindingID || proposed.KeyID != pair.KeyID ||
		proposed.PublicationEpoch < 0 {
		return result, ErrNextcloudSourcePairingConflict
	}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		lock := tx.Exec(`UPDATE knowledge_bases SET ever_had_nextcloud_source = ever_had_nextcloud_source
			WHERE id = ? AND tenant_id = ? AND deleted_at IS NULL`, pair.KnowledgeBaseID, pair.TenantID)
		if lock.Error != nil {
			return lock.Error
		}
		if lock.RowsAffected != 1 {
			return ErrNextcloudSourcePairingConflict
		}
		var current NextcloudSourcePairing
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("operation_id = ? AND tenant_id = ? AND state = 'active'", pair.OperationID, pair.TenantID).
			Take(&current).Error; err != nil || current.DataSourceID != pair.DataSourceID ||
			current.ConfigSHA != pair.ConfigSHA || current.KeyID != pair.KeyID ||
			current.InstanceID != pair.InstanceID || current.BindingID != pair.BindingID ||
			current.BaseURL != pair.BaseURL {
			return ErrNextcloudSourcePairingConflict
		}
		var existing NextcloudSourceDecommission
		err := tx.Where("pair_operation_id = ?", pair.OperationID).Take(&existing).Error
		if err == nil {
			if !sameSourceDecommission(existing, proposed) {
				return ErrNextcloudSourcePairingConflict
			}
			result = existing
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		var virgin nextcloudSourceVirgin
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("pair_operation_id = ?", pair.OperationID).Take(&virgin).Error; err != nil || virgin.EverTouched {
			return ErrNextcloudSourcePairingConflict
		}
		var liveRotation int64
		if err := tx.Table("nextcloud_source_rotations").
			Where("pair_operation_id = ? AND state IN ?", pair.OperationID,
				[]string{"pending", "committed", "switched"}).
			Count(&liveRotation).Error; err != nil || liveRotation != 0 {
			return ErrNextcloudSourcePairingConflict
		}
		var source types.DataSource
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND deleted_at IS NULL",
				pair.DataSourceID, pair.TenantID, pair.KnowledgeBaseID).Take(&source).Error; err != nil {
			return ErrNextcloudSourcePairingConflict
		}
		base, hash, binding, err := NextcloudEventDataSourceIdentity(source.Config)
		if err != nil || source.Type != types.ConnectorTypeNextcloud ||
			base != pair.BaseURL || hash != pair.ConfigSHA || binding != pair.BindingID ||
			(source.Status != types.DataSourceStatusActive && source.Status != types.DataSourceStatusPaused) ||
			source.LastSyncAt != nil || len(source.LastSyncCursor) != 0 || len(source.LastSyncResult) != 0 {
			return ErrNextcloudSourcePairingConflict
		}
		config, err := source.ParseConfig()
		if err != nil || config == nil || config.Credentials["key_id"] != pair.KeyID {
			return ErrNextcloudSourcePairingConflict
		}
		checks := []struct {
			table, where string
			args         []any
		}{
			{"sync_logs", "data_source_id = ?", []any{pair.DataSourceID}},
			{"nextcloud_event_connections", "datasource_id = ?", []any{pair.DataSourceID}},
			{"nextcloud_source_versions", "datasource_id = ?", []any{pair.DataSourceID}},
			{"nextcloud_gc_jobs", "datasource_id = ?", []any{pair.DataSourceID}},
			{"knowledges", "knowledge_base_id = ?", []any{pair.KnowledgeBaseID}},
			{"chunks", "knowledge_base_id = ?", []any{pair.KnowledgeBaseID}},
		}
		for _, check := range checks {
			var count int64
			if err := tx.Table(check.table).Where(check.where, check.args...).Count(&count).Error; err != nil {
				return err
			}
			if count != 0 {
				return ErrNextcloudSourcePairingConflict
			}
		}
		// Explicitly inspect persisted event receipts, dispatch leases and
		// checkpoints too. Their foreign keys make them impossible when the
		// connection count above is zero; a failed join still fails closed.
		for _, table := range []string{
			"nextcloud_event_inbox", "nextcloud_event_dispatch",
			"nextcloud_event_checkpoint",
		} {
			var count int64
			if err := tx.Table(table+" AS item").Joins(
				"JOIN nextcloud_event_connections AS connection ON connection.connection_id = item.connection_id").
				Where("connection.datasource_id = ?", pair.DataSourceID).Count(&count).Error; err != nil {
				return err
			}
			if count != 0 {
				return ErrNextcloudSourcePairingConflict
			}
		}
		// Manual and scheduled sync admission creates a sync log before
		// enqueue; the virgin trigger burns the proof at that point.
		if err := tx.Model(&types.DataSource{}).Where("id = ? AND status IN ?", source.ID,
			[]string{types.DataSourceStatusActive, types.DataSourceStatusPaused}).
			Update("status", types.DataSourceStatusPaused).Error; err != nil {
			return err
		}
		proposed.State = "prepared"
		proposed.InventorySHA256 = ""
		proposed.CreatedAt = time.Now().UTC()
		proposed.UpdatedAt = proposed.CreatedAt
		if err := tx.Create(&proposed).Error; err != nil {
			return err
		}
		result = proposed
		return nil
	})
	if err != nil {
		return NextcloudSourceDecommission{}, err
	}
	return result, nil
}

// SourceDecommission loads a source-decommission record.
func (r *NextcloudSourcePairingRepository) SourceDecommission(
	ctx context.Context, pair NextcloudSourcePairing,
) (NextcloudSourceDecommission, error) {
	var row NextcloudSourceDecommission
	err := r.db.WithContext(ctx).Where("pair_operation_id = ? AND tenant_id = ?",
		pair.OperationID, pair.TenantID).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return row, ErrNextcloudSourcePairingMissing
	}
	if err != nil {
		return row, err
	}
	if row.KnowledgeBaseID != pair.KnowledgeBaseID || row.DataSourceID != pair.DataSourceID ||
		row.InstanceID != pair.InstanceID || row.BindingID != pair.BindingID || row.KeyID != pair.KeyID {
		return row, ErrNextcloudSourcePairingConflict
	}
	return row, nil
}

// RecordEmptyDecommissionAck records an empty source-inventory acknowledgement.
func (r *NextcloudSourcePairingRepository) RecordEmptyDecommissionAck(
	ctx context.Context, row NextcloudSourceDecommission,
) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current NextcloudSourceDecommission
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("operation_id = ? AND tenant_id = ?", row.OperationID, row.TenantID).
			Take(&current).Error; err != nil || !sameSourceDecommission(current, row) {
			return ErrNextcloudSourcePairingConflict
		}
		if current.State == "acknowledged" && current.InventorySHA256 == EmptyNextcloudInventorySHA256 {
			return nil
		}
		if current.State != "prepared" {
			return ErrNextcloudSourcePairingConflict
		}
		updated := tx.Model(&NextcloudSourceDecommission{}).
			Where("operation_id = ? AND state = 'prepared'", row.OperationID).
			Updates(map[string]any{
				"state":            "acknowledged",
				"inventory_sha256": EmptyNextcloudInventorySHA256,
				"updated_at":       time.Now().UTC(),
			})
		if updated.Error != nil {
			return updated.Error
		}
		if updated.RowsAffected != 1 {
			return ErrNextcloudSourcePairingConflict
		}
		return nil
	})
}
