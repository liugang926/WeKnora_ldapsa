package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// ErrNextcloudSourcePairingConflict reports a conflicting source-pairing operation.
var ErrNextcloudSourcePairingConflict = apperrors.NewProtocolError(
	errors.New("nextcloud source pairing conflicts with existing source"),
	"Nextcloud source pairing conflicts with existing source",
)

// ErrNextcloudSourcePairingMissing reports an unavailable source-pairing operation.
var ErrNextcloudSourcePairingMissing = apperrors.NewProtocolError(
	errors.New("nextcloud source pairing not found"), "Nextcloud source pairing not found",
)

// NextcloudSourcePairing contains only recoverable public metadata. The
// machine secret lives solely in the encrypted data-source configuration.
type NextcloudSourcePairing struct {
	OperationID      string    `json:"operation_id" gorm:"column:operation_id;primaryKey"`
	TenantID         uint64    `json:"-" gorm:"column:tenant_id"`
	KnowledgeBaseID  string    `json:"knowledge_base_id" gorm:"column:knowledge_base_id"`
	DataSourceID     string    `json:"data_source_id" gorm:"column:datasource_id"`
	InstanceID       string    `json:"instance_id" gorm:"column:nextcloud_instance_id"`
	BindingID        string    `json:"binding_id" gorm:"column:binding_id"`
	BaseURL          string    `json:"base_url" gorm:"column:datasource_base_url"`
	ConfigSHA        string    `json:"-" gorm:"column:datasource_config_sha256"`
	PublicationEpoch int64     `json:"publication_epoch" gorm:"column:publication_epoch"`
	KeyID            string    `json:"key_id" gorm:"column:key_id"`
	State            string    `json:"state" gorm:"column:state"`
	LastErrorCode    string    `json:"last_error_code,omitempty" gorm:"column:last_error_code"`
	CreatedAt        time.Time `json:"created_at" gorm:"column:created_at"`
	UpdatedAt        time.Time `json:"updated_at" gorm:"column:updated_at"`
}

// TableName returns the source-pairing table name.
func (NextcloudSourcePairing) TableName() string { return "nextcloud_source_pairings" }

// NextcloudSourcePairingAbort is a credential-free tombstone for a removed
// empty pending source. It keeps the operation UUID permanently non-reusable.
type NextcloudSourcePairingAbort NextcloudSourcePairing

// TableName returns the source-pairing abort table name.
func (NextcloudSourcePairingAbort) TableName() string { return "nextcloud_source_pairing_aborts" }

// NextcloudSourcePairingRepository persists source-pairing and rotation operations.
type NextcloudSourcePairingRepository struct{ db *gorm.DB }

// NewNextcloudSourcePairingRepository returns the source-pairing repository.
func NewNextcloudSourcePairingRepository(db *gorm.DB) *NextcloudSourcePairingRepository {
	return &NextcloudSourcePairingRepository{db: db}
}

// PrepareSourcePairing inserts the paused source and intent in one transaction.
// Retrying the same operation reads the one existing source and never creates
// another. KB locking serializes this with generic source create/update.
func (r *NextcloudSourcePairingRepository) PrepareSourcePairing(
	ctx context.Context, proposed NextcloudSourcePairing, token string,
) (NextcloudSourcePairing, bool, error) {
	if proposed.OperationID == "" || proposed.TenantID == 0 || proposed.KnowledgeBaseID == "" ||
		proposed.InstanceID == "" || proposed.BindingID == "" || proposed.BaseURL == "" ||
		proposed.PublicationEpoch < 0 || proposed.KeyID == "" || token == "" ||
		len(utils.GetAESKey()) != 32 {
		return NextcloudSourcePairing{}, false, ErrNextcloudSourcePairingConflict
	}
	cfg := &types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		Credentials: map[string]interface{}{"token": token, "key_id": proposed.KeyID},
		ResourceIDs: []string{proposed.BindingID},
		Settings:    map[string]interface{}{"base_url": proposed.BaseURL},
	}
	var result NextcloudSourcePairing
	created := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		lock := tx.Exec(`UPDATE knowledge_bases SET ever_had_nextcloud_source = ever_had_nextcloud_source
			WHERE id = ? AND tenant_id = ? AND deleted_at IS NULL`, proposed.KnowledgeBaseID, proposed.TenantID)
		if lock.Error != nil {
			return lock.Error
		}
		if lock.RowsAffected != 1 {
			return ErrNextcloudSourcePairingConflict
		}
		var aborted NextcloudSourcePairing
		abortErr := tx.Table("nextcloud_source_pairing_aborts").
			Where("operation_id = ?", proposed.OperationID).Take(&aborted).Error
		if abortErr == nil {
			return ErrNextcloudSourcePairingConflict
		}
		if !errors.Is(abortErr, gorm.ErrRecordNotFound) {
			return abortErr
		}
		var existing NextcloudSourcePairing
		err := tx.Where("operation_id = ?", proposed.OperationID).Take(&existing).Error
		if err == nil {
			if existing.TenantID != proposed.TenantID || existing.KnowledgeBaseID != proposed.KnowledgeBaseID ||
				existing.InstanceID != proposed.InstanceID || existing.BindingID != proposed.BindingID ||
				existing.BaseURL != proposed.BaseURL || existing.PublicationEpoch != proposed.PublicationEpoch ||
				existing.KeyID != proposed.KeyID {
				return ErrNextcloudSourcePairingConflict
			}
			var ds types.DataSource
			if err := tx.Where("id = ? AND deleted_at IS NULL", existing.DataSourceID).Take(&ds).Error; err != nil {
				return ErrNextcloudSourcePairingConflict
			}
			stored, err := ds.ParseConfig()
			if err != nil || stored == nil {
				return ErrNextcloudSourcePairingConflict
			}
			storedToken, _ := stored.Credentials["token"].(string)
			if subtle.ConstantTimeCompare([]byte(storedToken), []byte(token)) != 1 {
				return ErrNextcloudSourcePairingConflict
			}
			result = existing
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		// A legacy generic source may already target the same binding in a
		// different KB. Such a source is not silently adopted or considered
		// securely paired.
		var oldSources []types.DataSource
		if err := tx.Where(
			"type = ? AND deleted_at IS NULL",
			types.ConnectorTypeNextcloud,
		).Find(&oldSources).Error; err !=
			nil {
			return err
		}
		for i := range oldSources {
			oldCfg, err := oldSources[i].ParseConfig()
			if err != nil || oldCfg == nil {
				return ErrNextcloudSourcePairingConflict
			}
			oldBase, _ := oldCfg.Settings["base_url"].(string)
			if oldBase ==
				proposed.BaseURL &&
				len(oldCfg.ResourceIDs) ==
					1 &&
				oldCfg.ResourceIDs[0] ==
					proposed.BindingID {
				return ErrNextcloudSourcePairingConflict
			}
		}
		blob, err := cfg.ToJSON()
		if err != nil {
			return err
		}
		ds := &types.DataSource{
			ID: uuid.NewString(), TenantID: proposed.TenantID,
			KnowledgeBaseID: proposed.KnowledgeBaseID, Name: "Nextcloud · " + proposed.BindingID,
			Type: types.ConnectorTypeNextcloud, Config: blob,
			Status: types.DataSourceStatusPaused, SyncMode: types.SyncModeIncremental, SyncDeletions: true,
		}
		if err := admitDataSourceKnowledgeBase(tx, ds, false); err != nil {
			return fmt.Errorf("admit dedicated Nextcloud knowledge base: %w", err)
		}
		if err := tx.Create(ds).Error; err != nil {
			return err
		}
		// PostgreSQL jsonb rewrites the JSON representation on INSERT. Pin
		// the bytes actually returned by the database, since activation and
		// event admission always read this persisted representation.
		var persisted types.DataSource
		if err := tx.Where("id = ? AND tenant_id = ?", ds.ID, ds.TenantID).Take(&persisted).Error; err != nil {
			return err
		}
		base, hash, bindingID, err := NextcloudEventDataSourceIdentity(persisted.Config)
		if err != nil || base != proposed.BaseURL || bindingID != proposed.BindingID {
			return ErrNextcloudSourcePairingConflict
		}
		proposed.DataSourceID = ds.ID
		proposed.ConfigSHA = hash
		proposed.State = "pending"
		proposed.CreatedAt = time.Now().UTC()
		proposed.UpdatedAt = proposed.CreatedAt
		if err := tx.Create(&proposed).Error; err != nil {
			return err
		}
		result = proposed
		created = true
		return nil
	})
	if err != nil {
		return NextcloudSourcePairing{}, false, err
	}
	return result, created, nil
}

// SourcePairing loads a source-pairing operation.
func (r *NextcloudSourcePairingRepository) SourcePairing(
	ctx context.Context,
	tenantID uint64,
	operationID string,
) (NextcloudSourcePairing, *types.DataSource, error) {
	var pair NextcloudSourcePairing
	if err := r.db.WithContext(
		ctx,
	).Where("operation_id = ? AND tenant_id = ?", operationID, tenantID).Take(&pair).Error; err !=
		nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = r.db.WithContext(ctx).Table("nextcloud_source_pairing_aborts").
				Where("operation_id = ? AND tenant_id = ?", operationID, tenantID).Take(&pair).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return pair, nil, ErrNextcloudSourcePairingMissing
			}
			if err == nil {
				return pair, nil, nil
			}
		}
		return pair, nil, err
	}
	var ds types.DataSource
	if err := r.db.WithContext(
		ctx,
	).Where("id = ? AND tenant_id = ? AND deleted_at IS NULL", pair.DataSourceID, tenantID).Take(&ds).Error; err !=
		nil {
		return pair, nil, ErrNextcloudSourcePairingConflict
	}
	return pair, &ds, nil
}

// ActiveSourcePairingForDataSource lets the settings page discover only the
// pairing for its selected source. No credential or cross-tenant lookup is used.
func (r *NextcloudSourcePairingRepository) ActiveSourcePairingForDataSource(ctx context.Context,
	tenantID uint64, datasourceID string,
) (NextcloudSourcePairing, *types.DataSource, error) {
	var pair NextcloudSourcePairing
	err := r.db.WithContext(ctx).Where("tenant_id = ? AND datasource_id = ? AND state = 'active'",
		tenantID, datasourceID).Take(&pair).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return pair, nil, ErrNextcloudSourcePairingMissing
	}
	if err != nil {
		return pair, nil, err
	}
	var ds types.DataSource
	err = r.db.WithContext(ctx).Where(("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND " +
		"type = ? AND status = ? AND deleted_at IS NULL"),
		datasourceID, tenantID, pair.KnowledgeBaseID, types.ConnectorTypeNextcloud, types.DataSourceStatusActive).Take(
		&ds,
	).Error
	if err != nil {
		return pair, nil, ErrNextcloudSourcePairingConflict
	}
	base, hash, binding, err := NextcloudEventDataSourceIdentity(ds.Config)
	if err != nil || base != pair.BaseURL || hash != pair.ConfigSHA || binding != pair.BindingID {
		return pair, nil, ErrNextcloudSourcePairingConflict
	}
	return pair, &ds, nil
}

// AbortPendingSourcePairing runs only after Nextcloud confirms the exact
// pending intent was aborted. The empty paused source is physically removed
// and a credential-free tombstone makes the operation permanently non-reusable.
func (r *NextcloudSourcePairingRepository) AbortPendingSourcePairing(ctx context.Context,
	pair NextcloudSourcePairing,
) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		lock := tx.Exec(`UPDATE knowledge_bases SET ever_had_nextcloud_source = ever_had_nextcloud_source
			WHERE id = ? AND tenant_id = ? AND deleted_at IS NULL`, pair.KnowledgeBaseID, pair.TenantID)
		if lock.Error != nil {
			return lock.Error
		}
		if lock.RowsAffected != 1 {
			return ErrNextcloudSourcePairingConflict
		}
		var current NextcloudSourcePairing
		err := tx.Where("operation_id = ? AND tenant_id = ?", pair.OperationID, pair.TenantID).
			Take(&current).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			var tombstone NextcloudSourcePairing
			if err := tx.Table("nextcloud_source_pairing_aborts").
				Where("operation_id = ? AND tenant_id = ?", pair.OperationID, pair.TenantID).
				Take(&tombstone).Error; err != nil {
				return ErrNextcloudSourcePairingConflict
			}
			if tombstone.DataSourceID == pair.DataSourceID &&
				tombstone.KnowledgeBaseID == pair.KnowledgeBaseID &&
				tombstone.InstanceID == pair.InstanceID &&
				tombstone.BindingID == pair.BindingID && tombstone.State == "aborted" {
				return nil
			}
			return ErrNextcloudSourcePairingConflict
		}
		if err != nil {
			return err
		}
		if current.State != "pending" || current.DataSourceID != pair.DataSourceID ||
			current.KnowledgeBaseID != pair.KnowledgeBaseID ||
			current.InstanceID != pair.InstanceID || current.BindingID != pair.BindingID ||
			current.BaseURL != pair.BaseURL || current.ConfigSHA != pair.ConfigSHA ||
			current.PublicationEpoch != pair.PublicationEpoch || current.KeyID != pair.KeyID {
			return ErrNextcloudSourcePairingConflict
		}
		var ds types.DataSource
		if err := tx.Where("id = ? AND tenant_id = ? AND deleted_at IS NULL",
			pair.DataSourceID, pair.TenantID).Take(&ds).Error; err != nil {
			return ErrNextcloudSourcePairingConflict
		}
		base, hash, binding, err := NextcloudEventDataSourceIdentity(ds.Config)
		if err != nil || hash != pair.ConfigSHA || base != pair.BaseURL ||
			binding != pair.BindingID || ds.KnowledgeBaseID != pair.KnowledgeBaseID ||
			ds.Status != types.DataSourceStatusPaused {
			return ErrNextcloudSourcePairingConflict
		}
		for _, tableAndWhere := range [][2]string{
			{"sync_logs", "data_source_id = ?"},
			{"nextcloud_event_connections", "datasource_id = ?"},
			{"knowledges", "knowledge_base_id = ?"},
		} {
			var count int64
			value := pair.DataSourceID
			if tableAndWhere[0] == "knowledges" {
				value = pair.KnowledgeBaseID
			}
			if err := tx.Table(tableAndWhere[0]).Where(tableAndWhere[1], value).
				Count(&count).Error; err != nil || count != 0 {
				return ErrNextcloudSourcePairingConflict
			}
		}
		insert := tx.Exec(`INSERT INTO nextcloud_source_pairing_aborts
			(operation_id, tenant_id, knowledge_base_id, datasource_id,
			nextcloud_instance_id, binding_id, datasource_base_url,
			datasource_config_sha256, publication_epoch, key_id, state,
			last_error_code, created_at, updated_at)
			SELECT operation_id, tenant_id, knowledge_base_id, datasource_id,
			nextcloud_instance_id, binding_id, datasource_base_url,
			datasource_config_sha256, publication_epoch, key_id, 'aborted', '',
			created_at, CURRENT_TIMESTAMP
			FROM nextcloud_source_pairings WHERE operation_id = ? AND tenant_id = ?
			AND state = 'pending'`, pair.OperationID, pair.TenantID)
		if insert.Error != nil || insert.RowsAffected != 1 {
			return ErrNextcloudSourcePairingConflict
		}
		deleted := tx.Where("operation_id = ? AND tenant_id = ? AND state = 'pending'",
			pair.OperationID, pair.TenantID).Delete(&NextcloudSourcePairing{})
		if deleted.Error != nil || deleted.RowsAffected != 1 {
			return ErrNextcloudSourcePairingConflict
		}
		removed := tx.Unscoped().Delete(&ds)
		if removed.Error != nil || removed.RowsAffected != 1 {
			return ErrNextcloudSourcePairingConflict
		}
		return nil
	})
}

// ActivateSourcePairing runs only after an exact remote ACK. A crash between
// ACK and this commit is repaired by repeating the same signed POST.
func (
	r *NextcloudSourcePairingRepository,
) ActivateSourcePairing(ctx context.Context, pair NextcloudSourcePairing) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		lock := tx.Exec(`UPDATE knowledge_bases SET ever_had_nextcloud_source = ever_had_nextcloud_source
			WHERE id = ? AND tenant_id = ? AND deleted_at IS NULL`, pair.KnowledgeBaseID, pair.TenantID)
		if lock.Error != nil {
			return lock.Error
		}
		if lock.RowsAffected != 1 {
			return ErrNextcloudSourcePairingConflict
		}
		var current NextcloudSourcePairing
		if err := tx.Where(
			"operation_id = ? AND tenant_id = ?",
			pair.OperationID,
			pair.TenantID,
		).Take(&current).Error; err !=
			nil {
			return err
		}
		if current.DataSourceID != pair.DataSourceID || current.InstanceID != pair.InstanceID ||
			current.BindingID != pair.BindingID || current.KnowledgeBaseID != pair.KnowledgeBaseID ||
			current.ConfigSHA != pair.ConfigSHA || current.PublicationEpoch != pair.PublicationEpoch {
			return ErrNextcloudSourcePairingConflict
		}
		var ds types.DataSource
		if err := tx.Where(
			"id = ? AND tenant_id = ? AND deleted_at IS NULL",
			pair.DataSourceID,
			pair.TenantID,
		).Take(&ds).Error; err !=
			nil {
			return err
		}
		base, hash, binding, err := NextcloudEventDataSourceIdentity(ds.Config)
		if err != nil || base != pair.BaseURL || binding != pair.BindingID {
			return ErrNextcloudSourcePairingConflict
		}
		if current.State == "active" {
			if hash != pair.ConfigSHA || ds.Status != types.DataSourceStatusActive {
				return ErrNextcloudSourcePairingConflict
			}
			return nil
		}
		if current.State != "pending" || ds.Status != types.DataSourceStatusPaused {
			return ErrNextcloudSourcePairingConflict
		}
		if hash != pair.ConfigSHA {
			// A pairing prepared by the old PostgreSQL path pinned the
			// pre-jsonb JSON bytes. The remote exact-tuple commit succeeded
			// before this method was called. Repair only if re-encoding the
			// persisted JSON as the original Go struct proves that the old
			// digest described this very source. The row guard deliberately
			// permits deleting only a pending pair with a paused source.
			var original types.DataSourceConfig
			if ds.Type != types.ConnectorTypeNextcloud || json.Unmarshal(ds.Config, &original) != nil {
				return ErrNextcloudSourcePairingConflict
			}
			oldJSON, err := json.Marshal(&original)
			if err != nil {
				return ErrNextcloudSourcePairingConflict
			}
			oldSum := sha256.Sum256(oldJSON)
			if hex.EncodeToString(oldSum[:]) != pair.ConfigSHA {
				return ErrNextcloudSourcePairingConflict
			}
			deleted := tx.Where("operation_id = ? AND tenant_id = ? AND state = 'pending'",
				current.OperationID, current.TenantID).Delete(&NextcloudSourcePairing{})
			if deleted.Error != nil {
				return deleted.Error
			}
			if deleted.RowsAffected != 1 {
				return ErrNextcloudSourcePairingConflict
			}
			current.ConfigSHA = hash
			if err := tx.Create(&current).Error; err != nil {
				return err
			}
		}
		if err := tx.Model(
			&current,
		).Updates(map[string]interface{}{
			"state":           "active",
			"last_error_code": "",
			"updated_at":      time.Now().UTC(),
		}).Error; err !=
			nil {
			return err
		}
		return tx.Model(&ds).Update("status", types.DataSourceStatusActive).Error
	})
}

// RecordSourcePairingFailure records the source-pairing failure state.
func (r *NextcloudSourcePairingRepository) RecordSourcePairingFailure(ctx context.Context,
	pair NextcloudSourcePairing, code string,
) error {
	if code == "" {
		return nil
	}
	return r.db.WithContext(ctx).Model(&NextcloudSourcePairing{}).
		Where("operation_id = ? AND tenant_id = ? AND state = 'pending'", pair.OperationID, pair.TenantID).
		Updates(map[string]interface{}{"last_error_code": code, "updated_at": time.Now().UTC()}).Error
}

// HasActiveSourcePairing gates sync and generic resume paths. Legacy rows
// intentionally return false; they require explicit migration/re-pairing.
func (
	r *NextcloudSourcePairingRepository,
) HasActiveSourcePairing(ctx context.Context, ds *types.DataSource, observedInstanceID ...string) (
	bool,
	error,
) {
	if ds == nil || ds.Type != types.ConnectorTypeNextcloud {
		return false, nil
	}
	var pair NextcloudSourcePairing
	err := r.db.WithContext(ctx).Where(("datasource_id = ? AND tenant_id = ? AND knowledge_base_" +
		"id = ? AND state = 'active'"),
		ds.ID, ds.TenantID, ds.KnowledgeBaseID).Take(&pair).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(observedInstanceID) > 0 &&
		(len(observedInstanceID) != 1 || observedInstanceID[0] == "" || pair.InstanceID != observedInstanceID[0]) {
		return false, nil
	}
	base, hash, binding, err := NextcloudEventDataSourceIdentity(ds.Config)
	return err == nil && base == pair.BaseURL && hash == pair.ConfigSHA && binding == pair.BindingID, nil
}

// ActiveForSync loads source-pairing admission for a sync operation.
func (r *NextcloudSourcePairingRepository) ActiveForSync(
	ctx context.Context,
	datasourceID string,
	tenantID uint64,
	observedInstanceID ...string,
) (bool, error) {
	var ds types.DataSource
	if err := r.db.WithContext(
		ctx,
	).Where("id = ? AND tenant_id = ? AND deleted_at IS NULL", datasourceID, tenantID).Take(&ds).Error; err !=
		nil {
		return false, err
	}
	return r.HasActiveSourcePairing(ctx, &ds, observedInstanceID...)
}

// PairedSourceConfigImmutable is called under the same KB lock as updates.
func pairedSourceConfigImmutable(tx *gorm.DB, previous, next *types.DataSource) error {
	var pair NextcloudSourcePairing
	err := tx.Where("datasource_id = ?", previous.ID).Take(&pair).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(next.Config) != 0 && !bytes.Equal(next.Config, previous.Config) {
		return ErrNextcloudSourcePairingConflict
	}
	if pair.State == "pending" && next.Status != "" && next.Status != types.DataSourceStatusPaused {
		return ErrNextcloudSourcePairingConflict
	}
	return nil
}

// Event receipt and dispatch must use the same secure source mapping as sync.
// A legacy event connection never upgrades a generic source implicitly.
func activeNextcloudSourcePairing(tx *gorm.DB, tenantID uint64, kbID, datasourceID,
	instanceID, bindingID, baseURL, configSHA string,
) error {
	var pair NextcloudSourcePairing
	err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND state = 'active'",
		tenantID, kbID, datasourceID).Take(&pair).Error
	if err != nil {
		return ErrNextcloudEventScope
	}
	if pair.InstanceID != instanceID || pair.BindingID != bindingID ||
		pair.BaseURL != baseURL || pair.ConfigSHA != configSHA {
		return ErrNextcloudEventScope
	}
	return nil
}
