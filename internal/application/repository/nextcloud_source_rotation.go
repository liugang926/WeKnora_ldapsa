package repository

import (
	"context"
	"crypto/subtle"
	"errors"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
	"gorm.io/gorm"
)

// NextcloudSourceRotation records a durable source-rotation operation.
// The new credential remains encrypted in new_config until the remote commit
// has been acknowledged. Status responses never include this field.
type NextcloudSourceRotation struct {
	OperationID     string     `json:"operation_id" gorm:"column:operation_id;primaryKey"`
	PairOperationID string     `json:"pair_operation_id" gorm:"column:pair_operation_id"`
	TenantID        uint64     `json:"-" gorm:"column:tenant_id"`
	KnowledgeBaseID string     `json:"knowledge_base_id" gorm:"column:knowledge_base_id"`
	DataSourceID    string     `json:"data_source_id" gorm:"column:datasource_id"`
	InstanceID      string     `json:"instance_id" gorm:"column:nextcloud_instance_id"`
	BindingID       string     `json:"binding_id" gorm:"column:binding_id"`
	OldKeyID        string     `json:"old_key_id" gorm:"column:old_key_id"`
	NewKeyID        string     `json:"new_key_id" gorm:"column:new_key_id"`
	OldConfigSHA    string     `json:"-" gorm:"column:old_config_sha256"`
	NewConfigSHA    string     `json:"-" gorm:"column:new_config_sha256"`
	OldConfig       types.JSON `json:"-" gorm:"column:old_config;type:jsonb"`
	NewConfig       types.JSON `json:"-" gorm:"column:new_config;type:jsonb"`
	State           string     `json:"state" gorm:"column:state"`
	LastErrorCode   string     `json:"last_error_code,omitempty" gorm:"column:last_error_code"`
	CreatedAt       time.Time  `json:"created_at" gorm:"column:created_at"`
	UpdatedAt       time.Time  `json:"updated_at" gorm:"column:updated_at"`
}

// TableName returns the source-rotation table name.
func (NextcloudSourceRotation) TableName() string { return "nextcloud_source_rotations" }

// PrepareSourceRotation prepares a durable source-rotation operation.
func (r *NextcloudSourcePairingRepository) PrepareSourceRotation(ctx context.Context,
	proposed NextcloudSourceRotation, token string,
) (NextcloudSourceRotation, bool, error) {
	if proposed.OperationID == "" || proposed.PairOperationID == "" || proposed.TenantID == 0 ||
		proposed.NewKeyID == "" || token == "" || len(utils.GetAESKey()) != 32 {
		return NextcloudSourceRotation{}, false, ErrNextcloudSourcePairingConflict
	}
	var result NextcloudSourceRotation
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
		var pair NextcloudSourcePairing
		if err := tx.Where("operation_id = ? AND tenant_id = ? AND state = 'active'",
			proposed.PairOperationID, proposed.TenantID).Take(&pair).Error; err != nil {
			return ErrNextcloudSourcePairingConflict
		}
		if pair.KnowledgeBaseID != proposed.KnowledgeBaseID || pair.DataSourceID != proposed.DataSourceID ||
			pair.InstanceID != proposed.InstanceID || pair.BindingID != proposed.BindingID {
			return ErrNextcloudSourcePairingConflict
		}
		var current NextcloudSourceRotation
		err := tx.Where("operation_id = ?", proposed.OperationID).Take(&current).Error
		if err == nil {
			if current.PairOperationID != pair.OperationID || current.TenantID != pair.TenantID ||
				current.KnowledgeBaseID != pair.KnowledgeBaseID || current.DataSourceID != pair.DataSourceID ||
				current.InstanceID != pair.InstanceID || current.BindingID != pair.BindingID ||
				current.NewKeyID != proposed.NewKeyID || current.State == "aborted" {
				return ErrNextcloudSourcePairingConflict
			}
			cfg, err := SourceRotationConfig(current)
			if err != nil {
				return ErrNextcloudSourcePairingConflict
			}
			stored, _ := cfg.Credentials["token"].(string)
			if subtle.ConstantTimeCompare([]byte(stored), []byte(token)) != 1 {
				return ErrNextcloudSourcePairingConflict
			}
			result = current
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err := tx.Where("pair_operation_id = ? AND state IN ?", pair.OperationID,
			[]string{"pending", "committed", "switched"}).Take(&current).Error; err == nil {
			return ErrNextcloudSourcePairingConflict
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		var ds types.DataSource
		if err := tx.Where("id = ? AND tenant_id = ? AND deleted_at IS NULL", pair.DataSourceID, pair.TenantID).
			Take(&ds).Error; err != nil || ds.Status != types.DataSourceStatusActive {
			return ErrNextcloudSourcePairingConflict
		}
		base, hash, binding, err := NextcloudEventDataSourceIdentity(ds.Config)
		if err != nil || base != pair.BaseURL || binding != pair.BindingID ||
			hash != pair.ConfigSHA || pair.KeyID == proposed.NewKeyID {
			return ErrNextcloudSourcePairingConflict
		}
		oldCfg, err := ds.ParseConfig()
		if err != nil || oldCfg == nil || oldCfg.Credentials["key_id"] != pair.KeyID {
			return ErrNextcloudSourcePairingConflict
		}
		newCfg := &types.DataSourceConfig{
			Type:        types.ConnectorTypeNextcloud,
			Credentials: map[string]interface{}{"token": token, "key_id": proposed.NewKeyID},
			ResourceIDs: []string{pair.BindingID},
			Settings:    map[string]interface{}{"base_url": pair.BaseURL},
		}
		newBlob, err := newCfg.ToJSON()
		if err != nil {
			return err
		}
		newBase, newHash, newBinding, err := NextcloudEventDataSourceIdentity(newBlob)
		if err != nil || newBase != pair.BaseURL || newBinding != pair.BindingID {
			return ErrNextcloudSourcePairingConflict
		}
		proposed.OldKeyID = pair.KeyID
		proposed.OldConfigSHA = pair.ConfigSHA
		proposed.NewConfigSHA = newHash
		proposed.OldConfig = ds.Config
		proposed.NewConfig = newBlob
		proposed.State = "pending"
		proposed.CreatedAt = time.Now().UTC()
		proposed.UpdatedAt = proposed.CreatedAt
		if err := tx.Create(&proposed).Error; err != nil {
			return err
		}
		// PostgreSQL jsonb may normalize key order on storage. Pin the
		// fingerprint to the bytes that a later source read will return.
		var stored NextcloudSourceRotation
		if err := tx.Where("operation_id = ?", proposed.OperationID).Take(&stored).Error; err != nil {
			return err
		}
		_, persistedHash, _, err := NextcloudEventDataSourceIdentity(stored.NewConfig)
		if err != nil {
			return ErrNextcloudSourcePairingConflict
		}
		if persistedHash != newHash {
			if err := tx.Model(&stored).Update("new_config_sha256", persistedHash).Error; err != nil {
				return err
			}
			stored.NewConfigSHA = persistedHash
		}
		result = stored
		created = true
		return nil
	})
	if err != nil {
		return NextcloudSourceRotation{}, false, err
	}
	return result, created, nil
}

// SourceRotationConfig returns the configuration stored for a source rotation.
func SourceRotationConfig(rotation NextcloudSourceRotation) (*types.DataSourceConfig, error) {
	ds := &types.DataSource{Config: rotation.NewConfig}
	return ds.ParseConfig()
}

// SourceRotation loads a source-rotation operation.
func (r *NextcloudSourcePairingRepository) SourceRotation(ctx context.Context,
	tenantID uint64, operationID string,
) (NextcloudSourceRotation, error) {
	var rotation NextcloudSourceRotation
	err := r.db.WithContext(ctx).Where("operation_id = ? AND tenant_id = ?", operationID, tenantID).
		Take(&rotation).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return rotation, ErrNextcloudSourcePairingMissing
	}
	return rotation, err
}

// SwitchSourceRotation switches the prepared source-rotation scope.
// Called only after Nextcloud commits the new key. The source config, pinned
// fingerprint, and rotation state switch atomically under the owning KB lock.
func (r *NextcloudSourcePairingRepository) SwitchSourceRotation(ctx context.Context,
	rotation NextcloudSourceRotation,
) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		lock := tx.Exec(`UPDATE knowledge_bases SET ever_had_nextcloud_source = ever_had_nextcloud_source
			WHERE id = ? AND tenant_id = ? AND deleted_at IS NULL`, rotation.KnowledgeBaseID, rotation.TenantID)
		if lock.Error != nil {
			return lock.Error
		}
		if lock.RowsAffected != 1 {
			return ErrNextcloudSourcePairingConflict
		}
		var current NextcloudSourceRotation
		if err := tx.Where("operation_id = ? AND tenant_id = ?", rotation.OperationID, rotation.TenantID).
			Take(&current).Error; err != nil {
			return err
		}
		var pair NextcloudSourcePairing
		if err := tx.Where("operation_id = ? AND tenant_id = ? AND state = 'active'",
			current.PairOperationID, current.TenantID).Take(&pair).Error; err != nil {
			return ErrNextcloudSourcePairingConflict
		}
		var ds types.DataSource
		if err := tx.Where("id = ? AND tenant_id = ? AND deleted_at IS NULL",
			current.DataSourceID, current.TenantID).Take(&ds).Error; err != nil ||
			ds.Status != types.DataSourceStatusActive || ds.KnowledgeBaseID != current.KnowledgeBaseID {
			return ErrNextcloudSourcePairingConflict
		}
		_, hash, _, err := NextcloudEventDataSourceIdentity(ds.Config)
		if err != nil {
			return ErrNextcloudSourcePairingConflict
		}
		if current.State == "switched" || current.State == "finalized" {
			if pair.KeyID == current.NewKeyID && pair.ConfigSHA == current.NewConfigSHA &&
				hash == current.NewConfigSHA {
				return nil
			}
			return ErrNextcloudSourcePairingConflict
		}
		if current.State != "pending" || pair.KeyID != current.OldKeyID ||
			pair.ConfigSHA != current.OldConfigSHA || hash != current.OldConfigSHA ||
			pair.KnowledgeBaseID != current.KnowledgeBaseID || pair.InstanceID != current.InstanceID ||
			pair.BindingID != current.BindingID || pair.DataSourceID != current.DataSourceID {
			return ErrNextcloudSourcePairingConflict
		}
		if err := tx.Model(&current).Update("state", "committed").Error; err != nil {
			return err
		}
		if err := tx.Model(&ds).Update("config", current.NewConfig).Error; err != nil {
			return err
		}
		if err := tx.Model(&pair).Updates(map[string]interface{}{
			"key_id": current.NewKeyID, "datasource_config_sha256": current.NewConfigSHA,
			"updated_at": time.Now().UTC(),
		}).Error; err != nil {
			return err
		}
		return tx.Model(&current).Updates(map[string]interface{}{
			"state": "switched", "last_error_code": "", "updated_at": time.Now().UTC(),
		}).Error
	})
}

// FinalizeSourceRotation finalizes a source-rotation operation.
func (r *NextcloudSourcePairingRepository) FinalizeSourceRotation(ctx context.Context,
	rotation NextcloudSourceRotation,
) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current NextcloudSourceRotation
		if err := tx.Where("operation_id = ? AND tenant_id = ?", rotation.OperationID, rotation.TenantID).
			Take(&current).Error; err != nil {
			return err
		}
		if current.State == "finalized" {
			return nil
		}
		if current.State != "switched" {
			return ErrNextcloudSourcePairingConflict
		}
		return tx.Model(&current).Updates(map[string]interface{}{
			"state": "finalized", "last_error_code": "", "updated_at": time.Now().UTC(),
		}).Error
	})
}

// AbortSourceRotation aborts a source-rotation operation.
// Abort is safe only after Nextcloud's signed abort ACK. A committed remote
// operation cannot be discarded, even if the local switch has not run yet.
func (r *NextcloudSourcePairingRepository) AbortSourceRotation(ctx context.Context,
	rotation NextcloudSourceRotation,
) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		lock := tx.Exec(`UPDATE knowledge_bases SET ever_had_nextcloud_source = ever_had_nextcloud_source
			WHERE id = ? AND tenant_id = ? AND deleted_at IS NULL`, rotation.KnowledgeBaseID, rotation.TenantID)
		if lock.Error != nil {
			return lock.Error
		}
		if lock.RowsAffected != 1 {
			return ErrNextcloudSourcePairingConflict
		}
		var current NextcloudSourceRotation
		if err := tx.Where("operation_id = ? AND tenant_id = ?", rotation.OperationID, rotation.TenantID).
			Take(&current).Error; err != nil {
			return err
		}
		if current.State == "aborted" {
			return nil
		}
		if current.State != "pending" {
			return ErrNextcloudSourcePairingConflict
		}
		var pair NextcloudSourcePairing
		if err := tx.Where("operation_id = ? AND tenant_id = ? AND state = 'active'",
			current.PairOperationID, current.TenantID).Take(&pair).Error; err != nil ||
			pair.KeyID != current.OldKeyID || pair.ConfigSHA != current.OldConfigSHA ||
			pair.DataSourceID != current.DataSourceID {
			return ErrNextcloudSourcePairingConflict
		}
		var ds types.DataSource
		if err := tx.Where("id = ? AND tenant_id = ? AND deleted_at IS NULL",
			current.DataSourceID, current.TenantID).Take(&ds).Error; err != nil ||
			string(ds.Config) != string(current.OldConfig) {
			return ErrNextcloudSourcePairingConflict
		}
		return tx.Model(&current).Updates(map[string]interface{}{
			"state": "aborted", "old_config": types.JSON(`{}`), "new_config": types.JSON(`{}`),
			"last_error_code": "", "updated_at": time.Now().UTC(),
		}).Error
	})
}

// RecordSourceRotationFailure records the source-rotation failure state.
func (r *NextcloudSourcePairingRepository) RecordSourceRotationFailure(ctx context.Context,
	rotation NextcloudSourceRotation, code string,
) error {
	if code == "" {
		return nil
	}
	return r.db.WithContext(ctx).Model(&NextcloudSourceRotation{}).
		Where("operation_id = ? AND tenant_id = ? AND state IN ?", rotation.OperationID,
			rotation.TenantID, []string{"pending", "switched"}).
		Updates(map[string]interface{}{"last_error_code": code, "updated_at": time.Now().UTC()}).Error
}
