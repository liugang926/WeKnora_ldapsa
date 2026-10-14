package repository

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrNextcloudEventRebindUnsafe means a prior queue admission or a different
// dispatch failure still needs operator review. Rebinding must not erase it.
var ErrNextcloudEventRebindUnsafe = apperrors.NewProtocolError(
	errors.New("nextcloud event dispatch requires manual review before source rotation rebind"),
	"Nextcloud event dispatch requires manual review before source rotation rebind",
)

// RebindFinalizedSourceRotation keeps the event connection and its HMAC key,
// inbox, nonces, and receipt/application watermarks after the exact source
// machine-key rotation. The live source inspection is performed by the handler;
// the transaction repeats every durable identity check under the connection
// lock. A replay of the same completed operation is read-only.
func (r *NextcloudEventInboxRepository) RebindFinalizedSourceRotation(
	ctx context.Context, identity NextcloudEventPairingIdentity, rotationID string,
) (bool, error) {
	parsed, err := uuid.Parse(rotationID)
	if err != nil || parsed.String() != rotationID || identity.TenantID == 0 ||
		identity.DatasourceID == "" || identity.KnowledgeBaseID == "" ||
		identity.PublicationState != "active" || identity.PublicationEpoch < 0 {
		return false, ErrNextcloudEventScope
	}
	changed := false
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Ingest, dispatch, key rotation and revoke all lock the connection
		// first. SQLite needs a real write before its first transactional read.
		if tx.Name() == "sqlite" {
			lock := tx.Exec(`UPDATE nextcloud_event_connections SET status = status
				WHERE tenant_id = ? AND datasource_id = ? AND status = 'active'`,
				identity.TenantID, identity.DatasourceID)
			if lock.Error != nil {
				return fmt.Errorf("lock Nextcloud event connection: %w", lock.Error)
			}
			if lock.RowsAffected != 1 {
				return ErrNextcloudEventConnectionMissing
			}
		}
		var connection nextcloudEventConnection
		if err := tx.Table("nextcloud_event_connections").Clauses(clause.Locking{Strength: "UPDATE"}).
			Where(("tenant_id = ? AND datasource_id = ? AND status = 'activ" +
				"e'"), identity.TenantID, identity.DatasourceID).
			Take(&connection).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNextcloudEventConnectionMissing
			}
			return fmt.Errorf("read Nextcloud event connection: %w", err)
		}
		if connection.KnowledgeBaseID != identity.KnowledgeBaseID ||
			connection.NextcloudInstanceID != identity.NextcloudInstanceID ||
			connection.BindingID != identity.BindingID ||
			connection.DatasourceBaseURL != identity.DatasourceBaseURL {
			return ErrNextcloudEventScope
		}
		// This takes the KB serialization lock only after the connection lock.
		// Source rotation takes the same KB lock when switching the config.
		if err := checkNextcloudPairingSource(tx, identity); err != nil {
			return err
		}
		var activeSources int64
		if err := tx.Model(&types.DataSource{}).
			Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND status = ?",
				identity.DatasourceID, identity.TenantID, identity.KnowledgeBaseID,
				types.DataSourceStatusActive).Count(&activeSources).Error; err != nil {
			return fmt.Errorf("check active Nextcloud rebind source: %w", err)
		}
		if activeSources != 1 {
			return ErrNextcloudEventScope
		}
		var rotation NextcloudSourceRotation
		if err := tx.Where("operation_id = ? AND tenant_id = ?", rotationID, identity.TenantID).
			Take(&rotation).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNextcloudEventScope
			}
			return fmt.Errorf("read Nextcloud source rotation: %w", err)
		}
		if rotation.State != "finalized" || rotation.KnowledgeBaseID != identity.KnowledgeBaseID ||
			rotation.DataSourceID != identity.DatasourceID || rotation.InstanceID != identity.NextcloudInstanceID ||
			rotation.BindingID != identity.BindingID || rotation.OldConfigSHA == rotation.NewConfigSHA ||
			rotation.NewConfigSHA != identity.DatasourceConfigSHA {
			return ErrNextcloudEventScope
		}
		oldBase, oldHash, oldBinding, oldErr := NextcloudEventDataSourceIdentity(rotation.OldConfig)
		newBase, newHash, newBinding, newErr := NextcloudEventDataSourceIdentity(rotation.NewConfig)
		if oldErr != nil || newErr != nil || oldBase != identity.DatasourceBaseURL ||
			newBase != identity.DatasourceBaseURL || oldBinding != identity.BindingID ||
			newBinding != identity.BindingID || oldHash != rotation.OldConfigSHA ||
			newHash != rotation.NewConfigSHA || !nextcloudRotationOnlyChangesMachineKey(rotation) {
			return ErrNextcloudEventScope
		}
		var pair NextcloudSourcePairing
		if err := tx.Where("operation_id = ? AND tenant_id = ? AND state = 'active'",
			rotation.PairOperationID, identity.TenantID).Take(&pair).Error; err != nil {
			return ErrNextcloudEventScope
		}
		if pair.KnowledgeBaseID != identity.KnowledgeBaseID || pair.DataSourceID != identity.DatasourceID ||
			pair.InstanceID != identity.NextcloudInstanceID || pair.BindingID != identity.BindingID ||
			pair.BaseURL != identity.DatasourceBaseURL || pair.ConfigSHA != rotation.NewConfigSHA ||
			pair.KeyID != rotation.NewKeyID || pair.PublicationEpoch != identity.PublicationEpoch {
			return ErrNextcloudEventScope
		}
		var unfinishedRotations int64
		if err := tx.Model(&NextcloudSourceRotation{}).
			Where("pair_operation_id = ? AND operation_id <> ? AND state IN ?", pair.OperationID,
				rotation.OperationID, []string{"pending", "committed", "switched"}).
			Count(&unfinishedRotations).Error; err != nil {
			return fmt.Errorf("check other Nextcloud source rotations: %w", err)
		}
		if unfinishedRotations != 0 {
			return ErrNextcloudEventScope
		}
		if connection.DatasourceConfigSHA256 == rotation.NewConfigSHA {
			return nil // exact operation replay; never rewrite a live dispatch
		}
		if connection.DatasourceConfigSHA256 != rotation.OldConfigSHA {
			return ErrNextcloudEventScope
		}
		var dispatch nextcloudEventDispatchRow
		if err := tx.Table("nextcloud_event_dispatch").Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("connection_id = ?", connection.ConnectionID).Take(&dispatch).Error; err != nil {
			return fmt.Errorf("read Nextcloud event dispatch: %w", err)
		}
		var checkpoint nextcloudEventCheckpoint
		if err := tx.Table("nextcloud_event_checkpoint").Where("connection_id = ?", connection.ConnectionID).
			Take(&checkpoint).Error; err != nil {
			return fmt.Errorf("read Nextcloud event checkpoint: %w", err)
		}
		if checkpoint.ReceivedID < dispatch.DispatchedID || dispatch.DispatchedID != dispatch.AppliedID ||
			dispatch.TargetEventID > dispatch.AppliedID || dispatch.LeaseToken != nil || dispatch.LeaseUntil != nil ||
			(dispatch.State != "idle" && (dispatch.State != "blocked" || dispatch.LastErrorCode != "source_changed")) {
			return ErrNextcloudEventRebindUnsafe
		}
		// A running manual/scheduled task may still be writing this source even
		// when the event dispatch row is idle. Do not guess its outcome.
		var running int64
		if err := tx.Table("sync_logs").Where("data_source_id = ? AND status = ?",
			identity.DatasourceID, types.SyncLogStatusRunning).Count(&running).Error; err != nil {
			return fmt.Errorf("check Nextcloud sync admission: %w", err)
		}
		if running != 0 {
			return ErrNextcloudEventRebindUnsafe
		}
		updated := tx.Table(
			"nextcloud_event_connections",
		).Where(("connection_id = ? AND status = 'active' AND datasource_" +
			"config_sha256 = ?"),
			connection.ConnectionID, rotation.OldConfigSHA).
			Updates(map[string]any{"datasource_config_sha256": rotation.NewConfigSHA, "updated_at": time.Now().UTC()})
		if updated.Error != nil {
			return fmt.Errorf("rebind Nextcloud event connection: %w", updated.Error)
		}
		if updated.RowsAffected != 1 {
			return ErrNextcloudEventScope
		}
		if dispatch.State == "blocked" {
			if err := tx.Table(
				"nextcloud_event_dispatch",
			).Where(("connection_id = ? AND state = 'blocked' AND last_error_" +
				"code = 'source_changed'"),
				connection.ConnectionID).Updates(map[string]any{
				"state": "idle", "last_error_code": "",
				"next_attempt_at": time.Now().UTC(), "updated_at": time.Now().UTC(),
			}).Error; err != nil {
				return fmt.Errorf("resume Nextcloud event dispatch: %w", err)
			}
		}
		changed = true
		return nil
	})
	return changed, err
}

func nextcloudRotationOnlyChangesMachineKey(rotation NextcloudSourceRotation) bool {
	oldSource := &types.DataSource{Config: rotation.OldConfig}
	newSource := &types.DataSource{Config: rotation.NewConfig}
	oldConfig, oldErr := oldSource.ParseConfig()
	newConfig, newErr := newSource.ParseConfig()
	if oldErr != nil || newErr != nil || oldConfig == nil || newConfig == nil ||
		oldConfig.Credentials["key_id"] != rotation.OldKeyID ||
		newConfig.Credentials["key_id"] != rotation.NewKeyID || rotation.OldKeyID == rotation.NewKeyID {
		return false
	}
	oldToken, okOld := oldConfig.Credentials["token"].(string)
	newToken, okNew := newConfig.Credentials["token"].(string)
	if !okOld || !okNew || oldToken == "" || newToken == "" || oldToken == newToken {
		return false
	}
	delete(oldConfig.Credentials, "token")
	delete(oldConfig.Credentials, "key_id")
	delete(newConfig.Credentials, "token")
	delete(newConfig.Credentials, "key_id")
	return reflect.DeepEqual(oldConfig, newConfig)
}
