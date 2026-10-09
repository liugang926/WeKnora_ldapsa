package repository

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrNextcloudEventConnectionExists reports an already configured event connection.
var ErrNextcloudEventConnectionExists = apperrors.NewProtocolError(
	errors.New("nextcloud event connection already exists"), "Nextcloud event connection already exists",
)

// ErrNextcloudEventConnectionMissing reports an unavailable event connection.
var ErrNextcloudEventConnectionMissing = apperrors.NewProtocolError(
	errors.New("nextcloud event connection is not active"), "Nextcloud event connection is not active",
)

const nextcloudEventRotationGrace = 2 * time.Minute

// NextcloudEventPairingIdentity is populated from a signed, live Nextcloud
// capabilities/bindings read and the corresponding committed data source.
type NextcloudEventPairingIdentity struct {
	TenantID            uint64
	KnowledgeBaseID     string
	DatasourceID        string
	NextcloudInstanceID string
	BindingID           string
	DatasourceBaseURL   string
	DatasourceConfigSHA string
	PublicationEpoch    int64
	PublicationState    string
}

// NextcloudEventConnectionCredential is returned only by a successful pair
// or rotation. The secret is never recoverable from a management read API.
type NextcloudEventConnectionCredential struct {
	ConnectionID        string `json:"connection_id"`
	NextcloudInstanceID string `json:"nextcloud_instance_id"`
	BindingID           string `json:"binding_id"`
	KeyID               string `json:"key_id"`
	Secret              string `json:"secret"`
}

// NextcloudEventConnectionStatus deliberately omits the signing secret.
// The received watermark is a decimal string, matching the event protocol.
type NextcloudEventConnectionStatus struct {
	ConnectionID             string `json:"connection_id"`
	NextcloudInstanceID      string `json:"nextcloud_instance_id"`
	BindingID                string `json:"binding_id"`
	KeyID                    string `json:"key_id"`
	Status                   string `json:"status"`
	ReceivedThroughEventID   string `json:"received_through_event_id"`
	DatasourceBaseURL        string `json:"-"`
	DatasourceConfigSHA      string `json:"-"`
	DispatchedThroughEventID string `json:"dispatched_through_event_id"`
	AppliedThroughEventID    string `json:"applied_through_event_id"`
	BacklogCount             int64  `json:"backlog_count"`
	UndispatchedCount        int64  `json:"undispatched_count"`
	DispatchState            string `json:"dispatch_state"`
	LastErrorCode            string `json:"last_error_code"`
	LastSyncLogID            string `json:"last_sync_log_id,omitempty"`
}

// ConnectionStatus loads the event connection's persisted status.
func (r *NextcloudEventInboxRepository) ConnectionStatus(
	ctx context.Context,
	tenantID uint64,
	datasourceID string,
) (NextcloudEventConnectionStatus, error) {
	if tenantID == 0 || datasourceID == "" {
		return NextcloudEventConnectionStatus{}, ErrNextcloudEventScope
	}
	var connection nextcloudEventConnection
	err := r.db.WithContext(ctx).Table("nextcloud_event_connections").
		Where("tenant_id = ? AND datasource_id = ?", tenantID, datasourceID).
		Order("created_at DESC").Take(&connection).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return NextcloudEventConnectionStatus{}, ErrNextcloudEventConnectionMissing
		}
		return NextcloudEventConnectionStatus{}, fmt.Errorf("read Nextcloud event connection status: %w", err)
	}
	var checkpoint nextcloudEventCheckpoint
	if err := r.db.WithContext(ctx).Table("nextcloud_event_checkpoint").
		Where("connection_id = ?", connection.ConnectionID).Take(&checkpoint).Error; err != nil {
		return NextcloudEventConnectionStatus{}, fmt.Errorf("read Nextcloud event checkpoint: %w", err)
	}
	var dispatch nextcloudEventDispatchRow
	if err := r.db.WithContext(ctx).Table("nextcloud_event_dispatch").
		Where("connection_id = ?", connection.ConnectionID).Take(&dispatch).Error; err != nil {
		return NextcloudEventConnectionStatus{}, fmt.Errorf("read Nextcloud event dispatch: %w", err)
	}
	var backlog, undispatched int64
	if err := r.db.WithContext(ctx).Table("nextcloud_event_inbox").
		Where("connection_id = ? AND event_id > ?", connection.ConnectionID, dispatch.AppliedID).
		Count(&backlog).Error; err != nil {
		return NextcloudEventConnectionStatus{}, err
	}
	if err := r.db.WithContext(ctx).Table("nextcloud_event_inbox").
		Where("connection_id = ? AND event_id > ?", connection.ConnectionID, dispatch.DispatchedID).
		Count(&undispatched).Error; err != nil {
		return NextcloudEventConnectionStatus{}, err
	}
	status := connection.Status
	if status == "active" {
		if err := activeNextcloudSourcePairing(r.db.WithContext(ctx), connection.TenantID,
			connection.KnowledgeBaseID, connection.DatasourceID,
			connection.NextcloudInstanceID, connection.BindingID,
			connection.DatasourceBaseURL, connection.DatasourceConfigSHA256); err != nil {
			status = "source_unpaired"
		}
	}
	syncLogID := ""
	if dispatch.LastSyncLogID != nil {
		syncLogID = *dispatch.LastSyncLogID
	}
	return NextcloudEventConnectionStatus{
		ConnectionID: connection.ConnectionID, NextcloudInstanceID: connection.NextcloudInstanceID,
		BindingID: connection.BindingID, KeyID: connection.CurrentKeyID,
		Status: status, ReceivedThroughEventID: strconv.FormatInt(checkpoint.ReceivedID, 10),
		DatasourceBaseURL:        connection.DatasourceBaseURL,
		DatasourceConfigSHA:      connection.DatasourceConfigSHA256,
		DispatchedThroughEventID: strconv.FormatInt(dispatch.DispatchedID, 10),
		AppliedThroughEventID:    strconv.FormatInt(dispatch.AppliedID, 10),
		BacklogCount:             backlog, UndispatchedCount: undispatched,
		DispatchState: dispatch.State, LastErrorCode: dispatch.LastErrorCode,
		LastSyncLogID: syncLogID,
	}, nil
}

func newNextcloudEventCredential() (string, string, string, error) {
	keyBytes := make([]byte, 16)
	secretBytes := make([]byte, 32)
	if _, err := rand.Read(keyBytes); err != nil {
		return "", "", "", fmt.Errorf("generate Nextcloud event key ID: %w", err)
	}
	if _, err := rand.Read(secretBytes); err != nil {
		return "", "", "", fmt.Errorf("generate Nextcloud event secret: %w", err)
	}
	secret := base64.RawURLEncoding.EncodeToString(secretBytes)
	key := utils.GetAESKey()
	if len(key) != 32 {
		return "", "", "", apperrors.NewProtocolError(errors.New(
			"nextcloud event encryption key unavailable",
		), "Nextcloud event encryption key unavailable")
	}
	ciphertext, err := utils.EncryptAESGCM(secret, key)
	if err != nil || ciphertext == secret {
		return "", "", "", apperrors.NewProtocolError(errors.New(
			"nextcloud event encryption key unavailable",
		), "Nextcloud event encryption key unavailable")
	}
	return "evt_" + hex.EncodeToString(keyBytes), secret, ciphertext, nil
}

// Pair creates a new connection only after the live-validated source identity
// still matches the committed row. It never reissues an existing secret.
func (
	r *NextcloudEventInboxRepository,
) Pair(ctx context.Context, identity NextcloudEventPairingIdentity) (NextcloudEventConnectionCredential, error) {
	keyID, secret, ciphertext, err := newNextcloudEventCredential()
	if err != nil {
		return NextcloudEventConnectionCredential{}, err
	}
	connectionID := uuid.NewString()
	result := NextcloudEventConnectionCredential{
		ConnectionID: connectionID, NextcloudInstanceID: identity.NextcloudInstanceID,
		BindingID: identity.BindingID, KeyID: keyID, Secret: secret,
	}
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// An existing active row is a terminal Pair conflict. Check it before
		// taking the KB writer lock: a concurrent receiver/rebind owns the
		// connection row first and will next take that same KB lock. Waiting
		// on the active unique index while holding KB would deadlock them.
		var active int64
		if err := tx.Table("nextcloud_event_connections").
			Where("datasource_id = ? AND status = 'active'", identity.DatasourceID).
			Count(&active).Error; err != nil {
			return fmt.Errorf("check active Nextcloud event connection: %w", err)
		}
		if active != 0 {
			return ErrNextcloudEventConnectionExists
		}
		if err := checkNextcloudPairingSource(tx, identity); err != nil {
			return err
		}
		row := map[string]any{
			"connection_id": connectionID, "tenant_id": identity.TenantID,
			"knowledge_base_id": identity.KnowledgeBaseID, "datasource_id": identity.DatasourceID,
			"nextcloud_instance_id": identity.NextcloudInstanceID, "binding_id": identity.BindingID,
			"datasource_base_url":      identity.DatasourceBaseURL,
			"datasource_config_sha256": identity.DatasourceConfigSHA,
			"status":                   "active", "current_key_id": keyID,
			"current_secret_ciphertext": ciphertext,
			"created_at":                time.Now().UTC(), "updated_at": time.Now().UTC(),
		}
		insert := tx.Table("nextcloud_event_connections").Clauses(clause.OnConflict{DoNothing: true}).Create(row)
		if insert.Error != nil {
			return fmt.Errorf("create Nextcloud event connection: %w", insert.Error)
		}
		if insert.RowsAffected != 1 {
			return ErrNextcloudEventConnectionExists
		}
		checkpoint := map[string]any{"connection_id": connectionID, "received_id": int64(0)}
		if err := tx.Table("nextcloud_event_checkpoint").Create(checkpoint).Error; err != nil {
			return fmt.Errorf("initialize Nextcloud event checkpoint: %w", err)
		}
		dispatch := map[string]any{
			"connection_id": connectionID, "dispatched_id": int64(0),
			"applied_id": int64(0), "state": "idle", "target_event_id": int64(0),
			"attempt_count": 0, "next_attempt_at": time.Now().UTC(), "last_error_code": "",
			"updated_at": time.Now().UTC(),
		}
		if err := tx.Table("nextcloud_event_dispatch").Create(dispatch).Error; err != nil {
			return fmt.Errorf("initialize Nextcloud event dispatch: %w", err)
		}
		return nil
	})
	if err != nil {
		return NextcloudEventConnectionCredential{}, err
	}
	return result, nil
}

// Rotate accepts the old and new keys for a short, server-clock grace period.
// A second rotation replaces the previous key, so no more than two are valid.
func (
	r *NextcloudEventInboxRepository,
) Rotate(ctx context.Context, identity NextcloudEventPairingIdentity) (NextcloudEventConnectionCredential, error) {
	keyID, secret, ciphertext, err := newNextcloudEventCredential()
	if err != nil {
		return NextcloudEventConnectionCredential{}, err
	}
	var result NextcloudEventConnectionCredential
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Name() == "sqlite" {
			lock := tx.Exec(("UPDATE nextcloud_event_connections SET status = status\n" +
				"\t\t\t\tWHERE tenant_id = ? AND datasource_id = ? AND statu" +
				"s = 'active'"), identity.TenantID, identity.DatasourceID)
			if lock.Error != nil {
				return fmt.Errorf("lock Nextcloud event connection: %w", lock.Error)
			}
			if lock.RowsAffected != 1 {
				return ErrNextcloudEventConnectionMissing
			}
		}
		var connection nextcloudEventConnection
		if err := tx.Table("nextcloud_event_connections").
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where(("tenant_id = ? AND datasource_id = ? AND status = 'activ" +
				"e'"), identity.TenantID, identity.DatasourceID).
			Take(&connection).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNextcloudEventConnectionMissing
			}
			return fmt.Errorf("read Nextcloud event connection: %w", err)
		}
		// Receive takes the connection lock before checking the KB/source.
		// Keep that order to avoid a PostgreSQL deadlock with event ingestion.
		if err := checkNextcloudPairingSource(tx, identity); err != nil {
			return err
		}
		if connection.KnowledgeBaseID != identity.KnowledgeBaseID ||
			connection.NextcloudInstanceID != identity.NextcloudInstanceID ||
			connection.BindingID != identity.BindingID ||
			connection.DatasourceBaseURL != identity.DatasourceBaseURL ||
			connection.DatasourceConfigSHA256 != identity.DatasourceConfigSHA {
			return ErrNextcloudEventScope
		}
		now := time.Now().UTC()
		updates := map[string]any{
			"current_key_id": keyID, "current_secret_ciphertext": ciphertext,
			"previous_key_id":            connection.CurrentKeyID,
			"previous_secret_ciphertext": connection.CurrentSecretCiphertext,
			"previous_valid_until":       now.Add(nextcloudEventRotationGrace),
			"updated_at":                 now,
		}
		if err := tx.Table("nextcloud_event_connections").
			Where("connection_id = ? AND status = 'active'", connection.ConnectionID).
			Updates(updates).Error; err != nil {
			return fmt.Errorf("rotate Nextcloud event connection: %w", err)
		}
		result = NextcloudEventConnectionCredential{
			ConnectionID:        connection.ConnectionID,
			NextcloudInstanceID: connection.NextcloudInstanceID,
			BindingID:           connection.BindingID, KeyID: keyID, Secret: secret,
		}
		return nil
	})
	if err != nil {
		return NextcloudEventConnectionCredential{}, err
	}
	return result, nil
}

// Revoke remains possible after credentials or endpoint drift so a tenant
// administrator can terminate a stale connection without contacting source.
func (r *NextcloudEventInboxRepository) Revoke(ctx context.Context, tenantID uint64, datasourceID string) error {
	if tenantID == 0 || datasourceID == "" {
		return ErrNextcloudEventScope
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Name() == "sqlite" {
			lock := tx.Exec(`UPDATE nextcloud_event_connections SET status = status
				WHERE tenant_id = ? AND datasource_id = ? AND status = 'active'`, tenantID, datasourceID)
			if lock.Error != nil {
				return lock.Error
			}
			if lock.RowsAffected != 1 {
				return ErrNextcloudEventConnectionMissing
			}
		}
		var connection nextcloudEventConnection
		if err := tx.Table("nextcloud_event_connections").Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("tenant_id = ? AND datasource_id = ? AND status = 'active'", tenantID, datasourceID).
			Take(&connection).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNextcloudEventConnectionMissing
			}
			return fmt.Errorf("read Nextcloud event connection: %w", err)
		}
		result := tx.Table(
			"nextcloud_event_connections",
		).Where("connection_id = ? AND status = 'active'", connection.ConnectionID).
			Updates(map[string]any{
				"status": "revoked", "previous_key_id": nil,
				"previous_secret_ciphertext": nil, "previous_valid_until": nil,
				"updated_at": time.Now().UTC(),
			})
		if result.Error != nil {
			return fmt.Errorf("revoke Nextcloud event connection: %w", result.Error)
		}
		if result.RowsAffected != 1 {
			return ErrNextcloudEventConnectionMissing
		}
		return tx.Table("nextcloud_event_dispatch").Where("connection_id = ?", connection.ConnectionID).
			Updates(map[string]any{
				"state": "blocked", "lease_token": nil, "lease_until": nil,
				"last_error_code": "revoked", "updated_at": time.Now().UTC(),
			}).Error
	})
}

func checkNextcloudPairingSource(tx *gorm.DB, identity NextcloudEventPairingIdentity) error {
	if identity.TenantID == 0 || identity.KnowledgeBaseID == "" || identity.DatasourceID == "" ||
		identity.NextcloudInstanceID == "" || identity.BindingID == "" ||
		identity.DatasourceBaseURL == "" || len(identity.DatasourceConfigSHA) != 64 {
		return ErrNextcloudEventScope
	}
	// Source writers already use this KB row as the serialization point.
	// SQLite needs an actual write before any reads; PostgreSQL uses the same
	// no-op UPDATE as the writer to serialize with config edits/deletes.
	lock := tx.Exec(`UPDATE knowledge_bases
		SET ever_had_nextcloud_source = ever_had_nextcloud_source
		WHERE id = ? AND tenant_id = ? AND deleted_at IS NULL`, identity.KnowledgeBaseID, identity.TenantID)
	if lock.Error != nil {
		return fmt.Errorf("lock Nextcloud pairing knowledge base: %w", lock.Error)
	}
	if lock.RowsAffected != 1 {
		return ErrNextcloudEventScope
	}
	var kb types.KnowledgeBase
	if err := tx.Where("id = ? AND tenant_id = ?", identity.KnowledgeBaseID, identity.TenantID).Take(&kb).Error; err !=
		nil {
		return ErrNextcloudEventScope
	}
	if !kb.EverHadNextcloudSource {
		return ErrNextcloudEventScope
	}
	var ds types.DataSource
	if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND type = ?",
		identity.DatasourceID, identity.TenantID, identity.KnowledgeBaseID, types.ConnectorTypeNextcloud).
		Take(&ds).Error; err != nil {
		return ErrNextcloudEventScope
	}
	var otherSources int64
	if err := tx.Unscoped().Model(&types.DataSource{}).
		Where("knowledge_base_id = ? AND tenant_id = ? AND id <> ?",
			identity.KnowledgeBaseID, identity.TenantID, identity.DatasourceID).
		Count(&otherSources).Error; err != nil {
		return fmt.Errorf("check dedicated Nextcloud pairing source: %w", err)
	}
	if otherSources != 0 {
		return ErrNextcloudEventScope
	}
	baseURL, configSHA, bindingID, err := NextcloudEventDataSourceIdentity(ds.Config)
	if err != nil || baseURL != identity.DatasourceBaseURL ||
		configSHA != identity.DatasourceConfigSHA || bindingID != identity.BindingID {
		return ErrNextcloudEventScope
	}
	return activeNextcloudSourcePairing(tx, identity.TenantID, identity.KnowledgeBaseID,
		identity.DatasourceID, identity.NextcloudInstanceID, identity.BindingID,
		identity.DatasourceBaseURL, identity.DatasourceConfigSHA)
}
