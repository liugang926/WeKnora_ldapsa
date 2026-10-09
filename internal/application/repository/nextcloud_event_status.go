package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// SignedNextcloudEventStatusRequest is authenticated with the connection's
// event key. Its canonical bytes bind the method, path and exact query string.
type SignedNextcloudEventStatusRequest struct {
	ConnectionID     string
	KeyID            string
	Nonce            string
	Signature        []byte
	CanonicalRequest []byte
	Now              time.Time
}

// SignedConnectionStatus consumes the replay nonce and reads the persisted
// watermarks in one transaction. The applied watermark is advanced elsewhere
// only after source reconciliation and completed publication are proven.
func (r *NextcloudEventInboxRepository) SignedConnectionStatus(
	ctx context.Context, request SignedNextcloudEventStatusRequest,
) (NextcloudEventConnectionStatus, error) {
	var status NextcloudEventConnectionStatus
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Name() == "sqlite" {
			lock := tx.Exec(`UPDATE nextcloud_event_connections SET status = status
				WHERE connection_id = ? AND status = 'active'`, request.ConnectionID)
			if lock.Error != nil {
				return fmt.Errorf("lock Nextcloud event connection: %w", lock.Error)
			}
			if lock.RowsAffected != 1 {
				return ErrNextcloudEventUnauthorized
			}
		}
		var connection nextcloudEventConnection
		if err := tx.Table("nextcloud_event_connections").
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("connection_id = ? AND status = 'active'", request.ConnectionID).
			Take(&connection).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNextcloudEventUnauthorized
			}
			return fmt.Errorf("read Nextcloud event connection: %w", err)
		}
		if err := verifyNextcloudEventSignature(connection, SignedNextcloudEventBatch{
			KeyID: request.KeyID, Signature: request.Signature,
			CanonicalRequest: request.CanonicalRequest, Now: request.Now,
		}); err != nil {
			return err
		}
		if err := checkNextcloudEventDataSource(tx, connection); err != nil {
			return err
		}
		if err := tx.Table("nextcloud_event_nonces").
			Where("connection_id = ? AND expires_at < ?", request.ConnectionID, request.Now).
			Delete(nil).Error; err != nil {
			return fmt.Errorf("prune Nextcloud event status nonces: %w", err)
		}
		nonce := map[string]any{
			"connection_id": request.ConnectionID,
			"key_id":        request.KeyID,
			"nonce":         request.Nonce,
			"expires_at":    request.Now.Add(601 * time.Second),
		}
		insert := tx.Table("nextcloud_event_nonces").Clauses(clause.OnConflict{DoNothing: true}).Create(nonce)
		if insert.Error != nil {
			return fmt.Errorf("record Nextcloud event status nonce: %w", insert.Error)
		}
		if insert.RowsAffected != 1 {
			return ErrNextcloudEventUnauthorized
		}
		var err error
		status, err = (&NextcloudEventInboxRepository{db: tx}).ConnectionStatus(
			ctx, connection.TenantID, connection.DatasourceID)
		if err != nil {
			return err
		}
		if status.ConnectionID != request.ConnectionID || status.Status != "active" ||
			status.NextcloudInstanceID != connection.NextcloudInstanceID ||
			status.BindingID != connection.BindingID {
			return ErrNextcloudEventScope
		}
		return nil
	})
	return status, err
}
