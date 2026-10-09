package repository

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// These errors contain no credentials or source paths and are safe to map to
// a machine response. The incoming events are hints, never delete commands.
var (
	ErrNextcloudEventUnauthorized = apperrors.NewProtocolError(errors.New(
		"nextcloud event credential is invalid",
	), "Nextcloud event credential is invalid")
	ErrNextcloudEventScope = apperrors.NewProtocolError(
		errors.New(
			("nextcloud event connection is no longer bound to this d" +
				"ata source")), "Nextcloud event connection is no longer bound to this data source")
	ErrNextcloudEventConflict = apperrors.NewProtocolError(errors.New(
		"nextcloud event batch conflicts with the durable inbox",
	), "Nextcloud event batch conflicts with the durable inbox")
)

// NextcloudEventHint contains an untrusted source event hint.
type NextcloudEventHint struct {
	EventID       int64
	PayloadSHA256 string
	EventType     string
	FileID        *int64
	ETag          *string
	Path          *string
	RelativePath  *string
}

// SignedNextcloudEventBatch contains a signed source event batch.
type SignedNextcloudEventBatch struct {
	ConnectionID        string
	NextcloudInstanceID string
	BindingID           string
	KeyID               string
	Nonce               string
	Signature           []byte
	CanonicalRequest    []byte
	AfterEventID        int64
	Events              []NextcloudEventHint
	Now                 time.Time
}

// NextcloudEventInboxRepository persists source event batches and checkpoints.
type NextcloudEventInboxRepository struct{ db *gorm.DB }

// NewNextcloudEventInboxRepository returns the durable source event-inbox repository.
func NewNextcloudEventInboxRepository(db *gorm.DB) *NextcloudEventInboxRepository {
	return &NextcloudEventInboxRepository{db: db}
}

type nextcloudEventConnection struct {
	ConnectionID             string     `gorm:"column:connection_id"`
	TenantID                 uint64     `gorm:"column:tenant_id"`
	KnowledgeBaseID          string     `gorm:"column:knowledge_base_id"`
	DatasourceID             string     `gorm:"column:datasource_id"`
	NextcloudInstanceID      string     `gorm:"column:nextcloud_instance_id"`
	BindingID                string     `gorm:"column:binding_id"`
	DatasourceBaseURL        string     `gorm:"column:datasource_base_url"`
	DatasourceConfigSHA256   string     `gorm:"column:datasource_config_sha256"`
	Status                   string     `gorm:"column:status"`
	CurrentKeyID             string     `gorm:"column:current_key_id"`
	CurrentSecretCiphertext  string     `gorm:"column:current_secret_ciphertext"`
	PreviousKeyID            *string    `gorm:"column:previous_key_id"`
	PreviousSecretCiphertext *string    `gorm:"column:previous_secret_ciphertext"`
	PreviousValidUntil       *time.Time `gorm:"column:previous_valid_until"`
}

type nextcloudEventCheckpoint struct {
	ConnectionID string `gorm:"column:connection_id"`
	ReceivedID   int64  `gorm:"column:received_id"`
}

type nextcloudEventInboxRow struct {
	ConnectionID  string    `gorm:"column:connection_id"`
	EventID       int64     `gorm:"column:event_id"`
	PayloadSHA256 string    `gorm:"column:payload_sha256"`
	EventType     string    `gorm:"column:event_type"`
	FileID        *int64    `gorm:"column:file_id"`
	ETag          *string   `gorm:"column:etag"`
	Path          *string   `gorm:"column:path"`
	RelativePath  *string   `gorm:"column:relative_path"`
	ReceivedAt    time.Time `gorm:"column:received_at"`
	State         string    `gorm:"column:state"`
}

// IngestSigned atomically consumes a nonce, stores every event, and advances
// the receipt watermark. A 202 response from its caller means only that the
// inbox transaction committed. It does not acknowledge source reconciliation,
// parsing, or publication.
func (r *NextcloudEventInboxRepository) IngestSigned(ctx context.Context, batch SignedNextcloudEventBatch) (
	int64,
	error,
) {
	if batch.AfterEventID < 0 || len(batch.Events) == 0 || len(batch.Events) > 200 {
		return 0, ErrNextcloudEventConflict
	}
	previousID := batch.AfterEventID
	for _, event := range batch.Events {
		if event.EventID <= previousID || len(event.PayloadSHA256) != 64 ||
			(event.ETag != nil && len(*event.ETag) > 255) ||
			(event.Path != nil && len(*event.Path) > 4096) ||
			(event.RelativePath != nil && len(*event.RelativePath) > 4096) {
			return 0, ErrNextcloudEventConflict
		}
		previousID = event.EventID
	}
	var receivedID int64
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Name() == "sqlite" {
			// SQLite ignores SELECT FOR UPDATE. Acquire its write lock before
			// reading the paired source so a concurrent source reconfiguration
			// cannot slip between our identity check and durable receipt.
			lock := tx.Exec(`UPDATE nextcloud_event_connections SET status = status
				WHERE connection_id = ? AND status = 'active'`, batch.ConnectionID)
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
			Where("connection_id = ? AND status = ?", batch.ConnectionID, "active").
			Take(&connection).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNextcloudEventUnauthorized
			}
			return fmt.Errorf("read Nextcloud event connection: %w", err)
		}
		if connection.NextcloudInstanceID != batch.NextcloudInstanceID ||
			connection.BindingID != batch.BindingID {
			return ErrNextcloudEventScope
		}
		if err := verifyNextcloudEventSignature(connection, batch); err != nil {
			return err
		}
		if err := checkNextcloudEventDataSource(tx, connection); err != nil {
			return err
		}

		// The nonce and inbox writes share one transaction: a database error
		// never burns a nonce while returning a false durable receipt.
		if err := tx.Table("nextcloud_event_nonces").
			Where("connection_id = ? AND expires_at < ?", batch.ConnectionID, batch.Now).
			Delete(nil).Error; err != nil {
			return fmt.Errorf("prune Nextcloud event nonces: %w", err)
		}
		nonce := map[string]any{
			"connection_id": batch.ConnectionID,
			"key_id":        batch.KeyID,
			"nonce":         batch.Nonce,
			"expires_at":    batch.Now.Add(601 * time.Second),
		}
		insertNonce := tx.Table("nextcloud_event_nonces").Clauses(clause.OnConflict{DoNothing: true}).Create(nonce)
		if insertNonce.Error != nil {
			return fmt.Errorf("record Nextcloud event nonce: %w", insertNonce.Error)
		}
		if insertNonce.RowsAffected != 1 {
			return ErrNextcloudEventUnauthorized
		}

		// Lock the checkpoint so two signed, individually valid requests cannot
		// each advance from the same after_event_id. Event IDs are global and
		// can have gaps within this binding, so equality is against the prior
		// batch watermark, not a numeric +1 rule.
		var checkpoint nextcloudEventCheckpoint
		if err := tx.Table("nextcloud_event_checkpoint").
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("connection_id = ?", batch.ConnectionID).
			Take(&checkpoint).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNextcloudEventScope
			}
			return fmt.Errorf("read Nextcloud event checkpoint: %w", err)
		}
		if batch.AfterEventID > checkpoint.ReceivedID {
			return ErrNextcloudEventConflict
		}
		if batch.AfterEventID < checkpoint.ReceivedID {
			for _, hint := range batch.Events {
				if hint.EventID > checkpoint.ReceivedID {
					return ErrNextcloudEventConflict
				}
				var existing nextcloudEventInboxRow
				if err := tx.Table("nextcloud_event_inbox").
					Where("connection_id = ? AND event_id = ?", batch.ConnectionID, hint.EventID).
					Take(&existing).Error; err != nil {
					if errors.Is(err, gorm.ErrRecordNotFound) {
						return ErrNextcloudEventConflict
					}
					return fmt.Errorf("read duplicate Nextcloud event: %w", err)
				}
				if existing.PayloadSHA256 != hint.PayloadSHA256 {
					return ErrNextcloudEventConflict
				}
			}
			receivedID = checkpoint.ReceivedID
			return nil
		}

		for _, hint := range batch.Events {
			row := nextcloudEventInboxRow{
				ConnectionID:  batch.ConnectionID,
				EventID:       hint.EventID,
				PayloadSHA256: hint.PayloadSHA256,
				EventType:     hint.EventType,
				FileID:        hint.FileID,
				ETag:          hint.ETag,
				Path:          hint.Path,
				RelativePath:  hint.RelativePath,
				ReceivedAt:    batch.Now,
				State:         "pending",
			}
			insert := tx.Table("nextcloud_event_inbox").Clauses(clause.OnConflict{DoNothing: true}).Create(&row)
			if insert.Error != nil {
				return fmt.Errorf("record Nextcloud event: %w", insert.Error)
			}
			if insert.RowsAffected == 0 {
				var existing nextcloudEventInboxRow
				if err := tx.Table("nextcloud_event_inbox").
					Where("connection_id = ? AND event_id = ?", batch.ConnectionID, hint.EventID).
					Take(&existing).Error; err != nil {
					return fmt.Errorf("read existing Nextcloud event: %w", err)
				}
				if existing.PayloadSHA256 != hint.PayloadSHA256 {
					return ErrNextcloudEventConflict
				}
			}
		}
		last := batch.Events[len(batch.Events)-1].EventID
		update := tx.Table("nextcloud_event_checkpoint").
			Where("connection_id = ? AND received_id = ?", batch.ConnectionID, batch.AfterEventID).
			Update("received_id", last)
		if update.Error != nil {
			return fmt.Errorf("advance Nextcloud event receipt: %w", update.Error)
		}
		if update.RowsAffected != 1 {
			return ErrNextcloudEventConflict
		}
		receivedID = last
		return nil
	})
	return receivedID, err
}

func verifyNextcloudEventSignature(connection nextcloudEventConnection, batch SignedNextcloudEventBatch) error {
	var ciphertext string
	switch {
	case batch.KeyID == connection.CurrentKeyID:
		ciphertext = connection.CurrentSecretCiphertext
	case connection.PreviousKeyID != nil && batch.KeyID == *connection.PreviousKeyID &&
		connection.PreviousValidUntil != nil && time.Now().UTC().Before(*connection.PreviousValidUntil) &&
		connection.PreviousSecretCiphertext != nil:
		ciphertext = *connection.PreviousSecretCiphertext
	default:
		return ErrNextcloudEventUnauthorized
	}
	if !strings.HasPrefix(ciphertext, utils.EncPrefix) {
		return ErrNextcloudEventUnauthorized
	}
	secret, err := utils.DecryptStoredSecret(ciphertext)
	if err != nil {
		// Missing/rotated at-rest encryption keys are a service fault, never
		// a reason to treat ciphertext as a usable signing secret.
		return fmt.Errorf("decrypt Nextcloud event key: %w", err)
	}
	if len(secret) < 32 || len(secret) > 128 {
		return ErrNextcloudEventUnauthorized
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(batch.CanonicalRequest)
	if !hmac.Equal(mac.Sum(nil), batch.Signature) {
		return ErrNextcloudEventUnauthorized
	}
	return nil
}

func checkNextcloudEventDataSource(tx *gorm.DB, connection nextcloudEventConnection) error {
	var kb types.KnowledgeBase
	if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).
		Where("id = ? AND tenant_id = ?", connection.KnowledgeBaseID, connection.TenantID).
		Take(&kb).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrNextcloudEventScope
		}
		return fmt.Errorf("read Nextcloud event knowledge base: %w", err)
	}
	var ds types.DataSource
	if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).
		Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND type = ?",
			connection.DatasourceID, connection.TenantID, connection.KnowledgeBaseID, types.ConnectorTypeNextcloud).
		Take(&ds).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrNextcloudEventScope
		}
		return fmt.Errorf("read Nextcloud event data source: %w", err)
	}
	if ds.Status != types.DataSourceStatusActive {
		return ErrNextcloudEventScope
	}
	baseURL, configSHA256, bindingID, err := NextcloudEventDataSourceIdentity(ds.Config)
	if err != nil || bindingID != connection.BindingID ||
		baseURL != connection.DatasourceBaseURL || configSHA256 != connection.DatasourceConfigSHA256 {
		return ErrNextcloudEventScope
	}
	return activeNextcloudSourcePairing(tx, connection.TenantID, connection.KnowledgeBaseID,
		connection.DatasourceID, connection.NextcloudInstanceID, connection.BindingID,
		connection.DatasourceBaseURL, connection.DatasourceConfigSHA256)
}

// NextcloudEventDataSourceIdentity pins a connection to the exact committed
// connector configuration, including the encrypted outbound machine token.
// Credential rotation, clearing/reprovisioning, an endpoint edit, or changing
// the selected binding changes this identity and requires a new pairing. This
// reads the persisted JSON bytes, not an in-memory, potentially stale DS copy.
func NextcloudEventDataSourceIdentity(raw types.JSON) (baseURL, configSHA256, bindingID string, err error) {
	if len(raw) == 0 {
		return "", "", "", ErrNextcloudEventScope
	}
	var cfg struct {
		ResourceIDs []string               `json:"resource_ids"`
		Settings    map[string]interface{} `json:"settings"`
		Credentials map[string]interface{} `json:"credentials"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil || len(cfg.ResourceIDs) != 1 {
		return "", "", "", ErrNextcloudEventScope
	}
	base, _ := cfg.Settings["base_url"].(string)
	token, _ := cfg.Credentials["token"].(string)
	if !strings.HasPrefix(token, utils.EncPrefix) || len(token) <= len(utils.EncPrefix) {
		return "", "", "", ErrNextcloudEventScope
	}
	if plaintext, decryptErr := utils.DecryptStoredSecret(token); decryptErr != nil || plaintext == "" {
		return "", "", "", ErrNextcloudEventScope
	}
	baseURL, err = normalizeNextcloudEventBaseURL(base)
	if err != nil {
		return "", "", "", ErrNextcloudEventScope
	}
	sum := sha256.Sum256(raw)
	return baseURL, hex.EncodeToString(sum[:]), cfg.ResourceIDs[0], nil
}

func normalizeNextcloudEventBaseURL(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed == nil || parsed.Hostname() == "" || parsed.Opaque != "" ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.RawFragment != "" ||
		strings.Contains(parsed.Path, "//") {
		return "", ErrNextcloudEventScope
	}
	for _, segment := range strings.Split(parsed.EscapedPath(), "/") {
		decoded, err := url.PathUnescape(segment)
		if err != nil || decoded == "." || decoded == ".." ||
			strings.ContainsAny(decoded, "/\\\r\n") {
			return "", ErrNextcloudEventScope
		}
	}
	parsed.Host = strings.ToLower(parsed.Host)
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawPath = ""
	return parsed.String(), nil
}

// NextcloudEventPayloadHash is the canonical per-hint fingerprint used for
// duplicate event-ID detection. It is not an authentication primitive.
func NextcloudEventPayloadHash(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
