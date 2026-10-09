package repository

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// SignedNextcloudFileStatusRequest is a nonce-bound query by the Nextcloud
// instance that owns an active event connection. The response is signed with
// that same connection key, including the exact request signature.
type SignedNextcloudFileStatusRequest struct {
	SignedNextcloudEventStatusRequest
	FileID           int64
	SourceETag       string
	RequestSignature string
}

// NextcloudFileStatus contains the public source-file status projection.
type NextcloudFileStatus struct {
	ConnectionID        string  `json:"connection_id"`
	NextcloudInstanceID string  `json:"nextcloud_instance_id"`
	BindingID           string  `json:"binding_id"`
	TenantID            string  `json:"tenant_id"`
	KnowledgeBaseID     string  `json:"knowledge_base_id"`
	DataSourceID        string  `json:"data_source_id"`
	FileID              string  `json:"file_id"`
	SourceETag          string  `json:"source_etag"`
	KnowledgeState      string  `json:"knowledge_state"`
	FailureCode         string  `json:"failure_code,omitempty"`
	PublishedSourceETag *string `json:"published_source_etag"`
	KnowledgeReadyAt    *int64  `json:"knowledge_ready_at"`
	QAAvailable         bool    `json:"qa_available"`
}

// SignedFileStatus reads one source version only after authenticating the
// connection and its current pairing. A historical published row is never
// enough to report ready for a different source ETag.
func (r *NextcloudEventInboxRepository) SignedFileStatus(
	ctx context.Context, request SignedNextcloudFileStatusRequest,
) ([]byte, string, error) {
	if request.FileID < 1 || request.SourceETag == "" || len(request.SourceETag) > 256 {
		return nil, "", ErrNextcloudEventScope
	}
	var body []byte
	var signature string
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Name() == "sqlite" {
			lock := tx.Exec(`UPDATE nextcloud_event_connections SET status = status
				WHERE connection_id = ? AND status = 'active'`, request.ConnectionID)
			if lock.Error != nil {
				return fmt.Errorf("lock Nextcloud file status connection: %w", lock.Error)
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
			return fmt.Errorf("read Nextcloud file status connection: %w", err)
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
		// A valid signature for another operation cannot reuse its nonce here.
		if err := tx.Table("nextcloud_event_nonces").
			Where("connection_id = ? AND expires_at < ?", request.ConnectionID, request.Now).
			Delete(nil).Error; err != nil {
			return fmt.Errorf("prune Nextcloud file status nonces: %w", err)
		}
		nonce := map[string]any{
			"connection_id": request.ConnectionID,
			"key_id":        request.KeyID,
			"nonce":         request.Nonce,
			"expires_at":    request.Now.Add(601 * time.Second),
		}
		insert := tx.Table("nextcloud_event_nonces").Clauses(clause.OnConflict{DoNothing: true}).Create(nonce)
		if insert.Error != nil {
			return fmt.Errorf("record Nextcloud file status nonce: %w", insert.Error)
		}
		if insert.RowsAffected != 1 {
			return ErrNextcloudEventUnauthorized
		}

		status := NextcloudFileStatus{
			ConnectionID: connection.ConnectionID, NextcloudInstanceID: connection.NextcloudInstanceID,
			BindingID: connection.BindingID, TenantID: strconv.FormatUint(connection.TenantID, 10),
			KnowledgeBaseID: connection.KnowledgeBaseID, DataSourceID: connection.DatasourceID,
			FileID: strconv.FormatInt(request.FileID, 10), SourceETag: request.SourceETag,
			KnowledgeState: "unverified", QAAvailable: false,
		}
		if err := readNextcloudFileVersionStatus(tx, connection, request, &status); err != nil {
			return err
		}
		var err error
		body, err = json.Marshal(status)
		if err != nil {
			return err
		}
		secret, err := nextcloudEventResponseSecret(connection, request.KeyID)
		if err != nil {
			return err
		}
		canonical := nextcloudFileStatusResponseCanonical(request, body)
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write(canonical)
		signature = hex.EncodeToString(mac.Sum(nil))
		return nil
	})
	return body, signature, err
}

func readNextcloudFileVersionStatus(tx *gorm.DB, connection nextcloudEventConnection,
	request SignedNextcloudFileStatusRequest, status *NextcloudFileStatus,
) error {
	externalID := "nextcloud:" + connection.NextcloudInstanceID + ":" + status.FileID
	var version nextcloudSourceVersion
	err := nextcloudVersionQuery(tx, connection.TenantID, connection.KnowledgeBaseID,
		connection.DatasourceID, externalID).
		Clauses(clause.Locking{Strength: "SHARE"}).Take(&version).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read Nextcloud source version: %w", err)
	}
	if version.State == "tombstone" || version.DesiredETag == "" || version.CandidateKnowledgeID == "" {
		return nil
	}
	var candidate types.Knowledge
	err = tx.Clauses(clause.Locking{Strength: "SHARE"}).
		Where("tenant_id = ? AND knowledge_base_id = ? AND id = ? AND channel = ?",
			connection.TenantID, connection.KnowledgeBaseID, version.CandidateKnowledgeID,
			types.ConnectorTypeNextcloud).Take(&candidate).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read Nextcloud candidate: %w", err)
	}
	metadata, err := nextcloudMetadata(&candidate)
	if err != nil || nextcloudMetadataString(metadata, "datasource_id") != connection.DatasourceID ||
		nextcloudMetadataString(metadata, "external_id") != externalID ||
		nextcloudMetadataString(metadata, "nextcloud_instance_id") != connection.NextcloudInstanceID ||
		nextcloudMetadataString(metadata, "nextcloud_binding_id") != connection.BindingID ||
		nextcloudMetadataString(metadata, "nextcloud_file_id") != status.FileID ||
		nextcloudMetadataString(metadata, "nextcloud_target_etag") != version.DesiredETag {
		return nil
	}
	if version.DesiredETag == request.SourceETag {
		if candidate.ParseStatus == types.ParseStatusFailed {
			status.KnowledgeState = "failed"
			if candidate.ErrorMessage == NextcloudNoRetrievableContentCode {
				status.FailureCode = NextcloudNoRetrievableContentCode
			}
			return nil
		}
		if NextcloudTextChunkRequired(candidate.FileType) &&
			candidate.ParseStatus == types.ParseStatusCompleted && candidate.EnableStatus == "enabled" {
			hasText, err := nextcloudHasRetrievableTextChunk(tx, &candidate)
			if err != nil {
				return fmt.Errorf("read Nextcloud file status text chunks: %w", err)
			}
			if !hasText {
				status.KnowledgeState = "failed"
				status.FailureCode = NextcloudNoRetrievableContentCode
				return nil
			}
		}
	}
	if version.State == "published" && candidate.ParseStatus == types.ParseStatusCompleted &&
		candidate.EnableStatus == "enabled" &&
		nextcloudMetadataString(metadata, "nextcloud_etag") == version.DesiredETag {
		published := version.DesiredETag
		status.PublishedSourceETag = &published
		if published == request.SourceETag {
			status.KnowledgeState = "ready"
			readyAt := version.UpdatedAt.Unix()
			if readyAt > 0 {
				status.KnowledgeReadyAt = &readyAt
			}
		} else {
			status.KnowledgeState = "updating"
		}
		return nil
	}
	if version.State == "staging" {
		if version.DesiredETag != request.SourceETag {
			status.KnowledgeState = "updating"
		} else {
			status.KnowledgeState = "updating"
		}
	}
	return nil
}

func nextcloudEventResponseSecret(connection nextcloudEventConnection, keyID string) ([]byte, error) {
	var ciphertext string
	switch {
	case keyID == connection.CurrentKeyID:
		ciphertext = connection.CurrentSecretCiphertext
	case connection.PreviousKeyID != nil && keyID == *connection.PreviousKeyID &&
		connection.PreviousValidUntil != nil && time.Now().UTC().Before(*connection.PreviousValidUntil) &&
		connection.PreviousSecretCiphertext != nil:
		ciphertext = *connection.PreviousSecretCiphertext
	default:
		return nil, ErrNextcloudEventUnauthorized
	}
	if !isEncryptedNextcloudSecret(ciphertext) {
		return nil, ErrNextcloudEventUnauthorized
	}
	secret, err := utils.DecryptStoredSecret(ciphertext)
	if err != nil {
		return nil, fmt.Errorf("decrypt Nextcloud status key: %w", err)
	}
	if len(secret) < 32 || len(secret) > 128 {
		return nil, ErrNextcloudEventUnauthorized
	}
	return []byte(secret), nil
}

func isEncryptedNextcloudSecret(ciphertext string) bool {
	return len(ciphertext) > len(utils.EncPrefix) && ciphertext[:len(utils.EncPrefix)] == utils.EncPrefix
}

func nextcloudFileStatusResponseCanonical(request SignedNextcloudFileStatusRequest, body []byte) []byte {
	bodyHash := sha256.Sum256(body)
	return []byte("weknora-file-status-hmac-sha256-v1\n" + request.RequestSignature + "\n" +
		hex.EncodeToString(bodyHash[:]) + "\n" + request.ConnectionID + "\n" +
		request.KeyID + "\n" + request.Nonce)
}
