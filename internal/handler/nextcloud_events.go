package handler

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/gin-gonic/gin"
)

const (
	nextcloudEventPath          = "/api/v1/integrations/nextcloud/events"
	maxNextcloudEventBodyBytes  = 1 << 20
	maxNextcloudEventBatchItems = 200
)

// NextcloudEventHandler receives signed Nextcloud event hints.
type NextcloudEventHandler struct {
	inbox *repository.NextcloudEventInboxRepository
}

// NewNextcloudEventHandler constructs the signed event-hint handler.
func NewNextcloudEventHandler(inbox *repository.NextcloudEventInboxRepository) *NextcloudEventHandler {
	return &NextcloudEventHandler{inbox: inbox}
}

type nextcloudEventRequest struct {
	ConnectionID        string                   `json:"connection_id"`
	NextcloudInstanceID string                   `json:"nextcloud_instance_id"`
	BindingID           string                   `json:"binding_id"`
	AfterEventID        string                   `json:"after_event_id"`
	Events              []nextcloudEventHintJSON `json:"events"`
}

type nextcloudEventHintJSON struct {
	EventID      string  `json:"event_id"`
	FileID       *int64  `json:"file_id"`
	Type         string  `json:"type"`
	OldPath      *string `json:"old_path"`
	Path         *string `json:"path"`
	RelativePath *string `json:"relative_path"`
	ETag         *string `json:"etag"`
	CreatedAt    *int64  `json:"created_at"`
}

// Receive stores signed change hints. Neither a successful HTTP response nor
// a hint in the inbox means that the corresponding document was published.
func (h *NextcloudEventHandler) Receive(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	if c.Request.Method != http.MethodPost || c.Request.URL.EscapedPath() != nextcloudEventPath ||
		c.Request.URL.RawQuery != "" || c.Request.URL.ForceQuery {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request_target"})
		return
	}
	mediaType, _, mediaErr := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if mediaErr != nil || mediaType != "application/json" {
		c.JSON(http.StatusUnsupportedMediaType, gin.H{"error": "json_required"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, maxNextcloudEventBodyBytes+1))
	if err != nil || len(body) > maxNextcloudEventBodyBytes {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "event_batch_too_large"})
		return
	}
	var request nextcloudEventRequest
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || decoder.Decode(new(any)) != io.EOF {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_event_batch"})
		return
	}
	if !validNextcloudOpaqueID(request.ConnectionID, 16, 128) ||
		!validNextcloudOpaqueID(request.NextcloudInstanceID, 1, 128) ||
		!validNextcloudOpaqueID(request.BindingID, 1, 128) ||
		len(request.Events) == 0 || len(request.Events) > maxNextcloudEventBatchItems {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_event_batch"})
		return
	}
	afterID, ok := parseNextcloudEventID(request.AfterEventID, true)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_event_batch"})
		return
	}

	connectionID, okConnection := oneNextcloudHeader(c.Request, "X-Nextcloud-Connection-Id")
	keyID, okKey := oneNextcloudHeader(c.Request, "X-Nextcloud-Key-Id")
	timestamp, okTimestamp := oneNextcloudHeader(c.Request, "X-Nextcloud-Timestamp")
	nonce, okNonce := oneNextcloudHeader(c.Request, "X-Nextcloud-Nonce")
	signatureHex, okSignature := oneNextcloudHeader(c.Request, "X-Nextcloud-Signature")
	if !okConnection || connectionID != request.ConnectionID || !okKey ||
		!validNextcloudKeyID(keyID) || !okTimestamp || !okNonce || !okSignature ||
		!isLowerHex(nonce, 32) || !isLowerHex(signatureHex, 64) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	timestampValue, ok := parseNextcloudEventID(timestamp, false)
	now := time.Now().UTC()
	if !ok || timestampValue < now.Add(-5*time.Minute).Unix() ||
		timestampValue > now.Add(5*time.Minute).Unix() {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	signature, err := hex.DecodeString(signatureHex)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}

	hints := make([]repository.NextcloudEventHint, 0, len(request.Events))
	previousID := afterID
	for _, event := range request.Events {
		id, valid := parseNextcloudEventID(event.EventID, false)
		if !valid || id <= previousID || !validNextcloudEventType(event.Type) ||
			(event.FileID != nil && *event.FileID <= 0) ||
			(event.Type == "delete" && event.FileID == nil) ||
			!validOptionalNextcloudPath(event.OldPath) || !validOptionalNextcloudPath(event.Path) ||
			!validOptionalNextcloudRelativePath(event.RelativePath) ||
			(event.ETag != nil && len(*event.ETag) > 255) ||
			(event.CreatedAt != nil && *event.CreatedAt < 0) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_event_batch"})
			return
		}
		serialized, err := json.Marshal(event)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_event_batch"})
			return
		}
		hints = append(hints, repository.NextcloudEventHint{
			EventID: id, PayloadSHA256: repository.NextcloudEventPayloadHash(serialized),
			EventType: event.Type, FileID: event.FileID, ETag: event.ETag,
			Path: event.Path, RelativePath: event.RelativePath,
		})
		previousID = id
	}
	canonical := nextcloudEventCanonicalRequest(body, timestamp, nonce, connectionID, keyID)
	receivedID, err := h.inbox.IngestSigned(c.Request.Context(), repository.SignedNextcloudEventBatch{
		ConnectionID: request.ConnectionID, NextcloudInstanceID: request.NextcloudInstanceID,
		BindingID: request.BindingID, KeyID: keyID, Nonce: nonce,
		Signature: signature, CanonicalRequest: canonical, AfterEventID: afterID,
		Events: hints, Now: now,
	})
	if err != nil {
		switch {
		case errors.Is(err, repository.ErrNextcloudEventUnauthorized):
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		case errors.Is(err, repository.ErrNextcloudEventScope):
			c.JSON(http.StatusForbidden, gin.H{"error": "connection_unavailable"})
		case errors.Is(err, repository.ErrNextcloudEventConflict):
			c.JSON(http.StatusConflict, gin.H{"error": "event_sequence_conflict"})
		default:
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "event_inbox_unavailable"})
		}
		return
	}
	c.JSON(http.StatusAccepted, gin.H{
		"connection_id":             request.ConnectionID,
		"received_through_event_id": strconv.FormatInt(receivedID, 10),
		"durable_receipt_only":      true,
	})
}

func nextcloudEventCanonicalRequest(body []byte, timestamp, nonce, connectionID, keyID string) []byte {
	return []byte(strings.Join([]string{
		"nextcloud-event-hmac-sha256-v1",
		http.MethodPost,
		nextcloudEventPath,
		"", // This endpoint does not accept query parameters.
		repository.NextcloudEventPayloadHash(body),
		timestamp,
		nonce,
		connectionID,
		keyID,
	}, "\n"))
}

func oneNextcloudHeader(r *http.Request, name string) (string, bool) {
	values := r.Header.Values(name)
	if len(values) != 1 || strings.Contains(values[0], ",") || strings.TrimSpace(values[0]) != values[0] {
		return "", false
	}
	return values[0], values[0] != ""
}

func parseNextcloudEventID(raw string, allowZero bool) (int64, bool) {
	if raw == "" || len(raw) > 19 || (len(raw) > 1 && raw[0] == '0') {
		return 0, false
	}
	for _, ch := range raw {
		if ch < '0' || ch > '9' {
			return 0, false
		}
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	return id, err == nil && (allowZero || id > 0)
}

func validNextcloudOpaqueID(value string, minLength, maxLength int) bool {
	if len(value) < minLength || len(value) > maxLength {
		return false
	}
	for _, ch := range value {
		if (ch < 'A' || ch > 'Z') && (ch < 'a' || ch > 'z') &&
			(ch < '0' || ch > '9') && ch != '_' && ch != '-' {
			return false
		}
	}
	return true
}

func validNextcloudKeyID(value string) bool {
	if len(value) < 1 || len(value) > 64 {
		return false
	}
	for _, ch := range value {
		if (ch < 'A' || ch > 'Z') && (ch < 'a' || ch > 'z') &&
			(ch < '0' || ch > '9') && ch != '.' && ch != '_' && ch != '-' {
			return false
		}
	}
	return true
}

func isLowerHex(raw string, expectedLength int) bool {
	if len(raw) != expectedLength {
		return false
	}
	for _, ch := range raw {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

func validNextcloudEventType(kind string) bool {
	switch kind {
	case "upsert", "metadata", "delete", "subtree_scan", "subtree_moved", "subtree_deleted", "reconcile":
		return true
	default:
		return false
	}
}

func validOptionalNextcloudPath(path *string) bool {
	return path == nil || len(*path) <= 4096 && !strings.ContainsRune(*path, 0)
}

func validOptionalNextcloudRelativePath(relative *string) bool {
	if relative == nil {
		return true
	}
	if *relative == "" || len(*relative) > 4096 || strings.HasPrefix(*relative, "/") ||
		strings.ContainsAny(*relative, "\\\x00") {
		return false
	}
	for _, component := range strings.Split(*relative, "/") {
		if component == "" || component == "." || component == ".." {
			return false
		}
	}
	return true
}
