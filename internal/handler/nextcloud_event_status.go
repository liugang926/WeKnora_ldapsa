package handler

import (
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/gin-gonic/gin"
)

const nextcloudEventStatusPath = "/api/v1/integrations/nextcloud/events/status"

// Status returns publication progress to the same machine connection that
// sends event hints. A received or dispatched watermark is never treated as
// applied acknowledgement.
func (h *NextcloudEventHandler) Status(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	connectionID := c.Query("connection_id")
	query := "connection_id=" + connectionID
	if c.Request.Method != http.MethodGet || c.Request.URL.EscapedPath() != nextcloudEventStatusPath ||
		c.Request.URL.ForceQuery || !validNextcloudOpaqueID(connectionID, 16, 128) ||
		c.Request.URL.RawQuery != query {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request_target"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, 1))
	if err != nil || len(body) != 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request_body"})
		return
	}
	headerConnection, okConnection := oneNextcloudHeader(c.Request, "X-Nextcloud-Connection-Id")
	keyID, okKey := oneNextcloudHeader(c.Request, "X-Nextcloud-Key-Id")
	timestamp, okTimestamp := oneNextcloudHeader(c.Request, "X-Nextcloud-Timestamp")
	nonce, okNonce := oneNextcloudHeader(c.Request, "X-Nextcloud-Nonce")
	signatureHex, okSignature := oneNextcloudHeader(c.Request, "X-Nextcloud-Signature")
	if !okConnection || headerConnection != connectionID || !okKey || !validNextcloudKeyID(keyID) ||
		!okTimestamp || !okNonce || !okSignature ||
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
	canonical := nextcloudEventStatusCanonicalRequest(query, timestamp, nonce, connectionID, keyID)
	status, err := h.inbox.SignedConnectionStatus(c.Request.Context(), repository.SignedNextcloudEventStatusRequest{
		ConnectionID: connectionID, KeyID: keyID, Nonce: nonce,
		Signature: signature, CanonicalRequest: canonical, Now: now,
	})
	if err != nil {
		switch {
		case errors.Is(err, repository.ErrNextcloudEventUnauthorized):
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		case errors.Is(err, repository.ErrNextcloudEventScope):
			c.JSON(http.StatusForbidden, gin.H{"error": "connection_unavailable"})
		default:
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "event_status_unavailable"})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"connection_id":               status.ConnectionID,
		"nextcloud_instance_id":       status.NextcloudInstanceID,
		"binding_id":                  status.BindingID,
		"status":                      status.Status,
		"received_through_event_id":   status.ReceivedThroughEventID,
		"dispatched_through_event_id": status.DispatchedThroughEventID,
		"applied_through_event_id":    status.AppliedThroughEventID,
		"backlog_count":               status.BacklogCount,
		"undispatched_count":          status.UndispatchedCount,
		"dispatch_state":              status.DispatchState,
		"last_error_code":             status.LastErrorCode,
		"last_sync_log_id":            status.LastSyncLogID,
	})
}

func nextcloudEventStatusCanonicalRequest(query, timestamp, nonce, connectionID, keyID string) []byte {
	return []byte(strings.Join([]string{
		"nextcloud-event-status-hmac-sha256-v1",
		http.MethodGet,
		nextcloudEventStatusPath,
		query,
		repository.NextcloudEventPayloadHash(nil),
		timestamp,
		nonce,
		connectionID,
		keyID,
	}, "\n"))
}
