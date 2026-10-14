package handler

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/gin-gonic/gin"
)

const nextcloudFileStatusPath = "/api/v1/integrations/nextcloud/files/status"

// FileStatus is machine-only. It reports a persisted publication version,
// never an event receipt, sync cursor, or browser user's retrieval grant.
func (h *NextcloudEventHandler) FileStatus(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	connectionID := c.Query("connection_id")
	fileID := c.Query("file_id")
	sourceETag := c.Query("source_etag")
	query := "connection_id=" + connectionID + "&file_id=" + fileID + "&source_etag=" + sourceETag
	parsedFileID, parseErr := strconv.ParseInt(fileID, 10, 64)
	if c.Request.Method != http.MethodGet || c.Request.URL.EscapedPath() != nextcloudFileStatusPath ||
		c.Request.URL.ForceQuery || c.Request.URL.RawQuery != query ||
		!validNextcloudOpaqueID(connectionID, 16, 128) ||
		parseErr != nil || parsedFileID < 1 || strconv.FormatInt(parsedFileID, 10) != fileID ||
		!validNextcloudFileETag(sourceETag) {
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
	request := repository.SignedNextcloudFileStatusRequest{
		SignedNextcloudEventStatusRequest: repository.SignedNextcloudEventStatusRequest{
			ConnectionID: connectionID, KeyID: keyID, Nonce: nonce,
			Signature: signature, CanonicalRequest: nextcloudFileStatusCanonicalRequest(query,
				timestamp, nonce, connectionID, keyID), Now: now,
		},
		FileID: parsedFileID, SourceETag: sourceETag, RequestSignature: signatureHex,
	}
	response, responseSignature, err := h.inbox.SignedFileStatus(c.Request.Context(), request)
	if err != nil {
		switch {
		case errors.Is(err, repository.ErrNextcloudEventUnauthorized):
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		case errors.Is(err, repository.ErrNextcloudEventScope):
			c.JSON(http.StatusForbidden, gin.H{"error": "connection_unavailable"})
		default:
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "file_status_unavailable"})
		}
		return
	}
	c.Header("X-WeKnora-Status-Signature", responseSignature)
	c.Data(http.StatusOK, "application/json", response)
}

func validNextcloudFileETag(etag string) bool {
	if len(etag) < 1 || len(etag) > 256 {
		return false
	}
	for _, char := range etag {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') &&
			(char < '0' || char > '9') && char != '.' && char != '_' && char != ':' && char != '-' {
			return false
		}
	}
	return true
}

func nextcloudFileStatusCanonicalRequest(query, timestamp, nonce, connectionID, keyID string) []byte {
	emptyHash := sha256.Sum256(nil)
	return []byte(strings.Join([]string{
		"nextcloud-file-status-hmac-sha256-v1", http.MethodGet,
		nextcloudFileStatusPath, query, hex.EncodeToString(emptyHash[:]),
		timestamp, nonce, connectionID, keyID,
	}, "\n"))
}
