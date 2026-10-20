package handler

import (
	"errors"
	"net/http"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

// NextcloudConnectionHandler owns the one-time administrator connection that
// turns selected workspaces into scoped Nextcloud onboarding credentials.
type NextcloudConnectionHandler struct {
	store *repository.NextcloudOnboardingStore
}

// NewNextcloudConnectionHandler constructs the connection setup handler.
func NewNextcloudConnectionHandler(
	store *repository.NextcloudOnboardingStore,
) *NextcloudConnectionHandler {
	return &NextcloudConnectionHandler{store: store}
}

func (h *NextcloudConnectionHandler) caller(c *gin.Context) (string, bool, bool) {
	ctx := c.Request.Context()
	if scope, ok := types.TenantAPIKeyScopeFromContext(ctx); ok &&
		scope.ScopeType == types.APIKeyScopePlatform {
		return "", true, true
	}
	if userID, ok := types.UserIDFromContext(ctx); ok && userID != "" {
		return userID, false, true
	}
	return "", false, false
}

func connectionError(c *gin.Context, err error) {
	status := http.StatusServiceUnavailable
	code := "nextcloud_connection_unavailable"
	switch {
	case errors.Is(err, repository.ErrNextcloudOnboardingDenied):
		status = http.StatusForbidden
		code = "nextcloud_connection_denied"
	case errors.Is(err, repository.ErrNextcloudOnboardingConflict):
		status = http.StatusConflict
		code = "nextcloud_connection_conflict"
	}
	c.JSON(status, gin.H{"error": code})
}

// Workspaces lists workspaces that the authenticated administrator may
// authorize. Ordinary workspace members and source-scoped accounts see none.
func (h *NextcloudConnectionHandler) Workspaces(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	userID, platform, ok := h.caller(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{"error": "nextcloud_connection_denied"})
		return
	}
	spaces, err := h.store.ConnectionWorkspaces(c.Request.Context(), userID, platform)
	if err != nil {
		connectionError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"spaces": spaces})
}

// Authorize creates one scoped credential per selected workspace and returns
// the credentials exactly once.
func (h *NextcloudConnectionHandler) Authorize(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Header("Pragma", "no-cache")
	userID, platform, ok := h.caller(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{"error": "nextcloud_connection_denied"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	var req types.NextcloudConnectionAuthorizeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_nextcloud_connection"})
		return
	}
	result, err := h.store.AuthorizeConnection(c.Request.Context(), req, userID, platform)
	if err != nil {
		connectionError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// Revoke disables exactly the credentials issued for one Nextcloud connection.
func (h *NextcloudConnectionHandler) Revoke(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	userID, platform, ok := h.caller(c)
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{"error": "nextcloud_connection_denied"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	var req types.NextcloudConnectionRevokeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_nextcloud_connection"})
		return
	}
	revoked, err := h.store.RevokeConnection(c.Request.Context(), req.CredentialIDs, userID, platform)
	if err != nil {
		connectionError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"revoked": revoked})
}
