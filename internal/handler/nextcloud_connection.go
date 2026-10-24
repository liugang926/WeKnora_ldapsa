package handler

import (
	"encoding/base64"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// NextcloudConnectionHandler owns the administrator connection setup that
// turns selected workspaces into scoped Nextcloud onboarding credentials.
type NextcloudConnectionHandler struct {
	store  *repository.NextcloudOnboardingStore
	worker interfaces.TaskInspector
}

// NewNextcloudConnectionHandler constructs the connection setup handler.
func NewNextcloudConnectionHandler(
	store *repository.NextcloudOnboardingStore,
	worker interfaces.TaskInspector,
) *NextcloudConnectionHandler {
	return &NextcloudConnectionHandler{store: store, worker: worker}
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
	query := strings.ToLower(strings.TrimSpace(c.Query("q")))
	filtered := make([]types.NextcloudConnectionWorkspace, 0, len(spaces))
	for _, space := range spaces {
		if query == "" || strings.Contains(strings.ToLower(space.TenantName), query) ||
			strconv.FormatUint(space.TenantID, 10) == query {
			filtered = append(filtered, space)
		}
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		if filtered[i].TenantName == filtered[j].TenantName {
			return filtered[i].TenantID < filtered[j].TenantID
		}
		return filtered[i].TenantName < filtered[j].TenantName
	})
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "50"))
	if err != nil || limit < 1 || limit > 100 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_workspace_page"})
		return
	}
	offset := 0
	if raw := c.Query("cursor"); raw != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(raw)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_workspace_page"})
			return
		}
		offset, err = strconv.Atoi(string(decoded))
		if err != nil || offset < 0 || offset > len(filtered) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_workspace_page"})
			return
		}
	}
	end := offset + limit
	if end > len(filtered) {
		end = len(filtered)
	}
	nextCursor := ""
	if end < len(filtered) {
		nextCursor = base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	}
	health, err := nextcloudConnectionHealth(c.Request.Context(), h.store, h.worker)
	if err != nil {
		connectionError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"contract_version": types.NextcloudOnboardingContractVersion,
		"request_id":       "req_" + uuid.NewString(),
		"spaces":           filtered[offset:end],
		"next_cursor":      nextCursor,
		"health":           health,
	})
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
	health, err := nextcloudConnectionHealth(c.Request.Context(), h.store, h.worker)
	if err != nil {
		connectionError(c, err)
		return
	}
	c.Header("X-WeKnora-Onboarding-Contract", "2")
	c.JSON(http.StatusOK, gin.H{
		"connection_id":    result.ConnectionID,
		"credentials":      result.Credentials,
		"contract_version": types.NextcloudOnboardingContractVersion,
		"health":           health,
	})
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
