package handler

import (
	"net/http"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

// NextcloudGCHandler exposes only tenant-scoped status and retry controls.
// The exact inventory remains internal because it contains storage locators.
type NextcloudGCHandler struct{ store *repository.NextcloudGCStore }

// NewNextcloudGCHandler constructs the tenant-scoped garbage-collection handler.
func NewNextcloudGCHandler(store *repository.NextcloudGCStore) *NextcloudGCHandler {
	return &NextcloudGCHandler{store: store}
}

func (h *NextcloudGCHandler) authorize(c *gin.Context) (uint64, bool) {
	c.Header("Cache-Control", "no-store")
	if _, isAPIKey := types.TenantAPIKeyScopeFromContext(c.Request.Context()); isAPIKey ||
		!types.TenantRoleFromContext(c.Request.Context()).HasPermission(types.TenantRoleAdmin) {
		c.JSON(http.StatusForbidden, gin.H{"error": "admin_session_required"})
		return 0, false
	}
	tenantID := c.GetUint64(types.TenantIDContextKey.String())
	if tenantID == 0 {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return 0, false
	}
	return tenantID, true
}

// List returns tenant-scoped garbage-collection status.
func (h *NextcloudGCHandler) List(c *gin.Context) {
	tenantID, ok := h.authorize(c)
	if !ok {
		return
	}
	jobs, err := h.store.ListStatus(c.Request.Context(), tenantID, 100)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "nextcloud_gc_unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"jobs": jobs})
}

// Retry schedules an eligible tenant-scoped garbage-collection retry.
func (h *NextcloudGCHandler) Retry(c *gin.Context) {
	tenantID, ok := h.authorize(c)
	if !ok {
		return
	}
	retried, err := h.store.RetryJob(c.Request.Context(), tenantID, c.Param("id"), time.Now().UTC())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "nextcloud_gc_unavailable"})
		return
	}
	if !retried {
		c.JSON(http.StatusNotFound, gin.H{"error": "nextcloud_gc_job_unavailable"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"status": "retry_scheduled"})
}
