package handler

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
)

// NextcloudGCHandler serves derived source data collection operations.
// Administrative responses expose category/count/expiry only, never locators.
type NextcloudGCHandler struct {
	store *repository.NextcloudGCStore
	actor *repository.KnowledgeInputProvenanceStore
}

// NewNextcloudGCHandler constructs the configured NextcloudGCHandler.
func NewNextcloudGCHandler(
	store *repository.NextcloudGCStore,
	actor *repository.KnowledgeInputProvenanceStore,
) *NextcloudGCHandler {
	return &NextcloudGCHandler{store, actor}
}

func (h *NextcloudGCHandler) authorize(c *gin.Context) (context.Context, uint64, bool) {
	c.Header("Cache-Control", "no-store")
	if h.actor == nil || h.store == nil {
		c.JSON(http.StatusForbidden, gin.H{"error": "current_admin_session_required"})
		return nil, 0, false
	}
	if _, machine := types.TenantAPIKeyScopeFromContext(c.Request.Context()); machine {
		c.JSON(http.StatusForbidden, gin.H{"error": "current_admin_session_required"})
		return nil, 0, false
	}
	ctx, e := h.actor.RefreshCurrentReadActor(c.Request.Context())
	if e != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": "current_admin_session_required"})
		return nil, 0, false
	}
	caller := types.CallerFromContext(ctx)
	principal, _ := types.PrincipalFromContext(ctx)
	if caller.TenantID == 0 || caller.UserID == "" || principal.Type != types.PrincipalWebUser ||
		principal.ID != caller.UserID ||
		!caller.Role.HasPermission(types.TenantRoleAdmin) {
		c.JSON(http.StatusForbidden, gin.H{"error": "current_admin_session_required"})
		return nil, 0, false
	}
	return ctx, caller.TenantID, true
}

func (h *NextcloudGCHandler) List(c *gin.Context) {
	ctx, tenant, ok := h.authorize(c)
	if !ok {
		return
	}
	jobs, e := h.store.ListStatus(ctx, tenant, 100, c.Query("knowledge_base_id"), c.Query("cursor"))
	if e != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "nextcloud_gc_unavailable"})
		return
	}
	if _, _, ok = h.authorize(c); !ok {
		return
	}
	cursor := ""
	if len(jobs) == 100 {
		cursor = jobs[len(jobs)-1].ID
	}
	c.JSON(http.StatusOK, gin.H{"jobs": jobs, "next_cursor": cursor})
}

// Preview previews derived source artifacts eligible for collection.
func (h *NextcloudGCHandler) Preview(c *gin.Context) {
	ctx, tenant, ok := h.authorize(c)
	if !ok {
		return
	}
	preview, e := h.store.PreviewJob(ctx, tenant, c.Param("id"))
	if e != nil {
		h.failure(c, e)
		return
	}
	if _, _, ok = h.authorize(c); !ok {
		return
	}
	c.JSON(http.StatusOK, gin.H{"preview": preview})
}

// Collect collects eligible derived artifacts under current source controls.
func (h *NextcloudGCHandler) Collect(c *gin.Context) {
	ctx, tenant, ok := h.authorize(c)
	if !ok {
		return
	}
	preview, e := h.store.CollectJob(ctx, h.actor, tenant, c.Param("id"))
	if e != nil {
		h.failure(c, e)
		return
	}
	if _, _, ok = h.authorize(c); !ok {
		return
	}
	c.JSON(http.StatusOK, gin.H{"preview": preview})
}

func (h *NextcloudGCHandler) Retry(c *gin.Context) {
	ctx, tenant, ok := h.authorize(c)
	if !ok {
		return
	}
	retried, e := h.store.RetryJobAsAdmin(ctx, h.actor, tenant, c.Param("id"))
	if e != nil {
		h.failure(c, e)
		return
	}
	if !retried {
		c.JSON(http.StatusNotFound, gin.H{"error": "nextcloud_gc_job_unavailable"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"status": "retry_scheduled"})
}

func (h *NextcloudGCHandler) failure(c *gin.Context, e error) {
	switch {
	case errors.Is(e, repository.ErrNextcloudGCAdmin):
		c.JSON(http.StatusForbidden, gin.H{"error": "current_admin_session_required"})
	case errors.Is(e, repository.ErrNextcloudGCJobUnavailable):
		c.JSON(http.StatusNotFound, gin.H{"error": "nextcloud_gc_job_unavailable"})
	case errors.Is(e, repository.ErrNextcloudGCRetention):
		c.JSON(http.StatusConflict, gin.H{"error": "retention_window_open"})
	default:
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "nextcloud_gc_unavailable"})
	}
}
