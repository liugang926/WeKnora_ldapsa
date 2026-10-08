package handler

import (
	"context"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"net/http"
)

type NextcloudCapacityHandler struct {
	store     *repository.NextcloudCapacityStore
	actor     *repository.KnowledgeInputProvenanceStore
	kb        interfaces.KnowledgeBaseService
	admission *service.NextcloudCapacityAdmission
}

func NewNextcloudCapacityHandler(s *repository.NextcloudCapacityStore, a *repository.KnowledgeInputProvenanceStore, k interfaces.KnowledgeBaseService, admission *service.NextcloudCapacityAdmission) *NextcloudCapacityHandler {
	return &NextcloudCapacityHandler{s, a, k, admission}
}
func (h *NextcloudCapacityHandler) authorize(c *gin.Context) (context.Context, *types.KnowledgeBase, bool) {
	c.Header("Cache-Control", "no-store")
	ctx, err := h.actor.RefreshCurrentReadActor(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": "current_admin_session_required"})
		return nil, nil, false
	}
	caller := types.CallerFromContext(ctx)
	principal, _ := types.PrincipalFromContext(ctx)
	if principal.Type != types.PrincipalWebUser || !caller.Role.HasPermission(types.TenantRoleAdmin) {
		c.JSON(http.StatusForbidden, gin.H{"error": "current_admin_session_required"})
		return nil, nil, false
	}
	kb, err := h.kb.GetKnowledgeBaseByIDOnly(ctx, c.Param("id"))
	if err != nil || kb == nil || kb.TenantID != caller.TenantID {
		c.JSON(http.StatusNotFound, gin.H{"error": "knowledge_base_unavailable"})
		return nil, nil, false
	}
	return ctx, kb, true
}
func (h *NextcloudCapacityHandler) Get(c *gin.Context) {
	ctx, kb, ok := h.authorize(c)
	if !ok {
		return
	}
	status, err := h.admission.Status(ctx, kb)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "storage_capacity_unavailable"})
		return
	}
	if _, _, ok = h.authorize(c); !ok {
		return
	}
	c.JSON(http.StatusOK, gin.H{"capacity": status})
}
func (h *NextcloudCapacityHandler) Set(c *gin.Context) {
	ctx, kb, ok := h.authorize(c)
	if !ok {
		return
	}
	var input struct {
		Quota   int64 `json:"retention_quota_bytes"`
		Warning int   `json:"warning_percent"`
		Pause   int   `json:"pause_percent"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || input.Quota < 0 || input.Warning <= 0 || input.Warning >= input.Pause || input.Pause > 100 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_capacity_policy"})
		return
	}
	if _, err := h.actor.AuthorizeCurrentKnowledgeInputWrite(ctx, kb); err != nil {
		c.JSON(http.StatusForbidden, gin.H{"error": "current_admin_session_required"})
		return
	}
	err := h.store.SetPolicy(ctx, repository.NextcloudCapacityPolicy{TenantID: kb.TenantID, KnowledgeBaseID: kb.ID, RetentionQuotaBytes: input.Quota, WarningPercent: input.Warning, PausePercent: input.Pause})
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "capacity_policy_unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"updated": true})
}
