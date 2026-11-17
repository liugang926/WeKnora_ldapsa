package handler

import (
	"context"
	"errors"
	"net/http"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type dingTalkScheduleStatusReader interface {
	DingTalkScheduleAuthorizationStatus(context.Context, uint64, string) (string, error)
}

// WithDingTalkScheduleStatus exposes live schedule status through the fork's
// authenticated workspace access routes.
func WithDingTalkScheduleStatus(
	h *GroupAccessHandler, store *repository.KnowledgeInputProvenanceStore,
) *GroupAccessHandler {
	h.scheduleAuthorization = store
	return h
}

func (h *GroupAccessHandler) dingTalkScheduleAuthorizationStatus(c *gin.Context) {
	ctx := c.Request.Context()
	tenant, ok := types.TenantIDFromContext(ctx)
	if !ok || tenant == 0 {
		c.JSON(http.StatusForbidden, gin.H{"error": "an active workspace is required"})
		return
	}
	c.Header("Cache-Control", "no-store")
	if h.scheduleAuthorization == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "cannot verify schedule authorization"})
		return
	}
	status, err := h.scheduleAuthorization.DingTalkScheduleAuthorizationStatus(ctx, tenant, c.Param("id"))
	switch {
	case err == nil:
		c.JSON(http.StatusOK, gin.H{"status": status})
	case errors.Is(err, gorm.ErrRecordNotFound), errors.Is(err, access.ErrNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": "data source not found"})
	case errors.Is(err, access.ErrForbidden), errors.Is(err, repository.ErrKnowledgeInputChanged),
		errors.Is(err, types.ErrSourceLineageUnknown):
		c.JSON(http.StatusForbidden, gin.H{"error": "knowledge base edit permission required"})
	default:
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "cannot verify schedule authorization"})
	}
}
