package handler

import (
	"context"
	"net/http"

	"github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

func (h *KnowledgeHandler) KnowledgeRecoveryEligibility(c *gin.Context) {
	id := c.Param("id")
	_, ctx, err := h.resolveKnowledgeAndValidateKBAccess(c, id, types.OrgRoleEditor)
	if err != nil {
		c.Error(err)
		return
	}
	service, ok := h.kgService.(interface {
		KnowledgeRecoveryEligibility(context.Context, string) (*types.KnowledgeRecoveryEligibility, error)
	})
	if !ok {
		c.Error(errors.NewInternalServerError("恢复状态不可用"))
		return
	}
	result, err := service.KnowledgeRecoveryEligibility(ctx, id)
	if err != nil {
		c.Error(err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": result})
}
