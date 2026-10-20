package router

import (
	"github.com/Tencent/WeKnora/internal/handler"
	"github.com/gin-gonic/gin"
)

// RegisterNextcloudCapacityRoutes registers source capacity administration routes.
func RegisterNextcloudCapacityRoutes(r *gin.RouterGroup, h *handler.NextcloudCapacityHandler, g *rbacGuards) {
	kb := r.Group("knowledge-bases")
	kb.GET("/:id/nextcloud-capacity", g.Admin(), h.Get)
	kb.PUT("/:id/nextcloud-capacity", g.Admin(), h.Set)
}
