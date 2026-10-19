package router

import (
	"github.com/Tencent/WeKnora/internal/handler"
	"github.com/gin-gonic/gin"
)

// RegisterKnowledgeEnrichmentTemplateRoutes registers enrichment template administration routes.
func RegisterKnowledgeEnrichmentTemplateRoutes(
	r *gin.RouterGroup,
	h *handler.KnowledgeEnrichmentTemplateHandler,
	g *rbacGuards,
) {
	kb := r.Group("knowledge-bases")
	kb.GET("/:id/enrichment-templates/:purpose", g.Admin(), g.KBAccessRead("id"), h.Read)
	kb.PUT("/:id/enrichment-templates/:purpose", g.Admin(), g.KBAccessWrite("id"), h.Set)
	kb.POST("/:id/enrichment-templates/:purpose/generate", g.Admin(), g.KBAccessWrite("id"), h.Generate)
}
