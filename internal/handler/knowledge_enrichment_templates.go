package handler

import (
	"net/http"

	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
)

type KnowledgeEnrichmentTemplateHandler struct {
	service *service.KnowledgeEnrichmentTemplateService
	bases   interfaces.KnowledgeBaseService
}

func NewKnowledgeEnrichmentTemplateHandler(
	s *service.KnowledgeEnrichmentTemplateService,
	bases interfaces.KnowledgeBaseService,
) *KnowledgeEnrichmentTemplateHandler {
	return &KnowledgeEnrichmentTemplateHandler{s, bases}
}

type enrichmentTemplateActualEdit struct {
	Content string `json:"content" binding:"required"`
}
type enrichmentTemplateParentSelection struct {
	KnowledgeIDs []string `json:"knowledge_ids" binding:"required"`
}

func (h *KnowledgeEnrichmentTemplateHandler) scope(c *gin.Context) (uint64, string, string, error) {
	tenant, ok := types.TenantIDFromContext(c.Request.Context())
	if !ok || tenant == 0 {
		return 0, "", "", types.ErrSourceLineageUnknown
	}
	id := c.Param("id")
	purpose := c.Param("purpose")
	if id == "" || (purpose != "summary" && purpose != "question") {
		return 0, "", "", types.ErrSourceLineageUnknown
	}
	kb, err := h.bases.GetKnowledgeBaseByIDOnly(c.Request.Context(), id)
	if err != nil {
		return 0, "", "", err
	}
	if kb == nil || kb.ID != id || kb.TenantID != tenant {
		return 0, "", "", types.ErrSourceLineageUnknown
	}
	return tenant, id, purpose, nil
}

// Set receives the actual new human edit. Client lineage/actor/proof fields
// are not bound or consulted by this endpoint.
func (h *KnowledgeEnrichmentTemplateHandler) Set(c *gin.Context) {
	tenant, id, purpose, err := h.scope(c)
	if err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}
	var input enrichmentTemplateActualEdit
	if err := c.ShouldBindJSON(&input); err != nil {
		_ = c.Error(errors.NewBadRequestError("invalid enrichment template edit"))
		return
	}
	_, err = h.service.SetIndependentTemplate(c.Request.Context(), tenant, id, purpose, input.Content)
	if err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "knowledge_base_id": id, "purpose": purpose, "configured": true})
}

// Generate accepts selectors and reads every parent itself through the actual
// original input reader. Cached text and body/lineage JSON are never inputs.
func (h *KnowledgeEnrichmentTemplateHandler) Generate(c *gin.Context) {
	tenant, id, purpose, err := h.scope(c)
	if err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}
	var input enrichmentTemplateParentSelection
	if err := c.ShouldBindJSON(&input); err != nil {
		_ = c.Error(errors.NewBadRequestError("invalid template parent selection"))
		return
	}
	_, err = h.service.GenerateDerivedTemplate(c.Request.Context(), tenant, id, purpose, input.KnowledgeIDs)
	if err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "knowledge_base_id": id, "purpose": purpose, "configured": true})
}

// Read repeats both the original configuration and source/grant checks at
// the response boundary. The generic KB response omits template bodies.
func (h *KnowledgeEnrichmentTemplateHandler) Read(c *gin.Context) {
	tenant, id, purpose, err := h.scope(c)
	if err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}
	snapshot, err := h.service.ReadTemplate(c.Request.Context(), tenant, id, purpose)
	if err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}

	defer snapshot.Close()
	prior := c.Writer
	c.Writer = &knowledgeCollectionAuthorizationWriter{
		ResponseWriter: prior,
		check:          func() error { return knowledgeOutputHTTPError(snapshot.Check(c.Request.Context())) },
	}
	defer func() { c.Writer = prior }()
	c.JSON(http.StatusOK, gin.H{"success": true, "data": gin.H{"purpose": purpose, "content": snapshot.Content}})
}
