package handler

import (
	"context"
	"net/http"

	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/errors"
	"github.com/gin-gonic/gin"
)

// ConfigureFAQHandlerOutputLineage configures the faqhandler output lineage boundary.
func ConfigureFAQHandlerOutputLineage(h *FAQHandler, authorizer *service.FAQOutputAuthorizer) {
	if h != nil {
		h.outputAuthorizer = authorizer
	}
}

func (h *FAQHandler) requireFAQOutput(c *gin.Context) bool {
	if h == nil || h.outputAuthorizer == nil {
		_ = c.Error(errors.NewServiceUnavailableError("Original FAQ output verifier is unavailable"))
		return false
	}
	return true
}

func (h *FAQHandler) writeFAQOutput(ctx context.Context, c *gin.Context, data any) {
	guard := service.FAQOutputGuardFromContext(ctx)
	if guard == nil {
		_ = c.Error(errors.NewServiceUnavailableError("Original FAQ producer output is unavailable"))
		return
	}
	check := func() error { return knowledgeOutputHTTPError(guard.Check(ctx, data)) }
	if err := check(); err != nil {
		_ = c.Error(err)
		return
	}
	prior := c.Writer
	c.Writer = &knowledgeCollectionAuthorizationWriter{ResponseWriter: prior, check: check}
	defer func() { c.Writer = prior }()
	c.JSON(http.StatusOK, gin.H{"success": true, "data": data})
}

func (h *FAQHandler) writeFAQData(ctx context.Context, c *gin.Context, contentType string, original, published []byte) {
	guard := service.FAQOutputGuardFromContext(ctx)
	if guard == nil {
		_ = c.Error(errors.NewServiceUnavailableError("Original FAQ export producer is unavailable"))
		return
	}
	check := func() error { return knowledgeOutputHTTPError(guard.Check(ctx, original)) }
	if err := check(); err != nil {
		_ = c.Error(err)
		return
	}
	prior := c.Writer
	c.Writer = &knowledgeCollectionAuthorizationWriter{ResponseWriter: prior, check: check}
	defer func() { c.Writer = prior }()
	c.Data(http.StatusOK, contentType, published)
}
