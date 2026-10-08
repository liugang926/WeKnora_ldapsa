package handler

import (
	"context"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"net/http"
)

func ConfigureTagHandlerOutputLineage(h *TagHandler, authorizer *service.TagOutputAuthorizer) {
	if h != nil {
		h.outputAuthorizer = authorizer
	}
}
func (h *TagHandler) requireTagOutput(c *gin.Context) bool {
	if h == nil || h.outputAuthorizer == nil {
		_ = c.Error(errors.NewServiceUnavailableError("Original tag output verifier is unavailable"))
		return false
	}
	return true
}
func (h *TagHandler) respondCommittedTag(c *gin.Context, ctx context.Context, returned *types.KnowledgeTag) {
	snapshot, err := repository.TagCommittedOutput(ctx)
	if err == nil && (returned == nil || snapshot.Request.TagID != returned.ID || snapshot.Request.TenantID != returned.TenantID || snapshot.Request.KnowledgeBaseID != returned.KnowledgeBaseID) {
		err = repository.ErrKnowledgeInputChanged
	}
	if err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}
	ctx, closeRead, err := beginCollectionHTTPRead(c, ctx, h.outputAuthorizer.ContentLeaseStore(), snapshot.Request.KnowledgeBaseID)
	if err != nil {
		_ = c.Error(err)
		return
	}
	defer closeRead()
	guard, err := h.outputAuthorizer.BeginCommitted(ctx, snapshot)
	if err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}
	tag, err := guard.Tag()
	if err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}
	h.writeTagOutput(c, func() error { return knowledgeOutputHTTPError(guard.Check(ctx, tag)) }, tag)
}
func (h *TagHandler) respondTagList(c *gin.Context, ctx context.Context, kb string, page types.Pagination, keyword string) {
	tenant, ok := types.TenantIDFromContext(ctx)
	if !ok || tenant == 0 {
		_ = c.Error(errors.NewForbiddenError("Current tag scope is required"))
		return
	}
	ctx, closeRead, err := beginCollectionHTTPRead(c, ctx, h.outputAuthorizer.ContentLeaseStore(), kb)
	if err != nil {
		_ = c.Error(err)
		return
	}
	defer closeRead()
	guard, err := h.outputAuthorizer.Begin(ctx, types.TagOutputReadRequest{TenantID: tenant, KnowledgeBaseID: kb})
	if err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}
	selection, err := guard.Select(ctx, types.TagOutputSelectionRequest{Page: page, Keyword: keyword})
	if err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}
	data := types.NewPageResult(selection.Total, &selection.Request.Page, selection.Tags)
	h.writeTagOutput(c, func() error { return knowledgeOutputHTTPError(guard.Check(ctx, selection)) }, data)
}
func (h *TagHandler) writeTagOutput(c *gin.Context, check func() error, data any) {
	prior := c.Writer
	c.Writer = &knowledgeCollectionAuthorizationWriter{ResponseWriter: prior, check: check}
	defer func() { c.Writer = prior }()
	c.JSON(http.StatusOK, gin.H{"success": true, "data": data})
}
