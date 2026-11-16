package handler

import (
	"context"
	"net/http"
	"reflect"

	"github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

// Both gallery transports use the same authorized data set for rows and
// totals. Re-evaluate it at Write, after Gin has serialized the response.
func (h *KnowledgeHandler) writeImageGallery(
	ctx context.Context, c *gin.Context, kb string, tenant uint64,
	page *types.Pagination, filter *types.ImageListFilter,
) {
	h.constrainGalleryFilter(ctx, kb, filter)
	filter.AuthorizeSource = func(ctx context.Context, doc *types.Knowledge) (bool, error) {
		err := h.checkCurrentNextcloudKBAccess(ctx, c, doc, types.OrgRoleViewer)
		if err == nil {
			err = h.checkSourcePublication(ctx, doc)
		}
		if app, ok := errors.IsAppError(err); ok &&
			(app.HTTPCode == http.StatusForbidden || app.HTTPCode == http.StatusNotFound) {
			return false, nil
		}
		return err == nil, err
	}
	load := func() (*types.PageResult, error) {
		target := &types.Knowledge{TenantID: tenant, KnowledgeBaseID: kb}
		if err := h.checkCurrentNextcloudKBAccess(ctx, c, target, types.OrgRoleViewer); err != nil {
			return nil, err
		}
		result, err := h.chunkService.ListImagesByKnowledgeBaseID(ctx, kb, page, filter)
		if err != nil || result == nil {
			return nil, errors.NewServiceUnavailableError("Cannot verify gallery access")
		}
		return result, nil
	}
	result, err := load()
	if err != nil {
		_ = c.Error(err)
		return
	}
	prior := c.Writer
	c.Writer = &knowledgeCollectionAuthorizationWriter{ResponseWriter: prior, check: func() error {
		current, err := load()
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(result, current) {
			return errors.NewForbiddenError("Gallery visibility changed")
		}
		return nil
	}}
	defer func() { c.Writer = prior }()
	c.JSON(http.StatusOK, gin.H{
		"success":   true,
		"data":      result.Data,
		"total":     result.Total,
		"page":      result.Page,
		"page_size": result.PageSize,
	})
}
