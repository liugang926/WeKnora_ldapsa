package handler

import (
	"context"
	stderrors "errors"
	"net/http"
	"strconv"

	"github.com/Tencent/WeKnora/internal/application/repository"
	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

type nextcloudAskTargetResolver interface {
	FindPublished(context.Context, string, string, int64, string) (*types.Knowledge, error)
}

// NextcloudAskTarget converts a Files-sidebar deep link into one currently
// published document for the authenticated human. The URL is not a grant.
func (h *KnowledgeHandler) NextcloudAskTarget(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	ctx := c.Request.Context()
	principal, principalOK := ctx.Value(types.PrincipalContextKey).(types.Principal)
	caller, callerOK := ctx.Value(types.CallerContextKey).(types.Caller)
	_, machine := types.TenantAPIKeyScopeFromContext(ctx)
	if !principalOK || !callerOK || machine || principal.Type != types.PrincipalWebUser ||
		caller.TenantID == 0 || caller.UserID == "" || principal.ID != caller.UserID ||
		types.IsSyntheticUserID(caller.UserID) {
		_ = c.Error(apperrors.NewForbiddenError("Interactive user authorization required"))
		return
	}
	query := c.Request.URL.Query()
	if len(query) != 4 || len(query["instance_id"]) != 1 || len(query["binding_id"]) != 1 ||
		len(query["file_id"]) != 1 || len(query["source_etag"]) != 1 {
		_ = c.Error(apperrors.NewBadRequestError("Invalid Nextcloud ask target"))
		return
	}
	fileID, err := strconv.ParseInt(query.Get("file_id"), 10, 64)
	if err != nil || fileID < 1 || strconv.FormatInt(fileID, 10) != query.Get("file_id") {
		_ = c.Error(apperrors.NewBadRequestError("Invalid Nextcloud file ID"))
		return
	}
	if h.askTargets == nil {
		_ = c.Error(apperrors.NewServiceUnavailableError("Nextcloud ask target unavailable"))
		return
	}
	lookup := func() (*types.Knowledge, bool) {
		knowledge, findErr := h.askTargets.FindPublished(ctx, query.Get("instance_id"),
			query.Get("binding_id"), fileID, query.Get("source_etag"))
		if stderrors.Is(findErr, repository.ErrNextcloudAskTargetNotFound) {
			_ = c.Error(apperrors.NewNotFoundError("Published file is no longer available"))
			return nil, false
		}
		if findErr != nil || knowledge == nil {
			_ = c.Error(apperrors.NewServiceUnavailableError("Nextcloud ask target unavailable"))
			return nil, false
		}
		return knowledge, true
	}
	before, ok := lookup()
	if !ok {
		return
	}
	knowledge, scoped, err := h.resolveKnowledgeAndValidateKBAccess(c, before.ID, types.OrgRoleViewer)
	if err != nil {
		_ = c.Error(err)
		return
	}
	if knowledge.ID != before.ID || knowledge.TenantID != before.TenantID ||
		knowledge.KnowledgeBaseID != before.KnowledgeBaseID {
		_ = c.Error(apperrors.NewServiceUnavailableError("Nextcloud ask target changed"))
		return
	}
	if err := h.checkPublication(scoped, knowledge); err != nil {
		_ = c.Error(err)
		return
	}
	// Source authorization is a network call. A concurrent version switch or
	// pair withdrawal must invalidate the link before any target is returned.
	after, ok := lookup()
	if !ok {
		return
	}
	if after.ID != before.ID || after.TenantID != before.TenantID ||
		after.KnowledgeBaseID != before.KnowledgeBaseID {
		_ = c.Error(apperrors.NewServiceUnavailableError("Nextcloud ask target changed"))
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": gin.H{
		"knowledge_id": after.ID, "knowledge_base_id": after.KnowledgeBaseID,
		"title": after.Title, "source_etag": query.Get("source_etag"),
	}})
}
