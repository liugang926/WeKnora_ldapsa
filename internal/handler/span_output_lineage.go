package handler

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

// ServeOriginalKnowledgeSpans is the production GetKnowledgeSpans hook. The
// caller supplies DI's authorizer; all attempt content comes from its original
// immutable Begin receipts, never the current knowledge error/metadata body.
func ServeOriginalKnowledgeSpans(h *KnowledgeHandler, c *gin.Context, a *service.SpanOutputAuthorizer) {
	ctx := c.Request.Context()
	id := strings.TrimSpace(c.Param("id"))
	if id == "" {
		_ = c.Error(errors.NewBadRequestError("Knowledge ID cannot be empty"))
		return
	}
	knowledge, _, err := h.resolveKnowledgeAndValidateKBAccess(c, id, types.OrgRoleViewer)
	if err != nil {
		_ = c.Error(err)
		return
	}
	if err := h.checkSourcePublication(ctx, knowledge); err != nil {
		_ = c.Error(err)
		return
	}
	// This response has its own original attempt body proof. A current
	// knowledge metadata proof would falsely mix current diagnostics into
	// historical attempts and is not part of the span response's authority.
	leases, err := acquireNextcloudHTTPReadLeases(ctx, h.contentLeases, []*types.Knowledge{knowledge}, h.leaseHeartbeat)
	if err != nil {
		_ = c.Error(err)
		return
	}
	defer leases.Close()
	ctx = leases.Context(ctx)
	attachNextcloudLeasedRequest(c, leases)
	attempt := 0
	if value := strings.TrimSpace(c.Query("attempt")); value != "" {
		attempt, err = strconv.Atoi(value)
		if err != nil || attempt <= 0 {
			_ = c.Error(errors.NewBadRequestError("Invalid attempt"))
			return
		}
	}
	guard, err := a.Begin(
		ctx,
		types.SpanOutputReadRequest{
			TenantID:        knowledge.TenantID,
			KnowledgeBaseID: knowledge.KnowledgeBaseID,
			KnowledgeID:     knowledge.ID,
			Attempt:         attempt,
		},
	)
	if err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}
	snapshot, err := guard.Snapshot()
	if err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}
	parseStatus := snapshot.CurrentParseStatus
	// Current status is labelled separately. It cannot make an old or absent
	// attempt look successful: synthesized rows always remain unknown/pending.
	tree, current, lastError := buildSpanTree(knowledge.ID, snapshot.Attempt, snapshot.Rows, "")
	var markSynthetic func(*types.SpanTreeNode)
	markSynthetic = func(node *types.SpanTreeNode) {
		if node == nil {
			return
		}
		if node.ContentProvenance == "" {
			node.ContentProvenance = "unknown"
		}
		for _, child := range node.Children {
			markSynthetic(child)
		}
	}
	markSynthetic(tree)
	resp := gin.H{
		"knowledge_id":    knowledge.ID,
		"attempt":         snapshot.Attempt,
		"latest_attempt":  snapshot.LatestAttempt,
		"parse_status":    parseStatus,
		"current_attempt": snapshot.Attempt,
		"current_stage":   current,
		"trace":           tree,
	}
	// A current knowledge.ErrorMessage cannot certify an old attempt's error.
	if lastError != nil && lastError.ContentProvenance == "complete" {
		resp["last_error"] = knowledgeSpansLastError(
			snapshot.Attempt,
			snapshot.LatestAttempt,
			knowledge.ParseStatus,
			"",
			knowledge.UpdatedAt,
			lastError,
		)
	}
	if err := guard.Check(ctx, snapshot); err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}
	if err := leases.Verify(); err != nil {
		_ = c.Error(nextcloudHTTPLeaseError(err))
		return
	}
	defer guardNextcloudResponseWrites(c, leases, func(checkCtx context.Context) error {
		if err := guard.Check(checkCtx, snapshot); err != nil {
			return knowledgeOutputHTTPError(err)
		}
		return h.checkSourcePublication(checkCtx, knowledge)
	})()
	c.JSON(http.StatusOK, gin.H{"success": true, "data": resp})
}
