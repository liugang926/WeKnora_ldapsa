package handler

import (
	"context"
	"net/http"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

func ConfigureKnowledgeHandlerCollectionOutput(h *KnowledgeHandler, authorizer *service.KnowledgeCollectionOutputAuthorizer) {
	if h != nil {
		h.collectionOutputAuthorizer = authorizer
	}
}

func (h *KnowledgeHandler) requireKnowledgeCollectionOutput(c *gin.Context) bool {
	if h == nil || h.collectionOutputAuthorizer == nil {
		_ = c.Error(errors.NewServiceUnavailableError("Original knowledge collection verifier is unavailable"))
		return false
	}
	return true
}

type knowledgeCollectionScopeResolver interface {
	ResolveKnowledgeCollectionScopes(context.Context, []types.KnowledgeSearchScope) ([]types.KnowledgeSearchScope, error)
}

// This writer guards ordinary, source-empty and Nextcloud collections alike.
// Gin may serialize JSON before Write, so Check also compares the retained
// typed selection after every authorization callback and local snapshot.
type knowledgeCollectionAuthorizationWriter struct {
	gin.ResponseWriter
	check func() error
}

func (w *knowledgeCollectionAuthorizationWriter) deny(err error) {
	if w.ResponseWriter.Written() {
		return
	}
	status := http.StatusForbidden
	if app, ok := errors.IsAppError(err); ok {
		status = app.HTTPCode
	}
	w.ResponseWriter.Header().Del("Content-Type")
	w.ResponseWriter.Header().Del("Content-Length")
	w.ResponseWriter.WriteHeader(status)
	w.ResponseWriter.WriteHeaderNow()
}
func (w *knowledgeCollectionAuthorizationWriter) Write(body []byte) (int, error) {
	if err := w.check(); err != nil {
		w.deny(err)
		return 0, err
	}
	return w.ResponseWriter.Write(body)
}
func (w *knowledgeCollectionAuthorizationWriter) WriteString(body string) (int, error) {
	if err := w.check(); err != nil {
		w.deny(err)
		return 0, err
	}
	return w.ResponseWriter.WriteString(body)
}
func (w *knowledgeCollectionAuthorizationWriter) WriteHeaderNow() {
	if err := w.check(); err != nil {
		w.deny(err)
		return
	}
	w.ResponseWriter.WriteHeaderNow()
}

func (h *KnowledgeHandler) respondKnowledgeCollection(c *gin.Context, ctx context.Context, scopes []types.KnowledgeSearchScope, request types.KnowledgeCollectionSelectionRequest, scope access.LineageReadScope) {
	var fences []*nextcloudHTTPReadLeases
	defer func() {
		for _, fence := range fences {
			fence.Close()
		}
	}()
	if h.contentLeases != nil {
		for _, target := range scopes {
			fence, err := acquireNextcloudHTTPKBReadLease(ctx, h.contentLeases, target.TenantID, target.KBID, h.leaseHeartbeat)
			if err != nil {
				_ = c.Error(err)
				return
			}
			fences = append(fences, fence)
			ctx = fence.Context(ctx)
		}
	}
	guard, err := h.collectionOutputAuthorizer.Begin(ctx, types.KnowledgeCollectionReadRequest{Scopes: scopes}, scope)
	if err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}
	selection, err := guard.Select(ctx, request)
	if err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}
	check := func() error {
		for _, fence := range fences {
			if err := fence.Verify(); err != nil {
				return nextcloudHTTPLeaseError(err)
			}
		}
		if err := guard.Check(ctx, selection); err != nil {
			return knowledgeOutputHTTPError(err)
		}
		for _, fence := range fences {
			if err := fence.Verify(); err != nil {
				return nextcloudHTTPLeaseError(err)
			}
		}
		return nil
	}
	prior := c.Writer
	c.Writer = &knowledgeCollectionAuthorizationWriter{ResponseWriter: prior, check: check}
	defer func() { c.Writer = prior }()
	switch request.Mode {
	case "list":
		c.JSON(http.StatusOK, gin.H{"success": true, "data": selection.Knowledge, "total": selection.Total, "page": selection.Page, "page_size": selection.PageSize})
	case "search":
		c.JSON(http.StatusOK, gin.H{"success": true, "data": selection.Knowledge, "has_more": selection.HasMore, "total": selection.Total})
	case "folders":
		c.JSON(http.StatusOK, gin.H{"success": true, "data": selection.Folders})
	}
}

func (h *KnowledgeHandler) searchKnowledgeCollection(c *gin.Context, ctx context.Context, keyword string, offset, limit int, fileTypes []string) {
	resolver, ok := h.kgService.(knowledgeCollectionScopeResolver)
	if !ok {
		_ = c.Error(errors.NewServiceUnavailableError("Cannot resolve original knowledge collection scope"))
		return
	}
	var selected []types.KnowledgeSearchScope
	var readScope access.LineageReadScope
	if agentID := c.Query("agent_id"); agentID != "" {
		agent, err := resolveSharedAgentForRequest(c, agentID, h.agentShareService)
		if err != nil {
			_ = c.Error(err)
			return
		}
		if err := h.authorizeAgentGroupUse(ctx, agent); err != nil {
			_ = c.Error(err)
			return
		}
		ctx = access.WithSharedAgent(ctx, agent)
		readScope.AgentID, readScope.AgentTenantID = agent.ID, agent.TenantID
		agentScope := types.NewSharedAgentKBScope(agent)
		selected = []types.KnowledgeSearchScope{}
		if agentScope.IsAll() {
			kbs, err := h.kbService.ListKnowledgeBasesByTenantID(ctx, agent.TenantID)
			if err != nil {
				_ = c.Error(err)
				return
			}
			for _, kb := range filterKnowledgeBasesForSharedAgent(kbs, agent) {
				if kb.Type == types.KnowledgeBaseTypeDocument {
					selected = append(selected, types.KnowledgeSearchScope{TenantID: kb.TenantID, KBID: kb.ID})
				}
			}
		} else {
			for _, id := range agentScope.IDs() {
				if id != "" {
					selected = append(selected, types.KnowledgeSearchScope{TenantID: agent.TenantID, KBID: id})
				}
			}
		}
		selected = filterKnowledgeSearchScopesForAPIKey(ctx, selected)
	} else if scopes, restricted := tenantAPIKeySearchScopes(ctx); restricted {
		selected = scopes
		if selected == nil {
			selected = []types.KnowledgeSearchScope{}
		}
	}
	scopes, err := resolver.ResolveKnowledgeCollectionScopes(ctx, selected)
	if err != nil {
		_ = c.Error(err)
		return
	}
	h.respondKnowledgeCollection(c, ctx, scopes, types.KnowledgeCollectionSelectionRequest{Mode: "search", Keyword: keyword, Offset: offset, Limit: limit, FileTypes: fileTypes}, readScope)
}
