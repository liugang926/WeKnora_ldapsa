package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
)

// NextcloudScopedSessionReader loads sessions for scoped route authorization.
type NextcloudScopedSessionReader interface {
	ScopedSession(context.Context, uint64, string, string) (bool, error)
}

// NextcloudScopedDocumentReader loads documents for scoped route authorization.
type NextcloudScopedDocumentReader interface {
	ScopedDocument(context.Context, uint64, string) (string, error)
}

// RequireNextcloudScopedRoutes enforces the route boundary of scoped source sessions.
// Source-only entrants can create a private QA session and query explicit
// authorized KBs. Agent tools, arbitrary model inputs, attachments and general
// tenant history remain closed until their own scoped contracts are validated.
func RequireNextcloudScopedRoutes(
	groups interfaces.GroupAccessService,
	sessions ...NextcloudScopedSessionReader,
) gin.HandlerFunc {
	return func(c *gin.Context) {
		if groups == nil {
			c.Next()
			return
		}
		ctx := c.Request.Context()
		p, ok := types.PrincipalFromContext(ctx)
		if !ok || p.Type != types.PrincipalWebUser {
			c.Next()
			return
		}
		tenant, ok := types.TenantIDFromContext(ctx)
		if !ok {
			c.Next()
			return
		}
		role, err := groups.EffectiveTenantRole(ctx, p.ID, tenant, time.Now().UTC())
		if err != nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "permission_scope_unavailable"})
			return
		}
		if !role.ScopedOnly {
			c.Next()
			return
		}
		deny := func() { c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "scoped_knowledge_base_only"}) }
		if !role.Member {
			deny()
			return
		}
		path := c.FullPath()
		// Only personal authentication state is available outside the KB scope.
		personalAuth := path == "/api/v1/auth/me" && c.Request.Method == http.MethodGet ||
			path == "/api/v1/auth/validate" && c.Request.Method == http.MethodGet ||
			path == "/api/v1/auth/me/preferences" && c.Request.Method == http.MethodPut ||
			(path == "/api/v1/auth/switch-tenant" || path == "/api/v1/auth/logout" || path == "/api/v1/auth/refresh") &&
				c.Request.Method == http.MethodPost
		if personalAuth {
			c.Next()
			return
		}
		if path == "/api/v1/sessions" && c.Request.Method == http.MethodPost {
			c.Next()
			return
		}
		qa := path == "/api/v1/knowledge-chat/:session_id"
		search := path == "/api/v1/knowledge-search"
		if qa || search {
			body, err := io.ReadAll(io.LimitReader(c.Request.Body, (1<<20)+1))
			if err != nil || len(body) > 1<<20 {
				deny()
				return
			}
			c.Request.Body = io.NopCloser(bytes.NewReader(body))
			var request struct {
				KBIDs             []string          `json:"knowledge_base_ids"`
				KBID              string            `json:"knowledge_base_id"`
				KnowledgeIDs      []string          `json:"knowledge_ids"`
				Mentions          []json.RawMessage `json:"mentioned_items"`
				AgentID           string            `json:"agent_id"`
				AgentEnabled      bool              `json:"agent_enabled"`
				ModelID           string            `json:"summary_model_id"`
				MCP               []string          `json:"mcp_service_ids"`
				Skills            []string          `json:"skill_names"`
				Images            []json.RawMessage `json:"images"`
				AttachmentUploads []json.RawMessage `json:"attachment_uploads"`
				AttachmentIDs     []string          `json:"attachment_ids"`
				WebSearch         bool              `json:"web_search_enabled"`
				Browser           bool              `json:"local_browser_enabled"`
			}
			if json.Unmarshal(body, &request) != nil || request.AgentID != "" || request.AgentEnabled ||
				request.ModelID != "" ||
				len(request.Mentions) > 0 ||
				len(request.MCP) > 0 ||
				len(request.Skills) > 0 ||
				len(request.Images) > 0 ||
				len(request.AttachmentUploads) > 0 ||
				len(request.AttachmentIDs) > 0 ||
				request.WebSearch ||
				request.Browser {
				deny()
				return
			}
			if request.KBID != "" {
				request.KBIDs = append(request.KBIDs, request.KBID)
			}
			if len(request.KBIDs) == 0 {
				deny()
				return
			}
			for _, kb := range request.KBIDs {
				if groups.Authorize(
					ctx,
					tenant,
					types.GroupResourceTypeKnowledgeBase,
					kb,
					types.ResourceActionRead,
				) != nil {
					deny()
					return
				}
			}
			if len(request.KnowledgeIDs) > 0 {
				if !qa || len(request.KnowledgeIDs) > 100 || len(sessions) == 0 {
					deny()
					return
				}
				documents, ok := sessions[0].(NextcloudScopedDocumentReader)
				if !ok {
					deny()
					return
				}
				for _, id := range request.KnowledgeIDs {
					kb, err := documents.ScopedDocument(ctx, tenant, id)
					matched := false
					for _, selected := range request.KBIDs {
						matched = matched || selected == kb
					}
					if err != nil || !matched {
						deny()
						return
					}
				}
			}
		}
		sessionRoute := strings.HasPrefix(path, "/api/v1/sessions/") || strings.HasPrefix(path, "/api/v1/messages/") ||
			qa
		if sessionRoute {
			allowed := qa || path == "/api/v1/sessions/:id" && c.Request.Method == http.MethodGet ||
				path == "/api/v1/sessions/:session_id/stop" ||
				path == "/api/v1/sessions/continue-stream/:session_id" ||
				path == "/api/v1/messages/:session_id/load"
			if !allowed || len(sessions) == 0 || sessions[0] == nil {
				deny()
				return
			}
			id := c.Param("session_id")
			if id == "" {
				id = c.Param("id")
			}
			valid, err := sessions[0].ScopedSession(ctx, tenant, p.ID, id)
			if err != nil || !valid {
				deny()
				return
			}
			c.Next()
			return
		}
		if search {
			c.Next()
			return
		}
		allowed := false
		switch path {
		case "/api/v1/knowledge-bases", "/api/v1/knowledge-bases/:id", "/api/v1/knowledge-bases/:id/hybrid-search",
			"/api/v1/knowledge-bases/:id/knowledge", "/api/v1/knowledge-bases/:id/knowledge/folders",
			"/api/v1/knowledge/batch",
			"/api/v1/knowledge/:id",
			"/api/v1/knowledge/:id/stages",
			"/api/v1/knowledge/:id/spans",
			"/api/v1/knowledge/:id/preview",
			"/api/v1/knowledge/search",
			"/api/v1/chunks/:knowledge_id", "/api/v1/chunks/by-id/:id", "/api/v1/integrations/nextcloud/ask-target":
			allowed = true
		}

		if !allowed ||
			(c.Request.Method != http.MethodGet && !strings.Contains(path, "hybrid-search") && !strings.Contains(
				path,
				"/ask",
			)) {
			deny()
			return
		}
		c.Next()
	}
}
