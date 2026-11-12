package middleware

import (
	"net/http"
	"strings"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

// Employee management write-block (W-ACL-01, unit BE2).
//
// Requirement: an ordinary employee (a human session whose effective role in
// the active tenant is below Admin) may only chat and browse history. Every
// management write must be rejected SERVER-SIDE, not merely hidden in the
// frontend: knowledge-base management / upload, model & agent configuration,
// organization settings, API-key creation, public sharing, MCP endpoint
// configuration, etc.
//
// This middleware is a TIGHTENING layer on top of the existing RBAC guards
// (RequireRole / RequireOwnershipOrRole / KBAccess). It does not replace or
// re-implement them: role resolution happened earlier in middleware/auth.go
// (resolveTenantRole / EffectiveTenantRole) and is only read back here. The
// reason it exists as a separate layer is that several management writes are
// reachable at Viewer/Contributor level today (knowledge tags / folder /
// batch-reparse / move, KB creation/copy, agent authoring, ...), and the
// RBAC guards intentionally honour KB ownership — a Contributor who created a
// KB may edit it, which is correct for the generic product but wrong for the
// "employees are chat-only" deployment this layer serves.
//
// Allow rules, in order (each one is a regression red line):
//
//  1. Non-write methods (GET/HEAD/OPTIONS) always pass — citations, lists and
//     detail reads against management resources stay available to employees.
//  2. Paths outside the blocked prefixes always pass — chat, sessions,
//     retrieval (knowledge-chat / knowledge-search / search), suggestions,
//     memory, per-user favourites and all self-service routes.
//  3. API-key principals (authSession.APIKeyScope) always pass. Machine
//     channels (including the Nextcloud sync link) are governed by the
//     API-key gate's capability/scope system, not by the human role ladder.
//  4. System admins pass.
//  5. Callers whose tenant role is Admin or Owner pass.
//
// Everything else — an authenticated human with a role below Admin sending
// POST/PUT/PATCH/DELETE at a blocked prefix — gets 403 with
// {"success":false,"message":"此操作需要管理员权限"}.
//
// Unlike the RBAC guards, this check is NOT gated on cfg.Tenant.EnableRBAC:
// the employee policy is a product requirement of this deployment shape, not
// a rollout window that should fail open.
//
// Matching uses gin's registered route template (c.FullPath(), e.g.
// "/api/v1/knowledge-bases/:id"), never the raw URL, so numeric IDs in the
// path can't evade the prefix and the exemption list stays exact. The same
// technique is used by RequireNextcloudScopedRoutes and the API-key gate.

// employeeBlockedWritePrefixes is the single, centralized declaration of the
// management surfaces whose WRITE methods are reserved for admins. A prefix
// matches a route template either exactly or at a "/" boundary, so
// "/api/v1/knowledge" does NOT cover "/api/v1/knowledge-chat" or
// "/api/v1/knowledge-search".
//
// Prefix → requirement category (W-ACL-01):
//
//	Category 1 (knowledge management / upload & import)
//	  /api/v1/knowledge-bases   KB lifecycle + content subroutes (knowledge,
//	                            faq, tags, shares, enrichment-templates,
//	                            nextcloud-capacity)
//	  /api/v1/knowledge         documents (update/delete/reparse/tags/folder/
//	                            move/batch-*) — incl. the Viewer-level writes
//	  /api/v1/knowledgebase     wiki pages (note: singular, distinct from
//	                            knowledge-bases)
//	  /api/v1/chunks            chunk mutations
//	  (no /datasets or /documents prefixes exist in this codebase; document
//	  writes live under /knowledge and /knowledge-bases/:id/knowledge)
//
//	Category 2 (model & agent configuration, incl. tenant infra config)
//	  /api/v1/models            model CRUD + credentials
//	  /api/v1/agents            agent authoring/copy/shares + embed/im
//	                            channel creation under agents
//	  /api/v1/skills            skill catalog install/remove (writes are all
//	                            under /skills/catalog)
//	  /api/v1/sandbox-configs   sandbox provider configs + baked skills
//	  /api/v1/initialization    KB model/storage initialization & model probes
//	  /api/v1/weknoracloud      tenant SaaS credentials
//	  /api/v1/vector-stores     tenant vector-store configs
//	  /api/v1/storage-backends  tenant object-storage configs
//	  /api/v1/web-search-providers tenant search-provider configs + creds
//	  /api/v1/datasource        external data sources, sync control and the
//	                            Nextcloud pairing/GC admin surface
//	  /api/v1/evaluation        evaluation runs/reviews (cost-driving)
//	  /api/v1/im-channels       IM channel config (carries bot credentials)
//	  /api/v1/wechat            WeChat QR login for IM channels
//	  /api/v1/shared-agents/disabled  space-wide shared-agent setting
//	  /api/v1/system            system config probes + /system/admin platform
//	                            surface (human system admins pass via rule 4)
//
//	Category 3 (organization settings)
//	  /api/v1/tenants           tenant settings/members/roles/invitations/
//	                            directory & sync (POST /tenants itself and
//	                            /tenants/:id/leave are self-service and
//	                            exempted below)
//	  /api/v1/organizations     org create/update/member-role/invite writes
//	  /api/v1/group-access      directory group resource grants
//
//	Category 4 (API keys)
//	  covered by /api/v1/tenants (tenant API keys: /tenants/:id/api-keys,
//	  /tenants/:id/api-principal-*) and /api/v1/system (platform API keys:
//	  /system/admin/api-keys)
//
//	Category 5 (public sharing)
//	  /api/v1/embed-channels    public embed channel management (create via
//	                            /agents/:id/embed-channels is covered by the
//	                            /api/v1/agents prefix; KB/agent shares live
//	                            under their resource prefixes; tenant
//	                            invite-links under /api/v1/tenants)
//
//	Category 6 (MCP endpoint configuration)
//	  /api/v1/mcp-endpoints     workspace MCP endpoint CRUD + token rotation
//	  /api/v1/mcp-services      MCP tool-server configs (per-user OAuth flows
//	                            are exempted below)
var employeeBlockedWritePrefixes = []string{
	// Category 1: knowledge management / upload & import
	"/api/v1/knowledge-bases",
	"/api/v1/knowledge",
	"/api/v1/knowledgebase",
	"/api/v1/chunks",

	// Category 2: model & agent configuration (+ tenant infra config)
	"/api/v1/models",
	"/api/v1/agents",
	"/api/v1/skills",
	"/api/v1/sandbox-configs",
	"/api/v1/initialization",
	"/api/v1/weknoracloud",
	"/api/v1/vector-stores",
	"/api/v1/storage-backends",
	"/api/v1/web-search-providers",
	"/api/v1/datasource",
	"/api/v1/evaluation",
	"/api/v1/im-channels",
	"/api/v1/wechat",
	"/api/v1/shared-agents/disabled",
	"/api/v1/system",

	// Category 3: organization settings
	"/api/v1/tenants",
	"/api/v1/organizations",
	"/api/v1/group-access",

	// Category 5: public sharing
	"/api/v1/embed-channels",

	// Category 6: MCP endpoint configuration
	"/api/v1/mcp-services",
	"/api/v1/mcp-endpoints",
}

// employeeWriteBlockExemptions lists exact route templates that sit inside a
// blocked prefix but must stay reachable for ordinary employees. Every entry
// is either a READ that happens to travel over POST, or a SELF-SERVICE
// action on the caller's own state; each one keeps its own downstream guard
// (Viewer floor / KBAccess / handler-side enforcement), so exempting it here
// does not open any management capability.
var employeeWriteBlockExemptions = map[string]struct{}{
	// Retrieval: hybrid search prefers POST (GET kept for compatibility) —
	// blocking it would break citation/search for employees.
	"/api/v1/knowledge-bases/:id/hybrid-search": {},
	// Per-(user,kb) pin toggle — personal UI state, Viewer+ by design.
	"/api/v1/knowledge-bases/:id/pin": {},
	// Read-only export of originals via POST; KBEditOrContributor still
	// applies downstream.
	"/api/v1/knowledge-bases/:id/knowledge/batch-download": {},
	// FAQ search is a read route served over POST.
	"/api/v1/knowledge-bases/:id/faq/search": {},
	// Self-service: create another workspace for yourself (identity-level
	// route, see isTenantOptionalAPI in auth.go).
	"/api/v1/tenants": {},
	// Self-service: leave the current workspace (any member may quit).
	"/api/v1/tenants/:id/leave": {},
	// Per-user MCP OAuth flows used by the in-chat agent tool flow; the
	// handler only ever touches the caller's own token. metadata/refresh
	// writes a principal-scoped snapshot for Viewer callers (tenant-wide
	// refresh for static-auth services stays Admin+-enforced in the handler).
	"/api/v1/mcp-services/:id/metadata/refresh":    {},
	"/api/v1/mcp-services/:id/oauth/authorize-url": {},
	"/api/v1/mcp-services/:id/oauth/token":         {},
}

// employeeWriteBlockedPath reports whether the registered route template is
// a management surface for the purposes of the employee write-block. Matching
// is exact-or-"/"-boundary so sibling prefixes (knowledge vs knowledge-chat,
// agents vs agent) never shadow each other.
func employeeWriteBlockedPath(fullPath string) bool {
	if fullPath == "" {
		return false
	}
	if _, exempt := employeeWriteBlockExemptions[fullPath]; exempt {
		return false
	}
	for _, prefix := range employeeBlockedWritePrefixes {
		if fullPath == prefix || strings.HasPrefix(fullPath, prefix+"/") {
			return true
		}
	}
	return false
}

// employeeManagementWriteMethod reports whether the HTTP method carries
// state change semantics for this policy. GET/HEAD/OPTIONS are always
// allowed on management resources so listing, detail, citation and progress
// reads keep working for employees.
func employeeManagementWriteMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

// EmployeeManagementBlock returns the gin middleware implementing W-ACL-01.
// Register it on the authenticated /api/v1 group (after middleware.Auth and
// before the per-route RBAC guards) — see internal/router/router.go.
func EmployeeManagementBlock() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !employeeManagementWriteMethod(c.Request.Method) {
			c.Next()
			return
		}
		if !employeeWriteBlockedPath(c.FullPath()) {
			c.Next()
			return
		}
		ctx := c.Request.Context()

		// Machine channel: X-API-Key principals keep their capability/scope
		// authorization from the API-key gate (Nextcloud sync included).
		if _, isAPIKey := types.TenantAPIKeyScopeFromContext(ctx); isAPIKey {
			c.Next()
			return
		}
		// Platform system admins operate above the tenant role ladder.
		if types.IsSystemAdminFromContext(ctx) {
			c.Next()
			return
		}
		role := types.TenantRoleFromContext(ctx)
		if role.HasPermission(types.TenantRoleAdmin) {
			c.Next()
			return
		}

		// Ordinary employee (Viewer/Contributor human) on a management
		// write: reject server-side.
		uid, _ := types.UserIDFromContext(ctx)
		tenantID := types.CallerFromContext(ctx).TenantID
		logger.Warnf(ctx,
			"[employee-block] non-admin write to management route rejected: "+
				"user=%s role=%s tenant=%d method=%s path=%s",
			uid, role, tenantID, c.Request.Method, c.FullPath())
		if svc := AuditServiceFromContext(c); svc != nil {
			_ = svc.LogDenied(ctx, c, tenantID, uid, string(role), types.TenantRoleAdmin)
		}
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
			"success": false,
			"message": "此操作需要管理员权限",
		})
	}
}
