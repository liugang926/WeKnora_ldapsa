package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

// employeeBlockPrincipal describes the auth session a test simulates. A nil
// apiKeyScope means a human (JWT) session; the role is what
// middleware/auth.go resolved via resolveTenantRole/EffectiveTenantRole.
type employeeBlockPrincipal struct {
	role        types.TenantRole
	systemAdmin bool
	apiKeyScope *types.TenantAPIKeyScope
}

var (
	viewerPrincipal    = employeeBlockPrincipal{role: types.TenantRoleViewer}
	contributorPending = employeeBlockPrincipal{role: types.TenantRoleContributor}
	adminPrincipal     = employeeBlockPrincipal{role: types.TenantRoleAdmin}
	ownerPrincipal     = employeeBlockPrincipal{role: types.TenantRoleOwner}
	// systemAdminPrincipal is a Viewer in the tenant but a platform system
	// admin — rule 4 must let them through.
	systemAdminPrincipal = employeeBlockPrincipal{role: types.TenantRoleViewer, systemAdmin: true}
	// machinePrincipal simulates the X-API-Key channel: role is synthesized
	// as Viewer for legacy-guard compatibility, the real authority is the
	// API-key scope.
	machinePrincipal = employeeBlockPrincipal{
		role: types.TenantRoleViewer,
		apiKeyScope: &types.TenantAPIKeyScope{
			ScopeType:    types.APIKeyScopeTenant,
			FullAccess:   false,
			Capabilities: types.StringArray{string(types.APIKeyCapabilityIngest)},
		},
	}
)

// employeeBlockHarness registers one route (method + gin route template
// mirroring the real declaration in internal/router/routes_*.go), seeds the
// auth context the way middleware.Auth would, attaches the employee block
// and returns the response recorder.
func employeeBlockHarness(t *testing.T, method, template string, p employeeBlockPrincipal) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		ctx := c.Request.Context()
		if p.role != "" {
			ctx = context.WithValue(ctx, types.TenantRoleContextKey, p.role)
		}
		if p.systemAdmin {
			ctx = context.WithValue(ctx, types.SystemAdminContextKey, true)
		}
		if p.apiKeyScope != nil {
			ctx = types.WithTenantAPIKeyScope(ctx, *p.apiKeyScope)
		}
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})
	r.Use(EmployeeManagementBlock())
	r.Handle(method, template, func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(method, employeeConcretePath(template), nil))
	return w
}

// employeeConcretePath turns a gin route template into a requestable URL by
// substituting every :param and *catch-all with a dummy value.
func employeeConcretePath(template string) string {
	segments := strings.Split(template, "/")
	for i, seg := range segments {
		if strings.HasPrefix(seg, ":") || strings.HasPrefix(seg, "*") {
			segments[i] = "x"
		}
	}
	return strings.Join(segments, "/")
}

// employeeBlockedWriteSamples enumerates one representative management write
// per requirement category (W-ACL-01). Templates are copied verbatim from
// internal/router/routes_*.go so path drift shows up here.
var employeeBlockedWriteSamples = []struct {
	method   string
	template string
	category string
}{
	// Category 1 — knowledge-base management / upload & import
	{http.MethodPost, "/api/v1/knowledge-bases", "knowledge"},
	{http.MethodPut, "/api/v1/knowledge-bases/:id", "knowledge"},
	{http.MethodDelete, "/api/v1/knowledge-bases/:id", "knowledge"},
	{http.MethodPost, "/api/v1/knowledge-bases/:id/knowledge/file", "knowledge"},
	{http.MethodPost, "/api/v1/knowledge-bases/:id/knowledge/url", "knowledge"},
	{http.MethodPut, "/api/v1/knowledge-bases/:id/knowledge/folders", "knowledge"},
	{http.MethodPut, "/api/v1/knowledge/tags", "knowledge"},
	{http.MethodPost, "/api/v1/knowledge/batch-reparse", "knowledge"},
	{http.MethodPost, "/api/v1/knowledge/folder", "knowledge"},
	{http.MethodPost, "/api/v1/knowledge/move", "knowledge"},
	{http.MethodPost, "/api/v1/knowledge/batch-delete", "knowledge"},
	{http.MethodDelete, "/api/v1/knowledge/:id", "knowledge"},
	{http.MethodPut, "/api/v1/knowledge/:id", "knowledge"},
	{http.MethodPut, "/api/v1/knowledgebase/:kb_id/wiki/pages/*slug", "knowledge"},
	{http.MethodPost, "/api/v1/knowledgebase/:kb_id/wiki/rebuild-links", "knowledge"},
	{http.MethodPut, "/api/v1/chunks/:knowledge_id/:id", "knowledge"},
	{http.MethodDelete, "/api/v1/chunks/:knowledge_id/:id", "knowledge"},
	{http.MethodPost, "/api/v1/knowledge-bases/:id/faq/entries", "knowledge"},
	{http.MethodPut, "/api/v1/knowledge-bases/:id/tags/:tag_id", "knowledge"},
	{http.MethodPut, "/api/v1/knowledge-bases/:id/enrichment-templates/:purpose", "knowledge"},
	{http.MethodPut, "/api/v1/knowledge-bases/:id/nextcloud-capacity", "knowledge"},

	// Category 2 — model & agent configuration (+ tenant infra config)
	{http.MethodPost, "/api/v1/models", "config"},
	{http.MethodPut, "/api/v1/models/:id", "config"},
	{http.MethodPut, "/api/v1/models/:id/credentials", "config"},
	{http.MethodPut, "/api/v1/agents/:id", "config"},
	{http.MethodPost, "/api/v1/agents/:id/copy", "config"},
	{http.MethodDelete, "/api/v1/agents/:id/shares/:share_id", "config"},
	{http.MethodPost, "/api/v1/skills/catalog/:id/install", "config"},
	{http.MethodPut, "/api/v1/sandbox-configs/:id", "config"},
	{http.MethodPatch, "/api/v1/sandbox-configs/:id/skills/:skillId", "config"},
	{http.MethodPost, "/api/v1/initialization/initialize/:kbId", "config"},
	{http.MethodPost, "/api/v1/weknoracloud/credentials", "config"},
	{http.MethodPost, "/api/v1/vector-stores", "config"},
	{http.MethodPut, "/api/v1/storage-backends/:id/default", "config"},
	{http.MethodPut, "/api/v1/web-search-providers/:id/credentials", "config"},
	{http.MethodPost, "/api/v1/datasource/:id/sync", "config"},
	{http.MethodPost, "/api/v1/evaluation", "config"},
	{http.MethodPut, "/api/v1/evaluation/:taskId/cases/:questionId/review", "config"},
	{http.MethodPut, "/api/v1/im-channels/:id", "config"},
	{http.MethodPost, "/api/v1/wechat/qrcode", "config"},
	{http.MethodPost, "/api/v1/shared-agents/disabled", "config"},
	{http.MethodPost, "/api/v1/system/admin/api-keys", "config"},

	// Category 3 — organization settings
	{http.MethodPut, "/api/v1/tenants/:id", "org"},
	{http.MethodDelete, "/api/v1/tenants/:id", "org"},
	{http.MethodPut, "/api/v1/tenants/kv/:key", "org"},
	{http.MethodPost, "/api/v1/tenants/:id/members", "org"},
	{http.MethodPut, "/api/v1/tenants/:id/members/:user_id", "org"},
	{http.MethodDelete, "/api/v1/tenants/:id/members/:user_id", "org"},
	{http.MethodPost, "/api/v1/tenants/:id/invitations", "org"},
	{http.MethodPost, "/api/v1/tenants/:id/invite-links", "org"},
	{http.MethodPost, "/api/v1/tenants/:id/directory/members", "org"},
	{http.MethodPost, "/api/v1/tenants/:id/directory-groups", "org"},
	{http.MethodPut, "/api/v1/group-access/:resource_type/:resource_id", "org"},
	{http.MethodPost, "/api/v1/organizations/:id/invite", "org"},
	{http.MethodPut, "/api/v1/organizations/:id/members/:tenant_id", "org"},

	// Category 4 — API keys (tenant + platform surfaces)
	{http.MethodPost, "/api/v1/tenants/:id/api-keys", "apikeys"},
	{http.MethodPut, "/api/v1/tenants/:id/api-keys/:key_id", "apikeys"},
	{http.MethodDelete, "/api/v1/tenants/:id/api-keys/:key_id", "apikeys"},
	{http.MethodPut, "/api/v1/tenants/:id/api-principal-config", "apikeys"},
	{http.MethodDelete, "/api/v1/system/admin/api-keys/:key_id", "apikeys"},

	// Category 5 — public sharing
	{http.MethodPost, "/api/v1/knowledge-bases/:id/shares", "sharing"},
	{http.MethodPut, "/api/v1/knowledge-bases/:id/shares/:share_id", "sharing"},
	{http.MethodPost, "/api/v1/agents/:id/shares", "sharing"},
	{http.MethodPut, "/api/v1/embed-channels/:channel_id", "sharing"},
	{http.MethodPost, "/api/v1/embed-channels/:channel_id/rotate-token", "sharing"},
	{http.MethodPost, "/api/v1/agents/:id/embed-channels", "sharing"},

	// Category 6 — MCP endpoint configuration
	{http.MethodPost, "/api/v1/mcp-endpoints", "mcp"},
	{http.MethodPut, "/api/v1/mcp-endpoints/:endpoint_id", "mcp"},
	{http.MethodPost, "/api/v1/mcp-endpoints/:endpoint_id/rotate-token", "mcp"},
	{http.MethodPost, "/api/v1/mcp-services", "mcp"},
	{http.MethodPut, "/api/v1/mcp-services/:id/credentials", "mcp"},
	{http.MethodPut, "/api/v1/mcp-services/:id/tool-approvals/:tool_name", "mcp"},
}

// TestEmployeeManagementBlock_EmployeeRejectedByCategory walks the full
// blocked-route sample table for an ordinary employee (Viewer). Every entry
// must 403 regardless of the Viewer/Contributor floor the underlying RBAC
// guard would apply.
func TestEmployeeManagementBlock_EmployeeRejectedByCategory(t *testing.T) {
	for _, tc := range employeeBlockedWriteSamples {
		t.Run(tc.category+" "+tc.method+" "+tc.template, func(t *testing.T) {
			w := employeeBlockHarness(t, tc.method, tc.template, viewerPrincipal)
			if w.Code != http.StatusForbidden {
				t.Fatalf("viewer write must be 403, got %d body=%s", w.Code, w.Body.String())
			}
			var body struct {
				Success bool   `json:"success"`
				Message string `json:"message"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("response is not the contract JSON: %v body=%s", err, w.Body.String())
			}
			if body.Success || body.Message != "此操作需要管理员权限" {
				t.Fatalf("unexpected body: %+v", body)
			}
		})
	}
}

// TestEmployeeManagementBlock_ContributorRejected: the requirement counts
// every non-admin human as an ordinary employee, so Contributor is blocked
// too — including on routes a Contributor owns (the ownership guards below
// would pass them, this layer must not).
func TestEmployeeManagementBlock_ContributorRejected(t *testing.T) {
	blockedForContributor := []struct{ method, template string }{
		{http.MethodPost, "/api/v1/knowledge-bases"},
		{http.MethodPut, "/api/v1/knowledge-bases/:id"}, // would pass EditableKBOrAdmin for a KB the contributor created
		{http.MethodPut, "/api/v1/knowledge/tags"},
		{http.MethodPost, "/api/v1/knowledge/move"},
		{http.MethodPut, "/api/v1/agents/:id"},
		{http.MethodPost, "/api/v1/tenants/:id/api-keys"},
	}
	for _, tc := range blockedForContributor {
		t.Run(tc.method+" "+tc.template, func(t *testing.T) {
			w := employeeBlockHarness(t, tc.method, tc.template, contributorPending)
			if w.Code != http.StatusForbidden {
				t.Fatalf("contributor write must be 403, got %d", w.Code)
			}
		})
	}
}

// TestEmployeeManagementBlock_PrivilegedPrincipalsPass: tenant Admin, tenant
// Owner, platform system admin and the API-key machine channel all pass on
// every management write.
func TestEmployeeManagementBlock_PrivilegedPrincipalsPass(t *testing.T) {
	passing := []employeeBlockPrincipal{adminPrincipal, ownerPrincipal, systemAdminPrincipal, machinePrincipal}
	names := []string{"admin", "owner", "system-admin", "api-key"}
	for i, p := range passing {
		t.Run(names[i], func(t *testing.T) {
			for _, tc := range employeeBlockedWriteSamples {
				w := employeeBlockHarness(t, tc.method, tc.template, p)
				if w.Code != http.StatusOK {
					t.Fatalf("%s must pass %s %s, got %d body=%s",
						names[i], tc.method, tc.template, w.Code, w.Body.String())
				}
			}
		})
	}
}

// TestEmployeeManagementBlock_EmployeeReadsPass: the same management routes
// opened with GET/HEAD must flow through for an employee — listing,
// citation and progress reads are a regression red line.
func TestEmployeeManagementBlock_EmployeeReadsPass(t *testing.T) {
	reads := []struct{ method, template string }{
		{http.MethodGet, "/api/v1/knowledge-bases"},
		{http.MethodGet, "/api/v1/knowledge-bases/:id"},
		{http.MethodGet, "/api/v1/knowledge-bases/:id/knowledge"},
		{http.MethodGet, "/api/v1/knowledge/:id"},
		{http.MethodGet, "/api/v1/knowledge/search"},
		{http.MethodGet, "/api/v1/knowledge/batch"},
		{http.MethodGet, "/api/v1/chunks/:knowledge_id"},
		{http.MethodGet, "/api/v1/chunks/by-id/:id"},
		{http.MethodGet, "/api/v1/knowledge-bases/:id/faq/entries"},
		{http.MethodGet, "/api/v1/knowledgebase/:kb_id/wiki/pages"},
		{http.MethodGet, "/api/v1/knowledge-bases/:id/tags"},
		{http.MethodGet, "/api/v1/models"},
		{http.MethodGet, "/api/v1/tenants/:id"},
		{http.MethodGet, "/api/v1/tenants/:id/members"},
		{http.MethodGet, "/api/v1/tenants/:id/api-keys"},
		{http.MethodGet, "/api/v1/agents"},
		{http.MethodGet, "/api/v1/mcp-endpoints"},
		{http.MethodGet, "/api/v1/knowledge-bases/:id/hybrid-search"},
		{http.MethodHead, "/api/v1/knowledge-bases/:id"},
	}
	for _, tc := range reads {
		t.Run(tc.method+" "+tc.template, func(t *testing.T) {
			w := employeeBlockHarness(t, tc.method, tc.template, viewerPrincipal)
			if w.Code != http.StatusOK {
				t.Fatalf("employee read must pass, got %d body=%s", w.Code, w.Body.String())
			}
		})
	}
	// OPTIONS (CORS preflight) is never a management write either; gin has
	// no route registered here so 404 is fine — 403 is not.
	w := employeeBlockHarness(t, http.MethodOptions, "/api/v1/knowledge-bases/:id", viewerPrincipal)
	if w.Code == http.StatusForbidden {
		t.Fatalf("OPTIONS must not be treated as a management write, got %d", w.Code)
	}
}

// TestEmployeeManagementBlock_ChatAndSelfServicePass is the regression red
// line list: chat, session history, retrieval, suggestions, memory, personal
// favourites and user self-service routes must stay reachable for employees.
func TestEmployeeManagementBlock_ChatAndSelfServicePass(t *testing.T) {
	allowed := []struct{ method, template string }{
		// Chat / sessions
		{http.MethodPost, "/api/v1/sessions"},
		{http.MethodPut, "/api/v1/sessions/:id"},
		{http.MethodDelete, "/api/v1/sessions/:id"},
		{http.MethodPost, "/api/v1/sessions/:session_id/generate_title"},
		{http.MethodPost, "/api/v1/sessions/:session_id/attachments"},
		{http.MethodPost, "/api/v1/sessions/:session_id/stop"},
		{http.MethodPost, "/api/v1/sessions/:session_id/fork"},
		{http.MethodPost, "/api/v1/sessions/:session_id/rewind"},
		{http.MethodPost, "/api/v1/sessions/:session_id/pin"},
		{http.MethodPost, "/api/v1/sessions/:session_id/messages/:message_id/suggestions"},
		{http.MethodPost, "/api/v1/sessions/:session_id/suggestion-events"},
		{http.MethodPost, "/api/v1/knowledge-chat/:session_id"},
		{http.MethodPost, "/api/v1/agent-chat/:session_id"},
		{http.MethodPost, "/api/v1/knowledge-search"},
		// Message history
		{http.MethodPost, "/api/v1/messages/search"},
		{http.MethodDelete, "/api/v1/messages/:session_id/:id"},
		// Personal favourites
		{http.MethodPost, "/api/v1/user/favorites"},
		{http.MethodDelete, "/api/v1/user/favorites/:type/:id"},
		// Memory (conversation memory)
		{http.MethodPut, "/api/v1/memory/settings"},
		{http.MethodPost, "/api/v1/memory/items"},
		{http.MethodDelete, "/api/v1/memory/items/:id"},
		// In-chat agent flows (boundary check: /agent vs /agents)
		{http.MethodPost, "/api/v1/agent/tool-approvals/:pending_id"},
		{http.MethodPost, "/api/v1/agent/mcp-oauth-resolutions/:pending_id"},
		{http.MethodPost, "/api/v1/agent/mcp-oauth-resolutions/:pending_id/cancel"},
		// User self-service
		{http.MethodPost, "/api/v1/auth/switch-tenant"},
		{http.MethodPut, "/api/v1/auth/me/preferences"},
		{http.MethodPost, "/api/v1/auth/change-password"},
		{http.MethodPost, "/api/v1/auth/logout"},
		{http.MethodPost, "/api/v1/me/invitations/:inv_id/accept"},
		{http.MethodPost, "/api/v1/me/invitations/accept-by-token"},
		{http.MethodPut, "/api/v1/me/env-vars/skill"},
		{http.MethodPost, "/api/v1/me/browser"},
		// Read-only chunker preview (stateless, no persistence)
		{http.MethodPost, "/api/v1/chunker/preview"},
	}
	for _, tc := range allowed {
		t.Run(tc.method+" "+tc.template, func(t *testing.T) {
			w := employeeBlockHarness(t, tc.method, tc.template, viewerPrincipal)
			if w.Code != http.StatusOK {
				t.Fatalf("employee must keep access, got %d body=%s", w.Code, w.Body.String())
			}
		})
	}
}

// TestEmployeeManagementBlock_ExemptedReadsAndSelfService: routes inside a
// blocked prefix that are reads-over-POST or self-service actions stay open
// for employees; their own downstream guards still apply.
func TestEmployeeManagementBlock_ExemptedReadsAndSelfService(t *testing.T) {
	exempt := []struct{ method, template string }{
		{http.MethodPost, "/api/v1/knowledge-bases/:id/hybrid-search"}, // retrieval
		{http.MethodGet, "/api/v1/knowledge-bases/:id/hybrid-search"},
		{http.MethodPut, "/api/v1/knowledge-bases/:id/pin"},                       // per-user pin
		{http.MethodPost, "/api/v1/knowledge-bases/:id/knowledge/batch-download"}, // read-only export
		{http.MethodPost, "/api/v1/knowledge-bases/:id/faq/search"},               // search
		{http.MethodPost, "/api/v1/tenants"},                                      // self-service workspace creation
		{http.MethodPost, "/api/v1/tenants/:id/leave"},                            // self-service quit
		{http.MethodPost, "/api/v1/mcp-services/:id/metadata/refresh"},            // caller-scoped snapshot
		{http.MethodPost, "/api/v1/mcp-services/:id/oauth/authorize-url"},         // per-user OAuth
		{http.MethodDelete, "/api/v1/mcp-services/:id/oauth/token"},               // revoke own token
	}
	for _, tc := range exempt {
		t.Run(tc.method+" "+tc.template, func(t *testing.T) {
			w := employeeBlockHarness(t, tc.method, tc.template, viewerPrincipal)
			if w.Code != http.StatusOK {
				t.Fatalf("exempt route must pass for employee, got %d body=%s", w.Code, w.Body.String())
			}
		})
	}
}

// TestEmployeeManagementBlock_NextcloudMachineChannelPass: the Nextcloud
// sync link authenticates as an API-key machine principal and must never be
// caught by the employee block — document ingest, datasource sync and the
// machine-event surfaces all stay open for it.
func TestEmployeeManagementBlock_NextcloudMachineChannelPass(t *testing.T) {
	sync := []struct{ method, template string }{
		{http.MethodPost, "/api/v1/knowledge-bases/:id/knowledge/file"}, // ingest (document sync)
		{http.MethodPut, "/api/v1/knowledge/:id"},                       // document update
		{http.MethodPost, "/api/v1/datasource/:id/sync"},                // source sync control
		{http.MethodPost, "/api/v1/knowledge/folder"},
		{http.MethodPost, "/api/v1/tenants/:id/api-keys"},
		{http.MethodPost, "/api/v1/integrations/nextcloud/onboarding/operations"}, // machine task surface
		{http.MethodPost, "/api/v1/integrations/nextcloud/events"},
	}
	for _, tc := range sync {
		t.Run(tc.method+" "+tc.template, func(t *testing.T) {
			w := employeeBlockHarness(t, tc.method, tc.template, machinePrincipal)
			if w.Code != http.StatusOK {
				t.Fatalf("API-key machine channel must pass %s %s, got %d body=%s",
					tc.method, tc.template, w.Code, w.Body.String())
			}
		})
	}
}

// TestEmployeeWriteBlockedPath matcher unit tests: boundary safety and the
// exemption carve-outs, independent of the HTTP harness.
func TestEmployeeWriteBlockedPath(t *testing.T) {
	blocked := []string{
		"/api/v1/knowledge-bases",
		"/api/v1/knowledge-bases/",
		"/api/v1/knowledge-bases/:id",
		"/api/v1/knowledge-bases/:id/knowledge/file",
		"/api/v1/knowledge",
		"/api/v1/knowledge/tags",
		"/api/v1/knowledge-bases/:id/shares/:share_id",
		"/api/v1/knowledgebase/:kb_id/wiki/pages/*slug",
		"/api/v1/agents/:id",
		"/api/v1/tenants/:id",
		"/api/v1/tenants/:id/api-keys/:key_id",
		"/api/v1/tenants/kv/:key",
		"/api/v1/system/admin/promote",
		"/api/v1/system/parser-engines/check",
	}
	notBlocked := []string{
		"", // no matched route (404 path) — never treat as blocked
		"/api/v1/sessions",
		"/api/v1/sessions/:id",
		"/api/v1/knowledge-chat/:session_id",
		"/api/v1/knowledge-search",
		"/api/v1/agent/tool-approvals/:pending_id",
		"/api/v1/agent/mcp-oauth-resolutions/:pending_id",
		"/api/v1/memory/items/:id",
		"/api/v1/messages/:session_id/:id",
		"/api/v1/user/favorites/:type/:id",
		"/api/v1/auth/switch-tenant",
		"/api/v1/auth/me/preferences",
		"/api/v1/me/env-vars/skill",
		"/api/v1/integrations/nextcloud/events",
		"/api/v1/integrations/nextcloud/onboarding/operations/:operation_id/pair",
		"/api/v1/chunker/preview",
		"/api/v1/knowledge-bases-unknown-sibling", // boundary: no shadow matching
		"/api/v1/knowledgebase-extra",             // boundary
		"/api/v1/agents-foo",
		"/api/v1/tenants",           // exempt: self-service create
		"/api/v1/tenants/:id/leave", // exempt: self-service quit
		"/api/v1/knowledge-bases/:id/hybrid-search",
		"/api/v1/knowledge-bases/:id/pin",
		"/api/v1/knowledge-bases/:id/knowledge/batch-download",
		"/api/v1/knowledge-bases/:id/faq/search",
		"/api/v1/mcp-services/:id/oauth/authorize-url",
		"/api/v1/mcp-services/:id/oauth/token",
		"/api/v1/mcp-services/:id/metadata/refresh",
	}
	for _, p := range blocked {
		if !employeeWriteBlockedPath(p) {
			t.Errorf("employeeWriteBlockedPath(%q) = false, want true", p)
		}
	}
	for _, p := range notBlocked {
		if employeeWriteBlockedPath(p) {
			t.Errorf("employeeWriteBlockedPath(%q) = true, want false", p)
		}
	}
}

// TestEmployeeManagementWriteMethod: only state-changing verbs count;
// GET/HEAD/OPTIONS are reads for this policy.
func TestEmployeeManagementWriteMethod(t *testing.T) {
	for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		if !employeeManagementWriteMethod(m) {
			t.Errorf("employeeManagementWriteMethod(%q) = false, want true", m)
		}
	}
	for _, m := range []string{http.MethodGet, http.MethodHead, http.MethodOptions} {
		if employeeManagementWriteMethod(m) {
			t.Errorf("employeeManagementWriteMethod(%q) = true, want false", m)
		}
	}
}
