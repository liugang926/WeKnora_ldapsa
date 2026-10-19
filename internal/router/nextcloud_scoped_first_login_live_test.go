//go:build sqlite_fts5

package router

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/handler"
	sessionhandler "github.com/Tencent/WeKnora/internal/handler/session"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// Real NC association/grants + actual WK LDAPS sync/bind/tenantless registration
// + production Auth/scoped middleware/RBAC registrars/output guards. The actor
// owns its new DB and never reads a prelinked Admin JWT as this reader.
func TestNextcloudScopedFirstLoginActualLDAPSAndProductionRoutes(t *testing.T) {
	fixture := os.Getenv("WEKNORA_SCOPED_FIRST_LOGIN_FIXTURE")
	if fixture == "" {
		t.Skip("owned NC/Samba AD/private descriptors required")
	}
	var input scopedFirstLoginInput
	scopedFirstLoginPrivateJSON(t, fixture, &input)
	t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
	t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", input.BaseURL)
	f := newScopedFirstLoginFoundation(t, input, fixture)
	defer f.runtime.Stop()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(middleware.ErrorHandler())
	r.Use(func(c *gin.Context) {
		c.Next()
		t.Logf("actual route method=%s template=%s status=%d", c.Request.Method, c.FullPath(), c.Writer.Status())
	})
	machine := r.Group("/api/v1/integrations/nextcloud/onboarding")
	machine.GET("/candidates", f.onboardingHandler.Candidates)
	machine.POST("/operations", f.onboardingHandler.Create)
	machine.GET("/operations/:operation_id", f.onboardingHandler.Status)
	machine.POST("/operations/:operation_id/pair", f.onboardingHandler.Pair)
	machine.PUT("/operations/:operation_id/grants", f.onboardingHandler.Grants)
	eventHandler := handler.NewNextcloudEventHandler(repository.NewNextcloudEventInboxRepository(f.db))
	r.POST("/api/v1/integrations/nextcloud/events", eventHandler.Receive)
	r.Use(middleware.Auth(f.tenants, f.users, f.members, nil, f.cfg, f.groups))
	r.Use(middleware.RequireNextcloudScopedRoutes(f.groups, f.onboarding))
	guards := newRBACGuards(
		f.cfg,
		f.baseHandler,
		nil,
		f.knowledgeHandler,
		nil,
		nil,
		f.base,
		f.knowledge,
		nil,
		nil,
		nil,
		f.groups,
	)
	v1 := r.Group("/api/v1")
	v1.Use(guards.apiKeyAuthorizer.Middleware())
	RegisterAuthRoutes(v1, handler.NewAuthHandler(f.cfg, f.users, f.tenants, nil, nil, f.runtime), guards)
	v1.POST(
		"/auth/ldap/login",
		middleware.DirectoryAuthRateLimit(nil),
		handler.NewDirectoryHandler(f.runtime).LDAPLogin,
	)
	RegisterKnowledgeBaseRoutes(v1, f.baseHandler, guards)
	RegisterKnowledgeRoutes(v1, f.knowledgeHandler, guards)
	// These are the actual registrars. Scoped requests must stop before these
	// unused handler dependencies; unknown routes/echo endpoints cannot pass.
	RegisterModelRoutes(v1, &handler.ModelHandler{}, &handler.ModelCredentialsHandler{}, guards)
	RegisterSessionRoutes(v1, &sessionhandler.Handler{}, nil, guards)
	RegisterMessageRoutes(v1, &handler.MessageHandler{}, guards)
	RegisterCustomAgentRoutes(v1, &handler.CustomAgentHandler{}, guards)
	guards.assertAPIKeyPoliciesMatchRoutes(r)
	listener, err := net.Listen("tcp", "0.0.0.0:8089")
	require.NoError(t, err)
	server := &http.Server{Handler: r, ReadHeaderTimeout: 5 * time.Second}
	defer server.Close()
	go func() { _ = server.Serve(listener) }()
	scopedFirstLoginWrite(
		t,
		fixture+".ready",
		map[string]any{"actual_ldaps_snapshot": true, "directory": input.Directory, "reader_guid": input.ReaderGUID},
	)
	require.Eventually(
		t,
		func() bool { _, err := os.Stat(fixture + ".committed"); return err == nil },
		90*time.Second,
		100*time.Millisecond,
	)
	scopedFirstLoginPrivateJSON(t, fixture, &input)
	var op types.NextcloudOnboardingOperation
	require.NoError(t, f.db.Where("operation_id=?", input.Operation).Take(&op).Error)
	require.Equal(t, "active", op.State)
	require.Equal(t, input.KnowledgeBaseID, op.KnowledgeBaseID)
	directories := repository.NewDirectoryRepository(f.db)
	identity, err := directories.GetIdentityByObjectGUID(context.Background(), input.Directory, input.ReaderGUID)
	require.NoError(t, err)
	require.NotNil(t, identity)
	require.Nil(t, identity.UserID)
	var preloginGrants int64
	require.NoError(
		t,
		f.db.Model(&types.NextcloudSourceGrant{}).
			Where("operation_id=? AND origin=? AND kind=? AND principal_id=?", op.OperationID, "nextcloud", "user", identity.ID).
			Count(&preloginGrants).
			Error,
	)
	require.Positive(
		t,
		preloginGrants,
		"actual NC complete permission publication must grant the unlinked directory identity",
	)
	row := f.processSource(t, input)
	var tenantCount int64
	require.NoError(t, f.db.Model(&types.Tenant{}).Count(&tenantCount).Error)
	request := func(method, url, bearer, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, url, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		out := httptest.NewRecorder()
		r.ServeHTTP(out, req)
		return out
	}
	loginBody, err := json.Marshal(
		map[string]string{"identifier": f.ldap.LoginIdentifier, "password": f.ldap.LoginPassword},
	)
	require.NoError(t, err)
	login := request(http.MethodPost, "/api/v1/auth/ldap/login", "", string(loginBody))
	require.Equal(t, http.StatusOK, login.Code, "real LDAP login must succeed")
	var response struct {
		Success      bool               `json:"success"`
		Token        string             `json:"token"`
		RefreshToken string             `json:"refresh_token"`
		User         *types.User        `json:"user"`
		ActiveTenant *types.Tenant      `json:"active_tenant"`
		Memberships  []types.Membership `json:"memberships"`
	}
	require.NoError(t, json.Unmarshal(login.Body.Bytes(), &response))
	require.True(t, response.Success)
	require.NotEmpty(t, response.Token)
	require.NotNil(t, response.User)
	require.Zero(t, response.User.TenantID)
	require.NotNil(t, response.ActiveTenant)
	require.Equal(t, uint64(77), response.ActiveTenant.ID)
	require.Len(t, response.Memberships, 1)
	require.True(t, response.Memberships[0].ScopedOnly)
	require.Equal(t, types.TenantRoleViewer, response.Memberships[0].Role)
	user, tokenTenant, err := f.users.ValidateToken(context.Background(), response.Token)
	require.NoError(t, err)
	require.Equal(t, response.User.ID, user.ID)
	require.Equal(t, uint64(77), tokenTenant)
	identity, err = directories.GetIdentityByObjectGUID(context.Background(), input.Directory, input.ReaderGUID)
	require.NoError(t, err)
	require.NotNil(t, identity.UserID)
	require.Equal(t, user.ID, *identity.UserID)
	noMembership := func() {
		var n int64
		require.NoError(t, f.db.Unscoped().Model(&types.TenantMember{}).Where("user_id=?", user.ID).Count(&n).Error)
		require.Zero(t, n)
		require.NoError(t, f.db.Model(&types.Tenant{}).Count(&n).Error)
		require.Equal(t, tenantCount, n)
		var persisted types.User
		require.NoError(t, f.db.Where("id=?", user.ID).Take(&persisted).Error)
		require.Zero(t, persisted.TenantID)
	}
	noMembership()
	for _, rbac := range []bool{true, false} {
		f.cfg.Tenant.EnableRBAC = &rbac
		allowed := request(http.MethodGet, "/api/v1/knowledge/"+row.ID+"/preview", response.Token, "")
		require.Equal(t, http.StatusOK, allowed.Code)
		digest := sha256.Sum256(allowed.Body.Bytes())
		require.Equal(
			t,
			input.FileSHA256,
			hex.EncodeToString(digest[:]),
			"production preview must return the actual unchanged NC original",
		)
		detail := request(http.MethodGet, "/api/v1/knowledge-bases/"+input.KnowledgeBaseID, response.Token, "")
		require.Equal(t, http.StatusOK, detail.Code)
		listed := request(http.MethodGet, "/api/v1/knowledge-bases", response.Token, "")
		require.Equal(t, http.StatusOK, listed.Code)
		var list struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		require.NoError(t, json.Unmarshal(listed.Body.Bytes(), &list))
		require.Len(t, list.Data, 1)
		require.Equal(t, input.KnowledgeBaseID, list.Data[0].ID)
		for _, url := range []string{"/api/v1/knowledge-bases/scoped-login-unrelated", "/api/v1/models", "/api/v1/agents", "/api/v1/sessions"} {
			denied := request(http.MethodGet, url, response.Token, "")
			require.Equal(t, http.StatusForbidden, denied.Code, url)
			require.NotContains(t, denied.Body.String(), "SCOPED_FIRST_LOGIN_ORIGINAL")
		}
		deniedHistory := request(
			http.MethodPost,
			"/api/v1/messages/search",
			response.Token,
			`{"query":"SCOPED_FIRST_LOGIN_ORIGINAL","mode":"keyword"}`,
		)
		require.Equal(t, http.StatusForbidden, deniedHistory.Code)
		noMembership()
	}
	for _, credential := range []string{"", "invalid-access-token", response.RefreshToken} {
		denied := request(http.MethodGet, "/api/v1/knowledge/"+row.ID+"/preview", credential, "")
		require.Equal(t, http.StatusUnauthorized, denied.Code)
	}
	// A real complete sync refreshes the existing snapshot before NC performs
	// its current grant publication; it never creates a synthetic identity.
	_, err = f.runtime.ManualSync(context.Background())
	require.NoError(t, err)
	noMembership()
	scopedFirstLoginWrite(
		t,
		fixture+".positive",
		map[string]any{
			"actual_ldaps_login":        true,
			"scoped_only":               true,
			"ordinary_membership_count": 0,
			"user_id":                   user.ID,
			"knowledge_id":              row.ID,
			"body_sha256":               input.FileSHA256,
		},
	)
	require.Eventually(
		t,
		func() bool { _, err := os.Stat(fixture + ".revoked"); return err == nil },
		90*time.Second,
		100*time.Millisecond,
	)
	// NC share removal changes source READ, not the active LDAP account. A real
	// identity token can remain valid while current scoped resource access ends.
	validated, tenantAfter, err := f.users.ValidateToken(context.Background(), response.Token)
	require.NoError(t, err)
	require.Equal(t, user.ID, validated.ID)
	require.Equal(t, uint64(77), tenantAfter)
	for _, rbac := range []bool{true, false} {
		f.cfg.Tenant.EnableRBAC = &rbac
		denied := request(http.MethodGet, "/api/v1/knowledge/"+row.ID+"/preview", response.Token, "")
		require.Equal(t, http.StatusForbidden, denied.Code)
		require.NotContains(t, denied.Body.String(), "SCOPED_FIRST_LOGIN_ORIGINAL")
		deniedOther := request(http.MethodGet, "/api/v1/knowledge-bases/scoped-login-unrelated", response.Token, "")
		require.Equal(t, http.StatusForbidden, deniedOther.Code)
		noMembership()
	}
	for _, table := range []string{"nextcloud_content_leases", "original_body_leases"} {
		var n int64
		require.NoError(
			t,
			f.db.Table(table).
				Where("released_at_ms IS NULL AND expires_at_ms>?", time.Now().UnixMilli()).
				Count(&n).
				Error,
		)
		require.Zero(t, n, "actual output guards must release every physical/body lease")
	}
	scopedFirstLoginWrite(
		t,
		fixture+".result",
		map[string]any{
			"status":                           "PASS",
			"directory":                        input.Directory,
			"reader_guid":                      input.ReaderGUID,
			"user_id":                          user.ID,
			"knowledge_base_id":                input.KnowledgeBaseID,
			"knowledge_id":                     row.ID,
			"body_sha256":                      input.FileSHA256,
			"actual_ldaps_bind":                true,
			"prelogin_ledger":                  true,
			"scoped_only":                      true,
			"ordinary_membership_count":        0,
			"rbac_enabled_and_disabled_fenced": true,
			"old_jwt_identity_valid_after_source_revoked": true,
			"known_revoke_http":                           403,
			"lease_count":                                 0,
			"actual_embedding_only":                       true,
			"graph_reranker_qa_claim":                     false,
		},
	)
}
