package handler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// onKnowledgeOutputRead observes the real SQL consumer after its read leases
// have been acquired. The retired Nextcloud ACL callback is not a read hook.
func onKnowledgeOutputRead(t *testing.T, db *gorm.DB, name string, run func(*gorm.DB)) *bool {
	t.Helper()
	fired := new(bool)
	require.NoError(t, db.Callback().Query().After("gorm:query").Register(name, func(tx *gorm.DB) {
		if *fired || tx.Statement.SkipHooks {
			return
		}
		switch tx.Statement.Table {
		case "knowledges", "chunks", "knowledge_tags", "knowledge_collection_scope_versions",
			"knowledge_tag_statistics_scope_versions":
		default:
			return
		}
		actual := tx.Session(&gorm.Session{NewDB: true, SkipHooks: true})
		var held int64
		require.NoError(t, actual.Table("nextcloud_content_leases").Where("released_at_ms IS NULL").Count(&held).Error)
		if held == 0 {
			return
		}
		*fired = true
		run(actual)
	}))
	t.Cleanup(func() { _ = db.Callback().Query().Remove(name) })
	return fired
}

// onKnowledgeOutputVersion mutates only after the actual consumer read its
// original version checkpoint, making even an ABA visible to the final check.
func onKnowledgeOutputVersion(t *testing.T, db *gorm.DB, name string, run func(*gorm.DB)) *bool {
	t.Helper()
	fired := new(bool)
	require.NoError(t, db.Callback().Query().After("gorm:query").Register(name, func(tx *gorm.DB) {
		if *fired || tx.Statement.SkipHooks || tx.Statement.Table != "knowledge_input_row_versions" {
			return
		}
		*fired = true
		run(tx.Session(&gorm.Session{NewDB: true, SkipHooks: true}))
	}))
	t.Cleanup(func() { _ = db.Callback().Query().Remove(name) })
	return fired
}

// knowledgeContractAuth uses the production authentication, API capability,
// role and (for KB routes) KB-access middleware. Direct handler fixtures carry
// a captured caller and cannot prove that a later HTTP request authenticates it.
func knowledgeContractAuth(
	ctx context.Context, t *testing.T, db *gorm.DB, req *http.Request,
) (*gin.Engine, *config.Config, interfaces.GroupAccessService) {
	t.Helper()
	require.NoError(t, db.AutoMigrate(&types.AuthToken{}))
	on := true
	cfg := &config.Config{Tenant: &config.TenantConfig{EnableRBAC: &on}}
	groups := service.NewGroupAccessService(repository.NewGroupAccessRepository(db))
	tenants := service.NewTenantService(faqHTTPTenants{tenant: &types.Tenant{ID: 7, Status: "active"}}, nil)
	users := repository.NewUserRepository(db)
	tokens := repository.NewAuthTokenRepository(db)
	members := service.NewTenantMemberService(repository.NewTenantMemberRepository(db), nil, users, tokens)
	userService := service.NewUserService(cfg, users, tokens, tenants, members, nil, nil, groups, nil, nil)
	keys := service.NewTenantAPIKeyService(repository.NewTenantAPIKeyRepository(db))
	r := gin.New()
	r.Use(middleware.ErrorHandler(), middleware.Auth(tenants, userService, members, keys, cfg, groups))
	if scope, api := types.TenantAPIKeyScopeFromContext(ctx); api {
		// These fixtures originally inserted SQL keys directly. Bind only the
		// test transport token; current scope/revocation remains in SQL.
		transportToken := fmt.Sprintf("isolated-FAQ-key-%d", scope.KeyID)
		digest := sha256.Sum256([]byte(transportToken))
		require.NoError(t, db.Model(&types.TenantAPIKey{}).Where("id=?", scope.KeyID).
			Update("key_hash", hex.EncodeToString(digest[:])).Error)
		req.Header.Set("X-API-Key", transportToken)
	} else {
		user, err := users.GetUserByID(context.Background(), types.CallerFromContext(ctx).UserID)
		require.NoError(t, err)
		token, _, err := userService.GenerateTokens(context.Background(), user)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return r, cfg, groups
}

func serveAuthenticatedFAQRead(
	ctx context.Context, t *testing.T, db *gorm.DB, h *FAQHandler, url string,
) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, url, nil)
	r, cfg, _ := knowledgeContractAuth(ctx, t, db, req)
	gate := middleware.NewAPIKeyRouteAuthorizer()
	progress := "/faq/import/progress/:task_id"
	gate.Register(http.MethodGet, progress, middleware.APIKeyRoutePolicy{Capabilities: []types.APIKeyCapability{
		types.APIKeyCapabilityIngest, types.APIKeyCapabilityRetrieve,
	}})
	r.Use(gate.Middleware())
	r.GET(progress, middleware.RequireRole(types.TenantRoleViewer, cfg), h.GetImportProgress)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func serveAuthenticatedTagHTTP(
	ctx context.Context, t *testing.T, db *gorm.DB, h *TagHandler, method, url, body string,
) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r, cfg, groups := knowledgeContractAuth(ctx, t, db, req)
	gate := middleware.NewAPIKeyRouteAuthorizer()
	path := "/knowledge-bases/:id/tags"
	gate.Register(http.MethodGet, path, middleware.APIKeyRoutePolicy{Capabilities: []types.APIKeyCapability{
		types.APIKeyCapabilityIngest, types.APIKeyCapabilityRetrieve,
	}})
	gate.Register(http.MethodPost, path, middleware.APIKeyRoutePolicy{Capabilities: []types.APIKeyCapability{
		types.APIKeyCapabilityIngest,
	}})
	r.Use(gate.Middleware())
	kbs := collectionHTTPKB{db: db}
	r.GET(path, middleware.RequireRole(types.TenantRoleViewer, cfg), middleware.RequireKBAccess(
		middleware.KBIDFromParam("id"), types.OrgRoleViewer, kbs, nil, nil, cfg, groups,
	), h.ListTags)
	r.POST(path, middleware.RequireOwnershipOrRoleWithGroupEdit(
		types.TenantRoleAdmin, func(c *gin.Context) (string, error) {
			kb, err := kbs.GetKnowledgeBaseByID(c.Request.Context(), c.Param("id"))
			if err != nil {
				return "", err
			}
			return kb.CreatorID, nil
		}, cfg, types.GroupResourceTypeKnowledgeBase, middleware.KBIDFromParam("id"), groups,
	), middleware.RequireKBAccess(
		middleware.KBIDFromParam("id"), types.OrgRoleEditor, kbs, nil, nil, cfg, groups,
	), h.CreateTag)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// onWorkspaceOutputActorRead interleaves a lifecycle mutation with the current
// WeKnora actor lookup, which is outside the material snapshot transaction.
func onWorkspaceOutputActorRead(t *testing.T, db *gorm.DB, run func(*gorm.DB)) *bool {
	t.Helper()
	fired := new(bool)
	name := "test_interleave_workspace_actor_read"
	require.NoError(t, db.Callback().Query().After("gorm:query").Register(name, func(tx *gorm.DB) {
		if *fired || tx.Statement.SkipHooks || tx.Statement.Table != "users" {
			return
		}
		*fired = true
		run(tx.Session(&gorm.Session{NewDB: true, SkipHooks: true}))
	}))
	t.Cleanup(func() { _ = db.Callback().Query().Remove(name) })
	return fired
}
