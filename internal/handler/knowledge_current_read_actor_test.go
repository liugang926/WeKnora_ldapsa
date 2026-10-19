package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestKnowledgeCurrentReadActorActualHTTP(t *testing.T) {
	for _, mode := range []string{"web-allowed", "web-disabled", "web-membership-withdrawn", "api-retrieve-only", "api-revoked", "api-expired", "api-allowlist", "api-capability", "api-tenant", "late-web-disabled", "late-api-revoked", "late-api-allowlist", "late-api-capability"} {
		t.Run(mode, func(t *testing.T) {
			h, db, _, groups := outputHTTPFixture(t, true)
			ctx := types.WithCaller(
				context.Background(),
				types.Caller{TenantID: 7, UserID: "user", Role: types.TenantRoleContributor},
			)
			ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "user"})
			var key *types.TenantAPIKey
			api := len(mode) >= 3 && mode[:3] == "api" || len(mode) >= 8 && mode[:8] == "late-api"
			if api {
				tenant := uint64(7)
				key = &types.TenantAPIKey{
					TenantID:         &tenant,
					ScopeType:        types.APIKeyScopeTenant,
					Name:             "actual read key",
					KeyHash:          "read-key-fixture",
					Capabilities:     types.StringArray{string(types.APIKeyCapabilityRetrieve)},
					KnowledgeBaseIDs: types.StringArray{"kb"},
				}
				require.NoError(t, db.Create(key).Error)
				ctx = types.WithCaller(ctx, types.Caller{TenantID: 7, Role: types.TenantRoleViewer})
				ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalAPITenant, ID: "7"})
				ctx = types.WithTenantAPIKeyScope(
					ctx,
					types.TenantAPIKeyScope{
						KeyID:            key.ID,
						ScopeType:        key.ScopeType,
						Capabilities:     key.Capabilities,
						KnowledgeBaseIDs: key.KnowledgeBaseIDs,
					},
				)
			}
			ctx = types.WithExecutionTenant(ctx, 7)
			mutate := func() {
				switch mode {
				case "web-disabled", "late-web-disabled":
					require.NoError(t, db.Model(&types.User{}).Where("id = 'user'").Update("is_active", false).Error)
				case "web-membership-withdrawn":
					require.NoError(
						t,
						db.Model(&types.TenantMember{}).
							Where("user_id = 'user'").
							Update("status", types.TenantMemberStatusSuspended).
							Error,
					)
				case "api-revoked", "late-api-revoked":
					require.NoError(t, db.Model(key).Update("revoked_at", time.Now()).Error)
				case "api-expired":
					require.NoError(t, db.Model(key).Update("expires_at", time.Now().Add(-time.Hour)).Error)
				case "api-allowlist", "late-api-allowlist":
					require.NoError(t, db.Model(key).Update("knowledge_base_ids", types.StringArray{"other-kb"}).Error)
				case "api-capability", "late-api-capability":
					require.NoError(
						t,
						db.Model(key).
							Update("capabilities", types.StringArray{string(types.APIKeyCapabilityIngest)}).
							Error,
					)
				case "api-tenant":
					require.NoError(t, db.Model(key).Update("tenant_id", uint64(8)).Error)
				}
			}
			late := len(mode) >= 5 && mode[:5] == "late-"
			fired := false
			if late {
				groups.hook = func(calls int) {
					if calls == 2 {
						fired = true
						mutate()
					}
				}
			} else {
				mutate()
			}
			r := documentHandlerRouter()
			r.Use(func(c *gin.Context) { c.Request = c.Request.WithContext(ctx); c.Next() })
			gate := middleware.NewAPIKeyRouteAuthorizer()
			gate.Register(
				http.MethodGet,
				"/knowledge/:id",
				middleware.APIKeyRoutePolicy{RequireFullAccess: true}.WithCapability(types.APIKeyCapabilityRetrieve),
			)
			r.Use(gate.Middleware())
			r.GET("/knowledge/:id", h.GetKnowledge)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/knowledge/document", nil))
			if mode == "web-allowed" || mode == "api-retrieve-only" {
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				require.Contains(t, w.Body.String(), "owned-output-title")
			} else {
				require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
				require.NotContains(t, w.Body.String(), "owned-output-title")
			}
			if late {
				require.True(t, fired, "real output boundary must reach the last authorization callback")
			}
		})
	}
}
