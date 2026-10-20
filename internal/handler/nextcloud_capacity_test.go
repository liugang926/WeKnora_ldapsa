package handler

import (
	"context"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type capacityHTTPFile struct {
	interfaces.FileService
	hook      func()
	available int64
}

func (f *capacityHTTPFile) MeasureStorageCapacity(context.Context) (types.StorageCapacity, error) {
	if f.hook != nil {
		f.hook()
	}
	return types.StorageCapacity{
		ScopeID:        strings.Repeat("a", 64),
		TotalBytes:     100000,
		AvailableBytes: f.available,
		MeasuredAt:     time.Now().UTC(),
	}, nil
}

func TestNextcloudCapacityActualAdminHTTPStatusAndPolicy(t *testing.T) {
	for _, mode := range []string{
		"normal",
		"paused",
		"set-policy",
		"disabled",
		"late-disabled",
		"api-key",
		"cross-tenant",
	} {
		t.Run(mode, func(t *testing.T) {
			_, db, actor, _, _, _ := collectionHTTPFixture(t)
			require.NoError(t, db.AutoMigrate(&types.StoredResource{}, &types.ResourceBinding{}))
			sql, err := os.ReadFile("../../migrations/sqlite/000065_nextcloud_capacity_admission.up.sql")
			require.NoError(t, err)
			require.NoError(t, db.Exec(string(sql)).Error)
			sql, err = os.ReadFile("../../migrations/sqlite/000067_nextcloud_capacity_write_intents.up.sql")
			require.NoError(t, err)
			require.NoError(t, db.Exec(string(sql)).Error)
			backend := "backend"
			require.NoError(
				t,
				db.Model(&types.KnowledgeBase{}).Where("id='kb'").Update("storage_backend_id", backend).Error,
			)
			provider := &capacityHTTPFile{available: 90000}
			if mode == "paused" {
				provider.available = 9000
			}
			if mode == "disabled" {
				require.NoError(t, db.Model(&types.User{}).Where("id='user'").Update("is_active", false).Error)
			}
			if mode == "late-disabled" {
				provider.hook = func() {
					require.NoError(t, db.Model(&types.User{}).Where("id='user'").Update("is_active", false).Error)
				}
			}
			store := repository.NewNextcloudCapacityStore(db)
			admission := service.NewNextcloudCapacityAdmission(
				store,
				func(context.Context, *types.KnowledgeBase) (interfaces.FileService, error) { return provider, nil },
			)
			h := NewNextcloudCapacityHandler(store, actor, collectionHTTPKB{db: db}, admission)
			r := documentHandlerRouter()
			r.Use(func(c *gin.Context) {
				ctx := types.WithPrincipal(
					c.Request.Context(),
					types.Principal{Type: types.PrincipalWebUser, ID: "user"},
				)
				if mode == "api-key" {
					ctx = types.WithTenantAPIKeyScope(
						ctx,
						types.TenantAPIKeyScope{
							KeyID:            1,
							ScopeType:        types.APIKeyScopeTenant,
							Capabilities:     types.StringArray{string(types.APIKeyCapabilityRetrieve)},
							KnowledgeBaseIDs: types.StringArray{"kb"},
						},
					)
					ctx = types.WithCaller(ctx, types.Caller{TenantID: 7, Role: types.TenantRoleViewer})
					ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalAPITenant, ID: "7"})
				}
				c.Request = c.Request.WithContext(ctx)
				c.Next()
			})
			r.GET("/knowledge-bases/:id/capacity", h.Get)
			r.PUT("/knowledge-bases/:id/capacity", h.Set)
			method, path := "GET", "/knowledge-bases/kb/capacity"
			body := ""
			if mode == "set-policy" {
				method = "PUT"
				body = `{"retention_quota_bytes":2048,"warning_percent":70,"pause_percent":85}`
			}
			if mode == "cross-tenant" {
				path = "/knowledge-bases/shared-kb/capacity"
			}
			req := httptest.NewRequest(method, path, strings.NewReader(body))
			if body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			switch mode {
			case "disabled", "late-disabled", "api-key":
				require.Equal(t, 403, w.Code, w.Body.String())
			case "cross-tenant":
				require.Equal(t, 404, w.Code, w.Body.String())
			default:
				require.Equal(t, 200, w.Code, w.Body.String())
				if mode == "paused" {
					require.Contains(t, w.Body.String(), `"state":"paused"`)
				}
				require.NotContains(t, w.Body.String(), "local://")
				require.NotContains(t, w.Body.String(), "physical_path")
			}
		})
	}
}
