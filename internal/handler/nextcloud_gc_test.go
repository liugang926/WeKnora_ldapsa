package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNextcloudGCAdminStatusIsTenantScopedAndHidesInventory(t *testing.T) {
	_, db, actor, _, _, _ := collectionHTTPFixture(t)
	var err error
	require.NoError(t, db.AutoMigrate(&types.AuditLog{}))
	require.NoError(t, db.Create(&types.TenantMember{UserID: "user", TenantID: 8, Role: types.TenantRoleAdmin, Status: types.TenantMemberStatusActive}).Error)
	script, err := os.ReadFile("../../migrations/sqlite/000038_nextcloud_gc.up.sql")
	require.NoError(t, err)
	for _, statement := range strings.Split(string(script), ";") {
		statement = strings.TrimSpace(statement)
		if statement != "" {
			require.NoError(t, db.Exec(statement).Error)
		}
	}
	now := time.Now().UTC()
	var tenantSevenJob string
	for _, tenant := range []uint64{7, 8} {
		id := uuid.NewString()
		if tenant == 7 {
			tenantSevenJob = id
		}
		require.NoError(t, db.Exec(`INSERT INTO nextcloud_gc_jobs
			(id,tenant_id,knowledge_base_id,datasource_id,external_id,knowledge_id,reason,state,not_before,
			original_not_before,next_attempt_at,last_error_code,created_at,updated_at)
			VALUES (?,?,?,?,?,?,'retired','blocked',?,?,?,?,?,?)`, id, tenant, "kb", "ds", "node",
			id, now, now, now, "safe_object_cleanup_unavailable", now, now).Error)
		require.NoError(t, db.Exec(`INSERT INTO nextcloud_gc_items
			(job_id,kind,object_ref,state,estimated_bytes,created_at,updated_at)
			VALUES (?,'source_file',?,'blocked',23,?,?)`, id, "resource://secret-inventory", now, now).Error)
	}
	gin.SetMode(gin.TestMode)
	h := NewNextcloudGCHandler(repository.NewNextcloudGCStore(db), actor)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		if tenant, ok := types.TenantIDFromContext(c.Request.Context()); ok {
			c.Set(types.TenantIDContextKey.String(), tenant)
		}
		c.Next()
	})
	router.GET("/gc", h.List)
	router.POST("/gc/:id/retry", h.Retry)
	request := func(tenant uint64, role types.TenantRole, apiKey bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/gc", nil)
		ctx := context.WithValue(req.Context(), types.TenantIDContextKey, tenant)
		ctx = types.WithCaller(ctx, types.Caller{TenantID: tenant, UserID: "user", Role: role})
		ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "user"})
		require.NoError(t, db.Model(&types.TenantMember{}).Where("user_id=? AND tenant_id=?", "user", tenant).Update("role", role).Error)
		if apiKey {
			ctx = types.WithTenantAPIKeyScope(ctx, types.TenantAPIKeyScope{KeyID: 1, FullAccess: true})
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req.WithContext(ctx))
		return rec
	}
	require.Equal(t, http.StatusForbidden, request(7, types.TenantRoleViewer, false).Code)
	require.Equal(t, http.StatusForbidden, request(7, types.TenantRoleAdmin, true).Code)
	admin := request(7, types.TenantRoleAdmin, false)
	require.Equal(t, http.StatusOK, admin.Code, admin.Body.String())
	require.Equal(t, 1, strings.Count(admin.Body.String(), `"knowledge_base_id"`))
	require.NotContains(t, admin.Body.String(), "resource://secret-inventory")
	require.Contains(t, admin.Body.String(), `"confirmed_released_bytes":0`)
	retry := func(tenant uint64) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/gc/"+tenantSevenJob+"/retry", nil)
		ctx := context.WithValue(req.Context(), types.TenantIDContextKey, tenant)
		ctx = types.WithCaller(ctx, types.Caller{TenantID: tenant, UserID: "user", Role: types.TenantRoleAdmin})
		ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "user"})
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req.WithContext(ctx))
		return rec
	}
	require.Equal(t, http.StatusNotFound, retry(8).Code)
	require.Equal(t, http.StatusAccepted, retry(7).Code)
}

func TestNextcloudGCActualAdminHTTPPreviewAndCurrentRevocation(t *testing.T) {
	for _, mode := range []string{"preview", "disabled", "member-removed", "demoted", "api-key", "cross-tenant", "missing-actor"} {
		t.Run(mode, func(t *testing.T) {
			_, db, actor, _, _, _ := collectionHTTPFixture(t)
			sql, e := os.ReadFile("../../migrations/sqlite/000038_nextcloud_gc.up.sql")
			require.NoError(t, e)
			require.NoError(t, db.Exec(string(sql)).Error)
			now := time.Now().UTC()
			require.NoError(t, db.Exec(`INSERT INTO nextcloud_gc_jobs(id,tenant_id,knowledge_base_id,datasource_id,external_id,knowledge_id,reason,state,not_before,original_not_before,next_attempt_at,last_error_code,created_at,updated_at) VALUES ('owned',7,'kb','ds','external','retired','retired','blocked',?,?,?,'content_lease_coverage_unverified',?,?)`, now, now.Add(7*24*time.Hour), now, now, now).Error)
			require.NoError(t, db.Exec(`INSERT INTO nextcloud_gc_items(job_id,kind,object_ref,state,estimated_bytes,confirmed_released_bytes,created_at,updated_at) VALUES ('owned','source_file','storage://private-secret','blocked',100,0,?,?)`, now, now).Error)
			ctx := collectionHumanInputContext(7)
			ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "user"})
			if mode == "disabled" {
				require.NoError(t, db.Model(&types.User{}).Where("id='user'").Update("is_active", false).Error)
			}
			if mode == "member-removed" {
				require.NoError(t, db.Where("tenant_id=7 AND user_id='user'").Delete(&types.TenantMember{}).Error)
			}
			if mode == "demoted" {
				require.NoError(t, db.Model(&types.TenantMember{}).Where("tenant_id=7 AND user_id='user'").Update("role", types.TenantRoleViewer).Error)
			}
			if mode == "api-key" {
				ctx = types.WithTenantAPIKeyScope(ctx, types.TenantAPIKeyScope{KeyID: 1, FullAccess: true})
			}
			if mode == "cross-tenant" {
				ctx = collectionHumanInputContext(9)
				ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "user"})
			}
			if mode == "missing-actor" {
				actor = nil
			}
			h := NewNextcloudGCHandler(repository.NewNextcloudGCStore(db), actor)
			r := gin.New()
			r.GET("/gc/:id/preview", h.Preview)
			w := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/gc/owned/preview", nil).WithContext(ctx)
			r.ServeHTTP(w, req)
			expected := http.StatusForbidden
			if mode == "preview" {
				expected = http.StatusOK
			}
			if mode == "cross-tenant" {
				expected = http.StatusNotFound
			}
			require.Equal(t, expected, w.Code, w.Body.String())
			require.NotContains(t, w.Body.String(), "storage://private-secret")
			if mode == "preview" {
				require.Contains(t, w.Body.String(), `"estimated_bytes":100`)
				require.Contains(t, w.Body.String(), `"confirmed_released_bytes":0`)
				require.Contains(t, w.Body.String(), `"collection_checks_repeated":true`)
			}
		})
	}
}
