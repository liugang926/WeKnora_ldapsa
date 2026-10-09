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
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestNextcloudGCAdminStatusIsTenantScopedAndHidesInventory(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+uuid.NewString()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
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
	h := NewNextcloudGCHandler(repository.NewNextcloudGCStore(db))
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
		ctx = context.WithValue(ctx, types.TenantRoleContextKey, role)
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
		ctx = context.WithValue(ctx, types.TenantRoleContextKey, types.TenantRoleAdmin)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req.WithContext(ctx))
		return rec
	}
	require.Equal(t, http.StatusNotFound, retry(8).Code)
	require.Equal(t, http.StatusAccepted, retry(7).Code)
}
