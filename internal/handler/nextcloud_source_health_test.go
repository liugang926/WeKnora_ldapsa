package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestNextcloudSourceHealthRequiresAdminSessionAndScopesTenant(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+uuid.NewString()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	defer func() { require.NoError(t, sqlDB.Close()) }()
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &types.SyncLog{},
		&repository.NextcloudSourcePairing{}, &repository.NextcloudSourcePairingAbort{}))
	require.NoError(t, db.Exec(`CREATE TABLE knowledges (id TEXT, tenant_id INTEGER,
		knowledge_base_id TEXT, deleted_at DATETIME, parse_status TEXT, enable_status TEXT)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE nextcloud_source_versions (tenant_id INTEGER,
		knowledge_base_id TEXT, datasource_id TEXT, external_id TEXT, state TEXT,
		candidate_knowledge_id TEXT, updated_at DATETIME)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE nextcloud_event_connections (connection_id TEXT,
		tenant_id INTEGER, datasource_id TEXT, nextcloud_instance_id TEXT,
		binding_id TEXT, created_at DATETIME)`).Error)
	ds := &types.DataSource{
		ID: "health-source", TenantID: 7,
		KnowledgeBaseID: "health-kb", Type: types.ConnectorTypeNextcloud,
		Status: types.DataSourceStatusActive, Config: []byte(`{"secret":"private-machine-token"}`),
	}
	require.NoError(t, db.Create(ds).Error)
	pair := &repository.NextcloudSourcePairing{
		OperationID: uuid.NewString(),
		TenantID:    7, KnowledgeBaseID: "health-kb", DataSourceID: ds.ID,
		InstanceID: "instance", BindingID: "binding", State: "active",
		BaseURL: "https://private.example.test",
	}
	require.NoError(t, db.Create(pair).Error)
	dsService := &stubDataSourceService{getDataSource: func(ctx context.Context, id string) (*types.DataSource, error) {
		var row types.DataSource
		err := db.WithContext(ctx).Where("id = ?", id).Take(&row).Error
		return &row, err
	}}
	kbService := &stubKBServiceForDS{getByID: func(_ context.Context, id string) (*types.KnowledgeBase, error) {
		return &types.KnowledgeBase{ID: id, TenantID: 7}, nil
	}}
	h := NewNextcloudSourcePairingHandler(repository.NewNextcloudSourcePairingRepository(db),
		NewDataSourceHandler(dsService, kbService))
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		if tenant, ok := types.TenantIDFromContext(c.Request.Context()); ok {
			c.Set(types.TenantIDContextKey.String(), tenant)
		}
		c.Next()
	})
	path := "/api/v1/datasource/nextcloud-source-pairings/" + pair.OperationID + "/health"
	router.GET("/api/v1/datasource/nextcloud-source-pairings/:operation_id/health", h.Health)
	request := func(tenant uint64, role types.TenantRole, apiKey bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
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
	require.Equal(t, http.StatusNotFound, request(8, types.TenantRoleAdmin, false).Code)
	admin := request(7, types.TenantRoleAdmin, false)
	require.Equal(t, http.StatusOK, admin.Code, admin.Body.String())
	require.Equal(t, "no-store", admin.Header().Get("Cache-Control"))
	require.Contains(t, admin.Body.String(), `"event_inbox":null`)
	for _, sensitive := range []string{"private-machine-token", "private.example.test", "secret"} {
		require.False(t, strings.Contains(admin.Body.String(), sensitive), admin.Body.String())
	}
}
