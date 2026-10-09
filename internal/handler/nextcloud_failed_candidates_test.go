package handler

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestNextcloudFailedCandidateAdminAndTenantBoundary(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	db, err := gorm.Open(sqlite.Open("file:"+uuid.NewString()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &repository.NextcloudSourcePairing{},
		&repository.NextcloudSourcePairingAbort{}))
	config, err := (&types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		ResourceIDs: []string{"binding"}, Settings: map[string]interface{}{"base_url": "https://nextcloud.example"},
		Credentials: map[string]interface{}{"token": "secret", "key_id": "pair_test"},
	}).ToJSON()
	require.NoError(t, err)
	ds := &types.DataSource{
		ID: "ds", TenantID: 7, KnowledgeBaseID: "kb", Type: types.ConnectorTypeNextcloud,
		Status: types.DataSourceStatusActive, Config: config,
	}
	require.NoError(t, db.Create(ds).Error)
	base, hash, binding, err := repository.NextcloudEventDataSourceIdentity(ds.Config)
	require.NoError(t, err)
	op := uuid.NewString()
	require.NoError(t, db.Create(&repository.NextcloudSourcePairing{
		OperationID: op, TenantID: 7,
		KnowledgeBaseID: "kb", DataSourceID: "ds", InstanceID: "instance", BindingID: binding,
		BaseURL: base, ConfigSHA: hash, State: "active",
	}).Error)
	h := NewNextcloudSourcePairingHandler(repository.NewNextcloudSourcePairingRepository(db),
		NewDataSourceHandler(&stubDataSourceService{}, &stubKBServiceForDS{getByID: func(_ context.Context,
			id string,
		) (*types.KnowledgeBase, error) {
			return &types.KnowledgeBase{ID: id, TenantID: 7}, nil
		}}))
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		if tenant, ok := c.Request.Context().Value(types.TenantIDContextKey).(uint64); ok {
			c.Set(types.TenantIDContextKey.String(), tenant)
		}
		c.Next()
	})
	listPath := "/api/v1/datasource/nextcloud-source-pairings/by-datasource/ds/failed-candidates"
	retryPath := "/api/v1/datasource/nextcloud-source-pairings/" + op + "/candidates/77/retry"
	router.GET("/api/v1/datasource/nextcloud-source-pairings/by-datasource/:id/failed-candidates", h.FailedCandidates)
	router.POST("/api/v1/datasource/nextcloud-source-pairings/:operation_id/candidates/:file_id/retry",
		h.RetryFailedCandidate)
	request := func(method, path string, tenant uint64, role types.TenantRole) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path,
			bytes.NewBufferString("{\"source_etag\":\"etag\",\"candidate_id\":\"candidate\"}"))
		req.Header.Set("Content-Type", "application/json")
		ctx := context.WithValue(req.Context(), types.TenantIDContextKey, tenant)
		ctx = context.WithValue(ctx, types.TenantRoleContextKey, role)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req.WithContext(ctx))
		return rec
	}
	for _, endpoint := range []struct{ method, path string }{{http.MethodGet, listPath}, {http.MethodPost, retryPath}} {
		require.Equal(t, http.StatusForbidden,
			request(endpoint.method, endpoint.path, 7, types.TenantRoleViewer).Code)
		require.Equal(t, http.StatusNotFound,
			request(endpoint.method, endpoint.path, 8, types.TenantRoleAdmin).Code)
	}
	require.Equal(t, http.StatusBadRequest,
		request(http.MethodGet, listPath+"?limit=51", 7, types.TenantRoleAdmin).Code)
}
