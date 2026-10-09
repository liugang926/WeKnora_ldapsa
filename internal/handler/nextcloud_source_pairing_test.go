package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestNextcloudSourcePairingPendingRetryWithoutSecretReadback(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Exec(`CREATE TABLE knowledge_bases (id TEXT PRIMARY KEY, tenant_id INTEGER NOT NULL,
		ever_had_nextcloud_source BOOLEAN NOT NULL DEFAULT 0, deleted_at DATETIME)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE knowledges (id TEXT PRIMARY KEY, tenant_id INTEGER,
		knowledge_base_id TEXT, deleted_at DATETIME)`).Error)
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &repository.NextcloudSourcePairing{},
		&repository.NextcloudSourcePairingAbort{}))
	require.NoError(t, db.Exec(`INSERT INTO knowledge_bases (id, tenant_id) VALUES ('kb-paired',7)`).Error)
	dsService := &stubDataSourceService{getDataSource: func(ctx context.Context, id string) (*types.DataSource, error) {
		var ds types.DataSource
		err := db.WithContext(ctx).Where("id = ?", id).Take(&ds).Error
		return &ds, err
	}}
	kbService := &stubKBServiceForDS{getByID: func(_ context.Context, id string) (*types.KnowledgeBase, error) {
		if id != "kb-paired" {
			return nil, errors.New("missing")
		}
		return &types.KnowledgeBase{ID: id, TenantID: 7}, nil
	}}
	h := NewNextcloudSourcePairingHandler(repository.NewNextcloudSourcePairingRepository(db),
		NewDataSourceHandler(dsService, kbService))
	h.inspect = func(context.Context, *types.DataSourceConfig) (nextcloud.PairingIdentity, error) {
		return nextcloud.PairingIdentity{
			InstanceID: "instance-1", BindingID: "dev-published",
			BaseURL: "https://nextcloud.example.test", PublicationState: "active", PublicationEpoch: 0,
		}, nil
	}
	commitAttempts := 0
	h.commit = func(_ context.Context, _ *types.DataSourceConfig, pair nextcloud.SourcePairingCommit) error {
		commitAttempts++
		require.Equal(t, uint64(7), pair.TenantID)
		if commitAttempts == 1 {
			return errors.New("lost response")
		}
		if commitAttempts == 2 {
			return &nextcloud.SourcePairingRemoteError{StatusCode: http.StatusConflict}
		}
		return nil
	}
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		if tenant, ok := c.Request.Context().Value(types.TenantIDContextKey).(uint64); ok {
			c.Set(types.TenantIDContextKey.String(), tenant)
		}
		c.Next()
	})
	base := "/api/v1/datasource/nextcloud-source-pairings"
	router.POST(base, h.Pair)
	router.GET(base+"/:operation_id", h.Status)
	router.POST(base+"/:operation_id/retry", h.Retry)
	body := map[string]any{
		"knowledge_base_id": "kb-paired", "base_url": "https://nextcloud.example.test",
		"binding_id": "dev-published", "operation_id": "82D62225-1EB6-47AD-9264-2759E33DFA03",
		"instance_id": "instance-1", "publication_epoch": 0,
		"key_id": "pair_82d622251eb647ad92642759e33dfa03", "token": "one-time-pairing-token",
	}
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	request := func(method, path string, requestBody []byte, tenantID uint64,
		role types.TenantRole,
	) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewReader(requestBody))
		req.Header.Set("Content-Type", "application/json")
		ctx := context.WithValue(req.Context(), types.TenantIDContextKey, tenantID)
		ctx = context.WithValue(ctx, types.TenantRoleContextKey, role)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req.WithContext(ctx))
		return rec
	}
	denied := request(http.MethodPost, base, encoded, 7, types.TenantRoleViewer)
	require.Equal(t, http.StatusForbidden, denied.Code)
	first := request(http.MethodPost, base, encoded, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())
	require.Contains(t, first.Body.String(), `"state":"pending"`)
	require.NotContains(t, first.Body.String(), "one-time-pairing-token")
	var count int64
	require.NoError(t, db.Model(&types.DataSource{}).Count(&count).Error)
	require.Equal(t, int64(1), count)
	path := base + "/82d62225-1eb6-47ad-9264-2759e33dfa03"
	status := request(http.MethodGet, path, nil, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusOK, status.Code)
	require.NotContains(t, status.Body.String(), "one-time-pairing-token")
	require.Equal(t, http.StatusNotFound, request(http.MethodGet, path, nil, 8, types.TenantRoleAdmin).Code)
	conflict := request(http.MethodPost, path+"/retry", nil, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusConflict, conflict.Code, conflict.Body.String())
	require.Contains(t, conflict.Body.String(), "remote_commit_conflict")
	status = request(http.MethodGet, path, nil, 7, types.TenantRoleAdmin)
	require.Contains(t, status.Body.String(), `"last_error_code":"remote_commit_conflict"`)
	retried := request(http.MethodPost, path+"/retry", nil, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusOK, retried.Code, retried.Body.String())
	require.Contains(t, retried.Body.String(), `"state":"active"`)
	require.NoError(t, db.Model(&types.DataSource{}).Count(&count).Error)
	require.Equal(t, int64(1), count)
	active := request(http.MethodPost, base, encoded, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusOK, active.Code, active.Body.String())
	body["operation_id"] = "82d622251eb647ad92642759e33dfa03"
	compact, err := json.Marshal(body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK,
		request(http.MethodPost, base, compact, 7, types.TenantRoleAdmin).Code)
	require.Equal(t, 3, commitAttempts)
	require.False(t, strings.Contains(active.Body.String(), "one-time-pairing-token"))
}

func TestNextcloudSourcePairingAbortWaitsForRemoteAckAndKeepsIdempotentStatus(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Exec(`CREATE TABLE knowledge_bases (id TEXT PRIMARY KEY, tenant_id INTEGER NOT NULL,
		ever_had_nextcloud_source BOOLEAN NOT NULL DEFAULT 0, deleted_at DATETIME)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE knowledges (id TEXT PRIMARY KEY, tenant_id INTEGER,
		knowledge_base_id TEXT, deleted_at DATETIME)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE sync_logs (data_source_id TEXT)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE nextcloud_event_connections (datasource_id TEXT)`).Error)
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &repository.NextcloudSourcePairing{},
		&repository.NextcloudSourcePairingAbort{}))
	require.NoError(t, db.Exec(`INSERT INTO knowledge_bases (id, tenant_id) VALUES ('kb-abort',7)`).Error)
	repo := repository.NewNextcloudSourcePairingRepository(db)
	opID := "67a915de-84ea-42a2-a609-a688fe03c46a"
	pair, _, err := repo.PrepareSourcePairing(context.Background(), repository.NextcloudSourcePairing{
		OperationID: opID, TenantID: 7, KnowledgeBaseID: "kb-abort",
		InstanceID: "instance-1", BindingID: "dev-published",
		BaseURL: "https://nextcloud.example.test", PublicationEpoch: 1,
		KeyID: "pair_67a915de84ea42a2a609a688fe03c46a",
	}, "one-time-abort-token")
	require.NoError(t, err)
	dsService := &stubDataSourceService{getDataSource: func(ctx context.Context, id string) (*types.DataSource, error) {
		var ds types.DataSource
		err := db.WithContext(ctx).Where("id = ?", id).Take(&ds).Error
		return &ds, err
	}}
	kbService := &stubKBServiceForDS{getByID: func(_ context.Context, id string) (*types.KnowledgeBase, error) {
		return &types.KnowledgeBase{ID: id, TenantID: 7}, nil
	}}
	h := NewNextcloudSourcePairingHandler(repo, NewDataSourceHandler(dsService, kbService))
	attempts := 0
	h.abort = func(_ context.Context, config *types.DataSourceConfig,
		intent nextcloud.SourcePairingCommit,
	) error {
		attempts++
		require.Equal(t, pair.OperationID, intent.OperationID)
		require.Equal(t, pair.DataSourceID, intent.DataSourceID)
		require.Equal(t, "one-time-abort-token", config.Credentials["token"])
		if attempts == 1 {
			return errors.New("lost remote ACK")
		}
		if attempts == 2 {
			return &nextcloud.SourcePairingRemoteError{StatusCode: http.StatusConflict}
		}
		return nil
	}
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		if tenant, ok := c.Request.Context().Value(types.TenantIDContextKey).(uint64); ok {
			c.Set(types.TenantIDContextKey.String(), tenant)
		}
		c.Next()
	})
	base := "/api/v1/datasource/nextcloud-source-pairings"
	path := base + "/" + opID
	router.POST(base+"/:operation_id/abort", h.Abort)
	router.GET(base+"/:operation_id", h.Status)
	request := func(method, url string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, url, nil)
		ctx := context.WithValue(req.Context(), types.TenantIDContextKey, uint64(7))
		ctx = context.WithValue(ctx, types.TenantRoleContextKey, types.TenantRoleAdmin)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, req.WithContext(ctx))
		return recorder
	}
	require.Equal(t, http.StatusAccepted, request(http.MethodPost, path+"/abort").Code)
	current, _, err := repo.SourcePairing(context.Background(), 7, opID)
	require.NoError(t, err)
	require.Equal(t, "pending", current.State, "uncertain remote abort must not delete source")
	require.Equal(t, http.StatusConflict, request(http.MethodPost, path+"/abort").Code)
	require.Equal(t, http.StatusOK, request(http.MethodPost, path+"/abort").Code)
	require.Equal(t, http.StatusOK, request(http.MethodPost, path+"/abort").Code)
	require.Equal(t, 3, attempts, "completed abort must not call Nextcloud again")
	require.Contains(t, request(http.MethodGet, path).Body.String(), `"state":"aborted"`)
	var count int64
	require.NoError(t, db.Unscoped().Model(&types.DataSource{}).
		Where("id = ?", pair.DataSourceID).Count(&count).Error)
	require.Zero(t, count)
}
