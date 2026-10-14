package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestNextcloudSourceRotationRecoversUncertainCommitAndFinalize(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Exec(`CREATE TABLE knowledge_bases (id TEXT PRIMARY KEY, tenant_id INTEGER NOT NULL,
		ever_had_nextcloud_source BOOLEAN NOT NULL DEFAULT 0, deleted_at DATETIME)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE knowledges (id TEXT PRIMARY KEY, tenant_id INTEGER,
		knowledge_base_id TEXT, deleted_at DATETIME)`).Error)
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &repository.NextcloudSourcePairing{},
		&repository.NextcloudSourcePairingAbort{},
		&repository.NextcloudSourceRotation{}))
	require.NoError(t, db.Exec(`INSERT INTO knowledge_bases (id, tenant_id) VALUES ('kb-paired', 7)`).Error)
	repo := repository.NewNextcloudSourcePairingRepository(db)
	pair, _, err := repo.PrepareSourcePairing(context.Background(), repository.NextcloudSourcePairing{
		OperationID: "82d62225-1eb6-47ad-9264-2759e33dfa03", TenantID: 7,
		KnowledgeBaseID: "kb-paired", InstanceID: "instance-1", BindingID: "dev-published",
		BaseURL: "https://nextcloud.example.test", PublicationEpoch: 0,
		KeyID: "pair_82d622251eb647ad92642759e33dfa03",
	}, "old-secret")
	require.NoError(t, err)
	require.NoError(t, repo.ActivateSourcePairing(context.Background(), pair))
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
	h := NewNextcloudSourcePairingHandler(repo, NewDataSourceHandler(dsService, kbService))
	h.inspect = func(context.Context, *types.DataSourceConfig) (nextcloud.PairingIdentity, error) {
		return nextcloud.PairingIdentity{
			InstanceID: pair.InstanceID, BindingID: pair.BindingID,
			BaseURL: pair.BaseURL, PublicationState: "active",
		}, nil
	}
	commits, finalizes := 0, 0
	h.rotateCommit = func(_ context.Context, cfg *types.DataSourceConfig, tuple nextcloud.SourceRotation) error {
		commits++
		require.Equal(t, pair.OperationID, tuple.PairOperationID)
		require.Equal(t, pair.DataSourceID, tuple.DataSourceID)
		require.Equal(t, "new-secret", cfg.Credentials["token"])
		if commits == 1 {
			return errors.New("commit ACK lost")
		}
		return nil
	}
	h.rotateFinalize = func(_ context.Context, _ *types.DataSourceConfig, tuple nextcloud.SourceRotation) error {
		finalizes++
		require.Equal(t, pair.DataSourceID, tuple.DataSourceID)
		if finalizes == 1 {
			return errors.New("finalize ACK lost")
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
	base := "/api/v1/datasource/nextcloud-source-pairings/" + pair.OperationID + "/rotations"
	router.POST("/api/v1/datasource/nextcloud-source-pairings/:operation_id/rotations", h.Rotate)
	router.GET("/api/v1/datasource/nextcloud-source-pairings/:operation_id/rotations/:rotation_id", h.RotationStatus)
	router.POST("/api/v1/datasource/nextcloud-source-pairings/:operation_id/rotations/:rotation_id/retry",
		h.RetryRotation)
	router.POST("/api/v1/datasource/nextcloud-source-pairings/:operation_id/rotations/:rotation_id/abort",
		h.AbortRotation)
	request := func(method, path string, body []byte, tenantID uint64,
		role types.TenantRole,
	) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		ctx := context.WithValue(req.Context(), types.TenantIDContextKey, tenantID)
		ctx = context.WithValue(ctx, types.TenantRoleContextKey, role)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req.WithContext(ctx))
		return rec
	}
	rotationID := "6e359042-69aa-489e-bd9b-8754bf5beee1"
	body, err := json.Marshal(map[string]string{
		"operation_id": rotationID, "new_key_id": "rot_6e35904269aa489ebd9b8754bf5beee1",
		"token": "new-secret",
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden,
		request(http.MethodPost, base, body, 7, types.TenantRoleViewer).Code)
	first := request(http.MethodPost, base, body, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())
	require.Contains(t, first.Body.String(), `"state":"pending"`)
	require.NotContains(t, first.Body.String(), "new-secret")
	path := base + "/" + rotationID
	require.Equal(t, http.StatusNotFound,
		request(http.MethodGet, path, nil, 8, types.TenantRoleAdmin).Code)
	second := request(http.MethodPost, path+"/retry", nil, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusAccepted, second.Code, second.Body.String())
	require.Contains(t, second.Body.String(), `"state":"switched"`)
	third := request(http.MethodPost, path+"/retry", nil, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusOK, third.Code, third.Body.String())
	require.Contains(t, third.Body.String(), `"state":"finalized"`)
	require.Equal(t, 2, commits)
	require.Equal(t, 2, finalizes)
	require.NotContains(t, third.Body.String(), "new-secret")
	status := request(http.MethodGet, path, nil, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusOK, status.Code)
	require.NotContains(t, status.Body.String(), "new-secret")

	// A deterministic rejection leaves the old source credential working. A
	// signed remote abort can be retried after its first ACK is lost.
	h.rotateCommit = func(context.Context, *types.DataSourceConfig, nextcloud.SourceRotation) error {
		return &nextcloud.SourcePairingRemoteError{StatusCode: http.StatusConflict}
	}
	abortAttempts := 0
	h.rotateAbort = func(_ context.Context, old *types.DataSourceConfig, _ nextcloud.SourceRotation) error {
		abortAttempts++
		require.Equal(t, "new-secret", old.Credentials["token"])
		if abortAttempts == 1 {
			return errors.New("abort ACK lost")
		}
		return nil
	}
	secondID := "e293b10b-4d80-43c5-8bb5-67425903e394"
	secondBody, err := json.Marshal(map[string]string{
		"operation_id": secondID, "new_key_id": "rot_e293b10b4d8043c58bb567425903e394",
		"token": "discarded-secret",
	})
	require.NoError(t, err)
	rejected := request(http.MethodPost, base, secondBody, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusConflict, rejected.Code, rejected.Body.String())
	abortPath := base + "/" + secondID + "/abort"
	firstAbort := request(http.MethodPost, abortPath, nil, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusAccepted, firstAbort.Code, firstAbort.Body.String())
	secondAbort := request(http.MethodPost, abortPath, nil, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusOK, secondAbort.Code, secondAbort.Body.String())
	require.Contains(t, secondAbort.Body.String(), `"state":"aborted"`)
	var aborted repository.NextcloudSourceRotation
	require.NoError(t, db.Where("operation_id = ?", secondID).Take(&aborted).Error)
	require.NotContains(t, string(aborted.NewConfig), "discarded-secret")
	require.Equal(t, "{}", string(aborted.NewConfig))
}
