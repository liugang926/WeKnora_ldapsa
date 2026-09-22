package handler

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type dataSourceGroupAccessStub struct {
	interfaces.GroupAccessService
	allowed bool
	calls   []types.ResourceAction
}

func (s *dataSourceGroupAccessStub) EffectivePermission(
	_ context.Context,
	_ uint64,
	_ types.ResourceType,
	_ string,
	action types.ResourceAction,
	_ time.Time,
) (types.EffectiveResourcePermission, error) {
	s.calls = append(s.calls, action)
	return types.EffectiveResourcePermission{Allowed: s.allowed}, nil
}

func TestDataSourceReadAppliesKnowledgeBaseGroupPolicy(t *testing.T) {
	logsCalled := false
	dsSvc := &stubDataSourceService{
		getDataSource: func(_ context.Context, id string) (*types.DataSource, error) {
			return &types.DataSource{ID: id, KnowledgeBaseID: "kb-1"}, nil
		},
		getSyncLogs: func(context.Context, string, int, int) ([]*types.SyncLog, error) {
			logsCalled = true
			return nil, nil
		},
	}
	kbSvc := &stubKBServiceForDS{getByID: func(context.Context, string) (*types.KnowledgeBase, error) {
		return &types.KnowledgeBase{ID: "kb-1", TenantID: 1}, nil
	}}
	access := &dataSourceGroupAccessStub{}
	h := NewDataSourceHandler(dsSvc, kbSvc)
	ConfigureDataSourceGroupAccess(h, access)

	w := httptest.NewRecorder()
	req := withDSCtx(httptest.NewRequest(http.MethodGet, "/datasource/ds-1/logs", nil), 1)
	newDataSourceTestRouter(h).ServeHTTP(w, req)

	require.Equal(t, http.StatusForbidden, w.Code)
	require.False(t, logsCalled)
	require.Equal(t, []types.ResourceAction{types.ResourceActionRead}, access.calls)
}

func TestDataSourceCredentialsApplyKnowledgeBaseGroupPolicy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dsSvc := &stubDataSourceService{getDataSource: func(_ context.Context, id string) (*types.DataSource, error) {
		return &types.DataSource{ID: id, KnowledgeBaseID: "kb-1"}, nil
	}}
	kbSvc := &stubKBServiceForDS{getByID: func(context.Context, string) (*types.KnowledgeBase, error) {
		return &types.KnowledgeBase{ID: "kb-1", TenantID: 1}, nil
	}}
	access := &dataSourceGroupAccessStub{}
	h := NewDataSourceCredentialsHandler(dsSvc, kbSvc)
	ConfigureDataSourceCredentialsGroupAccess(h, access)
	r := gin.New()
	r.Use(errorCapture())
	r.Use(func(c *gin.Context) {
		c.Set(types.TenantIDContextKey.String(), uint64(1))
		c.Next()
	})
	r.PUT("/datasource/:id/credentials", h.Put)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPut,
		"/datasource/ds-1/credentials",
		bytes.NewBufferString(`{"credentials":{"token":"secret"}}`),
	)
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusForbidden, w.Code)
	require.Equal(t, []types.ResourceAction{types.ResourceActionEdit}, access.calls)
}
