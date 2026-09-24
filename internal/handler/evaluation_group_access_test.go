package handler

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type evaluationServiceStub struct {
	interfaces.EvaluationService
	called bool
}

func (s *evaluationServiceStub) Evaluation(
	context.Context, string, string, string, string, string,
) (*types.EvaluationDetail, error) {
	s.called = true
	return &types.EvaluationDetail{}, nil
}

func TestEvaluationAppliesKnowledgeBaseGroupPolicy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	evaluations := &evaluationServiceStub{}
	access := &dataSourceGroupAccessStub{}
	kbService := &stubKBServiceForDS{getByID: func(context.Context, string) (*types.KnowledgeBase, error) {
		return &types.KnowledgeBase{ID: "kb-1", TenantID: 1}, nil
	}}
	h := NewEvaluationHandler(evaluations)
	ConfigureEvaluationGroupAccess(h, access, kbService)
	r := gin.New()
	r.Use(errorCapture())
	r.Use(func(c *gin.Context) {
		c.Set(types.TenantIDContextKey.String(), uint64(1))
		ctx := context.WithValue(c.Request.Context(), types.TenantIDContextKey, uint64(1))
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})
	r.POST("/evaluation", h.Evaluation)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost,
		"/evaluation",
		bytes.NewBufferString(`{"dataset_id":"set-1","knowledge_base_id":"kb-1"}`),
	)
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusForbidden, w.Code)
	require.False(t, evaluations.called)
	require.Equal(t, []types.ResourceAction{types.ResourceActionRead}, access.calls)
}

func TestEvaluationRejectsKnowledgeBaseFromAnotherWorkspace(t *testing.T) {
	gin.SetMode(gin.TestMode)
	evaluations := &evaluationServiceStub{}
	access := &dataSourceGroupAccessStub{allowed: true}
	kbService := &stubKBServiceForDS{getByID: func(context.Context, string) (*types.KnowledgeBase, error) {
		return &types.KnowledgeBase{ID: "foreign-kb", TenantID: 2}, nil
	}}
	h := NewEvaluationHandler(evaluations)
	ConfigureEvaluationGroupAccess(h, access, kbService)
	r := gin.New()
	r.Use(errorCapture())
	r.Use(func(c *gin.Context) {
		c.Set(types.TenantIDContextKey.String(), uint64(1))
		c.Request = c.Request.WithContext(
			context.WithValue(c.Request.Context(), types.TenantIDContextKey, uint64(1)),
		)
		c.Next()
	})
	r.POST("/evaluation", h.Evaluation)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost,
		"/evaluation",
		bytes.NewBufferString(`{"dataset_id":"set-1","knowledge_base_id":"foreign-kb"}`),
	)
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusForbidden, w.Code)
	require.False(t, evaluations.called)
	require.Empty(t, access.calls)
}
