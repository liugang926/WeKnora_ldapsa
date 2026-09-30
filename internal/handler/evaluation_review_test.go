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

type reviewEvaluationServiceStub struct {
	interfaces.EvaluationService
	called bool
}

func (s *reviewEvaluationServiceStub) ReviewEvaluationCase(
	_ context.Context, taskID string, questionID int, input types.EvaluationCaseReviewInput,
) (*types.EvaluationDetail, error) {
	s.called = true
	if taskID != "evaluation-one" || questionID != 1 ||
		input.Faithfulness != "pass" || input.CitationAccuracy != "fail" {
		return nil, types.ErrEvaluationReviewInvalid
	}
	return &types.EvaluationDetail{Task: &types.EvaluationTask{ID: taskID}}, nil
}

func TestReviewEvaluationCaseValidatesPathAndReturnsDurableDetail(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &reviewEvaluationServiceStub{}
	r := gin.New()
	r.Use(errorCapture())
	r.PUT("/evaluation/:taskId/cases/:questionId/review", NewEvaluationHandler(svc).ReviewEvaluationCase)

	for _, tc := range []struct {
		path string
		body string
		want int
	}{
		{"/evaluation/evaluation-one/cases/nope/review", `{}`, http.StatusBadRequest},
		{"/evaluation/evaluation-one/cases/1/review", `{bad`, http.StatusBadRequest},
		{
			"/evaluation/evaluation-one/cases/1/review",
			`{"faithfulness":"pass","citation_accuracy":"fail","abstention":"not_applicable"}`,
			http.StatusOK,
		},
	} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut, tc.path, bytes.NewBufferString(tc.body))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		require.Equal(t, tc.want, w.Code, w.Body.String())
	}
	require.True(t, svc.called)
}
