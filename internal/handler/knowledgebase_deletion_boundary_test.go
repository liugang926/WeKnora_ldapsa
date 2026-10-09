package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	appservice "github.com/Tencent/WeKnora/internal/application/service"
	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

type deletionBoundaryKBRepo struct {
	interfaces.KnowledgeBaseRepository
	row     *types.KnowledgeBase
	reads   int
	deletes int
}

func (r *deletionBoundaryKBRepo) GetKnowledgeBaseByID(
	_ context.Context, id string,
) (*types.KnowledgeBase, error) {
	r.reads++
	if id != r.row.ID {
		return nil, repository.ErrKnowledgeBaseNotFound
	}
	return r.row, nil
}

func (r *deletionBoundaryKBRepo) DeleteKnowledgeBase(context.Context, string) error {
	r.deletes++
	return nil
}

type deletionBoundaryDSRepo struct {
	interfaces.DataSourceRepository
	row     *types.DataSource
	reads   int
	deletes int
}

func (r *deletionBoundaryDSRepo) FindByKnowledgeBase(
	_ context.Context, id string,
) ([]*types.DataSource, error) {
	r.reads++
	if id != r.row.KnowledgeBaseID {
		return nil, nil
	}
	return []*types.DataSource{r.row}, nil
}

func (r *deletionBoundaryDSRepo) Delete(context.Context, string) error {
	r.deletes++
	return nil
}

type deletionBoundaryQueue struct{ calls int }

func (q *deletionBoundaryQueue) Enqueue(_ *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	q.calls++
	return &asynq.TaskInfo{ID: "owned-http-blocked-deletion"}, nil
}

func TestDeleteKnowledgeBaseRealServiceBoundaryPreservesHTTPEnvelopeAndGuards(t *testing.T) {
	const kbID = "owned-http-nextcloud-kb"
	kbs := &deletionBoundaryKBRepo{row: &types.KnowledgeBase{
		ID: kbID, TenantID: 7, Name: "owned blocked knowledge base",
	}}
	sources := &deletionBoundaryDSRepo{row: &types.DataSource{
		ID: "owned-http-nextcloud-source", TenantID: 7, KnowledgeBaseID: kbID,
		Type: types.ConnectorTypeNextcloud, Status: types.DataSourceStatusActive,
	}}
	queue := &deletionBoundaryQueue{}
	// Construct the production service, not a fake service returning the desired
	// domain error. Embedded repositories fail loudly if unrelated work is reached.
	svc := appservice.NewKnowledgeBaseService(
		kbs, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		queue, nil, nil, sources, nil, nil, nil, nil, nil, nil, nil,
	)
	h := &KnowledgeBaseHandler{service: svc}
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(middleware.ErrorHandler(), func(c *gin.Context) {
		ctx := types.WithCaller(c.Request.Context(), types.Caller{
			TenantID: 7, UserID: "owned-http-admin", Role: types.TenantRoleAdmin,
		})
		ctx = types.WithExecutionTenant(ctx, 7)
		c.Request = c.Request.WithContext(ctx)
		c.Set(types.TenantIDContextKey.String(), uint64(7))
		c.Set(types.UserIDContextKey.String(), "owned-http-admin")
		c.Next()
	})
	router.DELETE("/knowledge-bases/:id", h.DeleteKnowledgeBase)
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodDelete, "/knowledge-bases/"+kbID, nil)
	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusInternalServerError, response.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Equal(t, map[string]any{
		"success": false,
		"error": map[string]any{
			"code":    float64(apperrors.ErrInternalServer),
			"message": "Nextcloud source must be unpaired before deleting its knowledge base",
			"details": nil,
		},
	}, body)
	require.GreaterOrEqual(t, kbs.reads, 2, "handler lookup and real service must both read the KB")
	require.Equal(t, 1, sources.reads, "real service must inspect the source before soft-delete")
	require.Zero(t, kbs.deletes)
	require.Zero(t, sources.deletes)
	require.Zero(t, queue.calls)
	require.Equal(t, kbID, kbs.row.ID)
	require.Equal(t, types.DataSourceStatusActive, sources.row.Status)
}
