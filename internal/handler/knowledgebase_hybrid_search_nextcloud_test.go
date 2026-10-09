package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type hybridOutputKBService struct {
	interfaces.KnowledgeBaseService
	kb      *types.KnowledgeBase
	results []*types.SearchResult
}

func (s *hybridOutputKBService) GetKnowledgeBaseByID(context.Context, string) (*types.KnowledgeBase, error) {
	return s.kb, nil
}

func (s *hybridOutputKBService) GetKnowledgeBaseByIDOnly(context.Context, string) (*types.KnowledgeBase, error) {
	return s.kb, nil
}

func (s *hybridOutputKBService) HybridSearch(context.Context, string,
	types.SearchParams,
) ([]*types.SearchResult, error) {
	return s.results, nil
}

type hybridOutputKnowledgeService struct {
	interfaces.KnowledgeService
	row     *types.Knowledge
	checks  int
	onCheck func(int) error
}

func (s *hybridOutputKnowledgeService) GetKnowledgeByIDOnly(context.Context, string) (*types.Knowledge, error) {
	return s.row, nil
}

func (s *hybridOutputKnowledgeService) CheckKnowledgePublication(context.Context, *types.Knowledge) error {
	s.checks++
	if s.onCheck != nil {
		return s.onCheck(s.checks)
	}
	return nil
}

func runHybridOutputRequest(t *testing.T, h *KnowledgeBaseHandler) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(middleware.ErrorHandler(), func(c *gin.Context) {
		ctx := types.WithCaller(c.Request.Context(), types.Caller{
			TenantID: 7, UserID: "user-1", Role: types.TenantRoleContributor,
		})
		ctx = types.WithExecutionTenant(ctx, 7)
		c.Request = c.Request.WithContext(ctx)
		c.Set(types.TenantIDContextKey.String(), uint64(7))
		c.Set(types.UserIDContextKey.String(), "user-1")
		c.Next()
	})
	router.POST("/knowledge-bases/:id/hybrid-search", h.HybridSearch)
	w := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/knowledge-bases/kb-1/hybrid-search",
		strings.NewReader(`{"query_text":"source"}`))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, request)
	return w
}

func TestHybridSearchOutputBoundaryRejectsRevokedSource(t *testing.T) {
	db := leaseHandlerDB(t)
	store := repository.NewNextcloudContentLeaseStore(db)
	row := leaseHandlerKnowledge(t)
	service := &hybridOutputKnowledgeService{row: row}
	service.onCheck = func(n int) error {
		if n >= 3 { // changed after the handler's pre-output check
			return access.ErrNextcloudPublicationDenied
		}
		return nil
	}
	h := &KnowledgeBaseHandler{
		service: &hybridOutputKBService{
			kb: &types.KnowledgeBase{ID: "kb-1", TenantID: 7},
			results: []*types.SearchResult{{
				KnowledgeID: row.ID, KnowledgeBaseID: row.KnowledgeBaseID,
				KnowledgeChannel: row.Channel, Metadata: row.GetMetadata(), Content: "source secret",
			}},
		},
		knowledgeService: service, contentLeases: store,
	}
	w := runHybridOutputRequest(t, h)
	require.GreaterOrEqual(t, service.checks, 3)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "source secret")
	assertLeaseReleased(t, db)
}

func TestHybridSearchOutputBoundaryRejectsRevokedShare(t *testing.T) {
	db := leaseHandlerDB(t)
	store := repository.NewNextcloudContentLeaseStore(db)
	row := leaseHandlerKnowledge(t)
	row.TenantID = 8
	share := &leaseRevocableShare{}
	service := &hybridOutputKnowledgeService{row: row}
	service.onCheck = func(n int) error {
		if n == 2 { // revoke after the final handler check, before JSON writes
			share.revoked.Store(true)
		}
		return nil
	}
	h := &KnowledgeBaseHandler{
		service: &hybridOutputKBService{
			kb: &types.KnowledgeBase{ID: "kb-1", TenantID: 8},
			results: []*types.SearchResult{{
				KnowledgeID: row.ID, KnowledgeBaseID: row.KnowledgeBaseID,
				KnowledgeChannel: row.Channel, Metadata: row.GetMetadata(), Content: "shared secret",
			}},
		},
		knowledgeService: service, kbShareService: share, contentLeases: store,
	}
	w := runHybridOutputRequest(t, h)
	require.GreaterOrEqual(t, service.checks, 2)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "shared secret")
	assertLeaseReleased(t, db)
}
