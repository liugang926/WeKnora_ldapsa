package handler

import (
	"context"
	stderrors "errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
)

type storageBindingTestResolver struct {
	interfaces.StorageBackendResolver
}

func (storageBindingTestResolver) ResolveBackend(
	_ context.Context, _ *types.Tenant, id, _ string,
) (*types.StorageBackend, error) {
	return &types.StorageBackend{ID: id, Provider: "local"}, nil
}

type storageBindingTestKnowledgeService struct {
	interfaces.KnowledgeService
	total         int64
	deletingTotal int64
	err           error
	deletingErr   error
	calls         int
}

func (s *storageBindingTestKnowledgeService) ListPagedKnowledgeByKnowledgeBaseID(
	_ context.Context, _ string, _ *types.Pagination, filter types.KnowledgeListFilter,
) (*types.PageResult, error) {
	s.calls++
	if filter.ParseStatus == types.ParseStatusDeleting {
		if s.deletingErr != nil {
			return nil, s.deletingErr
		}
		return &types.PageResult{Total: s.deletingTotal}, nil
	}
	if s.err != nil {
		return nil, s.err
	}
	return &types.PageResult{Total: s.total}, nil
}

func TestKBStorageBindingRequiresVerifiedEmptyKB(t *testing.T) {
	cases := []struct {
		name            string
		oldID           string
		requestID       string
		requestProvider string
		total           int64
		deletingTotal   int64
		listErr         error
		deletingErr     error
		wantStatus      int
		wantID          string
		wantCalls       int
	}{
		{"legacy KB with files", "", "new", "local", 1, 0, nil, nil, http.StatusBadRequest, "", 1},
		{"bound KB with files", "old", "new", "local", 1, 0, nil, nil, http.StatusBadRequest, "old", 1},
		{"deleting-only legacy KB", "", "new", "local", 0, 1, nil, nil, http.StatusBadRequest, "", 2},
		{
			"deleting count denied", "old", "new", "local", 0, 0, nil, stderrors.New("deleting count denied"),
			http.StatusInternalServerError, "old", 2,
		},
		{
			"legacy KB count denied", "", "new", "local", 0, 0, stderrors.New("count denied"), nil,
			http.StatusInternalServerError, "", 1,
		},
		{
			"bound KB count denied", "old", "new", "local", 0, 0, stderrors.New("count denied"), nil,
			http.StatusInternalServerError, "old", 1,
		},
		{"legacy provider change with files", "", "", "minio", 1, 0, nil, nil, http.StatusBadRequest, "", 1},
		{"empty KB can bind", "", "new", "local", 0, 0, nil, nil, 0, "new", 2},
		{"same binding needs no count", "old", "old", "local", 1, 0, stderrors.New("count denied"), nil, 0, "old", 0},
		{"unchanged legacy KB needs no count", "", "", "local", 1, 0, stderrors.New("count denied"), nil, 0, "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kb := &types.KnowledgeBase{}
			if tc.oldID != "" {
				id := tc.oldID
				kb.StorageBackendID = &id
			}
			kb.SetStorageProvider("local")
			knowledge := &storageBindingTestKnowledgeService{
				total: tc.total, deletingTotal: tc.deletingTotal,
				err: tc.listErr, deletingErr: tc.deletingErr,
			}
			h := &InitializationHandler{
				storageResolver:  storageBindingTestResolver{},
				knowledgeService: knowledge,
			}
			req := &KBModelConfigRequest{
				StorageBackendID: tc.requestID,
				StorageProvider:  tc.requestProvider,
			}
			ctx := context.WithValue(context.Background(), types.TenantInfoContextKey, &types.Tenant{ID: 42})
			err := h.applyKBStorageBinding(ctx, kb, "kb-1", req)
			if tc.wantStatus == 0 {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			} else {
				var appErr *apperrors.AppError
				if !stderrors.As(err, &appErr) || appErr.HTTPCode != tc.wantStatus {
					t.Fatalf("error = %v, want HTTP %d", err, tc.wantStatus)
				}
			}
			gotID := ""
			if kb.StorageBackendID != nil {
				gotID = *kb.StorageBackendID
			}
			if gotID != tc.wantID || kb.GetStorageProvider() != "local" {
				t.Fatalf("binding mutated to id=%q provider=%q; want id=%q provider=local",
					gotID, kb.GetStorageProvider(), tc.wantID)
			}
			if knowledge.calls != tc.wantCalls {
				t.Fatalf("count calls = %d, want %d", knowledge.calls, tc.wantCalls)
			}
		})
	}
}

func TestUpdateKBConfigEmbeddingChangeFailsClosedOnCountError(t *testing.T) {
	for _, tc := range []struct {
		name          string
		total         int64
		deletingTotal int64
		listErr       error
		deletingErr   error
		wantStatus    int
		wantCalls     int
	}{
		{"files exist", 1, 0, nil, nil, http.StatusBadRequest, 1},
		{"deleting-only row", 0, 1, nil, nil, http.StatusBadRequest, 2},
		{"count denied", 0, 0, stderrors.New("count denied"), nil, http.StatusInternalServerError, 1},
		{"deleting count denied", 0, 0, nil, stderrors.New("deleting count denied"), http.StatusInternalServerError, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kb := &types.KnowledgeBase{ID: "kb-1", TenantID: 42, EmbeddingModelID: "old-model"}
			knowledge := &storageBindingTestKnowledgeService{
				total: tc.total, deletingTotal: tc.deletingTotal,
				err: tc.listErr, deletingErr: tc.deletingErr,
			}
			h := &InitializationHandler{
				kbService:        &stubInitializationKBService{kb: kb},
				knowledgeService: knowledge,
			}
			gin.SetMode(gin.TestMode)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Params = gin.Params{{Key: "kbId", Value: "kb-1"}}
			req := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(
				`{"llmModelId":"m-llm","embeddingModelId":"new-model"}`))
			req.Header.Set("Content-Type", "application/json")
			ctx := types.WithCaller(req.Context(), types.Caller{
				TenantID: 42, UserID: "u", Role: types.TenantRoleAdmin,
			})
			c.Request = req.WithContext(ctx)

			h.UpdateKBConfig(c)

			if len(c.Errors) != 1 {
				t.Fatalf("handler errors = %v, want one denial", c.Errors)
			}
			var appErr *apperrors.AppError
			if !stderrors.As(c.Errors[0].Err, &appErr) || appErr.HTTPCode != tc.wantStatus {
				t.Fatalf("error = %v, want HTTP %d", c.Errors[0].Err, tc.wantStatus)
			}
			if kb.EmbeddingModelID != "old-model" || knowledge.calls != tc.wantCalls {
				t.Fatalf("embedding=%q count calls=%d; want old-model and %d count calls",
					kb.EmbeddingModelID, knowledge.calls, tc.wantCalls)
			}
		})
	}
}
