package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type askTargetLookup struct {
	knowledge *types.Knowledge
	calls     int
	change    bool
}

func (s *askTargetLookup) FindPublished(_ context.Context, _, _ string, _ int64, _ string) (*types.Knowledge, error) {
	s.calls++
	if s.change && s.calls == 2 {
		changedKnowledge := *s.knowledge
		changedKnowledge.ID = "another-version"
		return &changedKnowledge, nil
	}
	return s.knowledge, nil
}

type askKnowledgeService struct {
	interfaces.KnowledgeService
	knowledge *types.Knowledge
}

func (s *askKnowledgeService) GetKnowledgeByIDOnly(_ context.Context, id string) (*types.Knowledge, error) {
	if id != s.knowledge.ID {
		return nil, nil
	}
	return s.knowledge, nil
}

func TestNextcloudAskTargetRequiresHumanAndRechecksSourceBeforeReturning(t *testing.T) {
	gin.SetMode(gin.TestMode)
	knowledge := &types.Knowledge{ID: "published", TenantID: 7, KnowledgeBaseID: "kb", Title: "private.md"}
	for _, tc := range []struct {
		name       string
		principal  types.Principal
		machine    bool
		change     bool
		denySource bool
		wantStatus int
		wantChecks int
	}{
		{"human", types.Principal{Type: types.PrincipalWebUser, ID: "alice"}, false, false, false, 200, 1},
		{"machine", types.Principal{Type: types.PrincipalAPITenant, ID: "alice"}, true, false, false, 403, 0},
		{"forged user", types.Principal{Type: types.PrincipalWebUser, ID: "bob"}, false, false, false, 403, 0},
		{"changed publication", types.Principal{Type: types.PrincipalWebUser, ID: "alice"}, false, true, false, 503, 1},
		{"revoked source", types.Principal{Type: types.PrincipalWebUser, ID: "alice"}, false, false, true, 403, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lookup := &askTargetLookup{knowledge: knowledge, change: tc.change}
			checks := 0
			h := &KnowledgeHandler{
				kgService:  &askKnowledgeService{knowledge: knowledge},
				askTargets: lookup,
				publicationCheck: func(context.Context, *types.Knowledge) error {
					checks++
					if tc.denySource {
						return apperrors.NewForbiddenError("Source access denied")
					}
					return nil
				},
			}
			router := gin.New()
			router.Use(middleware.ErrorHandler())
			router.GET("/ask-target", h.NextcloudAskTarget)
			request := httptest.NewRequest(http.MethodGet,
				"/ask-target?instance_id=instance&binding_id=binding&file_id=77&source_etag=etag-1", nil)
			ctx := types.WithCaller(request.Context(), types.Caller{
				TenantID: 7, UserID: "alice", Role: types.TenantRoleViewer,
			})
			ctx = types.WithPrincipal(ctx, tc.principal)
			if tc.machine {
				ctx = types.WithTenantAPIKeyScope(ctx, types.TenantAPIKeyScope{KeyID: 1, FullAccess: true})
			}
			record := httptest.NewRecorder()
			router.ServeHTTP(record, request.WithContext(ctx))
			require.Equal(t, tc.wantStatus, record.Code, record.Body.String())
			require.Equal(t, tc.wantChecks, checks)
			require.Equal(t, "no-store", record.Header().Get("Cache-Control"))
			if tc.wantStatus == http.StatusOK {
				require.Contains(t, record.Body.String(), `"knowledge_id":"published"`)
				require.Equal(t, 2, lookup.calls)
			} else {
				require.False(t, strings.Contains(record.Body.String(), "private.md"))
			}
		})
	}
}
