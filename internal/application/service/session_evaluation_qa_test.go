package service

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types/interfaces"

	"github.com/Tencent/WeKnora/internal/application/repository"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// A permissive model spy makes permission failures independent of model lookup
// failure. Even a foreign embedding request would succeed if authorization leaked.
type evaluationEmbeddingSpy struct {
	interfaces.ModelService
	calls atomic.Int64
}

func (s *evaluationEmbeddingSpy) GetEmbeddingModel(ctx context.Context, id string) (embedding.Embedder, error) {
	s.calls.Add(1)
	return s.ModelService.GetEmbeddingModel(ctx, id)
}

func (s *evaluationEmbeddingSpy) GetEmbeddingModelForTenant(
	ctx context.Context, id string, _ uint64,
) (embedding.Embedder, error) {
	s.calls.Add(1)
	return s.ModelService.GetEmbeddingModel(ctx, id)
}

func TestEvaluationQATargetTenantActualPipeline(t *testing.T) {
	for _, mode := range []string{
		"allowed",
		"default-execution-tenant",
		"missing-actor",
		"foreign-tenant",
		"extra-target",
		"image",
		"inactive-actor",
		"missing-kb",
	} {
		t.Run(mode, func(t *testing.T) {
			model := &ragEntryModel{}
			f := NewNormalReferencesHTTPFixtureForTest(t, false, model)
			svc := f.Sessions.(normalReferencesSessionForTest).SessionService.(*sessionService)
			embeddingSpy := &evaluationEmbeddingSpy{ModelService: f.Models}
			svc.modelService = embeddingSpy
			f.KBs.(*knowledgeBaseService).modelService = embeddingSpy
			ConfigureGroupAccessUserEligibility(
				svc.groupAccess,
				&userService{
					userRepo: repository.NewUserRepository(f.DB),
				})
			chatpipeline.NewPluginSearch(
				svc.eventManager,
				f.KBs,
				svc.knowledgeService,
				svc.chunkService,
				f.Config,
				nil,
				svc.tenantService,
				nil,
				nil,
				nil)
			chatpipeline.NewPluginChatCompletion(svc.eventManager, f.Models)
			// The pipeline is internal: HTTP admission checks workspace ownership,
			// and EvalDataset constructs its own targets. Exercise live resource
			// policies explicitly rather than the retired Nextcloud guard.
			ctx := f.Ctx
			restrict := func(tenant uint64, id string) {
				require.NoError(t, f.DB.Create(&types.ResourceAccessPolicy{
					TenantID: tenant, ResourceType: types.GroupResourceTypeKnowledgeBase,
					ResourceID: id, Mode: types.ResourceAccessRestricted,
				}).Error)
			}
			cm := &types.ChatManage{}
			cm.Query = "What is the current employee policy?"
			cm.RewriteQuery = cm.Query
			cm.ChatModelID = "normal-reference-model"
			cm.KnowledgeBaseIDs = []string{"kb"}
			cm.SearchTargets = types.SearchTargets{
				{
					Type:            types.SearchTargetTypeKnowledgeBase,
					TenantID:        7,
					KnowledgeBaseID: "kb",
				},
			}
			cm.EmbeddingTopK = 2
			cm.RerankTopK = 2
			cm.SummaryConfig.ContextTemplate = "{{query}}\n{{contexts}}"
			switch mode {
			case "missing-actor":
				restrict(7, "kb")
				ctx = context.WithValue(context.Background(), types.TenantIDContextKey, uint64(7))
			case "foreign-tenant":
				// Preserve the authenticated caller when changing execution scope,
				// as the real shared-resource APIs do. A protected foreign
				// resource must fail permission checks before model resolution.
				restrict(8, "foreign")
				ctx = types.WithExecutionTenant(ctx, 8)
				cm.SearchTargets[0].TenantID = 8
				cm.SearchTargets[0].KnowledgeBaseID = "foreign"
			case "default-execution-tenant":
				cm.SearchTargets[0].TenantID = 0
			case "extra-target":
				require.NoError(t, f.DB.Model(&types.TenantMember{}).
					Where("tenant_id = ? AND user_id = ?", 7, types.CallerFromContext(ctx).UserID).
					Update("role", types.TenantRoleViewer).Error)
				restrict(7, "other")
				cm.SearchTargets = append(
					cm.SearchTargets,
					&types.SearchTarget{
						Type:            types.SearchTargetTypeKnowledgeBase,
						TenantID:        7,
						KnowledgeBaseID: "other",
					})
			case "image":
				cm.Images = []string{"resource://image"}
			case "inactive-actor":
				require.NoError(t,
					f.DB.Model(&types.User{}).Where("id=?",
						types.CallerFromContext(ctx).UserID).Update(
						"is_active",
						false).Error)
			case "missing-kb":
				cm.SearchTargets[0].KnowledgeBaseID = "missing"
			}
			// The retired guard no longer rejects an omitted internal tenant.
			// Unaccounted image inputs still fail the live lineage accounting.
			if mode == "allowed" || mode == "default-execution-tenant" {
				err := svc.KnowledgeQAByEvent(ctx, cm, types.Pipline["rag"])
				require.NoError(t, err, "admitted internal RAG pipeline")
				require.Positive(t, embeddingSpy.calls.Load())
				require.Positive(t, model.calls(), "real generation, not only fallback")
				require.NotEmpty(t, cm.SearchResult)
				require.NotNil(t, cm.ChatResponse)
				require.NotEmpty(t, cm.ChatResponse.Content)
				return
			}
			err := svc.KnowledgeQAByEvent(ctx, cm, types.Pipline["rag"])
			require.Error(t, err)
			if mode == "missing-actor" || mode == "foreign-tenant" ||
				mode == "extra-target" || mode == "inactive-actor" {
				require.ErrorIs(t, err, ErrResourceAccessDenied)
			}
			if mode == "image" {
				require.ErrorIs(t, err, types.ErrSourceLineageUnknown)
			}
			if mode != "image" {
				require.Zero(t, embeddingSpy.calls.Load(), "permission refusal precedes embedding")
			}
			require.Zero(t, model.calls())
		})
	}
}
