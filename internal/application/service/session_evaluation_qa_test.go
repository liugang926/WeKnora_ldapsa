package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestEvaluationQATargetTenantActualPipeline(t *testing.T) {
	for _, mode := range []string{"allowed", "missing-actor", "foreign-tenant", "extra-target", "image", "inactive-actor", "missing-kb"} {
		t.Run(mode, func(t *testing.T) {
			model := &ragEntryModel{}
			f := NewNormalReferencesHTTPFixtureForTest(t, false, model)
			svc := f.Sessions.(normalReferencesSessionForTest).SessionService.(*sessionService)
			ConfigureGroupAccessUserEligibility(svc.groupAccess, &userService{userRepo: repository.NewUserRepository(f.DB)})
			chatpipeline.NewPluginSearch(svc.eventManager, f.KBs, svc.knowledgeService, svc.chunkService, f.Config, nil, svc.tenantService, nil, nil, nil)
			chatpipeline.NewPluginChatCompletion(svc.eventManager, f.Models)
			ctx := f.Ctx
			cm := &types.ChatManage{}
			cm.Query = "What is the current employee policy?"
			cm.RewriteQuery = cm.Query
			cm.ChatModelID = "normal-reference-model"
			cm.KnowledgeBaseIDs = []string{"kb"}
			cm.SearchTargets = types.SearchTargets{{Type: types.SearchTargetTypeKnowledgeBase, TenantID: 7, KnowledgeBaseID: "kb"}}
			cm.EmbeddingTopK = 2
			cm.RerankTopK = 2
			cm.SummaryConfig.ContextTemplate = "{{query}}\n{{contexts}}"
			switch mode {
			case "missing-actor":
				ctx = context.WithValue(context.Background(), types.TenantIDContextKey, uint64(7))
			case "foreign-tenant":
				ctx = context.WithValue(ctx, types.TenantIDContextKey, uint64(8))
				cm.SearchTargets[0].TenantID = 8
			case "extra-target":
				cm.SearchTargets = append(cm.SearchTargets, &types.SearchTarget{Type: types.SearchTargetTypeKnowledgeBase, TenantID: 7, KnowledgeBaseID: "other"})
			case "image":
				cm.Images = []string{"resource://image"}
			case "inactive-actor":
				require.NoError(t, f.DB.Model(&types.User{}).Where("id=?", types.CallerFromContext(ctx).UserID).Update("is_active", false).Error)
			case "missing-kb":
				cm.SearchTargets[0].KnowledgeBaseID = "missing"
			}
			if mode == "allowed" {
				cm.SearchTargets[0].TenantID = 0
				require.Error(t, svc.KnowledgeQAByEvent(ctx, cm, types.Pipline["rag"]))
				cm.SearchTargets[0].TenantID = 7
				err := svc.KnowledgeQAByEvent(ctx, cm, types.Pipline["rag"])
				require.NoError(t, err, "strict filled tenant comparison")
				require.NotEmpty(t, cm.SearchResult)
				require.NotNil(t, cm.ChatResponse)
				require.NotEmpty(t, cm.ChatResponse.Content)
				return
			}
			err := svc.KnowledgeQAByEvent(ctx, cm, types.Pipline["rag"])
			require.Error(t, err)
			require.Zero(t, model.calls())
		})
	}
}
