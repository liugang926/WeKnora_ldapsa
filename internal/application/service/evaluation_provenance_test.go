package service

import (
	"context"
	"os"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type evaluationProvenanceKB struct {
	interfaces.KnowledgeBaseService
	store *repository.KnowledgeInputProvenanceStore
}

func (s evaluationProvenanceKB) CreateKnowledgeBase(
	ctx context.Context,
	kb *types.KnowledgeBase) (*types.KnowledgeBase,
	error,
) {
	kb.ID = "evaluation-" + kb.EmbeddingModelID
	kb.TenantID = 7
	return kb, s.store.CreateKnowledgeBaseWithInput(ctx, kb)
}

func TestEvaluationConfigurationAdmitsOnlyFreshFixedConfiguration(t *testing.T) {
	_, store, db, ctx, _, _, _ := historyActorIntegrationFixture(t, "web")
	ddl, err := os.ReadFile("../../../migrations/sqlite/000059_knowledge_collection_display_inputs.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(ddl)).Error)
	service := &EvaluationService{knowledgeBaseService: evaluationProvenanceKB{store: store}}
	for _, model := range []string{"selected-model", "reference-model"} {
		kb, err := service.createEvaluationKnowledgeBase(ctx, model, "chat-model")
		require.NoError(t, err)
		require.Equal(t, "evaluation", kb.Name)
		require.Equal(t, model, kb.EmbeddingModelID)
		require.Nil(t, kb.ExtractConfig)
		var origin struct {
			OriginKind    string
			SourceLineage string
		}
		require.NoError(t,
			db.Table("knowledge_config_input_origins").Where(
				"knowledge_base_id = ?",
				kb.ID).Take(&origin).Error)
		require.Equal(t, "derived", origin.OriginKind)
	}
	raw := evaluationKnowledgeBase("unadmitted", "chat")
	raw.ID, raw.TenantID = "unadmitted", 7
	require.ErrorIs(t, store.CreateKnowledgeBaseWithInput(ctx, raw), types.ErrSourceLineageUnknown)
}
