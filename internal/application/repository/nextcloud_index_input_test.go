//go:build sqlite_fts5

package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type nextcloudIndexTableControls struct {
	Kind, SummaryModel, EmbeddingModel string
	SampleRows                         int
}

func nextcloudIndexInputFixture(t *testing.T) (*onboardingOriginalActualFixture, context.Context,
	*nextcloudIndexTableControls,
) {
	t.Helper()
	f, ctx, _, _ := machineTaskRepositoryFixtureWithConfiguration(t, func(f *onboardingOriginalActualFixture) {
		require.NoError(t, f.db.Create(&types.Model{
			ID: "index-actual-summary", TenantID: 7, Type: types.ModelTypeKnowledgeQA,
			Status: types.ModelStatusActive,
		}).Error)
		var template types.KnowledgeBase
		require.NoError(t, f.db.Where("id = ?", f.credential.TemplateKBID).Take(&template).Error)
		template.SummaryModelID = "index-actual-summary"
		template.IndexingStrategy = types.DefaultIndexingStrategy()
		require.NoError(t, f.store.ReplaceKnowledgeBaseConfigurationWithInput(
			WithIndependentKnowledgeInput(f.admin), &template))
	})
	original := KnowledgeBuildInputProofFromContext(ctx)
	actual, err := f.store.CaptureKnowledgeEnrichmentInputs(ctx, types.KnowledgeBuildInputCaptureRequest{
		TenantID: original.TenantID, KnowledgeBaseID: original.KnowledgeBaseID,
		KnowledgeID: original.KnowledgeID, Expected: original,
	})
	require.NoError(t, err)
	ctx = WithKnowledgeBuildInputProof(ctx, f.store, actual.Proof)
	controls := &nextcloudIndexTableControls{
		Kind:         types.TypeDataTableSummary,
		SummaryModel: actual.KnowledgeBase.SummaryModelID, EmbeddingModel: actual.KnowledgeBase.EmbeddingModelID,
		SampleRows: 10,
	}
	producer, err := f.store.AdmitKnowledgeProducerInput(ctx, actual.Proof, "summary", controls)
	require.NoError(t, err)
	ctx = WithKnowledgeProducerInput(ctx, producer, func() any { return controls })
	require.NoError(t, CheckKnowledgeProducerInput(ctx))
	return f, ctx, controls
}

func nextcloudIndexInputGuard(t *testing.T, f *onboardingOriginalActualFixture, ctx context.Context) error {
	t.Helper()
	proof := KnowledgeBuildInputProofFromContext(ctx)
	return f.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return GuardNextcloudLocalIndexWrites(ctx, tx, []*types.IndexInfo{{
			KnowledgeID: proof.KnowledgeID, KnowledgeBaseID: proof.KnowledgeBaseID,
			SourceID: "actual-table-model-output", ChunkID: "actual-table-model-output",
			SourceType: types.ChunkSourceType, Content: "Actual original table model output",
		}})
	})
}

func TestNextcloudIndexInputRechecksActualTableProducerBeforeSQLWrite(t *testing.T) {
	for _, mutation := range []string{"summary model", "embedding model", "KB controls", "table controls"} {
		t.Run(mutation, func(t *testing.T) {
			f, ctx, controls := nextcloudIndexInputFixture(t)
			require.NoError(t, nextcloudIndexInputGuard(t, f, ctx))
			proof := KnowledgeBuildInputProofFromContext(ctx)
			switch mutation {
			case "summary model", "embedding model":
				id := controls.SummaryModel
				if mutation == "embedding model" {
					id = controls.EmbeddingModel
				}
				require.NoError(t, f.db.Model(&types.Model{}).Where("id = ?", id).
					Update("name", "Changed while the actual provider was running").Error)
			case "KB controls":
				require.NoError(t, f.db.Model(&types.KnowledgeBase{}).Where("id = ?", proof.KnowledgeBaseID).
					Update("summary_model_id", controls.EmbeddingModel).Error)
			case "table controls":
				controls.SampleRows = 9
			}
			require.Error(t, nextcloudIndexInputGuard(t, f, ctx),
				"a prior provider checkpoint cannot certify changed producer/model inputs at vector commit")
		})
	}
}

func TestNextcloudIndexInputRejectsPresentedCrossScopeProof(t *testing.T) {
	f, ctx, _ := nextcloudIndexInputFixture(t)
	proof := KnowledgeBuildInputProofFromContext(ctx)
	for _, scope := range []struct {
		tenant uint64
		kb, kg string
	}{
		{proof.TenantID + 1, proof.KnowledgeBaseID, proof.KnowledgeID},
		{proof.TenantID, "another-kb", proof.KnowledgeID},
		{proof.TenantID, proof.KnowledgeBaseID, "another-knowledge"},
	} {
		require.Error(t, f.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return ValidateNextcloudIndexInputWriteInTx(ctx, tx, scope.tenant, scope.kb, scope.kg)
		}))
	}
}
