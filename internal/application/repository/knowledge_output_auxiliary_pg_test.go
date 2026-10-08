package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestPostgresKnowledgeOutputAuxiliaryExplicitOriginalMetadata(t *testing.T) {
	store, db := collectionPostgresFixture(t)
	ctx := types.WithCaller(context.Background(), types.Caller{TenantID: 7, UserID: "collection-user", Role: types.TenantRoleAdmin})
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "collection-user"})
	tags, knowledge := NewKnowledgeTagRepository(db), NewKnowledgeRepository(db)
	factory := func(tx *gorm.DB) *KnowledgeInputProvenanceStore {
		actual := NewKnowledgeInputProvenanceStore(tx)
		ConfigureKnowledgeInputActorGroupAccess(actual, displayTestGroups{db: tx})
		return actual
	}
	ConfigureKnowledgeDisplayInputWriters(store, tags, knowledge, factory)
	tag := &types.KnowledgeTag{ID: "metadata-tag", TenantID: 7, KnowledgeBaseID: "kb", Name: "original metadata tag", Color: "blue", SortOrder: 1}
	require.NoError(t, tags.Create(WithIndependentKnowledgeTagCreateInput(ctx, "kb", tag.Name, tag.Color, tag.SortOrder), tag))
	require.NoError(t, knowledge.SetKnowledgeTags(WithIndependentKnowledgeTagAssignmentInput(ctx, map[string][]string{"doc-a": {tag.ID}}), "doc-a", []string{tag.ID}))
	actual, err := store.CaptureKnowledgeOutputInputs(ctx, types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "doc-a"})
	require.NoError(t, err)
	proofs := []*types.KnowledgeBuildInputProof{actual.Proof}
	selections := []types.KnowledgeOutputDisplaySelection{{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "doc-a", Tags: true, KnowledgeBaseName: true}}
	snapshot, err := store.CaptureKnowledgeOutputAuxiliary(ctx, proofs, selections)
	require.NoError(t, err)
	require.Len(t, snapshot.Knowledge, 1)
	require.Equal(t, tag.Name, snapshot.Auxiliary.Tags["doc-a"][0].Name)
	require.Equal(t, "actual original human collection KB", snapshot.Auxiliary.KnowledgeBaseNames["kb"])
	require.NoError(t, store.RecheckKnowledgeOutputAuxiliary(ctx, snapshot))
	// An unrelated legacy material and an unconsumed KB display field must
	// not expand the original explicit document dependency set.
	require.NoError(t, db.Create(&types.Knowledge{ID: "unrelated-legacy", TenantID: 7, KnowledgeBaseID: "kb", Type: "manual", Channel: "web", Title: "unconsumed legacy", CustomMetadata: types.JSON(`{}`)}).Error)
	require.NoError(t, db.Model(&types.KnowledgeBase{}).Where("id='kb'").Update("name", "unproved unconsumed name").Error)
	selections[0].KnowledgeBaseName = false
	selected, err := store.CaptureKnowledgeOutputAuxiliary(ctx, proofs, selections)
	require.NoError(t, err)
	require.Empty(t, selected.Auxiliary.KnowledgeBaseNames)
	require.NoError(t, store.RecheckKnowledgeOutputAuxiliary(ctx, selected))
	require.Error(t, store.RecheckKnowledgeOutputAuxiliary(ctx, snapshot), "consumed original name cannot be replaced by its current raw database string")
	require.NoError(t, db.Model(&types.KnowledgeTag{}).Where("id='metadata-tag'").Update("name", "raw changed").Error)
	require.NoError(t, db.Model(&types.KnowledgeTag{}).Where("id='metadata-tag'").Update("name", tag.Name).Error)
	require.Error(t, store.RecheckKnowledgeOutputAuxiliary(ctx, selected), "tag ABA cannot restore the original producer generation")
}
