package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func promptInputFixture(
	t *testing.T,
) (*KnowledgeInputProvenanceStore, *gorm.DB, context.Context, *types.KnowledgeBase) {
	t.Helper()
	store, db, ctx, _, _, _ := displayTestFixture(t, false)
	initializeDisplayOriginalBody(t, db, false)
	kb := &types.KnowledgeBase{
		ID:               "prompt-empty",
		TenantID:         7,
		Name:             "Actual original empty KB",
		Type:             types.KnowledgeBaseTypeDocument,
		EmbeddingModelID: "embed",
		SummaryModelID:   "summary",
		IndexingStrategy: types.IndexingStrategy{VectorEnabled: true},
	}
	producer := WithIndependentKnowledgeBaseNameInput(WithIndependentKnowledgeInput(ctx), kb.ID, kb.Name)
	require.NoError(t, store.CreateKnowledgeBaseWithInput(producer, kb))
	return store, db, ctx, kb
}

func TestAgentPromptKBOriginalInputsEmptyAndNoFileEnumeration(t *testing.T) {
	store, db, ctx, kb := promptInputFixture(t)
	var fileReads int
	const name = "agent_prompt_forbid_file_enumeration"
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register(name, func(tx *gorm.DB) {
		if tx.Statement.Table == "knowledges" || tx.Statement.Table == "nextcloud_source_versions" {
			fileReads++
		}
	}))
	t.Cleanup(func() { require.NoError(t, db.Callback().Query().Remove(name)) })
	snapshot, err := store.CaptureKnowledgeBasePromptInputs(ctx, 7, kb.ID)
	require.NoError(t, err)
	defer func(original *KnowledgeBasePromptInputSnapshot) { require.NoError(t, original.Close()) }(snapshot)
	metadata := snapshot.Metadata()
	require.Equal(t, kb.Name, metadata.Name)
	require.Equal(t, []string{"chunks"}, metadata.Capabilities)
	require.NoError(t, metadata.SourceLineage.RequireComplete())
	require.Empty(t, metadata.SourceLineage.Sources)
	require.NoError(t, store.RecheckKnowledgeBasePromptInputs(ctx, snapshot))
	require.Zero(t, fileReads, "KB metadata must not enumerate a personal source binding")
	metadata.Name = "forged detached label"
	metadata.Capabilities[0] = "forged capability"
	metadata.SourceLineage.State = types.SourceLineageUnknown
	require.NoError(t, store.RecheckKnowledgeBasePromptInputs(ctx, snapshot))
	require.Equal(t, kb.Name, snapshot.Metadata().Name)
}

func TestAgentPromptKBRejectsChangedOriginalActorAndUnknown(t *testing.T) {
	for _, mutation := range []string{"name_aba", "configuration_aba", "actor", "forged", "closed", "raw_unknown"} {
		t.Run(mutation, func(t *testing.T) {
			store, db, ctx, kb := promptInputFixture(t)
			if mutation == "raw_unknown" {
				raw := &types.KnowledgeBase{
					ID:       "raw-no-origin",
					TenantID: 7,
					Name:     "unproven current name",
					Type:     "document",
				}
				require.NoError(t, db.Create(raw).Error)
				_, err := store.CaptureKnowledgeBasePromptInputs(ctx, 7, raw.ID)
				require.Error(t, err)
				return
			}
			snapshot, err := store.CaptureKnowledgeBasePromptInputs(ctx, 7, kb.ID)
			require.NoError(t, err)
			defer func(original *KnowledgeBasePromptInputSnapshot) { require.NoError(t, original.Close()) }(snapshot)
			checkCtx := ctx
			switch mutation {
			case "name_aba":
				require.NoError(
					t,
					db.Model(&types.KnowledgeBase{}).Where("id=?", kb.ID).Update("name", "foreign current label").Error,
				)
				require.NoError(t, db.Model(&types.KnowledgeBase{}).Where("id=?", kb.ID).Update("name", kb.Name).Error)
			case "configuration_aba":
				require.NoError(
					t,
					db.Model(&types.KnowledgeBase{}).
						Where("id=?", kb.ID).
						Update("indexing_strategy", types.IndexingStrategy{WikiEnabled: true}).
						Error,
				)
				require.NoError(
					t,
					db.Model(&types.KnowledgeBase{}).
						Where("id=?", kb.ID).
						Update("indexing_strategy", kb.IndexingStrategy).
						Error,
				)
			case "actor":
				checkCtx = types.WithCaller(
					ctx,
					types.Caller{TenantID: 7, UserID: "different-user", Role: types.TenantRoleAdmin},
				)
			case "forged":
				snapshot = &KnowledgeBasePromptInputSnapshot{metadata: snapshot.Metadata()}
			case "closed":
				require.NoError(t, snapshot.Close())
			}
			require.Error(t, store.RecheckKnowledgeBasePromptInputs(checkCtx, snapshot))
		})
	}
}
