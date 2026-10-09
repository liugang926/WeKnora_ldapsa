package repository

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func initializeDisplayOriginalBody(t *testing.T, db *gorm.DB, pg bool) {
	t.Helper()
	path := "../../../migrations/sqlite/000071_original_body_retention.up.sql"
	if pg {
		path = "../../../migrations/versioned/000152_original_body_retention.up.sql"
	}
	script, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	require.NoError(t, InstallOriginalBodyRetentionTriggers(db))
	require.NoError(t, InstallOriginalBodyRetentionCallbacks(db))
	directory := t.TempDir()
	require.NoError(t, os.Chmod(directory, 0700))
	key := filepath.Join(directory, "key")
	require.NoError(t, os.WriteFile(key, []byte(strings.Repeat("owned-display-key-", 3)), 0600))
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_DIRECTORY", directory)
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_KEY", key)
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_PIN", filepath.Join(directory, "pin"))
	require.NoError(t, NewOriginalBodyRetentionStore(db).InitializeJournal())
}

func runKnowledgeDisplayOriginalBody(t *testing.T, pg bool) {
	t.Run("actual_alias_cache_and_original_revision", func(t *testing.T) {
		store, db, ctx, tags, knowledge, k := displayTestFixture(t, pg)
		initializeDisplayOriginalBody(t, db, pg)
		tag := displayTag(t, tags, ctx, "body-display-tag")
		require.NoError(t, knowledge.SetKnowledgeTags(WithIndependentKnowledgeTagAssignmentInput(ctx, map[string][]string{k.ID: {tag.ID}}), k.ID, []string{tag.ID}))
		var kb types.KnowledgeBase
		require.NoError(t, db.Where("id='kb'").Take(&kb).Error)
		kb.Name = "actual body-backed KB name"
		require.NoError(t, store.UpdateKnowledgeBaseWithInput(WithIndependentKnowledgeBaseNameInput(WithIndependentKnowledgeInput(ctx), kb.ID, kb.Name), &kb))

		// Raw SQL deliberately bypasses body hydration to prove the real
		// producers stored references, rather than unchanged inline fixtures.
		var raw []string
		require.NoError(t, db.Raw("SELECT actual_input FROM knowledge_display_input_origins WHERE row_id=? OR (row_id=? AND kind='relations')", tag.ID, k.ID).Scan(&raw).Error)
		require.Len(t, raw, 4)
		for _, value := range raw {
			_, reference := bodyReference(value)
			require.True(t, reference)
		}
		cached, err := cacheKnowledgeDisplayInputs(db, []string{tag.ID, k.ID, kb.ID})
		require.NoError(t, err)
		cache := cached.Statement.Context.Value(knowledgeDisplayCacheKey{}).(*knowledgeDisplayCache)
		for _, kind := range []string{"tag-name", "tag-color", "tag-sort_order", "relations", "kb-name"} {
			id := tag.ID
			if kind == "relations" {
				id = k.ID
			} else if kind == "kb-name" {
				id = kb.ID
			}
			origin, present := cache.origins[knowledgeDisplayCacheID(kind, id)]
			require.True(t, present)
			_, reference := bodyReference(origin.ActualInput)
			require.False(t, reference, "aliased Query callback must resolve each actual original input")
		}
		original, err := store.CaptureKnowledgeOutputInputs(ctx, types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID})
		require.NoError(t, err)
		proofs := []*types.KnowledgeBuildInputProof{original.Proof}
		selections := []types.KnowledgeOutputDisplaySelection{{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Tags: true, KnowledgeBaseName: true}}
		snapshot, err := store.CaptureKnowledgeOutputAuxiliary(ctx, proofs, selections)
		require.NoError(t, err)
		require.Equal(t, tag.Name, snapshot.Auxiliary.Tags[k.ID][0].Name)
		require.Equal(t, kb.Name, snapshot.Auxiliary.KnowledgeBaseNames[kb.ID])
		require.NoError(t, store.RecheckKnowledgeOutputAuxiliary(ctx, snapshot))

		// A genuine authorized producer writes a new original revision. No
		// test edits a vault payload or invents an original receipt.
		name := "new authorized original tag name"
		tag.Name = name
		require.NoError(t, tags.Update(WithIndependentKnowledgeTagUpdateInput(ctx, tag.ID, &name, nil, nil), tag))
		require.Error(t, store.RecheckKnowledgeOutputAuxiliary(ctx, snapshot))
		current, err := store.CaptureKnowledgeOutputAuxiliary(ctx, proofs, selections)
		require.NoError(t, err)
		require.Equal(t, name, current.Auxiliary.Tags[k.ID][0].Name)
	})

	t.Run("derived_source_retirement", func(t *testing.T) {
		store, db, ctx, tags, knowledge, k := displayTestFixture(t, pg)
		initializeDisplayOriginalBody(t, db, pg)
		sourceID := "candidate"
		if pg {
			sourceID = "doc-a"
		}
		_, source, err := knowledge.(*knowledgeRepository).ResolveNextcloudSourceLineage(ctx, 7, "kb", sourceID)
		require.NoError(t, err)
		lineage, err := types.NewCompleteSourceLineage(source)
		require.NoError(t, err)
		derived, err := WithDerivedKnowledgeInput(ctx, lineage)
		require.NoError(t, err)
		input := &types.Knowledge{ID: "body-display-derived", TenantID: 7, KnowledgeBaseID: "kb", Type: types.KnowledgeTypeManual, Channel: "web", CustomMetadata: types.JSON(`{}`), ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled"}
		proof, err := store.CreateKnowledgeWithInput(derived, input, "actual source-derived display input")
		require.NoError(t, err)
		producer, err := WithDerivedKnowledgeDisplayInput(ctx, proof)
		require.NoError(t, err)
		tag := &types.KnowledgeTag{ID: "body-derived-tag", TenantID: 7, KnowledgeBaseID: "kb", Name: "actual source-derived tag", Color: "blue", SortOrder: 2}
		require.NoError(t, tags.Create(producer, tag))
		require.NoError(t, knowledge.SetKnowledgeTags(WithIndependentKnowledgeTagAssignmentInput(ctx, map[string][]string{k.ID: {tag.ID}}), k.ID, []string{tag.ID}))
		_, err = captureDisplayTest(t, db, k)
		require.NoError(t, err)
		require.NoError(t, NewNextcloudContentLeaseStore(db).RetireKnowledge(context.Background(), NextcloudContentScope{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: source.KnowledgeID, DataSourceID: source.DataSourceID, ExternalID: source.ExternalID}))
		_, err = captureDisplayTest(t, db, k)
		require.Error(t, err, "hydrating a vault input must preserve current source denial")
	})
}

func TestKnowledgeDisplayOriginalBodySQLite(t *testing.T) {
	runKnowledgeDisplayOriginalBody(t, false)
}

func TestPostgresKnowledgeDisplayOriginalBody(t *testing.T) {
	runKnowledgeDisplayOriginalBody(t, true)
}
