package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestGalleryVisibilityPrecedesCopySelectionAndTotals(t *testing.T) {
	t.Setenv("WEKNORA_NEXTCLOUD_READ_AUTHORITY", "")
	ctx := context.Background()
	store, db := knowledgeInputFixture(t)
	applyChunkImagesMigration(t, db)
	chunks := NewChunkRepository(db)
	reader := NewKnowledgeRepository(db).(*knowledgeRepository)
	_, source, err := reader.ResolveNextcloudSourceLineage(ctx, 7, "kb", "candidate")
	require.NoError(t, err)
	lineage, err := types.NewCompleteSourceLineage(source)
	require.NoError(t, err)
	derived, err := WithDerivedKnowledgeInput(ctx, lineage)
	require.NoError(t, err)
	add := func(origin context.Context, id, title, images string, minute int) {
		t.Helper()
		k := &types.Knowledge{
			ID:              id,
			TenantID:        7,
			KnowledgeBaseID: "kb",
			Type:            "passage",
			Channel:         "web",
			Title:           title,
			CustomMetadata:  types.JSON("{}"),
			ParseStatus:     types.ParseStatusCompleted,
			EnableStatus:    "enabled",
		}
		proof, err := store.CreateKnowledgeWithInput(origin, k, []string{title})
		require.NoError(t, err)
		at := time.Now().UTC().Add(time.Duration(minute) * time.Minute)
		c := &types.Chunk{
			ID:              "chunk-" + id,
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     id,
			Content:         title,
			IsEnabled:       true,
			IndexStatus:     "ready",
			ChunkType:       types.ChunkTypeImageOCR,
			ImageInfo:       images,
			CreatedAt:       at,
			UpdatedAt:       at,
		}
		require.NoError(t, chunks.CreateChunks(WithKnowledgeBuildInputProof(origin, store, proof), []*types.Chunk{c}))
	}
	add(WithIndependentKnowledgeInput(ctx), "ordinary", "public", `[{"url":"shared","ocr_text":"allowed"}]`, 0)
	add(
		derived, "derived", "sensitive",
		`[{"url":"shared","ocr_text":"hidden"},{"url":"secret-only","ocr_text":"sensitive"}]`, 1,
	)
	allowed := true
	query := &types.ImageAssetQuery{AuthorizeSource: func(_ context.Context, k *types.Knowledge) (bool, error) {
		require.Equal(t, source.KnowledgeID, k.ID)
		return allowed, nil
	}}
	rows, total, err := chunks.ListImageAssets(ctx, 7, "kb", query)
	require.NoError(t, err)
	require.EqualValues(t, 2, total)
	require.Len(t, rows, 2)
	require.Equal(t, "sensitive", rows[0].SourceName)
	crossRows, crossTotal, err := chunks.ListImageAssets(ctx, 8, "kb", query)
	require.NoError(t, err)
	require.Empty(t, crossRows)
	require.Zero(t, crossTotal)
	allowed = false
	rows, total, err = chunks.ListImageAssets(ctx, 7, "kb", query)
	require.NoError(t, err)
	require.EqualValues(t, 1, total)
	require.Len(t, rows, 1)
	require.Equal(t, "public", rows[0].SourceName)
	require.Equal(t, "allowed", rows[0].OCRText)
	query.Offset = 5
	rows, total, err = chunks.ListImageAssets(ctx, 7, "kb", query)
	require.NoError(t, err)
	require.EqualValues(t, 1, total)
	require.Empty(t, rows)
	query.Offset = 0
	query.KnowledgeIDs = []string{"derived"}
	rows, total, err = chunks.ListImageAssets(ctx, 7, "kb", query)
	require.NoError(t, err)
	require.Zero(t, total)
	require.Empty(t, rows)
	allowed = true
	require.NoError(t, NewNextcloudContentLeaseStore(db).RetireKnowledge(ctx, NextcloudContentScope{
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     source.KnowledgeID,
		DataSourceID:    source.DataSourceID,
		ExternalID:      source.ExternalID,
	}))
	rows, total, err = chunks.ListImageAssets(ctx, 7, "kb", query)
	require.NoError(t, err)
	require.Zero(t, total)
	require.Empty(t, rows)
}
