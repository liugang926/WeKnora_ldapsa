package chatpipeline

import (
	"context"
	"fmt"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type mergeSnapshotTestReader struct {
	rows    map[string]*types.Chunk
	lineage *types.SourceLineage
	calls   int
}

func (r *mergeSnapshotTestReader) HydrateRAGChunkAugmentations(_ context.Context, original *types.SearchResult, ids []string) ([]types.RAGChunkInputSnapshot, error) {
	r.calls++
	var out []types.RAGChunkInputSnapshot
	for _, id := range ids {
		chunk := r.rows[id]
		if chunk == nil {
			return nil, fmt.Errorf("missing scoped row")
		}
		out = append(out, types.RAGChunkInputSnapshot{Knowledge: &types.Knowledge{ID: original.KnowledgeID}, Chunk: chunk, SourceLineage: r.lineage})
	}
	return out, nil
}

func mergeRAGTestLineage(t *testing.T, file, document string) *types.SourceLineage {
	t.Helper()
	lineage, err := types.NewCompleteSourceLineage(types.SourceIdentity{Provider: types.ConnectorTypeNextcloud, TenantID: 7, KnowledgeBaseID: "kb", DataSourceID: "ds", PairOperationID: "pair",
		InstanceID: "instance", BindingID: "binding", FileID: file, ExternalID: "nextcloud:instance:" + file, KnowledgeID: document, Revision: 1, ETag: "etag-" + file})
	require.NoError(t, err)
	return lineage
}

func TestRAGMergeCapturedParentAndNeighborRetainOriginalReceipt(t *testing.T) {
	lineage := mergeRAGTestLineage(t, "77", "doc")
	reader := &mergeSnapshotTestReader{lineage: lineage, rows: map[string]*types.Chunk{
		"parent": {ID: "parent", KnowledgeID: "doc", KnowledgeBaseID: "kb", ChunkType: types.ChunkTypeParentText, Content: "actual captured parent"},
		"base":   {ID: "base", KnowledgeID: "doc", KnowledgeBaseID: "kb", ChunkType: types.ChunkTypeText, Content: "actual captured base", NextChunkID: "next"},
		"next":   {ID: "next", KnowledgeID: "doc", KnowledgeBaseID: "kb", ChunkType: types.ChunkTypeText, Content: "actual captured neighbor"},
	}}
	plugin := &PluginMerge{ragChunks: reader}
	result := &types.SearchResult{ID: "base", KnowledgeID: "doc", KnowledgeBaseID: "kb", ChunkType: types.ChunkTypeText, Content: "original captured child", ParentChunkID: "parent", SourceLineage: lineage}
	plugin.resolveCapturedParents(context.Background(), []*types.SearchResult{result})
	require.Contains(t, result.Content, "actual captured parent")
	require.Contains(t, result.Content, "original captured child")
	plugin.expandCapturedNeighbors(context.Background(), []*types.SearchResult{result})
	require.Contains(t, result.Content, "actual captured neighbor")
	require.Equal(t, lineage, result.SourceLineage)
	require.GreaterOrEqual(t, reader.calls, 3)
}

func TestRAGMergeLegacyHistoryCannotBeRestampedAndMissingCapabilityPoisonsTurn(t *testing.T) {
	lineage := mergeRAGTestLineage(t, "77", "doc")
	reader := &mergeSnapshotTestReader{lineage: lineage, rows: map[string]*types.Chunk{"parent": {ID: "parent", KnowledgeID: "doc", KnowledgeBaseID: "kb", ChunkType: types.ChunkTypeParentText, Content: "new secret parent"}}}
	old := &types.SearchResult{ID: "base", KnowledgeID: "doc", KnowledgeBaseID: "kb", ChunkType: types.ChunkTypeText, Content: "old serialized reference text", ParentChunkID: "parent"}
	(&PluginMerge{ragChunks: reader}).resolveCapturedParents(context.Background(), []*types.SearchResult{old})
	require.Zero(t, reader.calls, "unknown history must never fetch a current receipt to certify its old text")
	require.Equal(t, "old serialized reference text", old.Content)
	require.ErrorIs(t, old.SourceLineage.RequireComplete(), types.ErrSourceLineageUnknown)
	turn := types.NewRAGSourceLineage(func(context.Context, *types.SourceLineage) error { return nil })
	ctx := types.WithRAGSourceLineage(context.Background(), turn)
	current := &types.SearchResult{ID: "base", KnowledgeID: "doc", KnowledgeBaseID: "kb", ChunkType: types.ChunkTypeText, Content: "captured text", ParentChunkID: "parent", SourceLineage: lineage}
	(&PluginMerge{}).resolveCapturedParents(ctx, []*types.SearchResult{current})
	require.Equal(t, "captured text", current.Content)
	require.ErrorIs(t, turn.Check(ctx), types.ErrSourceLineageUnknown)
}

func TestRAGMergeUnionDoesNotDropAnEarlierDependency(t *testing.T) {
	a, b := mergeRAGTestLineage(t, "77", "doc-a"), mergeRAGTestLineage(t, "78", "doc-b")
	target, source := &types.SearchResult{ID: "a", SourceLineage: a}, &types.SearchResult{ID: "b", SourceLineage: b}
	recordMergedChild(context.Background(), "doc", target, source, "test")
	require.NoError(t, target.SourceLineage.RequireComplete())
	require.Len(t, target.SourceLineage.Sources, 2)
}
