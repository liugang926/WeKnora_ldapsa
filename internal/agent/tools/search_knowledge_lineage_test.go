package tools

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/models/rerank"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func actualAgentSearchFixture(
	t *testing.T,
	model rerank.Reranker,
) (*SearchKnowledgeTool, *stubKnowledgeBaseService, *leasedAgentKnowledgeService) {
	t.Helper()
	read, svc, db := actualAgentDocumentFixture(t)
	kbs := &stubKnowledgeBaseService{
		results: []*types.SearchResult{
			{
				ID:                      "c1",
				KnowledgeID:             "doc",
				KnowledgeBaseID:         "kb",
				Content:                 "stale index poison",
				KnowledgeTitle:          "stale index title",
				KnowledgeCustomMetadata: "unaccounted stale copied source header",
				Score:                   0.99,
			},
		},
	}
	search := NewSearchKnowledgeTool(kbs, svc, read.chunkService, read.searchTargets, model, nil)
	search.SetRAGChunkSnapshotReader(repository.NewRAGChunkSnapshotReader(db, nil))
	return search, kbs, svc
}

func TestAgentLineageSearchActualTextTitleReachRerankAndOutput(t *testing.T) {
	ranker := &stubReranker{scores: []float64{0.9}}
	tool, _, _ := actualAgentSearchFixture(t, ranker)
	turn, ctx := toolTurn()
	r := NewToolRegistry()
	r.RegisterTool(tool)
	result, err := r.ExecuteToolWithSourceLineage(
		ctx,
		ToolSearchKnowledge,
		json.RawMessage(`{"query":"database","mode":"keyword"}`),
	)
	require.NoError(t, err)
	require.True(t, result.Success)
	require.Equal(t, 1, ranker.calls)
	require.Equal(t, []string{"authoritative title\n\nauthoritative database body"}, ranker.documents)
	require.Contains(t, result.Output, "authoritative database body")
	require.Contains(t, result.Output, "authoritative title")
	require.NotContains(t, result.Output, "stale index")
	require.NotContains(t, result.Output, "unaccounted stale copied source header")
	require.Len(t, result.SourceLineage.Sources, 1)
	require.Equal(t, "original", result.SourceLineage.Sources[0].ETag)
	require.Equal(t, result.SourceLineage, turn.Snapshot())
}

func TestAgentLineageSearchACLNormalizesMalformedNextcloudSnapshot(t *testing.T) {
	tool, _, _ := actualAgentSearchFixture(t, &stubReranker{scores: []float64{0.9}})
	tool.SetRAGChunkSnapshotReader(&malformedLineageSnapshotReader{inner: tool.snapshotReader})
	turn := types.NewWeKnoraACLSourceLineage(func(context.Context, *types.SourceLineage) error {
		return nil
	})
	ctx := types.WithRAGSourceLineage(context.Background(), turn)
	r := NewToolRegistry()
	r.RegisterTool(tool)
	result, err := r.ExecuteToolWithSourceLineage(
		ctx,
		ToolSearchKnowledge,
		json.RawMessage(`{"query":"database","mode":"keyword"}`),
	)
	require.NoError(t, err)
	require.True(t, result.Success)
	require.Contains(t, result.Output, "authoritative database body")
	require.Empty(t, result.SourceLineage.Sources)
	require.NoError(t, turn.Snapshot().RequireComplete())
}

func TestAgentLineageSearchMaterialKeepsMalformedNextcloudSnapshotClosed(t *testing.T) {
	tool, _, _ := actualAgentSearchFixture(t, &stubReranker{scores: []float64{0.9}})
	tool.SetRAGChunkSnapshotReader(&malformedLineageSnapshotReader{inner: tool.snapshotReader})
	turn := types.NewRAGSourceLineage(func(context.Context, *types.SourceLineage) error {
		return types.ErrSourceLineageInvalid
	})
	ctx := types.WithRAGSourceLineage(context.Background(), turn)
	r := NewToolRegistry()
	r.RegisterTool(tool)
	result, err := r.ExecuteToolWithSourceLineage(
		ctx,
		ToolSearchKnowledge,
		json.RawMessage(`{"query":"database","mode":"keyword"}`),
	)
	require.ErrorIs(t, err, types.ErrSourceLineageInvalid)
	require.Nil(t, result)
}

type revokeSearchReranker struct {
	*stubReranker
	svc *leasedAgentKnowledgeService
}

type strictExpectedLineageReader struct {
	inner interface {
		HydrateRAGChunkSnapshots(context.Context, []types.RAGChunkReadRequest) ([]types.RAGChunkInputSnapshot, error)
	}
	expected *types.SourceLineage
}

func (r *strictExpectedLineageReader) HydrateRAGChunkSnapshots(
	ctx context.Context,
	requests []types.RAGChunkReadRequest,
) ([]types.RAGChunkInputSnapshot, error) {
	r.expected = nil
	for _, request := range requests {
		if request.ExpectedLineage == nil {
			continue
		}
		r.expected = request.ExpectedLineage
		if err := request.ExpectedLineage.RequireComplete(); err != nil {
			return nil, types.ErrSourceLineageUnknown
		}
	}
	return r.inner.HydrateRAGChunkSnapshots(ctx, requests)
}

func TestAgentLineageSearchACLDoesNotPassMalformedExpectedLineageToSnapshotReader(t *testing.T) {
	tool, kbs, _ := actualAgentSearchFixture(t, &stubReranker{scores: []float64{0.9}})
	reader := &strictExpectedLineageReader{inner: tool.snapshotReader}
	tool.SetRAGChunkSnapshotReader(reader)
	kbs.results[0].SourceLineage = malformedCompleteNextcloudLineage()
	turn := types.NewWeKnoraACLSourceLineage(func(context.Context, *types.SourceLineage) error {
		return nil
	})
	ctx := types.WithRAGSourceLineage(context.Background(), turn)
	r := NewToolRegistry()
	r.RegisterTool(tool)
	result, err := r.ExecuteToolWithSourceLineage(
		ctx,
		ToolSearchKnowledge,
		json.RawMessage(`{"query":"database","mode":"keyword"}`),
	)
	require.NoError(t, err)
	require.True(t, result.Success)
	require.Contains(t, result.Output, "authoritative database body")
	require.Nil(t, reader.expected)
	require.Len(t, result.SourceLineage.Sources, 1)
	require.Equal(t, "77", result.SourceLineage.Sources[0].FileID)
}

func TestAgentLineageSearchACLRetainsValidExpectedLineage(t *testing.T) {
	tool, kbs, _ := actualAgentSearchFixture(t, &stubReranker{scores: []float64{0.9}})
	reader := &strictExpectedLineageReader{inner: tool.snapshotReader}
	tool.SetRAGChunkSnapshotReader(reader)
	lineage := malformedCompleteNextcloudLineage()
	lineage.Sources[0].FileID = "77"
	lineage.Sources[0].ExternalID = "nextcloud:instance:77"
	kbs.results[0].SourceLineage = lineage
	turn := types.NewWeKnoraACLSourceLineage(func(context.Context, *types.SourceLineage) error {
		return nil
	})
	ctx := types.WithRAGSourceLineage(context.Background(), turn)
	r := NewToolRegistry()
	r.RegisterTool(tool)
	result, err := r.ExecuteToolWithSourceLineage(
		ctx,
		ToolSearchKnowledge,
		json.RawMessage(`{"query":"database","mode":"keyword"}`),
	)
	require.NoError(t, err)
	require.True(t, result.Success)
	require.NotNil(t, reader.expected)
	require.NoError(t, reader.expected.RequireComplete())
	require.Len(t, reader.expected.Sources, 1)
	require.Equal(t, "77", reader.expected.Sources[0].FileID)
}

func TestAgentLineageSearchMaterialKeepsMalformedExpectedLineageStrict(t *testing.T) {
	tool, kbs, _ := actualAgentSearchFixture(t, &stubReranker{scores: []float64{0.9}})
	reader := &strictExpectedLineageReader{inner: tool.snapshotReader}
	tool.SetRAGChunkSnapshotReader(reader)
	kbs.results[0].SourceLineage = malformedCompleteNextcloudLineage()
	turn := types.NewRAGSourceLineage(func(context.Context, *types.SourceLineage) error {
		return nil
	})
	ctx := types.WithRAGSourceLineage(context.Background(), turn)
	r := NewToolRegistry()
	r.RegisterTool(tool)
	result, err := r.ExecuteToolWithSourceLineage(
		ctx,
		ToolSearchKnowledge,
		json.RawMessage(`{"query":"database","mode":"keyword"}`),
	)
	require.Error(t, err)
	require.Nil(t, result)
	require.NotNil(t, reader.expected)
}

func (m *revokeSearchReranker) Rerank(ctx context.Context, query string, docs []string) ([]rerank.RankResult, error) {
	result, err := m.stubReranker.Rerank(ctx, query, docs)
	m.svc.revoked.Store(true)
	return result, err
}

func TestAgentLineageSearchRevocationAfterActualRerankReturnsNoSourceData(t *testing.T) {
	tool, _, svc := actualAgentSearchFixture(t, nil)
	model := &revokeSearchReranker{stubReranker: &stubReranker{scores: []float64{0.9}}, svc: svc}
	tool.rerankModel = model
	turn := types.NewRAGSourceLineage(func(_ context.Context, l *types.SourceLineage) error {
		if svc.revoked.Load() {
			return types.ErrSourceLineageUnknown
		}
		return l.RequireComplete()
	})
	ctx := types.WithRAGSourceLineage(context.Background(), turn)
	r := NewToolRegistry()
	r.RegisterTool(tool)
	result, err := r.ExecuteToolWithSourceLineage(
		ctx,
		ToolSearchKnowledge,
		json.RawMessage(`{"query":"database","mode":"keyword"}`),
	)
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, 1, model.calls)
	require.Error(t, turn.Check(ctx))
}

func TestAgentLineageSearchEmptyResultRetainsPriorSourceAndGraphCandidateIsUnknown(t *testing.T) {
	tool, kbs, _ := actualAgentSearchFixture(t, nil)
	turn, ctx := toolTurn()
	r := NewToolRegistry()
	r.RegisterTool(tool)
	first, err := r.ExecuteToolWithSourceLineage(
		ctx,
		ToolSearchKnowledge,
		json.RawMessage(`{"query":"database","mode":"keyword"}`),
	)
	require.NoError(t, err)
	kbs.results = nil
	empty, err := r.ExecuteToolWithSourceLineage(
		ctx,
		ToolSearchKnowledge,
		json.RawMessage(`{"query":"no rows","mode":"keyword"}`),
	)
	require.NoError(t, err)
	require.Empty(t, empty.SourceLineage.Sources)
	require.Equal(t, first.SourceLineage, turn.Snapshot())
	kbs.results = []*types.SearchResult{
		{
			ID:              "c1",
			KnowledgeID:     "doc",
			KnowledgeBaseID: "kb",
			Content:         "opaque graph-derived old entity",
			MatchType:       types.MatchTypeGraph,
		},
	}
	result, err := r.ExecuteToolWithSourceLineage(
		ctx,
		ToolSearchKnowledge,
		json.RawMessage(`{"query":"graph","mode":"keyword"}`),
	)
	require.Error(t, err)
	require.Nil(t, result)
	require.Error(t, turn.Check(ctx))
}
