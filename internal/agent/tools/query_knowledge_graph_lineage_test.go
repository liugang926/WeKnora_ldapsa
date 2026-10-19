package tools

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type graphLineageKnowledge struct {
	interfaces.KnowledgeService
	doc *types.Knowledge
}

func (k *graphLineageKnowledge) GetKnowledgeByIDOnly(context.Context, string) (*types.Knowledge, error) {
	return k.doc, nil
}

type graphLineageSnapshotReader struct {
	snapshot *types.KnowledgeGraphInputSnapshot
	read     func()
	calls    int
}

func (r *graphLineageSnapshotReader) ReadKnowledgeGraphSnapshot(
	context.Context,
	types.SearchTargets,
	types.NameSpace,
	[]string,
) (*types.KnowledgeGraphInputSnapshot, error) {
	r.calls++
	if r.read != nil {
		r.read()
	}
	return r.snapshot, nil
}

func graphLineageToolFixture(
	t *testing.T,
	authorize func(context.Context, *types.SourceLineage) error,
) (*ToolRegistry, context.Context, *graphLineageSnapshotReader) {
	t.Helper()
	source := func(file, etag string) types.SourceIdentity {
		return types.SourceIdentity{
			Provider:        types.ConnectorTypeNextcloud,
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     "doc",
			DataSourceID:    "ds",
			PairOperationID: "pair",
			InstanceID:      "instance",
			BindingID:       "binding",
			FileID:          file,
			ExternalID:      "nextcloud:instance:" + file,
			Revision:        1,
			ETag:            etag,
		}
	}
	lineage, err := types.NewCompleteSourceLineage(source("77", "body-original"), source("78", "config-original"))
	require.NoError(t, err)
	namespace := types.NameSpace{KnowledgeBase: "kb", Knowledge: "doc"}
	proof := &types.KnowledgeBuildInputProof{
		Version:         1,
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     "doc",
		AdmissionID:     "trusted-original-input",
		BuildGeneration: 1,
		InputDigest:     types.GraphDigest("actual input"),
		ConfigDigest:    types.GraphDigest("actual config"),
		BodyDigest:      types.GraphDigest("actual material"),
		ChunkIDs:        []string{"chunk"},
		SourceLineage:   lineage,
	}
	base := &types.GraphSourceReceipt{
		OriginalInput:   proof,
		Version:         1,
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     "doc",
		BuildGeneration: 1,
		InputDigest:     proof.InputDigest,
		ConfigDigest:    proof.ConfigDigest,
		WireDigest:      types.GraphDigest("actual wire"),
		SourceLineage:   lineage,
	}
	node := &types.GraphNode{
		Name:       "actual-entity",
		Chunks:     []string{"chunk"},
		Attributes: []string{"actual graph attributes"},
	}
	node.SourceReceipt, err = types.BindGraphSourceReceipt(base, types.GraphNodeBodyDigest(namespace, node))
	require.NoError(t, err)
	edge := &types.GraphRelation{Node1: "actual-entity", Node2: "other-entity", Type: "ACTUAL_REL"}
	edge.SourceReceipt, err = types.BindGraphSourceReceipt(base, types.GraphRelationBodyDigest(namespace, edge))
	require.NoError(t, err)
	graph := &types.GraphData{
		Node:          []*types.GraphNode{node},
		Relation:      []*types.GraphRelation{edge},
		SourceLineage: lineage,
	}
	snapshot := &types.KnowledgeGraphInputSnapshot{
		Graph:         graph,
		SourceLineage: lineage,
		Chunks: []*types.SearchResult{
			{
				ID:               "chunk",
				KnowledgeID:      "doc",
				KnowledgeBaseID:  "kb",
				KnowledgeTitle:   "actual title",
				Content:          "actual hydrated endpoint body",
				KnowledgeChannel: "manual",
				ChunkType:        types.ChunkTypeText,
				SourceLineage:    lineage,
			},
		},
		Close:   func() error { return nil },
		Recheck: func(context.Context) error { return nil },
	}
	reader := &graphLineageSnapshotReader{snapshot: snapshot}
	knowledge := &graphLineageKnowledge{
		doc: &types.Knowledge{ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", Title: "actual title", Channel: "manual"},
	}
	targets := types.SearchTargets{
		&types.SearchTarget{Type: types.SearchTargetTypeKnowledgeBase, TenantID: 7, KnowledgeBaseID: "kb"},
	}
	tool := NewQueryKnowledgeGraphTool(nil, targets).WithKnowledgeScope(knowledge)
	tool.SetKnowledgeGraphSnapshotReader(reader)
	registry := NewToolRegistry()
	registry.RegisterTool(tool)
	ctx := types.WithExecutionTenant(context.Background(), 7)
	ctx = types.WithRAGSourceLineage(ctx, types.NewRAGSourceLineage(authorize))
	return registry, ctx, reader
}

func TestKnowledgeGraphTypedToolReturnsActualGraphAndFullOriginalUnion(t *testing.T) {
	registry, ctx, reader := graphLineageToolFixture(
		t,
		func(context.Context, *types.SourceLineage) error { return nil },
	)
	result, err := registry.ExecuteToolWithSourceLineage(
		ctx,
		ToolQueryKnowledgeGraph,
		json.RawMessage(`{"knowledge_base_ids":["kb"],"query":"actual"}`),
	)
	require.NoError(t, err)
	require.Equal(t, 1, reader.calls)
	require.Contains(t, result.Output, "actual graph attributes")
	require.Contains(t, result.Output, "ACTUAL_REL")
	require.Contains(t, result.Output, "actual hydrated endpoint body")
	require.Len(t, result.SourceLineage.Sources, 2)
	require.Equal(t, result.SourceLineage, types.RAGSourceLineageFromContext(ctx).Snapshot())
	require.NotContains(t, result.Output, "ExtractConfig")
	require.Equal(t, "graph_query_results", result.Data["display_type"])
	rows := result.Data["results"].([]map[string]any)
	require.Equal(t, "chunk", rows[0]["chunk_id"])
}

func TestKnowledgeGraphTypedToolLateWithdrawalAndEmptyPreserveSource(t *testing.T) {
	denied := errors.New("config source withdrawn")
	revoked := false
	registry, ctx, reader := graphLineageToolFixture(t, func(_ context.Context, l *types.SourceLineage) error {
		if revoked && len(l.Sources) > 0 {
			return denied
		}
		return nil
	})
	first, err := registry.ExecuteToolWithSourceLineage(
		ctx,
		ToolQueryKnowledgeGraph,
		json.RawMessage(`{"knowledge_base_ids":["kb"],"query":"actual"}`),
	)
	require.NoError(t, err)
	empty, _ := types.NewCompleteSourceLineage()
	reader.snapshot = &types.KnowledgeGraphInputSnapshot{
		Graph:         &types.GraphData{SourceLineage: empty},
		SourceLineage: empty,
		Close:         func() error { return nil },
		Recheck:       func(context.Context) error { return nil },
	}
	second, err := registry.ExecuteToolWithSourceLineage(
		ctx,
		ToolQueryKnowledgeGraph,
		json.RawMessage(`{"knowledge_base_ids":["kb"],"query":"absent"}`),
	)
	require.NoError(t, err)
	require.Empty(t, second.SourceLineage.Sources)
	require.Equal(t, first.SourceLineage, types.RAGSourceLineageFromContext(ctx).Snapshot())
	reader.snapshot.Recheck = func(context.Context) error { revoked = true; return nil }
	result, err := registry.ExecuteToolWithSourceLineage(
		ctx,
		ToolQueryKnowledgeGraph,
		json.RawMessage(`{"knowledge_base_ids":["kb"],"query":"absent"}`),
	)
	require.Error(t, err)
	require.Nil(t, result)
}

func TestKnowledgeGraphTypedToolUnknownOriginalBodyRefusesWholeResult(t *testing.T) {
	for _, mode := range []string{"legacy", "changed attributes", "generation mismatch", "lease output withdraw"} {
		t.Run(mode, func(t *testing.T) {
			registry, ctx, reader := graphLineageToolFixture(
				t,
				func(context.Context, *types.SourceLineage) error { return nil },
			)
			closed := 0
			reader.snapshot.Close = func() error { closed++; return nil }
			switch mode {
			case "legacy":
				reader.snapshot.Graph.Node[0].SourceReceipt = nil
			case "changed attributes":
				reader.snapshot.Graph.Node[0].Attributes[0] = "poisoned newer body"
			case "generation mismatch":
				reader.snapshot.Graph.Node[0].SourceReceipt.OriginalInput.BuildGeneration++
			case "lease output withdraw":
				calls := 0
				reader.snapshot.Recheck = func(context.Context) error {
					calls++
					if calls >= 2 {
						return types.ErrSourceLineageUnknown
					}
					return nil
				}
			}
			result, err := registry.ExecuteToolWithSourceLineage(
				ctx,
				ToolQueryKnowledgeGraph,
				json.RawMessage(`{"knowledge_base_ids":["kb"],"query":"actual"}`),
			)
			require.Error(t, err)
			require.Nil(t, result)
			require.Equal(t, 1, closed, "the original snapshot read pins close exactly once on every refusal")
			require.Error(t, types.RAGSourceLineageFromContext(ctx).Snapshot().RequireComplete())
		})
	}
}

func TestKnowledgeGraphTypedToolDetachesActualGraphBeforeAuthorization(t *testing.T) {
	registry, ctx, reader := graphLineageToolFixture(
		t,
		func(context.Context, *types.SourceLineage) error { return nil },
	)
	reader.snapshot.Recheck = func(context.Context) error {
		reader.snapshot.Graph.Node[0].Name = "changed after authentication"
		reader.snapshot.Graph.Node[0].Attributes[0] = "unaccounted late replacement"
		reader.snapshot.Chunks[0].Content = "unaccounted late body"
		return nil
	}
	result, err := registry.ExecuteToolWithSourceLineage(
		ctx,
		ToolQueryKnowledgeGraph,
		json.RawMessage(`{"knowledge_base_ids":["kb"],"query":"actual"}`),
	)
	require.NoError(t, err)
	require.Contains(t, result.Output, "actual graph attributes")
	require.Contains(t, result.Output, "actual hydrated endpoint body")
	require.NotContains(t, result.Output, "unaccounted late")
	require.NotContains(t, result.Output, "changed after authentication")
}
