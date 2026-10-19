package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Tencent/WeKnora/internal/application/readlease"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// SetKnowledgeGraphSnapshotReader configures the reader used to hydrate original graph snapshots.
func (t *QueryKnowledgeGraphTool) SetKnowledgeGraphSnapshotReader(reader interfaces.KnowledgeGraphSnapshotReader) {
	t.graphReader = reader
}

// SetKnowledgeGraphSnapshotReader configures the reader used to hydrate original graph snapshots.
func (r *ToolRegistry) SetKnowledgeGraphSnapshotReader(reader interfaces.KnowledgeGraphSnapshotReader) {
	for _, tool := range r.tools {
		if adapter, ok := tool.(interface {
			SetKnowledgeGraphSnapshotReader(interfaces.KnowledgeGraphSnapshotReader)
		}); ok {
			adapter.SetKnowledgeGraphSnapshotReader(reader)
		}
	}
}

// ExecuteSourceLineage executes the tool with retained original input receipts.
// This adapter returns actual graph nodes and edges. ExtractConfig examples
// are model inputs of the writer; they are never displayed as retrieved graph.
func (t *QueryKnowledgeGraphTool) ExecuteSourceLineage(
	ctx context.Context,
	args json.RawMessage,
) (out *types.ToolResult, err error) {
	turn := types.RAGSourceLineageFromContext(ctx)
	defer func() {
		if err != nil && turn != nil {
			_ = turn.Add(types.UnknownSourceLineage())
		}
	}()
	if turn == nil || t.graphReader == nil || !t.scopeEnforced || t.scopeKnowledgeService == nil {
		return nil, ErrToolSourceLineageUnavailable
	}
	var input QueryKnowledgeGraphInput
	if err := json.Unmarshal(args, &input); err != nil {
		return nil, ErrToolSourceLineageUnavailable
	}
	if len(input.KnowledgeBaseIDs) == 0 || len(input.KnowledgeBaseIDs) > 10 || strings.TrimSpace(input.Query) == "" {
		return nil, ErrToolSourceLineageUnavailable
	}
	if err := validateKnowledgeBaseIDsInSearchTargets(t.searchTargets, input.KnowledgeBaseIDs); err != nil {
		return nil, err
	}
	ctx, guard, err := beginAgentRead(ctx, t.scopeKnowledgeService, t.searchTargets)
	if err != nil {
		return nil, err
	}
	if guard != nil {
		defer func() {
			// Only read pins are released here; cleanup does not publish protected material.
			_ = guard.Close()
		}()
	}
	if err := turn.Check(ctx); err != nil {
		return nil, err
	}
	snapshots := []*types.KnowledgeGraphInputSnapshot{}
	lineage := independentToolInput()
	nodes := map[string][]string{}
	edges := map[string]*types.GraphRelation{}
	results := map[string]*types.SearchResult{}
	for _, id := range input.KnowledgeBaseIDs {
		snapshot, err := t.graphReader.ReadKnowledgeGraphSnapshot(
			ctx,
			t.searchTargets,
			types.NameSpace{KnowledgeBase: id},
			[]string{input.Query},
		)
		if err != nil {
			return nil, err
		}
		if snapshot == nil || snapshot.Graph == nil || snapshot.SourceLineage.RequireComplete() != nil ||
			snapshot.Close == nil ||
			snapshot.Recheck == nil {
			return nil, ErrToolSourceLineageUnavailable
		}
		defer func() {
			// Only read pins are released here; cleanup does not publish protected material.
			_ = snapshot.Close()
		}()
		snapshot, err = types.CloneKnowledgeGraphInputSnapshot(snapshot)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
		if err := snapshot.Recheck(ctx); err != nil {
			return nil, err
		}
		graphLineage, err := types.ValidateGraphMaterial(snapshot.Graph)
		if err != nil {
			return nil, err
		}
		lineage, err = types.UnionSourceLineage(lineage, snapshot.SourceLineage, graphLineage)
		if err != nil {
			return nil, err
		}
		if err := turn.Add(lineage); err != nil {
			return nil, err
		}
		if err := turn.Check(ctx); err != nil {
			return nil, err
		}
		// The raw contributor proofs are checked before display-name collapse.
		for _, node := range snapshot.Graph.Node {
			allowed, err := searchTargetsAllowKnowledgeID(
				ctx,
				t.searchTargets,
				node.SourceReceipt.KnowledgeID,
				node.SourceReceipt.KnowledgeBaseID,
				t.scopeKnowledgeService,
			)
			if err != nil || !allowed {
				return nil, ErrToolSourceLineageUnavailable
			}
			nodes[node.Name] = append(nodes[node.Name], node.Attributes...)
		}
		for _, edge := range snapshot.Graph.Relation {
			key := types.GraphDigest([]string{edge.Node1, edge.Node2, edge.Type})
			edges[key] = edge
		}
		for _, result := range snapshot.Chunks {
			if result == nil || result.SourceLineage.RequireComplete() != nil {
				return nil, ErrToolSourceLineageUnavailable
			}
			lineage, err = types.UnionSourceLineage(lineage, result.SourceLineage)
			if err != nil {
				return nil, err
			}
			if err := turn.Add(result.SourceLineage); err != nil {
				return nil, err
			}
			results[result.ID] = result
		}
	}
	names := []string{}
	for name := range nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	output := strings.Builder{}
	output.WriteString("=== Knowledge Graph Query ===\n\n")
	graphNodes := []map[string]any{}
	for _, name := range names {
		attributes := uniqueStrings(nodes[name])
		_, _ = fmt.Fprintf(&output, "Entity: %s\n", name)
		for _, attribute := range attributes {
			output.WriteString("  " + attribute + "\n")
		}
		graphNodes = append(graphNodes, map[string]any{"name": name, "attributes": attributes})
	}
	edgeKeys := []string{}
	for key := range edges {
		edgeKeys = append(edgeKeys, key)
	}
	sort.Strings(edgeKeys)
	graphEdges := []map[string]any{}
	for _, key := range edgeKeys {
		edge := edges[key]
		_, _ = fmt.Fprintf(&output, "%s --[%s]--> %s\n", edge.Node1, edge.Type, edge.Node2)
		graphEdges = append(graphEdges, map[string]any{"source": edge.Node1, "target": edge.Node2, "type": edge.Type})
	}
	ids := []string{}
	for id := range results {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	chunks := []*types.SearchResult{}
	for _, id := range ids {
		chunks = append(chunks, results[id])
		_, _ = fmt.Fprintf(&output, "\nSource: %s\n%s\n", results[id].KnowledgeTitle, results[id].Content)
	}
	if len(names) == 0 && len(edgeKeys) == 0 {
		output.WriteString("No relevant graph information found.\n")
	}
	if err := t.finishLineageGraphRead(ctx, t.searchTargets, guard, chunks); err != nil {
		return nil, err
	}
	if err := turn.Add(lineage); err != nil {
		return nil, err
	}
	if err := turn.Check(ctx); err != nil {
		return nil, err
	}
	for _, snapshot := range snapshots {
		if err := snapshot.Recheck(ctx); err != nil {
			return nil, err
		}
	}
	formatted := []map[string]any{}
	for i, result := range chunks {
		formatted = append(
			formatted,
			map[string]any{
				"result_index":      i + 1,
				"chunk_id":          result.ID,
				"chunk_index":       result.ChunkIndex,
				"chunk_type":        result.ChunkType,
				"knowledge_id":      result.KnowledgeID,
				"knowledge_base_id": result.KnowledgeBaseID,
				"knowledge_title":   result.KnowledgeTitle,
				"content":           result.Content,
				"match_type":        types.MatchTypeGraph,
				"score":             result.Score,
			},
		)
	}
	graphData := map[string]any{"nodes": graphNodes, "edges": graphEdges}
	return &types.ToolResult{
		Success: true,
		Output:  output.String(),
		Data: map[string]any{
			"knowledge_base_ids": input.KnowledgeBaseIDs,
			"query":              input.Query,
			"graph":              graphData,
			"graph_data":         graphData,
			"results":            formatted,
			"count":              len(formatted),
			"display_type":       "graph_query_results",
		},
		SourceLineage: lineage,
	}, nil
}

func (t *QueryKnowledgeGraphTool) finishLineageGraphRead(
	ctx context.Context,
	targets types.SearchTargets,
	guard *readlease.NextcloudReadGuard,
	results []*types.SearchResult,
) error {
	documents := []*types.Knowledge{}
	seen := map[string]bool{}
	for _, result := range results {
		if result == nil || result.KnowledgeID == "" || result.SourceLineage.RequireComplete() != nil {
			return ErrToolSourceLineageUnavailable
		}
		if seen[result.KnowledgeID] {
			continue
		}
		seen[result.KnowledgeID] = true
		k, err := t.scopeKnowledgeService.GetKnowledgeByIDOnly(ctx, result.KnowledgeID)
		if err != nil || k == nil || k.ID != result.KnowledgeID ||
			k.TenantID != targets.GetTenantIDForKB(result.KnowledgeBaseID) ||
			k.KnowledgeBaseID != result.KnowledgeBaseID ||
			k.Title != result.KnowledgeTitle ||
			k.FileName != result.KnowledgeFilename ||
			k.Source != result.KnowledgeSource ||
			k.Channel != result.KnowledgeChannel {
			return ErrToolSourceLineageUnavailable
		}
		allowed, err := searchTargetsAllowKnowledgeID(ctx, targets, k.ID, k.KnowledgeBaseID, t.scopeKnowledgeService)
		if err != nil || !allowed {
			return ErrToolSourceLineageUnavailable
		}
		documents = append(documents, k)
	}
	return finishAgentRead(ctx, t.scopeKnowledgeService, targets, guard, documents)
}
