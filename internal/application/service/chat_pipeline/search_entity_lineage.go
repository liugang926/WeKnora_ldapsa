package chatpipeline

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

func (p *PluginSearchEntity) onLineageEntitySearch(
	ctx context.Context,
	manage *types.ChatManage,
	next func() *PluginError,
) *PluginError {
	if len(manage.Entity) == 0 {
		return next()
	}
	turn := types.RAGSourceLineageFromContext(ctx)
	fail := func(err error) *PluginError {
		if turn != nil {
			_ = turn.Add(types.UnknownSourceLineage())
		}
		return ErrSearch.WithError(err)
	}
	if p.graphReader == nil || turn == nil {
		return fail(types.ErrGraphSourceReceiptUnknown)
	}
	if err := turn.Check(ctx); err != nil {
		return fail(err)
	}
	empty, _ := types.NewCompleteSourceLineage()
	graph := &types.GraphData{SourceLineage: empty}
	namespaces := []types.NameSpace{}
	if len(manage.EntityKnowledge) > 0 {
		for id, kb := range manage.EntityKnowledge {
			namespaces = append(namespaces, types.NameSpace{KnowledgeBase: kb, Knowledge: id})
		}
	} else {
		for _, kb := range manage.EntityKBIDs {
			namespaces = append(namespaces, types.NameSpace{KnowledgeBase: kb})
		}
	}
	// SearchTargets are server resolved at entry and include effective source
	// tenant. Do not substitute the original caller's tenant for shared sources.
	targets := manage.SearchTargets
	for _, namespace := range namespaces {
		snapshot, err := p.graphReader.ReadKnowledgeGraphSnapshot(
			ctx,
			targets,
			namespace,
			append([]string(nil), manage.Entity...),
		)
		if err != nil {
			return fail(err)
		}
		if snapshot == nil || snapshot.Graph == nil || snapshot.SourceLineage.RequireComplete() != nil ||
			snapshot.Close == nil ||
			snapshot.Recheck == nil {
			return fail(types.ErrGraphSourceReceiptUnknown)
		}
		defer snapshot.Close()
		snapshot, err = types.CloneKnowledgeGraphInputSnapshot(snapshot)
		if err != nil {
			return fail(err)
		}
		if err := snapshot.Recheck(ctx); err != nil {
			return fail(err)
		}
		union, err := types.ValidateGraphMaterial(snapshot.Graph)
		if err != nil {
			return fail(err)
		}
		graph.SourceLineage, err = types.UnionSourceLineage(graph.SourceLineage, snapshot.SourceLineage, union)
		if err != nil {
			return fail(err)
		}
		if err := turn.Add(graph.SourceLineage); err != nil {
			return fail(err)
		}
		if err := turn.Check(ctx); err != nil {
			return fail(err)
		}
		graph.Node = append(graph.Node, snapshot.Graph.Node...)
		graph.Relation = append(graph.Relation, snapshot.Graph.Relation...)
		graph.InputReceipts = append(graph.InputReceipts, snapshot.Graph.InputReceipts...)
		for _, result := range snapshot.Chunks {
			if result == nil || result.SourceLineage.RequireComplete() != nil {
				return fail(types.ErrGraphSourceReceiptUnknown)
			}
			graph.SourceLineage, err = types.UnionSourceLineage(graph.SourceLineage, result.SourceLineage)
			if err != nil {
				return fail(err)
			}
			if err := turn.Add(result.SourceLineage); err != nil {
				return fail(err)
			}
		}
		manage.SearchResult = append(manage.SearchResult, snapshot.Chunks...)
		if err := snapshot.Recheck(ctx); err != nil {
			return fail(err)
		}
	}
	if err := turn.Check(ctx); err != nil {
		return fail(err)
	}
	manage.GraphResult = graph
	manage.SearchResult = removeDuplicateResults(manage.SearchResult)
	return next()
}
