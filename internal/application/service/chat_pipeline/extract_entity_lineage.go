package chatpipeline

import (
	"context"
	"github.com/Tencent/WeKnora/internal/types"
)

func (p *PluginExtractEntity) onLineageExtractEntity(ctx context.Context, manage *types.ChatManage, next func() *PluginError) *PluginError {
	turn := types.RAGSourceLineageFromContext(ctx)
	fail := func(err error) *PluginError {
		if turn != nil {
			_ = turn.Add(types.UnknownSourceLineage())
		}
		return ErrModelCall.WithError(err)
	}
	if turn == nil {
		return fail(types.ErrSourceLineageUnknown)
	}
	templateLineage, err := types.GraphPromptTemplateLineage(p.template)
	if err != nil {
		return fail(err)
	}
	// No raw ExtractConfig examples/names are read on the query path. The graph
	// writer receipts account its extraction config. Server-resolved target
	// selectors control this read and are reauthorized by the graph reader.
	if err := turn.Add(templateLineage); err != nil {
		return fail(err)
	}
	input, err := turn.CheckedSnapshot(ctx)
	if err != nil {
		return fail(err)
	}
	model, err := p.modelService.GetChatModel(ctx, manage.ChatModelID)
	if err != nil {
		return fail(err)
	}
	extractor := NewExtractor(model, p.template)
	graph, err := extractor.ExtractSourceLineage(ctx, manage.Query, input, templateLineage, func(ctx context.Context, lineage *types.SourceLineage) error {
		if err := turn.Add(lineage); err != nil {
			return err
		}
		return turn.Check(ctx)
	})
	if err != nil {
		return fail(err)
	}
	if err := turn.Add(graph.SourceLineage); err != nil {
		return fail(err)
	}
	if err := turn.Check(ctx); err != nil {
		return fail(err)
	}
	manage.Entity = []string{}
	for _, node := range graph.Node {
		manage.Entity = append(manage.Entity, node.Name)
	}
	manage.EntityKBIDs = manage.SearchTargets.GetAllKnowledgeBaseIDs()
	manage.EntityKnowledge = map[string]string{}
	return next()
}
