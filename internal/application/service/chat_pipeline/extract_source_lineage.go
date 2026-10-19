package chatpipeline

import (
	"context"
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
)

// ExtractSourceLineage detaches the exact rendered messages and options before
// current authorization. Both successful and citation-free model responses
// inherit the full body/config/control input union. No model-supplied receipt
// or claim of independence is accepted.
func (e *Extractor) ExtractSourceLineage(ctx context.Context, content string, contentLineage, templateLineage *types.SourceLineage, check func(context.Context, *types.SourceLineage) error) (*types.GraphData, error) {
	lineage, err := types.UnionSourceLineage(contentLineage, templateLineage)
	if err != nil || lineage.RequireComplete() != nil || check == nil {
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	generator := NewQAPromptGenerator(e.formater, e.template)
	messages := generator.Render(ctx, content)
	for i := range messages {
		messages[i].SourceLineage, err = types.UnionSourceLineage(lineage)
		if err != nil {
			return nil, err
		}
	}
	// Rendered extraction messages contain only controlled strings. JSON copies
	// the options' nested buffers/pointers before an authorization callback runs.
	encoded, err := json.Marshal(e.chatOpt)
	if err != nil {
		return nil, err
	}
	var options chat.ChatOptions
	if err := json.Unmarshal(encoded, &options); err != nil {
		return nil, err
	}
	wireDigest := types.GraphDigest(struct {
		Messages []chat.Message
		Options  chat.ChatOptions
		ModelID  string
	}{messages, options, e.chat.GetModelID()})
	if wireDigest == "" {
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	detached, err := types.UnionSourceLineage(lineage)
	if err != nil {
		return nil, err
	}
	if err := check(ctx, detached); err != nil {
		return nil, err
	}
	// The callback receives a copy; it cannot reduce the real inherited union.
	response, err := e.chat.Chat(types.WithLLMCallMetadata(ctx, "entity_extraction", ""), messages, &options)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	detached, err = types.UnionSourceLineage(lineage)
	if err != nil {
		return nil, err
	}
	if err := check(ctx, detached); err != nil {
		return nil, err
	}
	graph, err := e.formater.ParseGraph(ctx, response.Content)
	if err != nil {
		return nil, err
	}
	graph.SourceLineage, err = types.UnionSourceLineage(lineage)
	if err != nil {
		return nil, err
	}
	graph.WireDigest = wireDigest
	// Parsing/control transforms and their diagnostics remain source-derived.
	detached, err = types.UnionSourceLineage(lineage)
	if err != nil {
		return nil, err
	}
	if err := check(ctx, detached); err != nil {
		return nil, err
	}
	return graph, nil
}
