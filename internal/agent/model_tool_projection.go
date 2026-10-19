package agent

import (
	"bytes"
	"context"

	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
)

// modelToolDefinitionLineage validates the actual wire projection before checking
// its original registered producer. MCP routing aliases are generated only by
// this turn's private codec; a same-name or merely decodable schema is no receipt.
func (e *AgentEngine) modelToolDefinitionLineage(
	ctx context.Context, actual []chat.Tool,
) *types.SourceLineage {
	if e.toolRegistry == nil {
		return types.UnknownSourceLineage()
	}
	raw := e.rawToolsForLLM()
	// Detach parameter bytes from registry-owned values before projection.
	snapshot, err := freezeModelOptions(&chat.ChatOptions{Tools: raw})
	if err != nil {
		return types.UnknownSourceLineage()
	}
	raw = snapshot.Tools
	projected := e.modelContext.EncodeTools(raw)
	indices := make(map[string]int, len(projected))
	for i, tool := range projected {
		if _, duplicate := indices[tool.Function.Name]; duplicate {
			return types.UnknownSourceLineage()
		}
		indices[tool.Function.Name] = i
	}
	selected := make([]chat.Tool, 0, len(actual))
	for _, tool := range actual {
		i, ok := indices[tool.Function.Name]
		if !ok {
			return types.UnknownSourceLineage()
		}
		expected := projected[i]
		if tool.Type != expected.Type || tool.Function.Name != expected.Function.Name ||
			tool.Function.Description != expected.Function.Description ||
			!bytes.Equal(tool.Function.Parameters, expected.Function.Parameters) {
			return types.UnknownSourceLineage()
		}
		selected = append(selected, raw[i])
	}
	// This still verifies the current service/configuration, concrete definition,
	// tool policy, ordinary-session authority and the entire controlled prefix.
	return e.toolRegistry.DefinitionSourceLineageWithContext(ctx, selected)
}
