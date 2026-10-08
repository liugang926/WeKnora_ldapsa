package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/Tencent/WeKnora/internal/event"
	"reflect"

	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
)

var ErrToolSourceLineageUnavailable = errors.New("tool source lineage adapter is unavailable")

// SourceLineageTool is a trusted, in-process adapter. It must cover every
// actual read and influencing model call, and account for output, metadata,
// errors and images. It is never implemented from MCP/client/model JSON.
type SourceLineageTool interface {
	ExecuteSourceLineage(context.Context, json.RawMessage) (*types.ToolResult, error)
}

type SourceLineageDefinitionProvider interface {
	DefinitionSourceLineageFor(types.FunctionDefinition) *types.SourceLineage
}

type lineageToolExecutor struct {
	types.Tool
	adapter SourceLineageTool
}

func (t *lineageToolExecutor) Execute(ctx context.Context, args json.RawMessage) (*types.ToolResult, error) {
	return t.adapter.ExecuteSourceLineage(ctx, args)
}

func independentToolInput() *types.SourceLineage {
	l, _ := types.NewCompleteSourceLineage()
	return l
}

// definitionLineage explicitly inventories code-owned literal tool schemas.
// Dynamic MCP definitions, provider descriptions, and unrecognized tools need
// a trusted receipt even when the model never invokes that tool.
func definitionForTool(tool interface {
	Name() string
	Description() string
	Parameters() json.RawMessage
}) types.FunctionDefinition {
	return types.FunctionDefinition{Name: tool.Name(), Description: tool.Description(), Parameters: tool.Parameters()}
}
func sameDefinition(a, b types.FunctionDefinition) bool {
	return a.Name == b.Name && a.Description == b.Description && bytes.Equal(a.Parameters, b.Parameters)
}
func definitionLineage(tool types.Tool, actual types.FunctionDefinition) *types.SourceLineage {
	// Bind every actual wire field before consulting a receipt. A same-name
	// function is not a trusted projection of the registered definition.
	if !sameDefinition(actual, definitionForTool(tool)) {
		return types.UnknownSourceLineage()
	}
	if provider, ok := tool.(SourceLineageDefinitionProvider); ok {
		return provider.DefinitionSourceLineageFor(actual)
	}
	literal, ok := trustedLiteralDefinitions[reflect.TypeOf(tool)]
	if !ok || !sameDefinition(actual, literal) {
		return types.UnknownSourceLineage()
	}
	return independentToolInput()
}

// Immutable private copies prevent a caller mutating shared RawMessage bytes
// in a wire schema from changing both the instance and its comparison basis.
var trustedLiteralDefinitions = func() map[reflect.Type]types.FunctionDefinition {
	out := map[reflect.Type]types.FunctionDefinition{}
	bind := func(instance types.Tool, literal *BaseTool) {
		definition := definitionForTool(literal)
		definition.Parameters = append(json.RawMessage(nil), definition.Parameters...)
		out[reflect.TypeOf(instance)] = definition
	}
	bind(&SequentialThinkingTool{}, &sequentialThinkingTool)
	bind(&TodoWriteTool{}, &todoWriteTool)
	bind(&SearchKnowledgeTool{}, &searchKnowledgeTool)
	bind(&ReadDocumentTool{}, &readDocumentTool)
	bind(&ListDocumentsTool{}, &listDocumentsTool)
	bind(&QueryKnowledgeGraphTool{}, &queryKnowledgeGraphTool)
	bind(&SearchConversationsTool{}, &searchConversationsTool)
	bind(&SearchMemoryTool{}, &searchMemoryTool)
	bind(&DatabaseQueryTool{}, &databaseQueryTool)
	bind(&DataAnalysisTool{}, &dataAnalysisTool)
	bind(&DataSchemaTool{}, &dataSchemaTool)
	bind(&WebFetchTool{}, &webFetchTool)
	bind(&ListSandboxFilesTool{}, &listSandboxFilesTool)
	bind(&EditSandboxFileTool{}, &editSandboxFileTool)
	bind(&WriteSkillFileTool{}, &writeSkillFileTool)
	bind(&EditSkillFileTool{}, &editSkillFileTool)
	bind(&ShellExecTool{}, &shellExecTool)
	return out
}()

func (r *ToolRegistry) DefinitionSourceLineage(definitions []chat.Tool) *types.SourceLineage {
	inputs := []*types.SourceLineage{independentToolInput()}
	for _, definition := range definitions {
		tool, err := r.GetTool(definition.Function.Name)
		if err != nil {
			inputs = append(inputs, nil)
			continue
		}
		if definition.Type != "function" {
			inputs = append(inputs, types.UnknownSourceLineage())
			continue
		}
		inputs = append(inputs, definitionLineage(tool, types.FunctionDefinition{Name: definition.Function.Name, Description: definition.Function.Description, Parameters: definition.Function.Parameters}))
	}
	l, _ := types.UnionSourceLineage(inputs...)
	return l
}

// ExecuteToolWithSourceLineage gates before executing an uncovered producer.
// An empty opaque result is not retrospectively promoted to source-free.
func (r *ToolRegistry) ExecuteToolWithSourceLineage(ctx context.Context, name string, args json.RawMessage) (*types.ToolResult, error) {
	turn := types.RAGSourceLineageFromContext(ctx)
	if turn == nil {
		return nil, ErrToolSourceLineageUnavailable
	}
	tool, err := r.GetTool(name)
	if err != nil {
		return nil, ErrToolSourceLineageUnavailable
	}
	adapter, ok := tool.(SourceLineageTool)
	if !ok {
		_ = turn.Add(types.UnknownSourceLineage())
		return nil, ErrToolSourceLineageUnavailable
	}
	if err := turn.Add(definitionLineage(tool, definitionForTool(tool))); err != nil {
		return nil, err
	}
	if err := turn.Check(ctx); err != nil {
		return nil, err
	}
	result, execErr := r.execute(ctx, &lineageToolExecutor{Tool: tool, adapter: adapter}, args)
	if result == nil {
		_ = turn.Add(types.UnknownSourceLineage())
		return nil, ErrToolSourceLineageUnavailable
	}
	if err := turn.Add(result.SourceLineage); err != nil {
		return nil, err
	}
	if err := turn.Check(ctx); err != nil {
		return nil, err
	}
	return result, execErr
}

// These two producers only transform model-supplied control arguments and
// code-owned diagnostics; the enclosing turn preserves the model input union.
func (t *SequentialThinkingTool) ExecuteSourceLineage(ctx context.Context, args json.RawMessage) (*types.ToolResult, error) {
	result, err := t.Execute(ctx, args)
	if result != nil {
		result.SourceLineage = independentToolInput()
	}
	return result, err
}
func (t *TodoWriteTool) ExecuteSourceLineage(ctx context.Context, args json.RawMessage) (*types.ToolResult, error) {
	result, err := t.Execute(ctx, args)
	if result != nil {
		result.SourceLineage = independentToolInput()
	}
	return result, err
}

// EmitToolEvent is required for tool-originated incremental/control events.
// The producer supplies its typed receipt before a client-visible field is
// published. Missing receipts are unknown, including titles/error text.
func EmitToolEvent(ctx context.Context, evt event.Event) error {
	meta, ok := ToolExecFromContext(ctx)
	if !ok || meta.EmitEvent == nil {
		return ErrToolSourceLineageUnavailable
	}
	if _, ok := evt.SourceLineage.(*types.SourceLineage); !ok {
		evt.SourceLineage = types.UnknownSourceLineage()
	}
	return meta.EmitEvent(ctx, evt)
}
