package tools

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func toolTurn() (*types.RAGSourceLineage, context.Context) {
	turn := types.NewRAGSourceLineage(
		func(_ context.Context, l *types.SourceLineage) error { return l.RequireComplete() },
	)
	return turn, types.WithRAGSourceLineage(context.Background(), turn)
}

func TestAgentLineageOpaqueToolBlockedBeforeExecutionAndCannotUseBuiltinName(t *testing.T) {
	tool := &mockTool{name: ToolReadDocument}
	r := NewToolRegistry()
	r.RegisterTool(tool)
	turn, ctx := toolTurn()
	result, err := r.ExecuteToolWithSourceLineage(ctx, tool.Name(), json.RawMessage(`{}`))
	require.ErrorIs(t, err, ErrToolSourceLineageUnavailable)
	require.Nil(t, result)
	require.ErrorIs(t, turn.Snapshot().RequireComplete(), types.ErrSourceLineageUnknown)
	require.Error(
		t,
		r.DefinitionSourceLineage([]chat.Tool{{Function: chat.FunctionDef{Name: tool.Name()}}}).RequireComplete(),
	)
}

func TestAgentLineageThinkingPureControlArgumentsPreserveExistingSource(t *testing.T) {
	source := types.SourceIdentity{
		Provider:        "nextcloud",
		TenantID:        12,
		KnowledgeBaseID: "kb",
		DataSourceID:    "ds",
		PairOperationID: "pair",
		InstanceID:      "i",
		BindingID:       "b",
		FileID:          "1",
		ExternalID:      "nextcloud:i:1",
		KnowledgeID:     "k",
		Revision:        7,
		ETag:            "original",
	}
	lineage, err := types.NewCompleteSourceLineage(source)
	require.NoError(t, err)
	r := NewToolRegistry()
	r.RegisterTool(NewSequentialThinkingTool())
	turn, ctx := toolTurn()
	require.NoError(t, turn.Add(lineage))
	result, err := r.ExecuteToolWithSourceLineage(
		ctx,
		ToolThinking,
		json.RawMessage(
			("{\"thought\":\"citation-free reasoning from earlier context\",\"thought_nu" +
				"mber\":1,\"total_thoughts\":1,\"next_thought_needed\":false}"),
		),
	)
	require.NoError(t, err)
	require.True(t, result.Success)
	require.NoError(t, result.SourceLineage.RequireComplete())
	require.Empty(t, result.SourceLineage.Sources)
	require.Equal(t, lineage, turn.Snapshot())
}

func TestAgentLineageIncrementalToolEventRequiresReceipt(t *testing.T) {
	var received event.Event
	ctx := WithToolExecContext(
		context.Background(),
		&ToolExecContext{EmitEvent: func(_ context.Context, evt event.Event) error { received = evt; return nil }},
	)
	require.NoError(
		t,
		EmitToolEvent(ctx, event.Event{Type: event.EventAgentCommandOutput, Data: map[string]any{"title": "opaque"}}),
	)
	lineage, ok := received.SourceLineage.(*types.SourceLineage)
	require.True(t, ok)
	require.ErrorIs(t, lineage.RequireComplete(), types.ErrSourceLineageUnknown)
	require.ErrorIs(t, EmitToolEvent(context.Background(), event.Event{}), ErrToolSourceLineageUnavailable)
}

func TestNativeCatalogToolHonorsOrdinaryACLAndStrictBoundary(t *testing.T) {
	r := NewToolRegistry()
	docs := NewSearchWeKnoraDocsTool(testDocsLibrary(t))
	r.RegisterTool(docs)
	require.True(t, r.WorkspaceACLOnly(docs.Name()))
	allowed := true
	turn := types.NewWeKnoraACLSourceLineage(func(context.Context, *types.SourceLineage) error {
		if !allowed {
			return types.ErrSourceLineageUnknown
		}
		return nil
	})
	ctx := types.WithRAGSourceLineage(t.Context(), turn)
	result, err := r.ExecuteToolWithSourceLineage(ctx, docs.Name(), json.RawMessage(`{}`))
	require.NoError(t, err)
	require.True(t, result.Success)
	require.NotEmpty(t, result.Output)
	require.NoError(t, result.SourceLineage.RequireComplete())
	allowed = false
	result, err = r.ExecuteToolWithSourceLineage(ctx, docs.Name(), json.RawMessage(`{}`))
	require.Error(t, err)
	require.Nil(t, result)
	_, strict := toolTurn()
	result, err = r.ExecuteToolWithSourceLineage(strict, docs.Name(), json.RawMessage(`{}`))
	require.ErrorIs(t, err, ErrToolSourceLineageUnavailable)
	require.Nil(t, result)
}
