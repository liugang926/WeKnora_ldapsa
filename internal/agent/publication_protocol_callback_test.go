package agent

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	agenttools "github.com/Tencent/WeKnora/internal/agent/tools"
	"github.com/Tencent/WeKnora/internal/application/access"
	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type publicationProtocolCallbackTool struct {
	agenttools.BaseTool
	run func() (*types.ToolResult, error)
}

func (t *publicationProtocolCallbackTool) Execute(context.Context, json.RawMessage) (*types.ToolResult, error) {
	return t.run()
}

func TestRunToolCallPublicationDeniedKeepsPublicProtocolAndDropsProtectedOutput(t *testing.T) {
	cause := errors.Unwrap(access.ErrNextcloudPublicationDenied)
	require.NotNil(t, cause, "the test requires the new production typed sentinel, not old AA semantics")
	require.Equal(t, "nextcloud publication access denied", cause.Error())
	const legacy = "Nextcloud publication access denied"
	require.Equal(t, legacy, apperrors.PublicMessage(access.ErrNextcloudPublicationDenied))
	const protectedMarker = "PROTECTED_OWNED_AGENT_PROTOCOL_MARKER"
	model := &mockChat{}
	engine := newTestEngine(t, model)
	engine.toolRegistry = agenttools.NewToolRegistry()
	calls := 0
	var callbackError error
	engine.toolRegistry.RegisterTool(&publicationProtocolCallbackTool{
		BaseTool: agenttools.NewBaseTool(agenttools.ToolReadDocument, "owned protocol fixture",
			json.RawMessage(`{"type":"object"}`)),
		run: func() (*types.ToolResult, error) {
			calls++
			callbackError = access.ErrNextcloudPublicationDenied
			return &types.ToolResult{
				Success: true, Error: legacy, Output: protectedMarker,
				Data: map[string]interface{}{"protected": protectedMarker},
			}, callbackError
		},
	})
	var published []event.AgentToolResultData
	engine.eventBus.On(event.EventAgentToolResult, func(_ context.Context, evt event.Event) error {
		data, ok := evt.Data.(event.AgentToolResultData)
		require.True(t, ok)
		published = append(published, data)
		return nil
	})
	step := &types.AgentStep{}
	engine.executeSingleToolCall(context.Background(), types.LLMToolCall{
		ID: "owned-protocol-call", Function: types.FunctionCall{
			Name: agenttools.ToolReadDocument, Arguments: `{"id":"doc-1"}`,
		},
	}, 0, step, 1, 1, "owned-session", "owned-message")
	require.Equal(t, 1, calls, "actual registered callback must execute exactly once")
	require.Same(t, access.ErrNextcloudPublicationDenied, callbackError)
	require.ErrorIs(t, callbackError, access.ErrNextcloudPublicationDenied)
	require.ErrorIs(t, callbackError, cause)
	require.Equal(t, "nextcloud publication access denied", callbackError.Error())
	require.Len(t, step.ToolCalls, 1)
	result := step.ToolCalls[0].Result
	require.NotNil(t, result)
	require.False(t, result.Success)
	assert.Equal(t, legacy, result.Error)
	require.NotContains(t, result.Output, protectedMarker)
	require.Empty(t, result.Output)
	require.Empty(t, result.Data)
	require.Len(t, published, 1, "real Agent outcome publication must occur")
	require.False(t, published[0].Success)
	assert.Equal(t, legacy, published[0].Error)
	require.Empty(t, published[0].Output)
	require.Empty(t, published[0].Data)
	require.Zero(t, model.callCount, "the callback fixture must not invoke a model")
}
