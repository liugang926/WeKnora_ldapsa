package session

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// This is the progress-formatting unit. Shell command output has no supported
// material adapter; the publication boundary is tested separately below.
func TestCommandOutputFormattingDoesNotCompleteTool(t *testing.T) {
	ctx := context.Background()
	bus := event.NewEventBus()
	streams := &completionEventRecorder{}
	message := &types.Message{ID: "message"}
	h := NewAgentStreamHandler(
		ctx, "session", "message", "request", 1, time.Now(), message, streams, bus, nil, nil, nil,
	)
	for _, done := range []bool{false, true} {
		require.NoError(t, h.handleCommandOutput(ctx, event.Event{
			Type: event.EventAgentCommandOutput,
			Data: event.CommandOutputData{
				ToolCallID: "call-1", Command: "python analysis.py", Output: "reading CSV", Done: done,
			},
		}))
	}
	require.Len(t, streams.events, 2)
	for _, e := range streams.events {
		require.Error(t, e.SourceLineage.RequireComplete(), "formatting must not certify shell output")
		require.Equal(t, types.ResponseTypeCommandOutput, e.Type)
		require.False(t, e.Done, "command progress is not a completed assistant turn")
		frame := buildStreamResponse(e, "request")
		require.Equal(t, "call-1", frame.Data["tool_call_id"])
		require.Equal(t, "reading CSV", frame.Data["output"])
	}
	require.Equal(t, true, streams.events[1].Data["done"])
	require.Empty(t, message.Content)
	require.Empty(t, message.AgentSteps)
}

func TestCommandOutputUnknownProducerCannotEnterPublishedStream(t *testing.T) {
	ctx := context.Background()
	bus := event.NewEventBus()
	streams := &completionEventRecorder{}
	message := &types.Message{ID: "message"}
	h := NewAgentStreamHandler(
		ctx, "session", "message", "request", 1, time.Now(), message, streams, bus, nil, nil, nil,
	)
	h.Subscribe()
	require.ErrorIs(t, bus.Emit(ctx, event.Event{
		Type: event.EventAgentCommandOutput,
		Data: event.CommandOutputData{
			ToolCallID: "call-1", Command: "python analysis.py",
			Output: "unknown private command body", Done: true,
		},
	}), access.ErrNextcloudPublicationDenied)
	require.Empty(t, streams.events)
	require.Empty(t, message.Content)
	require.Error(t, message.SourceLineage.RequireComplete())
}
