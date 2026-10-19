package tools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// observeShellProgressAttempt inspects the buffering primitive's event attempts.
// It does not publish to an EventBus or certify the external command output.
func observeShellProgressAttempt(
	t *testing.T,
	observe func(event.CommandOutputData),
) func(context.Context, event.Event) error {
	t.Helper()
	return func(_ context.Context, evt event.Event) error {
		lineage, ok := evt.SourceLineage.(*types.SourceLineage)
		if !ok || lineage.RequireComplete() == nil {
			t.Error("unaccounted shell progress must remain unknown")
			return types.ErrSourceLineageUnknown
		}
		observe(evt.Data.(event.CommandOutputData))
		return nil
	}
}

func TestShellCommandOutputIsBoundedAndFinishes(t *testing.T) {
	bus := event.NewEventBus()

	updates := make(chan event.CommandOutputData, 16)
	next := func() event.CommandOutputData {
		select {
		case update := <-updates:
			return update
		case <-time.After(3 * time.Second):
			t.Fatal("missing progress event")
			return event.CommandOutputData{}
		}
	}
	ctx := WithToolExecContext(context.Background(), &ToolExecContext{
		EventBus: bus, ToolCallID: "cmd-1", SessionID: "session",
		EmitEvent: observeShellProgressAttempt(t, func(update event.CommandOutputData) { updates <- update }),
	})
	output, finish := shellCommandOutput(ctx, "uv pip install -r requirements.lock")
	defer finish()
	start := next()
	require.Equal(t, "cmd-1", start.ToolCallID)
	require.False(t, start.Done)
	output("stdout", []byte(strings.Repeat("x", 16000)))
	first := next()
	require.LessOrEqual(t, len(first.Output), 8192)
	output("stderr", []byte("\ndownloading wheel"))
	// A short burst followed by a long silent download must still flush before completion.
	progress := next()
	require.Contains(t, progress.Output, "downloading wheel")
	require.False(t, progress.Done)
	finish()
	finish()
	require.True(t, next().Done)
	output("stdout", []byte("late callback"))
	require.Empty(t, updates)
}

func TestShellExecPublishesCommandProgress(t *testing.T) {
	bus := event.NewEventBus()
	var updates []event.CommandOutputData
	executor := &fakeShellExecutor{}
	tool := NewShellExecTool(executor, nil)
	ctx := WithToolExecContext(context.Background(), &ToolExecContext{
		EventBus:   bus,
		SessionID:  "session",
		ToolCallID: "call",
		EmitEvent: observeShellProgressAttempt(
			t,
			func(update event.CommandOutputData) { updates = append(updates, update) },
		),
	})
	result, err := tool.Execute(ctx, json.RawMessage(`{"command":"cat","stdin":"private input"}`))
	require.NoError(t, err)
	require.True(t, result.Success)
	require.Len(t, updates, 2)
	require.Equal(t, "call", updates[0].ToolCallID)
	require.Equal(t, "cat", updates[0].Command, "progress shows the requested command, not the encoded stdin wrapper")
	require.False(t, updates[0].Done)
	require.True(t, updates[1].Done)
}

func TestShellCommandOutputRequiresReceiptBeforePublicBus(t *testing.T) {
	bus := event.NewEventBus()
	published := 0
	attempts := 0
	bus.On(event.EventAgentCommandOutput, func(context.Context, event.Event) error {
		published++
		return nil
	})
	ctx := WithToolExecContext(context.Background(), &ToolExecContext{
		EventBus: bus, ToolCallID: "unaccounted", SessionID: "session",
		EmitEvent: func(ctx context.Context, evt event.Event) error {
			attempts++
			lineage, ok := evt.SourceLineage.(*types.SourceLineage)
			if !ok {
				return types.ErrSourceLineageUnknown
			}
			if err := lineage.RequireComplete(); err != nil {
				return err
			}
			return bus.Emit(ctx, evt)
		},
	})
	output, finish := shellCommandOutput(ctx, "cat")
	output("stdout", []byte("unaccounted command output"))
	finish()
	require.Equal(t, 3, attempts)
	require.Zero(t, published, "unknown incremental output must not reach the public bus")
}
