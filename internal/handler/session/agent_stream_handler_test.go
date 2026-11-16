package session

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Tencent/WeKnora/internal/agent"
	agenttools "github.com/Tencent/WeKnora/internal/agent/tools"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// capturingStreamManager records every appended event so a handler's output
// can be asserted without a real stream backend. Embedding the interface keeps
// everything else nil-panicky so an un-stubbed call fails loudly.
type capturingStreamManager struct {
	interfaces.StreamManager
	events []interfaces.StreamEvent
}

func (s *capturingStreamManager) AppendEvent(
	_ context.Context, _, _ string, evt interfaces.StreamEvent,
) error {
	s.events = append(s.events, evt)
	return nil
}

// A failed tool execution is still a tool result: the SSE event must keep
// response_type=tool_result with success=false in its metadata, because
// response_type=error is reserved for internal agent failures (handleError).
func TestAgentStreamHandlerToolResultFailureKeepsToolResultType(t *testing.T) {
	streams := &capturingStreamManager{}
	h := NewAgentStreamHandler(
		context.Background(), "sess-1", "msg-1", "req-1", 1, time.Now(),
		&types.Message{}, streams, nil, nil, nil, nil,
	)

	err := h.handleToolResult(context.Background(), event.Event{
		ID: "evt-1", Type: event.EventAgentToolResult,
		Data: event.AgentToolResultData{
			ToolName:   "shell_exec",
			ToolCallID: "call-1",
			Success:    false,
			Error:      "exit status 1",
		},
	})
	require.NoError(t, err)
	require.Len(t, streams.events, 1)

	evt := streams.events[0]
	require.Equal(t, types.ResponseTypeToolResult, evt.Type)
	require.Equal(t, false, evt.Data["success"])
	require.Equal(t, "exit status 1", evt.Data["error"])
	require.Equal(t, "call-1", evt.Data["tool_call_id"])
}

// Internal error events keep response_type=error with done=true, so clients
// can still distinguish a crashed run from a failed tool call.
func TestAgentStreamHandlerInternalErrorKeepsErrorType(t *testing.T) {
	streams := &capturingStreamManager{}
	h := NewAgentStreamHandler(
		context.Background(), "sess-1", "msg-1", "req-1", 1, time.Now(),
		&types.Message{}, streams, nil, nil, nil, nil,
	)

	err := h.handleError(context.Background(), event.Event{
		ID: "evt-err", Type: event.EventError,
		Data: event.ErrorData{Stage: "think", Error: "llm unavailable"},
	})
	require.NoError(t, err)
	require.Len(t, streams.events, 1)

	evt := streams.events[0]
	require.Equal(t, types.ResponseTypeError, evt.Type)
	require.True(t, evt.Done)
}

func TestAgentStreamHandlerForwardsOperationDataChange(t *testing.T) {
	streams := &capturingStreamManager{}
	h := NewAgentStreamHandler(
		context.Background(), "sess-1", "msg-1", "req-1", 1, time.Now(),
		&types.Message{}, streams, nil, nil, nil, nil,
	)
	marker := map[string]any{"operation": "agent.delete", "area": "agent"}
	err := h.handleToolResult(context.Background(), event.Event{
		Type: event.EventAgentToolResult,
		Data: event.AgentToolResultData{
			ToolName: "run_operation", Success: true, Data: map[string]any{"data_change": marker},
		},
	})
	require.NoError(t, err)
	require.Len(t, streams.events, 1)
	require.Equal(t, types.ResponseTypeToolResult, streams.events[0].Type)
	require.Equal(t, marker, streams.events[0].Data["data_change"])
}

// A rejected page button makes the engine reclassify the first round's text
// as progress, then ask the model for the final answer in the next round.
type progressChatModel struct {
	rounds [][]types.StreamResponse
}

func (*progressChatModel) GetModelID() string   { return "progress-fixture" }
func (*progressChatModel) GetModelName() string { return "progress-fixture" }
func (*progressChatModel) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	return nil, errors.New("unexpected non-streaming call")
}

func (m *progressChatModel) ChatStream(
	context.Context, []chat.Message, *chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	if len(m.rounds) == 0 {
		return nil, errors.New("no more scripted rounds")
	}
	round := m.rounds[0]
	m.rounds = m.rounds[1:]
	stream := make(chan types.StreamResponse, len(round))
	for _, chunk := range round {
		stream <- chunk
	}
	close(stream)
	return stream, nil
}

func TestAgentStreamHandlerProgressIsSeparateFromReasoningAndFinalAnswer(t *testing.T) {
	const progress, thinking, final = "Opening the page.", "Check the page name.", "The page does not exist."
	ctx := context.WithValue(t.Context(), types.UserIDContextKey, "session-owner")
	streams := &capturingStreamManager{}
	bus := event.NewEventBus()
	message := &types.Message{}
	h := NewAgentStreamHandler(ctx, "s", "m", "r", 1, time.Now(), message, streams, bus, nil, nil, nil)
	h.Subscribe()

	model := &progressChatModel{rounds: [][]types.StreamResponse{
		{
			{ResponseType: types.ResponseTypeThinking, Content: thinking},
			{
				ResponseType: types.ResponseTypeAnswer, Content: progress, Done: true, FinishReason: "tool_calls",
				ToolCalls: []types.LLMToolCall{{
					ID: "page", Type: "function",
					Function: types.FunctionCall{Name: agenttools.ToolOpenPage, Arguments: `{"page":"invalid"}`},
				}},
			},
		},
		{{ResponseType: types.ResponseTypeAnswer, Content: final, Done: true, FinishReason: "stop"}},
	}}
	registry := agenttools.NewToolRegistry()
	registry.RegisterTool(agenttools.NewOpenPageTool(agenttools.OperationsRuntime{}))
	engine := agent.NewAgentEngine(&types.AgentConfig{MaxIterations: 2}, model, registry, bus, nil, nil, "s", "")
	require.NotNil(t, engine)
	engine.SetWeKnoraACLSourceLineageVerifier(func(ctx context.Context) error {
		if userID, _ := types.UserIDFromContext(ctx); userID != "session-owner" {
			return agent.ErrAgentSourceLineageDenied
		}
		return nil
	})
	_, err := engine.Execute(t.Context(), "s", "m", "Open the page.", nil)
	require.ErrorIs(t, err, agent.ErrAgentSourceLineageDenied)
	require.Empty(t, streams.events)
	state, err := engine.Execute(ctx, "s", "m", "Open the page.", nil)
	require.NoError(t, err)
	require.Len(t, state.RoundSteps, 2)
	require.Equal(t, final, state.FinalAnswer)
	require.Equal(t, final, h.finalAnswer)
	var updates []interfaces.StreamEvent
	var reasoning strings.Builder
	for _, frame := range streams.events {
		switch frame.Type {
		case types.ResponseTypeProgress:
			updates = append(updates, frame)
		case types.ResponseTypeThinking:
			reasoning.WriteString(frame.Content)
		}
	}
	require.Equal(t, thinking, reasoning.String())
	require.Len(t, updates, 1)
	update := updates[0]
	require.Equal(t, progress, update.Content)
	require.True(t, update.Done)
	require.NotEmpty(t, state.RoundSteps[0].ProgressEventID)
	require.Equal(t, state.RoundSteps[0].ProgressEventID, update.Data["event_id"])
	require.Equal(t, state.RoundSteps[0].Iteration, update.Data["iteration"])
	require.NoError(t, update.SourceLineage.RequireComplete())

	// A valid prefix never authorizes another producer's unaccounted bytes.
	count := len(streams.events)
	err = bus.Emit(ctx, event.Event{
		Type: event.EventAgentProgress,
		Data: event.AgentProgressData{Content: "unaccounted progress"},
	})
	require.ErrorIs(t, err, types.ErrSourceLineageUnknown)
	require.Len(t, streams.events, count)
	require.Equal(t, final, h.finalAnswer)
}

func TestPageButtonKeepsTheAnswerBeforeIt(t *testing.T) {
	h := NewAgentStreamHandler(
		context.Background(), "s", "m", "r", 1, time.Now(), &types.Message{},
		&capturingStreamManager{}, nil, nil, nil, nil,
	)
	ctx := context.Background()
	answer := func(id, content string) {
		require.NoError(t, h.handleFinalAnswer(ctx, event.Event{
			ID: id, Type: event.EventAgentFinalAnswer, Data: event.AgentFinalAnswerData{Content: content},
		}))
	}
	call := func(name string, args map[string]any) {
		require.NoError(t, h.handleToolCall(ctx, event.Event{
			ID: "call", Type: event.EventAgentToolCall,
			Data: event.AgentToolCallData{ToolCallID: "c-" + name, ToolName: name, Arguments: args},
		}))
	}

	answer("a1", "the report")
	call("open_page", nil)
	call("open_page", map[string]any{"page": "agents"})
	require.Equal(t, "the report", h.finalAnswer, "a page button follows the answer")

	call("view_page", map[string]any{"page": "settings"})
	require.Empty(t, h.finalAnswer, "viewing a page is more work, so the text was a preamble")

	answer("a2", "second report")
	require.NoError(t, h.handleProgress(ctx, event.Event{
		ID: "a2", Type: event.EventAgentProgress, Data: event.AgentProgressData{Content: "second report"},
	}))
	require.Empty(t, h.finalAnswer, "progress with the answer's id retracts it")
}

func TestProtocolAnswerReplacementOnlyResetsMatchingSegment(t *testing.T) {
	streams := &capturingStreamManager{}
	h := NewAgentStreamHandler(
		context.Background(),
		"s",
		"m",
		"r",
		1,
		time.Now(),
		&types.Message{},
		streams,
		nil,
		nil,
		nil,
		nil,
	)
	send := func(id, content string, replace bool) {
		require.NoError(
			t,
			h.handleFinalAnswer(context.Background(), event.Event{
				ID: id, Type: event.EventAgentFinalAnswer,
				Data: event.AgentFinalAnswerData{Content: content, Replace: replace},
			}),
		)
	}
	send("prior", "Earlier legitimate answer.", false)
	send("current", "refs\nprivate prefix", false)
	send("current", "", true)
	send("current", "## 进展\ncomplete report", false)
	require.Equal(t, "Earlier legitimate answer.", h.findAnswerSegment("prior").content)
	require.Equal(t, "## 进展\ncomplete report", h.findAnswerSegment("current").content)
	require.NotContains(t, h.finalAnswer, "private prefix")
	require.Equal(t, true, streams.events[2].Data["replace"])
	require.Equal(t, "current", streams.events[2].Data["event_id"])
}
