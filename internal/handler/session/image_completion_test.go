package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type imageCompletionMessages struct {
	interfaces.MessageService
	saved      *types.Message
	tenant     uint64
	err        error
	beforeSave func()
}

func (s *imageCompletionMessages) UpdateMessage(ctx context.Context, message *types.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.beforeSave != nil {
		s.beforeSave()
	}
	if s.err != nil {
		return s.err
	}
	s.tenant = types.MustTenantIDFromContext(ctx)
	snapshot := *message
	s.saved = &snapshot
	return nil
}

func TestQuickAnswerCompletionPersistsAfterGenerationCancellation(t *testing.T) {
	messages := &imageCompletionMessages{}
	stream := &imageCompletionStream{}
	h := &Handler{messageService: messages, streamManager: stream}
	bus := event.NewEventBus()
	message := &types.Message{
		ID: "m", SessionID: "s", Role: "assistant", AgentTenantID: 2,
	}
	ctx, cancel := context.WithCancel(types.WithExecutionTenant(context.Background(), 1))
	streamHandler := h.setupStreamHandler(ctx, "s", "m", "req", 1, time.Now(), message, bus)
	// The local mock model owns these literal bytes. Use the actual producer
	// handler before cancellation; pre-filling an unknown message is not proof.
	literal, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	require.NoError(t, bus.Emit(ctx, event.Event{
		ID: "local-literal-answer", Type: event.EventAgentFinalAnswer, SourceLineage: literal,
		Data: event.AgentFinalAnswerData{Content: "already streamed answer", Done: true},
	}))
	// The quick-answer caller accumulates the already-accounted answer; the
	// stream handler itself retains it in its answer segments until Complete.
	message.Content = streamHandler.finalAnswer
	require.Equal(t, "already streamed answer", message.Content)
	require.NoError(t, message.SourceLineage.RequireComplete())
	cancel()
	released := false
	h.completeQuickAnswerTurn(ctx, &sseStreamContext{
		eventBus: bus, streamHandler: streamHandler, assistantMessage: message,
		releaseTurn: func() { released = true },
	}, "", "")
	require.NotNil(t, messages.saved)
	require.Equal(t, "already streamed answer", messages.saved.Content)
	require.Equal(t, uint64(1), messages.tenant)
	require.True(t, released)
	require.NotEmpty(t, stream.events)
	require.Equal(t, types.ResponseTypeComplete, stream.events[len(stream.events)-1].Type)
}

type imageCompletionStream struct {
	completionEventRecorder
	onComplete func()
}

func (s *imageCompletionStream) AppendEvent(
	ctx context.Context, session, message string, e interfaces.StreamEvent,
) error {
	if e.Type == types.ResponseTypeComplete && s.onComplete != nil {
		s.onComplete()
	}
	return s.completionEventRecorder.AppendEvent(ctx, session, message, e)
}

func TestSharedImageCompletionWaitsForPersistedReferences(t *testing.T) {
	const image = "resource://AbCdEfGhIjKlMnOpQrStUv"
	for _, quick := range []bool{false, true} {
		name := "agent"
		if quick {
			name = "quick answer"
		}
		t.Run(name, func(t *testing.T) {
			for _, fail := range []bool{false, true} {
				t.Run(map[bool]string{false: "saved", true: "save failed"}[fail], func(t *testing.T) {
					messages := &imageCompletionMessages{}
					if fail {
						messages.err = errors.New("database unavailable")
					}
					stream := &imageCompletionStream{}
					h := &Handler{messageService: messages, streamManager: stream}
					bus := event.NewEventBus()
					message := &types.Message{
						ID: "m", SessionID: "s", Role: "assistant", AgentTenantID: 2,
					}
					// Execution uses the host workspace; the conversation belongs to
					// the visitor's workspace and must be saved there before notifying it.
					runCtx := types.WithExecutionTenant(context.Background(), 2)
					saveCtx := types.WithExecutionTenant(runCtx, 1)
					streamHandler := h.setupStreamHandler(runCtx, "s", "m", "req", 1, time.Now(), message, bus)
					streamCtx := &sseStreamContext{
						eventBus: bus, streamHandler: streamHandler, assistantMessage: message,
					}
					completed := 0
					stream.onComplete = func() {
						completed++
						require.NotNil(t, messages.saved, "a completion-triggered image GET must see persisted output")
						require.True(t, access.MessageReferencesFile(messages.saved, image))
						require.True(t, messages.saved.IsCompleted)
						require.Equal(t, uint64(1), messages.tenant)
					}
					messages.beforeSave = func() {
						require.Zero(t, completed, "completion must not escape while the database write is pending")
					}
					// This mock model owns only the local literal URL and text.
					// This is a persistence/notification unit, not an image
					// generation, attachment or file-material adapter.
					literal, err := types.NewCompleteSourceLineage()
					require.NoError(t, err)
					answer := "![host image](" + image + ")"
					if quick {
						require.NoError(t, bus.Emit(runCtx, event.Event{
							Type: event.EventAgentFinalAnswer, SourceLineage: literal,
							Data: event.AgentFinalAnswerData{Content: answer, Done: true},
						}))
						// Mirror the caller's exact accounted prefix, never a
						// legacy/unknown prefilled message body.
						message.Content = streamHandler.finalAnswer
						h.completeQuickAnswerTurn(saveCtx, streamCtx, "", "")
					} else {
						require.NoError(t, bus.Emit(runCtx, event.Event{
							Type: event.EventAgentComplete, SourceLineage: literal,
							Data: event.AgentCompleteData{MessageID: "m", FinalAnswer: answer},
						}))
						require.Zero(t, completed)
						h.completeStreamAssistantMessage(saveCtx, streamCtx, "", "")
					}
					require.NotEmpty(t, stream.events)
					last := stream.events[len(stream.events)-1]
					if fail {
						require.Zero(t, completed)
						require.Equal(t, types.ResponseTypeError, last.Type)
						require.True(t, last.Done)
						require.Equal(t, map[string]interface{}{"error_code": "generation_failed"}, last.Data)
						require.NotContains(t, last.Content, "database unavailable")
						require.Nil(t, messages.saved)
					} else {
						require.Equal(t, 1, completed)
						require.Equal(t, types.ResponseTypeComplete, last.Type)
						require.Equal(t, answer, last.Data["final_content"])
						require.NoError(t, streamHandler.publishCompletion(saveCtx))
						require.Equal(t, 1, completed, "completion must only be published once")
					}
				})
			}
		})
	}
}

func TestQuickAnswerUnknownPrefilledBodyCannotPublishCompletion(t *testing.T) {
	streams := &imageCompletionStream{}
	h := &Handler{messageService: &imageCompletionMessages{}, streamManager: streams}
	bus := event.NewEventBus()
	message := &types.Message{ID: "m", SessionID: "s", Role: "assistant", Content: "unknown prefilled private answer"}
	ctx := types.WithExecutionTenant(context.Background(), 1)
	h.setupStreamHandler(ctx, "s", "m", "req", 1, time.Now(), message, bus)
	require.ErrorIs(t, bus.Emit(ctx, event.Event{
		Type: event.EventAgentComplete,
		Data: event.AgentCompleteData{MessageID: "m"},
	}), access.ErrNextcloudPublicationDenied)
	require.Empty(t, streams.events)
	require.Error(t, message.SourceLineage.RequireComplete())
}
