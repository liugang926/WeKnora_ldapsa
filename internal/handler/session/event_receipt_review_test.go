package session

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestEventReceiptReviewCompleteRAGRuntimeCannotCertifyUnaccountedEventBody(t *testing.T) {
	for _, item := range []struct {
		name    string
		receipt any
	}{{"missing", nil}, {"wrong type", "not a typed producer receipt"}} {
		t.Run(item.name, func(t *testing.T) {
			ctx := types.WithRAGSourceLineage(
				context.Background(),
				types.NewRAGSourceLineage(
					func(_ context.Context, proof *types.SourceLineage) error { return proof.RequireComplete() },
				),
			)
			store := &ragPrefixStore{}
			bus := event.NewEventBus()
			message := &types.Message{Role: "assistant"}
			handler := NewAgentStreamHandler(
				ctx,
				"session",
				"answer",
				"request",
				7,
				message.CreatedAt,
				message,
				store,
				bus,
				nil,
				nil,
				nil,
			)
			handler.Subscribe()
			err := bus.Emit(
				ctx,
				event.Event{
					Type:          event.EventAgentFinalAnswer,
					SourceLineage: item.receipt,
					Data:          event.AgentFinalAnswerData{Content: "unaccounted source-derived body"},
				},
			)
			if err == nil {
				require.Len(t, store.events, 1, "actual event handler did not reach the durable append")
				require.Equal(t, "unaccounted source-derived body", store.events[0].Content)
			}
			require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
			require.Empty(t, store.events)
			require.Empty(t, message.Content)
		})
	}
}

func TestEventReceiptReviewCompletionCannotSmuggleNewDerivedFields(t *testing.T) {
	store := &ragPrefixStore{}
	bus := event.NewEventBus()
	message := &types.Message{ID: "answer", SessionID: "session", Role: "assistant"}
	handler := NewAgentStreamHandler(
		context.Background(),
		"session",
		"answer",
		"request",
		7,
		message.CreatedAt,
		message,
		store,
		bus,
		nil,
		nil,
		nil,
	)
	handler.Subscribe()
	empty, _ := types.NewCompleteSourceLineage()
	require.NoError(
		t,
		bus.Emit(
			context.Background(),
			event.Event{
				Type:          event.EventAgentFinalAnswer,
				SourceLineage: empty,
				Data:          event.AgentFinalAnswerData{Content: "accounted body"},
			},
		),
	)
	err := bus.Emit(
		context.Background(),
		event.Event{
			Type: event.EventAgentComplete,
			Data: event.AgentCompleteData{
				MessageID:     "answer",
				KnowledgeRefs: []any{&types.SearchResult{Content: "unaccounted private reference"}},
				Extra:         map[string]any{"derived": "new secret"},
			},
		},
	)
	require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
	require.False(t, message.IsCompleted)
	require.Empty(t, message.KnowledgeReferences)
}

func TestEventReceiptReviewPlainQuickCompletionSealsOnlyKnownPrefix(t *testing.T) {
	store := &ragPrefixStore{}
	bus := event.NewEventBus()
	message := &types.Message{ID: "answer", SessionID: "session", Role: "assistant"}
	handler := NewAgentStreamHandler(
		context.Background(),
		"session",
		"answer",
		"request",
		7,
		message.CreatedAt,
		message,
		store,
		bus,
		nil,
		nil,
		nil,
	)
	handler.Subscribe()
	empty, _ := types.NewCompleteSourceLineage()
	require.NoError(
		t,
		bus.Emit(
			context.Background(),
			event.Event{
				Type:          event.EventAgentFinalAnswer,
				SourceLineage: empty,
				Data:          event.AgentFinalAnswerData{Content: "accounted body"},
			},
		),
	)
	require.NoError(
		t,
		bus.Emit(
			context.Background(),
			event.Event{Type: event.EventAgentComplete, Data: event.AgentCompleteData{MessageID: "answer"}},
		),
	)
	require.True(t, message.IsCompleted)
	require.Equal(t, "accounted body", handler.finalAnswer)
	require.Empty(t, message.Content, "body is copied by the ordinary completion writer separately")
	require.Equal(t, empty, message.SourceLineage)
}
