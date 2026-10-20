package session

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type ragPrefixVerifier struct {
	interfaces.MessageService
	revoked bool
}

func (s *ragPrefixVerifier) CheckMessagePublication(_ context.Context, message *types.Message) error {
	if message.SourceLineage.RequireComplete() != nil || s.revoked && len(message.SourceLineage.Sources) > 0 {
		return access.ErrNextcloudPublicationDenied
	}
	return nil
}

func (s *ragPrefixVerifier) CheckLiveMessagePublication(ctx context.Context, message *types.Message) error {
	return s.CheckMessagePublication(ctx, message)
}

type ragPrefixStore struct {
	interfaces.StreamManager
	events []interfaces.StreamEvent
}

func (s *ragPrefixStore) AppendEvent(_ context.Context, _, _ string, frame interfaces.StreamEvent) error {
	payload, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	var stored interfaces.StreamEvent
	if err := json.Unmarshal(payload, &stored); err != nil {
		return err
	}
	s.events = append(s.events, stored)
	return nil
}

func ragPrefixProof(t *testing.T) *types.SourceLineage {
	t.Helper()
	proof, err := types.NewCompleteSourceLineage(types.SourceIdentity{
		Provider:        types.ConnectorTypeNextcloud,
		TenantID:        7,
		KnowledgeBaseID: "kb",
		DataSourceID:    "ds",
		PairOperationID: "pair",
		InstanceID:      "instance",
		BindingID:       "binding",
		FileID:          "1",
		ExternalID:      "nextcloud:instance:1",
		KnowledgeID:     "knowledge",
		Revision:        2,
		ETag:            "original",
	})
	require.NoError(t, err)
	return proof
}

func TestRAGSourceLineageDurablePrefixSurvivesCitationFreeReplay(t *testing.T) {
	proof := ragPrefixProof(t)
	bus := event.NewEventBus()
	store := &ragPrefixStore{}
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
	// Every source-bearing output is included even if the final model event
	// accidentally supplied a smaller receipt. Handler union never shrinks.
	require.NoError(
		t,
		bus.Emit(
			context.Background(),
			event.Event{
				Type:          event.EventAgentReferences,
				SourceLineage: proof,
				Data:          event.AgentReferencesData{References: []*types.SearchResult{}},
			},
		),
	)
	empty, _ := types.NewCompleteSourceLineage()
	require.NoError(
		t,
		bus.Emit(
			context.Background(),
			event.Event{
				Type:          event.EventAgentFinalAnswer,
				SourceLineage: empty,
				Data:          event.AgentFinalAnswerData{Content: "paraphrase without citations"},
			},
		),
	)
	require.Equal(t, proof, message.SourceLineage)
	require.Len(t, store.events, 2)
	require.Equal(t, proof, store.events[1].SourceLineage)
	payload, err := json.Marshal(buildStreamResponse(store.events[1], "request"))
	require.NoError(t, err)
	require.NotContains(t, string(payload), "source_lineage")
	require.NotContains(t, string(payload), "binding")
	verifier := &ragPrefixVerifier{}
	live := &types.Message{Role: "assistant"}
	require.NoError(t, checkLiveStreamPublication(context.Background(), verifier, live, store.events))
	require.Equal(t, proof, live.SourceLineage)
	verifier.revoked = true
	require.ErrorIs(
		t,
		checkLiveStreamPublication(context.Background(), verifier, live, store.events),
		access.ErrNextcloudPublicationDenied,
	)
	message.IsCompleted = true
	require.ErrorIs(
		t,
		checkReplayPublication(context.Background(), verifier, message, store.events),
		access.ErrNextcloudPublicationDenied,
	)
}

func TestRAGSourceLineageRejectsMissingPrefixAndUncoveredAgentEvents(t *testing.T) {
	store := &ragPrefixStore{}
	bus := event.NewEventBus()
	message := &types.Message{Role: "assistant"}
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
	require.ErrorIs(
		t,
		bus.Emit(
			context.Background(),
			event.Event{
				Type: event.EventAgentFinalAnswer,
				Data: event.AgentFinalAnswerData{Content: "unaccounted body"},
			},
		),
		access.ErrNextcloudPublicationDenied,
	)
	require.Empty(t, store.events, "uncovered producer persisted a replayable body")
	verifier := &ragPrefixVerifier{}
	empty, _ := types.NewCompleteSourceLineage()
	message.SourceLineage = empty
	legacy := []interfaces.StreamEvent{{Type: types.ResponseTypeAnswer, Content: "legacy secret"}}
	require.ErrorIs(
		t,
		checkLiveStreamPublication(context.Background(), verifier, message, legacy),
		access.ErrNextcloudPublicationDenied,
	)
	message.IsCompleted = true
	require.ErrorIs(
		t,
		checkReplayPublication(context.Background(), verifier, message, legacy),
		access.ErrNextcloudPublicationDenied,
	)
	foreign := []interfaces.StreamEvent{
		{Type: types.ResponseTypeAnswer, Content: "foreign source", SourceLineage: ragPrefixProof(t)},
	}
	require.ErrorIs(
		t,
		checkReplayPublication(context.Background(), verifier, message, foreign),
		access.ErrNextcloudPublicationDenied,
	)
}
