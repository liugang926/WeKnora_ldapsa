package agent

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// Existing queue/iteration fixtures explicitly opt into their old algorithm.
// No handler or other production sink can implement this package-private method.
func (*fakeSteerSink) agentSteerAlgorithmFixture() {}

type originalAgentSteerSink struct {
	events           []map[string]interface{}
	lineage          *types.SourceLineage
	userMessageID    string
	persistErr       error
	pollErr          error
	check            func() error
	close            func()
	boundaryCheck    func(context.Context) error
	materialCalls    int
	legacyCalls      int
	consumed         bool
	receivedSession  string
	receivedRun      string
	receivedSteer    string
	receivedContent  string
	receivedMentions types.MentionedItems
	receivedChannel  string
}

func (s *originalAgentSteerSink) PollSteer(context.Context, string, string, int) ([]map[string]interface{}, int, error) {
	if s.pollErr != nil {
		return nil, 0, s.pollErr
	}
	if s.consumed {
		return nil, 0, nil
	}
	return s.events, len(s.events), nil
}

func (s *originalAgentSteerSink) PersistSteerMessage(context.Context, string, string, string, string, types.MentionedItems, string) string {
	s.legacyCalls++
	return "cached-body-row"
}

func (s *originalAgentSteerSink) PersistSteerMaterial(
	_ context.Context, sessionID, messageID, steerID, content string,
	mentions types.MentionedItems, channel string,
) (string, *types.SourceLineage, func() error, func(), error) {
	s.materialCalls++
	s.receivedSession, s.receivedRun, s.receivedSteer = sessionID, messageID, steerID
	s.receivedContent, s.receivedMentions, s.receivedChannel = content, mentions, channel
	if s.persistErr == nil {
		s.consumed = true
	}
	return s.userMessageID, s.lineage, s.check, s.close, s.persistErr
}

func (s *originalAgentSteerSink) CheckOriginalSteerInputs(ctx context.Context) error {
	if s.consumed && s.check != nil {
		if err := s.check(); err != nil {
			return err
		}
	}
	if s.boundaryCheck != nil {
		return s.boundaryCheck(ctx)
	}
	return nil
}

func validOriginalAgentSteerSink(t *testing.T) *originalAgentSteerSink {
	t.Helper()
	return &originalAgentSteerSink{
		events:        []map[string]interface{}{steerEntry("steer-original", "source text")},
		lineage:       agentComplete(t),
		userMessageID: "original-user-row",
		check:         func() error { return nil },
		close:         func() {},
	}
}

func bindOriginalAgentSteerEngine(t *testing.T, model chat.Chat) *AgentEngine {
	t.Helper()
	e := newTestEngine(t, model)
	e.SetSourceLineageVerifier(func(_ context.Context, lineage *types.SourceLineage) error {
		return lineage.RequireComplete()
	})
	return e
}

func TestAgentSteerOriginalMaterialCanonicalInputPreservesParentUnionAndLifetime(t *testing.T) {
	ctx := context.Background()
	parent := agentLineageIdentity("1", "parent", 7)
	steer := agentLineageIdentity("2", "steer-original", 9)
	model := &mockChat{responses: []mockResponse{lineageAnswer("continued answer")}}
	e := bindOriginalAgentSteerEngine(t, model)
	require.NoError(t, e.addLineage(agentComplete(t, parent)))
	messages := []chat.Message{{Role: "user", Content: "unfinished task", SourceLineage: agentComplete(t, parent)}}
	state := &types.AgentState{}
	sink := validOriginalAgentSteerSink(t)
	sink.lineage = agentComplete(t, steer)
	sink.events[0]["content"] = "  original\r\nquoted\rtext  "
	sink.events[0]["mentioned_items"] = []interface{}{map[string]interface{}{"type": "file", "id": "knowledge-2", "name": "original mention"}}
	sink.events[0]["channel"] = "api"
	checks, closes := 0, 0
	sink.check = func() error {
		checks++
		if checks == 1 {
			require.Len(t, messages, 1, "the live consume guard must run before append")
			require.Empty(t, state.PendingSteerMessages)
		}
		if closes > 0 {
			return errors.New("consume guard was closed")
		}
		return nil
	}
	sink.close = func() { closes++ }
	var injected event.Event
	e.eventBus.On(event.EventUserMessageInjected, func(_ context.Context, evt event.Event) error {
		injected = evt
		return nil
	})
	e.SetSteerSink(sink)
	require.Equal(t, agentComplete(t, parent), e.SourceLineageSnapshot())

	require.Equal(t, 1, e.drainSteerMessages(ctx, state, &messages, "session", "run"))
	require.Equal(t, "session", sink.receivedSession)
	require.Equal(t, "run", sink.receivedRun)
	require.Equal(t, "steer-original", sink.receivedSteer)
	require.Equal(t, types.SteerCanonicalQuery("  original\r\nquoted\rtext  "), sink.receivedContent)
	require.Equal(t, types.MentionedItemsFromRaw(sink.events[0]["mentioned_items"]), sink.receivedMentions)
	require.Equal(t, "api", sink.receivedChannel)
	require.Zero(t, sink.legacyCalls)
	require.Equal(t, 1, sink.materialCalls)
	require.Equal(t, []string{"original-user-row"}, state.PendingSteerMessages)
	require.Len(t, messages, 2)
	require.Equal(t, types.SteerMessageContent("original\nquoted\ntext"), messages[1].Content)
	require.Equal(t, agentComplete(t, steer), messages[1].SourceLineage)
	require.Equal(t, agentComplete(t, parent, steer), e.SourceLineageSnapshot())
	require.Equal(t, agentComplete(t, parent, steer), injected.SourceLineage)

	// A sink's returned envelope cannot mutate the already-bound model input.
	sink.lineage.State = types.SourceLineageUnknown
	require.NoError(t, messages[1].SourceLineage.RequireComplete())
	_, err := e.executeLoop(ctx, state, "unfinished task", messages, nil, "session", "run")
	require.NoError(t, err)
	require.Len(t, model.calls, 1)
	found := false
	for _, message := range model.calls[0] {
		if strings.Contains(message.Content, types.SteerMessageContent("original\nquoted\ntext")) {
			found = true
			require.Equal(t, agentComplete(t, parent, steer), message.SourceLineage)
		}
	}
	require.True(t, found, "actual model payload retains the exact canonical steering input")
	require.Equal(t, agentComplete(t, parent, steer), state.SourceLineage)
	require.Zero(t, closes, "AgentComplete must leave consume guards live for handler persistence")
	require.Greater(t, checks, 1, "later model and publication boundaries must recheck the retained guard")
	sink.close() // The HTTP/producer boundary owns this release.
	require.Equal(t, 1, closes)
}

func TestAgentSteerOriginalMaterialMalformedCapabilitiesPoisonNextModel(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*originalAgentSteerSink)
	}{
		{"missing user row", func(s *originalAgentSteerSink) { s.userMessageID = "" }},
		{"nil lineage", func(s *originalAgentSteerSink) { s.lineage = nil }},
		{"unknown lineage", func(s *originalAgentSteerSink) { s.lineage = types.UnknownSourceLineage() }},
		{"malformed lineage", func(s *originalAgentSteerSink) { s.lineage.Version = 0 }},
		{"missing check", func(s *originalAgentSteerSink) { s.check = nil }},
		{"missing close", func(s *originalAgentSteerSink) { s.close = nil }},
		{"consume rejected", func(s *originalAgentSteerSink) { s.persistErr = errors.New("original rejected") }},
		{"live guard rejected", func(s *originalAgentSteerSink) { s.check = func() error { return errors.New("original revoked") } }},
		{"poll rejected", func(s *originalAgentSteerSink) { s.pollErr = errors.New("original selection rejected") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			model := &mockChat{responses: []mockResponse{lineageAnswer("must not be sent")}}
			e := bindOriginalAgentSteerEngine(t, model)
			sink := validOriginalAgentSteerSink(t)
			tc.mutate(sink)
			e.SetSteerSink(sink)
			messages := []chat.Message{{Role: "user", Content: "task", SourceLineage: agentComplete(t)}}
			state := &types.AgentState{}
			published := 0
			e.eventBus.On(event.EventUserMessageInjected, func(context.Context, event.Event) error { published++; return nil })

			require.Zero(t, e.drainSteerMessages(context.Background(), state, &messages, "session", "run"))
			require.Len(t, messages, 1)
			require.Empty(t, state.PendingSteerMessages)
			require.Zero(t, published)
			require.Zero(t, sink.legacyCalls)
			require.ErrorIs(t, e.lineageFailure(), ErrAgentSourceLineageDenied)

			// A later successful guard cannot clear an earlier rejected input.
			sink.pollErr, sink.persistErr = nil, nil
			sink.check = func() error { return nil }
			_, err := e.streamLLMToEventBus(context.Background(), messages, nil, nil)
			require.ErrorIs(t, err, ErrAgentSourceLineageDenied)
			require.Zero(t, model.callCount)
		})
	}
}

// A named member deliberately does not inherit fakeSteerSink's private marker.
type unaccountedAgentSteerSink struct{ queue fakeSteerSink }

func (s *unaccountedAgentSteerSink) PollSteer(ctx context.Context, session, run string, offset int) ([]map[string]interface{}, int, error) {
	return s.queue.PollSteer(ctx, session, run, offset)
}

func (s *unaccountedAgentSteerSink) PersistSteerMessage(ctx context.Context, session, run, steer, content string, mentions types.MentionedItems, channel string) string {
	return s.queue.PersistSteerMessage(ctx, session, run, steer, content, mentions, channel)
}

func TestAgentSteerUnknownSinkCannotCertifyCachedBodyOrEnterModel(t *testing.T) {
	model := &mockChat{responses: []mockResponse{lineageAnswer("must not be sent")}}
	e := bindOriginalAgentSteerEngine(t, model)
	sink := &unaccountedAgentSteerSink{queue: fakeSteerSink{queued: []map[string]interface{}{
		{"id": "cached", "content": "unaccounted cached body", "source_lineage": agentComplete(t)},
	}}}
	e.SetSteerSink(sink)
	messages := []chat.Message{{Role: "user", Content: "task", SourceLineage: agentComplete(t)}}
	state := &types.AgentState{}
	require.Zero(t, e.drainSteerMessages(context.Background(), state, &messages, "session", "run"))
	require.Len(t, messages, 1)
	require.Empty(t, sink.queue.persisted)
	require.ErrorIs(t, e.lineageFailure(), ErrAgentSourceLineageDenied)

	// Execute resets turn errors, so the bound capability check must itself
	// reject the unsupported sink again at the actual next model boundary.
	_, err := e.Execute(context.Background(), "session", "run", "task", nil)
	require.ErrorIs(t, err, ErrAgentSourceLineageDenied)
	require.Zero(t, model.callCount)
}

func TestAgentSteerOriginalCheckerComposesExistingRoundGuard(t *testing.T) {
	e := bindOriginalAgentSteerEngine(t, &mockChat{})
	parentCalls, steerCalls := 0, 0
	e.SetRoundGuard(func(context.Context) error { parentCalls++; return nil })
	sink := validOriginalAgentSteerSink(t)
	revoked := false
	sink.boundaryCheck = func(context.Context) error {
		steerCalls++
		if revoked {
			return errors.New("original revoked")
		}
		return nil
	}
	e.SetSteerSink(sink)
	require.NoError(t, e.roundGuard(context.Background()))
	require.Equal(t, 1, parentCalls)
	require.Equal(t, 1, steerCalls)
	e.SetSteerSink(nil)
	revoked = true
	require.ErrorIs(t, e.roundGuard(context.Background()), ErrAgentSourceLineageDenied)
	require.Equal(t, 2, parentCalls)
	revoked = false
	require.ErrorIs(t, e.checkLineage(context.Background()), ErrAgentSourceLineageDenied)
}

func TestAgentSteerOriginalCheckerPreservesParentAuthorization(t *testing.T) {
	parent := agentLineageIdentity("1", "parent", 7)
	steer := agentLineageIdentity("2", "steer-original", 9)
	allowed := map[types.SourceIdentity]bool{parent: true, steer: true}
	model := &mockChat{responses: []mockResponse{lineageAnswer("must not be sent")}}
	e := bindOriginalAgentSteerEngine(t, model)
	e.SetSourceLineageVerifier(grantChecker(allowed))
	require.NoError(t, e.addLineage(agentComplete(t, parent)))
	sink := validOriginalAgentSteerSink(t)
	sink.lineage = agentComplete(t, steer)
	e.SetSteerSink(sink)
	messages := []chat.Message{{Role: "user", Content: "parent task", SourceLineage: agentComplete(t, parent)}}
	require.Equal(t, 1, e.drainSteerMessages(context.Background(), &types.AgentState{}, &messages, "session", "run"))
	allowed[parent] = false
	_, err := e.streamLLMToEventBus(context.Background(), messages, nil, nil)
	require.ErrorIs(t, err, ErrAgentSourceLineageDenied)
	require.Zero(t, model.callCount)
}

func TestAgentSteerOriginalGuardRevocationAtModelOutputBlocksCollectedText(t *testing.T) {
	revoked := false
	model := &revokeAfterSendModel{
		mockChat: &mockChat{responses: []mockResponse{lineageAnswer("revoked source prose")}},
		revoke:   func() { revoked = true },
	}
	e := bindOriginalAgentSteerEngine(t, model)
	sink := validOriginalAgentSteerSink(t)
	sink.check = func() error {
		if revoked {
			return errors.New("original revoked")
		}
		return nil
	}
	closes := 0
	sink.close = func() { closes++ }
	e.SetSteerSink(sink)
	messages := []chat.Message{{Role: "user", Content: "task", SourceLineage: agentComplete(t)}}
	require.Equal(t, 1, e.drainSteerMessages(context.Background(), &types.AgentState{}, &messages, "session", "run"))
	collected := 0
	result, err := e.streamLLMToEventBus(context.Background(), messages, nil, func(*types.StreamResponse, string) { collected++ })
	require.ErrorIs(t, err, ErrAgentSourceLineageDenied)
	require.Nil(t, result)
	require.Equal(t, 1, model.callCount)
	require.Zero(t, collected)
	require.Zero(t, closes)
}

func TestAgentSteerOriginalGuardRevocationAtPublicationBlocksOutput(t *testing.T) {
	e := bindOriginalAgentSteerEngine(t, &mockChat{})
	sink := validOriginalAgentSteerSink(t)
	revoked := false
	sink.check = func() error {
		if revoked {
			return errors.New("original revoked")
		}
		return nil
	}
	e.SetSteerSink(sink)
	messages := []chat.Message{{Role: "user", Content: "task", SourceLineage: agentComplete(t)}}
	require.Equal(t, 1, e.drainSteerMessages(context.Background(), &types.AgentState{}, &messages, "session", "run"))
	published := 0
	e.eventBus.On(event.EventAgentFinalAnswer, func(context.Context, event.Event) error { published++; return nil })
	revoked = true
	err := e.emitEvent(context.Background(), event.Event{Type: event.EventAgentFinalAnswer, Data: event.AgentFinalAnswerData{Content: "revoked source prose"}})
	require.ErrorIs(t, err, ErrAgentSourceLineageDenied)
	require.Zero(t, published)
	revoked = false
	require.ErrorIs(t, e.roundGuard(context.Background()), ErrAgentSourceLineageDenied)
}

var _ types.OriginalSteerSink = (*originalAgentSteerSink)(nil)
