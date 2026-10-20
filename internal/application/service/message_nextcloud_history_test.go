package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type messageHistorySources struct {
	interfaces.DataSourceRepository
	byKB map[string][]*types.DataSource
	err  error
}

func (s messageHistorySources) FindByKnowledgeBaseIncludingDeleted(
	_ context.Context,
	kbID string,
) ([]*types.DataSource, error) {
	return s.byKB[kbID], s.err
}

func (s messageHistorySources) HasEverNextcloudSourceForTenant(context.Context, uint64) (bool, error) {
	return false, s.err
}

// The positive projection comes from actual producer writes and an admitted
// private material read. Unknown legacy answers below never receive a proof.
func historyFilteringActualOrdinary(t *testing.T) (
	*messageService, context.Context, *types.Message, *types.Message,
) {
	t.Helper()
	messages, _, _, _, ctx, session := messageMaterialActualFixture(t)
	ordinaryQuestion, ordinary := produceActualMessagePair(ctx, t, messages, session)
	question, err := messages.CreateMessage(ctx, &types.Message{
		SessionID: session.ID, RequestID: "unknown-filter-partner", Role: "user",
		Content: "actual independently authored question", IsCompleted: true,
	})
	require.NoError(t, err)
	read, err := messages.beginMessageMaterialRead(ctx, types.MessageMaterialReadRequest{
		TenantID: 7, SessionID: session.ID,
		MessageIDs: []string{ordinaryQuestion.ID, ordinary.ID, question.ID},
	})
	require.NoError(t, err)
	t.Cleanup(read.Close)
	var originalQuestion, originalOrdinary *types.Message
	for _, message := range read.snapshot.Messages {
		switch message.ID {
		case question.ID:
			originalQuestion = message
		case ordinary.ID:
			originalOrdinary = message
		}
	}
	require.NotNil(t, originalQuestion)
	require.NotNil(t, originalOrdinary)
	require.NoError(t, originalOrdinary.SourceLineage.RequireComplete())
	require.NoError(t, messages.CheckMessagePublication(read.Context(), originalOrdinary))
	require.NoError(t, read.Recheck())
	return messages, read.Context(), originalQuestion, originalOrdinary
}

func TestHistoryFilteringRemovesUnknownAnswerAndSearchPartner(t *testing.T) {
	s, ctx, question, ordinary := historyFilteringActualOrdinary(t)
	// This cached broad-scope legacy row has no original envelope. The old
	// sourcesMap fixture never represented an actual source authorization or
	// revocation; actual signed-source withdrawal is covered by native HTTP.
	answer := &types.Message{
		ID: "unknown-answer", SessionID: question.SessionID, RequestID: question.RequestID,
		Role: "assistant", Content: "unproved secret answer",
		ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{"source-kb"}},
	}
	require.Error(t, answer.SourceLineage.RequireComplete())
	filtered, err := s.filterHistoryMessages(ctx, []*types.Message{question, answer, ordinary})
	require.NoError(t, err)
	require.Len(t, filtered, 2)
	require.Equal(t, question.ID, filtered[0].ID)
	require.Equal(t, question.Content, filtered[0].Content)
	require.Empty(t, filtered[0].KnowledgeReferences)
	require.Empty(t, filtered[0].AgentSteps)
	require.Nil(t, filtered[0].SourceLineage, "raw-user projection must not retain generated metadata")
	require.Same(t, ordinary, filtered[1])
	items := []*types.MessageSearchResultItem{
		{MessageWithSession: types.MessageWithSession{Message: *question}},
		{MessageWithSession: types.MessageWithSession{Message: *answer}},
		{MessageWithSession: types.MessageWithSession{Message: *ordinary}},
	}
	search, err := s.filterHistoricalSearchPairs(ctx, items)
	require.NoError(t, err)
	require.Len(t, search, 1, "the unproved answer's harmless search partner must not reintroduce it")
	require.Equal(t, ordinary.ID, search[0].ID)
	require.Equal(t, ordinary.Content, search[0].Content)
}

func TestHistoryFilteringDoesNotServeWhenTypedVerifierUnavailable(t *testing.T) {
	s, ctx, _, ordinary := historyFilteringActualOrdinary(t)
	// Legacy source probes cannot replace the current typed publication port.
	s.historyGuard = access.NewNextcloudHistoryGuard(nil,
		messageHistorySources{err: errors.New("database down")}, nil)
	s.sourceLineageGuard = nil
	rows, err := s.filterHistoryMessages(ctx, []*types.Message{ordinary})
	require.ErrorIs(t, err, access.ErrNextcloudPublicationUnavailable)
	require.Nil(t, rows)
}

func (s messageHistorySources) HasEverNextcloudSourceForKnowledgeBase(_ context.Context, id string) (bool, error) {
	for _, source := range s.byKB[id] {
		if source != nil && source.Type == types.ConnectorTypeNextcloud {
			return true, s.err
		}
	}
	return false, s.err
}

type legacyHistorySourceLookup struct {
	interfaces.DataSourceRepository
}

func (legacyHistorySourceLookup) FindByKnowledgeBaseIncludingDeleted(
	context.Context,
	string,
) ([]*types.DataSource, error) {
	return nil, nil
}

func (legacyHistorySourceLookup) HasEverNextcloudSourceForTenant(context.Context, uint64) (bool, error) {
	return false, nil
}

func TestHistoryFilteringNeedsTypedCurrentActorVerifier(t *testing.T) {
	s, ctx, _, ordinary := historyFilteringActualOrdinary(t)
	s.historyGuard = access.NewNextcloudHistoryGuard(nil, legacyHistorySourceLookup{}, nil)
	// Even a complete original envelope still requires the durable SQL actor
	// capability. A source-empty legacy lookup is not a substitute for it.
	s.sourceLineageGuard = access.NewNextcloudSourceLineageGuard(nil, s.kbService, nil, nil, nil, nil)
	rows, err := s.filterHistoryMessages(ctx, []*types.Message{ordinary})
	require.ErrorIs(t, err, access.ErrNextcloudPublicationUnavailable)
	require.Nil(t, rows)
}
