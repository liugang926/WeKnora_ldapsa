package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type messageHistorySources struct {
	interfaces.DataSourceRepository
	byKB map[string][]*types.DataSource
	err  error
}

func (
	s messageHistorySources,
) FindByKnowledgeBaseIncludingDeleted(_ context.Context, kbID string) ([]*types.DataSource, error) {
	return s.byKB[kbID], s.err
}

func (s messageHistorySources) HasEverNextcloudSourceForTenant(context.Context, uint64) (bool, error) {
	return false, s.err
}

func TestHistoryFilteringRemovesRevokedAnswerAndSearchPartner(t *testing.T) {
	sources := messageHistorySources{byKB: map[string][]*types.DataSource{
		"source-kb": {{Type: types.ConnectorTypeNextcloud}},
		"web-kb":    {{Type: "web"}},
	}}
	s := &messageService{historyGuard: access.NewNextcloudHistoryGuard(nil, sources, nil)}
	ctx := context.Background()
	question := &types.Message{ID: "q", SessionID: "session", RequestID: "turn", Role: "user", Content: "question"}
	answer := &types.Message{
		ID: "a", SessionID: "session", RequestID: "turn", Role: "assistant", Content: "secret answer",
		ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{"source-kb"}},
	}
	ordinary := &types.Message{
		ID:               "ordinary",
		SessionID:        "session",
		RequestID:        "other",
		Role:             "assistant",
		Content:          "ordinary answer",
		ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{"web-kb"}},
	}
	filtered, err := s.filterHistoryMessages(ctx, []*types.Message{question, answer, ordinary})
	if err != nil || len(filtered) != 2 || filtered[0] != question || filtered[1] != ordinary {
		t.Fatalf("history filter = %#v, err=%v", filtered, err)
	}
	items := []*types.MessageSearchResultItem{
		{MessageWithSession: types.MessageWithSession{Message: *question}},
		{MessageWithSession: types.MessageWithSession{Message: *answer}},
		{MessageWithSession: types.MessageWithSession{Message: *ordinary}},
	}
	search, err := s.filterHistoricalSearchPairs(ctx, items)
	if err != nil || len(search) != 1 || search[0].ID != ordinary.ID {
		t.Fatalf("search filter = %#v, err=%v", search, err)
	}
}

func TestHistoryFilteringDoesNotServeWhenSourceCheckUnavailable(t *testing.T) {
	s := &messageService{historyGuard: access.NewNextcloudHistoryGuard(nil,
		messageHistorySources{err: errors.New("database down")}, nil)}
	_, err := s.filterHistoryMessages(context.Background(), []*types.Message{{
		Role: "assistant", ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{"source-kb"}},
	}})
	if !errors.Is(err, access.ErrNextcloudPublicationUnavailable) {
		t.Fatalf("unavailable verifier must not serve answer: %v", err)
	}
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

func TestHistoryFilteringNeedsDurableKBSourceVerifier(t *testing.T) {
	s := &messageService{historyGuard: access.NewNextcloudHistoryGuard(nil, legacyHistorySourceLookup{}, nil)}
	_, err := s.filterHistoryMessages(
		context.Background(),
		[]*types.Message{{Role: "assistant", ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{
			"kb",
		}}}})
	if !errors.Is(err, access.ErrNextcloudPublicationUnavailable) {
		t.Fatalf("legacy DS-only verifier admitted saved broad history: %v", err)
	}
}
