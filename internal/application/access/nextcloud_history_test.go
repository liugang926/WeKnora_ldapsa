package access

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
)

type historicalKnowledgeStub struct {
	rows map[string]*types.Knowledge
}

func (s historicalKnowledgeStub) GetKnowledgeByIDOnly(_ context.Context, id string) (*types.Knowledge, error) {
	return s.rows[id], nil
}

type historicalSourceStub struct {
	byKB map[string][]*types.DataSource
	ever bool
	err  error
}

func (
	s historicalSourceStub,
) FindByKnowledgeBaseIncludingDeleted(_ context.Context, kbID string) ([]*types.DataSource, error) {
	return s.byKB[kbID], s.err
}

func (s historicalSourceStub) HasEverNextcloudSourceForTenant(_ context.Context, _ uint64) (bool, error) {
	return s.ever, s.err
}

func TestNextcloudHistoryGuardRechecksSavedReference(t *testing.T) {
	publication, ctx, knowledge := publicationFixture(t)
	knowledge.ID = "document-1"
	history := &NextcloudHistoryGuard{
		knowledge: historicalKnowledgeStub{rows: map[string]*types.Knowledge{knowledge.ID: knowledge}},
		sources:   historicalSourceStub{}, publication: publication,
	}
	message := &types.Message{
		Role: "assistant", Content: "saved source answer",
		KnowledgeReferences: types.References{{
			KnowledgeID:      knowledge.ID,
			KnowledgeChannel: types.ConnectorTypeNextcloud,
		}},
	}
	if err := history.CheckMessage(ctx, message); err != nil {
		t.Fatalf("current source grant should allow saved answer: %v", err)
	}
	publication.authorize = func(context.Context, *types.DataSourceConfig, string, string, string, string, int64) (
		nextcloud.AuthorizationDecision,
		error,
	) {
		return nextcloud.AuthorizationDecision{Allow: false, Reason: "source_not_readable"}, nil
	}
	if err := history.CheckMessage(ctx, message); !errors.Is(err, ErrNextcloudPublicationDenied) {
		t.Fatalf("withdrawn source answer should be denied, got %v", err)
	}
}

func TestNextcloudHistoryGuardDeniesBroadAndLegacyScopes(t *testing.T) {
	ctx := types.WithCaller(context.Background(), types.Caller{TenantID: 12, UserID: "person-1"})
	history := &NextcloudHistoryGuard{
		knowledge: historicalKnowledgeStub{},
		sources: historicalSourceStub{
			byKB: map[string][]*types.DataSource{
				"nextcloud-kb": {{Type: types.ConnectorTypeNextcloud}},
				"ordinary-kb":  {{Type: "web"}},
			},
			ever: true,
		},
	}
	for _, message := range []*types.Message{
		{Role: "assistant", ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{
			"nextcloud-kb",
		}}},
		{Role: "assistant", AgentID: "legacy-agent", AgentTenantID: 12},
		{Role: "assistant", AgentID: "all-agent", AgentTenantID: 12, ExecutionContext: types.MessageExecutionContext{
			AgentKBSelectionMode: "all",
		}},
	} {
		if err := history.CheckMessage(ctx, message); !errors.Is(err, ErrNextcloudPublicationDenied) {
			t.Fatalf("ambiguous Nextcloud answer should be denied: %v", err)
		}
	}
	for _, message := range []*types.Message{
		{Role: "user", Content: "my own question"},
		{Role: "assistant", ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{"ordinary-kb"}}},
		{Role: "assistant", AgentID: "none-agent", AgentTenantID: 12, ExecutionContext: types.MessageExecutionContext{
			AgentKBSelectionMode: "none",
		}},
	} {
		if err := history.CheckMessage(ctx, message); err != nil {
			t.Fatalf("ordinary answer should remain readable: %v", err)
		}
	}
}

func TestNextcloudHistoryGuardFailsClosedForMissingSourceAndLookupFailure(t *testing.T) {
	publication, ctx, _ := publicationFixture(t)
	history := &NextcloudHistoryGuard{
		knowledge: historicalKnowledgeStub{rows: map[string]*types.Knowledge{}},
		sources:   historicalSourceStub{}, publication: publication,
	}
	message := &types.Message{Role: "assistant", KnowledgeReferences: types.References{{
		KnowledgeID: "removed-document", KnowledgeChannel: types.ConnectorTypeNextcloud,
	}}}
	if err := history.CheckMessage(ctx, message); !errors.Is(err, ErrNextcloudPublicationDenied) {
		t.Fatalf("removed source must stay hidden, got %v", err)
	}
	history.sources = historicalSourceStub{err: errors.New("database unavailable")}
	message = &types.Message{Role: "assistant", ExecutionContext: types.MessageExecutionContext{
		KnowledgeBaseIDs: []string{"nextcloud-kb"},
	}}
	if err := history.CheckMessage(ctx, message); !errors.Is(err, ErrNextcloudPublicationUnavailable) {
		t.Fatalf("source lookup failure must fail closed, got %v", err)
	}
}

func TestNextcloudHistoryGuardLiveRechecksSourceVersionBetweenChunks(t *testing.T) {
	publication, ctx, knowledge := publicationFixture(t)
	knowledge.ID = "live-document"
	history := &NextcloudHistoryGuard{
		knowledge: historicalKnowledgeStub{rows: map[string]*types.Knowledge{knowledge.ID: knowledge}},
		sources: historicalSourceStub{byKB: map[string][]*types.DataSource{
			"kb-1": {{Type: types.ConnectorTypeNextcloud}},
		}},
		publication: publication,
	}
	message := &types.Message{Role: "assistant", ExecutionContext: types.MessageExecutionContext{
		KnowledgeBaseIDs: []string{"kb-1"},
	}}
	if err := history.CheckLiveMessage(ctx, message); !errors.Is(err, ErrNextcloudPublicationDenied) {
		t.Fatalf("answer before provenance should be held, got %v", err)
	}
	message.KnowledgeReferences = types.References{{KnowledgeID: knowledge.ID}}
	if err := history.CheckLiveMessage(ctx, message); err != nil {
		t.Fatalf("current source should stream: %v", err)
	}
	publication.authorize = func(context.Context, *types.DataSourceConfig, string, string, string, string, int64) (
		nextcloud.AuthorizationDecision,
		error,
	) {
		return nextcloud.AuthorizationDecision{Allow: true, Reason: "authorized", SourceETag: "new-version"}, nil
	}
	if err := history.CheckLiveMessage(ctx, message); !errors.Is(err, ErrNextcloudPublicationDenied) {
		t.Fatalf("changed source version should stop next chunk, got %v", err)
	}
	message.ExecutionContext.KnowledgeBaseIDs = []string{"ordinary-kb"}
	message.KnowledgeReferences = nil
	if err := history.CheckLiveMessage(ctx, message); err != nil {
		t.Fatalf("ordinary KB should stream normally: %v", err)
	}
}

func (s historicalSourceStub) HasEverNextcloudSourceGlobally(context.Context) (bool, error) {
	return false, s.err
}

type globallyMarkedLiveSources struct{ historicalSourceStub }

func (globallyMarkedLiveSources) HasEverNextcloudSourceGlobally(context.Context) (bool, error) {
	return true, nil
}

func TestNextcloudLiveAgentWithoutHistoryPreservesOrdinaryAnswer(t *testing.T) {
	sources := globallyMarkedLiveSources{}
	guard := &NextcloudHistoryGuard{sources: sources, instanceSources: sources}
	ctx := types.WithCaller(context.Background(), types.Caller{TenantID: 12, UserID: "person"})
	message := &types.Message{
		Role:          "assistant",
		AgentID:       "agent",
		AgentTenantID: 12,
		ExecutionContext: types.MessageExecutionContext{
			AgentKBSelectionMode: "none",
		},
	}
	if err := guard.CheckLiveMessage(ctx, message); err != nil {
		t.Fatalf("ordinary live no-history turn denied: %v", err)
	}
	if err := guard.CheckMessage(ctx, message); !errors.Is(err, ErrNextcloudPublicationDenied) {
		t.Fatalf("saved same shape must remain blocked: %v", err)
	}
	message.AgentSteps = types.AgentSteps{{ToolCalls: []types.ToolCall{{
		ID:   types.PipelineToolCallIDPrefix + "step",
		Name: "search_knowledge",
	}}}}
	if err := guard.CheckMessage(ctx, message); err != nil {
		t.Fatalf("ordinary synthetic KnowledgeQA timeline denied: %v", err)
	}
}

func (s historicalSourceStub) HasEverNextcloudSourceForKnowledgeBase(_ context.Context, id string) (bool, error) {
	for _, source := range s.byKB[id] {
		if source != nil && source.Type == types.ConnectorTypeNextcloud {
			return true, s.err
		}
	}
	return false, s.err
}
