package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/models/rerank"
	"github.com/Tencent/WeKnora/internal/sandbox"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type safeCurrentTurnEngine struct {
	interfaces.AgentEngine
	contexts        [][]chat.Message
	queries         []string
	checkpointCalls int
}

func (
	e *safeCurrentTurnEngine,
) Execute(_ context.Context, _, _, query string, history []chat.Message, _ ...[]string) (*types.AgentState, error) {
	e.contexts = append(e.contexts, history)
	e.queries = append(e.queries, query)
	return &types.AgentState{IsComplete: true}, nil
}

func (e *safeCurrentTurnEngine) SetContextCheckpointSink(types.ContextCheckpointSink) {
	e.checkpointCalls++
}

type safeCurrentTurnAgent struct {
	interfaces.AgentService
	engine        *safeCurrentTurnEngine
	memoryEnabled *bool
	excluded      bool
	sandboxCalls  int
}

func (
	a *safeCurrentTurnAgent,
) CreateAgentEngine(
	_ context.Context,
	cfg *types.AgentConfig,
	_ chat.Chat,
	_ rerank.Reranker,
	_ *event.EventBus,
	_, _ string,
) (
	interfaces.AgentEngine,
	error,
) {
	a.memoryEnabled = cfg.MemoryEnabled
	a.excluded = cfg.ExcludeHistoricalContext
	return a.engine, nil
}

func (
	a *safeCurrentTurnAgent,
) sessionSandboxInputStore(context.Context, string, string) (sandbox.SessionFileStore, error) {
	a.sandboxCalls++
	return nil, nil
}

func (
	a *safeCurrentTurnAgent,
) stageSessionAttachments(context.Context, string, string, uint64, types.MessageAttachments) (
	[]stagedSessionAttachment,
	error,
) {
	return nil, nil
}

func TestAgentHistoryAdmissionRejectedContextStillRunsCurrentAgentQA(t *testing.T) {
	for _, scenario := range []string{"ever-source", "lookup-unavailable", "missing-policy"} {
		t.Run(scenario, func(t *testing.T) {
			sources := &instanceHistorySources{ever: scenario == "ever-source"}
			if scenario == "lookup-unavailable" {
				sources.err = errors.New("source state unavailable")
			}
			guard := access.NewNextcloudHistoryGuard(nil, sources, nil)
			if scenario == "missing-policy" {
				guard = nil
			}
			repo := &historyRepo{rows: storedTurns(
				5,
			), checkpoint: checkpointOn(storedTurns(5)[1], "secret old checkpoint")}
			engine := &safeCurrentTurnEngine{}
			agentSvc := &safeCurrentTurnAgent{engine: engine}
			svc := &sessionService{
				cfg:          &config.Config{},
				historyGuard: guard,
				messageRepo:  repo,
				agentService: agentSvc,
				modelService: &stubModelService{
					modelsByID: map[string]*types.Model{
						"chat": {
							ID:   "chat",
							Type: types.ModelTypeKnowledgeQA,
						},
					},
				},
			}
			ctx := context.WithValue(context.WithValue(context.Background(), types.TenantIDContextKey, uint64(
				1,
			)), types.TenantInfoContextKey, &types.Tenant{ID: 1})
			req := &types.QARequest{
				Session: &types.Session{
					ID:       "session",
					TenantID: 1,
				},
				AssistantMessageID: "new",
				Query:              "new user question",
				TurnLeaseHeld:      true,
				CustomAgent: &types.CustomAgent{
					ID:       "agent",
					TenantID: 1,
					Config: types.CustomAgentConfig{
						ModelID:             "chat",
						KBSelectionMode:     "none",
						WebSearchProviderID: "unused",
						AllowedTools: []string{
							"thinking",
						}, MultiTurnEnabled: true,
					},
				},
			}
			require.NoError(t, svc.AgentQA(ctx, req, event.NewEventBus()))
			require.Len(t, engine.contexts, 1)
			require.Empty(t, engine.contexts[0])
			require.Equal(t, []string{"new user question"}, engine.queries)
			require.Zero(t, repo.pages)
			require.Zero(t, repo.checkpointReads)
			require.Zero(t, engine.checkpointCalls)
			require.NotNil(t, agentSvc.memoryEnabled)
			require.False(t, *agentSvc.memoryEnabled)
			require.True(t, agentSvc.excluded)
			require.Zero(t, agentSvc.sandboxCalls)
		})
	}
}
