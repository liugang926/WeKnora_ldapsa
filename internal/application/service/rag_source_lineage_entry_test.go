package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type ragEntryModel struct {
	mu     sync.Mutex
	inputs [][]chat.Message
}

func (m *ragEntryModel) Chat(_ context.Context, messages []chat.Message, _ *chat.ChatOptions) (*types.ChatResponse, error) {
	m.mu.Lock()
	m.inputs = append(m.inputs, messages)
	m.mu.Unlock()
	return &types.ChatResponse{Content: `{"rewrite_query":"paraphrase the prior answer","intent":"follow_up"}`}, nil
}
func (m *ragEntryModel) ChatStream(_ context.Context, messages []chat.Message, _ *chat.ChatOptions) (<-chan types.StreamResponse, error) {
	m.mu.Lock()
	m.inputs = append(m.inputs, messages)
	m.mu.Unlock()
	responses := make(chan types.StreamResponse, 1)
	responses <- types.StreamResponse{ResponseType: types.ResponseTypeAnswer, Content: "A paraphrase without citations", Done: true}
	close(responses)
	return responses, nil
}
func (m *ragEntryModel) GetModelID() string   { return "model" }
func (m *ragEntryModel) GetModelName() string { return "model" }
func (m *ragEntryModel) calls() int           { m.mu.Lock(); defer m.mu.Unlock(); return len(m.inputs) }

type ragEntryModels struct {
	interfaces.ModelService
	model *ragEntryModel
}

func (s ragEntryModels) GetChatModel(context.Context, string) (chat.Chat, error) { return s.model, nil }

type ragEntryMessages struct {
	interfaces.MessageService
	rows []*types.Message
}

func (s *ragEntryMessages) GetRecentMessagesBySession(context.Context, string, int) ([]*types.Message, error) {
	return s.rows, nil
}

type ragEntryKnowledge struct{ interfaces.KnowledgeService }

func (ragEntryKnowledge) GetKnowledgeByIDOnly(context.Context, string) (*types.Knowledge, error) {
	return &types.Knowledge{ID: "knowledge", TenantID: 7, KnowledgeBaseID: "kb", Channel: types.ConnectorTypeNextcloud}, nil
}
func (ragEntryKnowledge) CheckKnowledgePublication(context.Context, *types.Knowledge) error {
	return nil
}

func ragEntryProof(t *testing.T) *types.SourceLineage {
	t.Helper()
	proof, err := types.NewCompleteSourceLineage(types.SourceIdentity{Provider: types.ConnectorTypeNextcloud, TenantID: 7,
		KnowledgeBaseID: "kb", DataSourceID: "ds", PairOperationID: "pair", InstanceID: "instance", BindingID: "binding",
		FileID: "1", ExternalID: "nextcloud:instance:1", KnowledgeID: "knowledge", Revision: 2, ETag: "original"})
	require.NoError(t, err)
	return proof
}

// This exercises the real service event loop, prompt renderer, retained-history
// loader and streaming plugin. The retrieval receipt is supplied as a trusted
// reader fixture; independent hydration tests verify its DB/version coupling.
func TestRAGSourceLineageActualEntryKeepsCitationFreeNoSearchDependency(t *testing.T) {
	proof := ragEntryProof(t)
	revoked := false
	check := func(_ context.Context, lineage *types.SourceLineage) error {
		if lineage.RequireComplete() != nil || revoked && len(lineage.Sources) > 0 {
			return access.ErrNextcloudPublicationDenied
		}
		return nil
	}
	model := &ragEntryModel{}
	history := &ragEntryMessages{}
	manager := chatpipeline.NewEventManager()
	chatpipeline.NewPluginLoadHistory(manager, history, &config.Config{})
	understandConfig := &config.Config{Conversation: &config.ConversationConfig{}}
	understandConfig.Conversation.RewritePromptUser = "{{conversation}}\n{{query}}"
	chatpipeline.NewPluginQueryUnderstand(manager, ragEntryModels{model: model}, history, nil, understandConfig)
	chatpipeline.NewPluginIntoChatMessage(manager, history, ragEntryKnowledge{})
	chatpipeline.NewPluginChatCompletionStream(manager, ragEntryModels{model: model})
	svc := &sessionService{eventManager: manager}
	newTurn := func(cm *types.ChatManage) (context.Context, <-chan *types.Message) {
		bus := event.NewEventBus()
		done := make(chan *types.Message, 1)
		bus.On(event.EventAgentFinalAnswer, func(_ context.Context, evt event.Event) error {
			data := evt.Data.(event.AgentFinalAnswerData)
			if data.Done {
				lineage, ok := evt.SourceLineage.(*types.SourceLineage)
				require.True(t, ok)
				done <- &types.Message{Role: "assistant", Content: data.Content, IsCompleted: true, RequestID: "prior", SourceLineage: lineage}
			}
			return nil
		})
		cm.EventBus = bus.AsEventBusInterface()
		return types.WithRAGSourceLineage(context.Background(), types.NewRAGSourceLineage(check)), done
	}
	wait := func(done <-chan *types.Message) *types.Message {
		select {
		case message := <-done:
			return message
		case <-time.After(3 * time.Second):
			t.Fatal("actual stream did not complete")
			return nil
		}
	}
	first := &types.ChatManage{}
	first.SessionID, first.Query = "session", "read this document"
	noCitations := false
	first.CitationEnabled = &noCitations
	first.SummaryConfig.ContextTemplate = "{{query}}\n{{contexts}}"
	first.MergeResult = []*types.SearchResult{{ID: "chunk", KnowledgeID: "knowledge", KnowledgeBaseID: "kb", Content: "private source detail", SourceLineage: proof}}
	ctx, done := newTurn(first)
	require.NoError(t, svc.KnowledgeQAByEvent(ctx, first, []types.EventType{types.INTO_CHAT_MESSAGE, types.CHAT_COMPLETION_STREAM}))
	savedFirst := wait(done)
	require.Equal(t, proof, savedFirst.SourceLineage)
	require.NotContains(t, savedFirst.Content, "[1]")
	model.mu.Lock()
	require.Contains(t, model.inputs[0][len(model.inputs[0])-1].Content, "private source detail")
	model.mu.Unlock()
	// The later turn has no retrieval, no references and no selected KB. Its
	// actual retained assistant prose still carries the original dependency.
	history.rows = []*types.Message{{Role: "user", RequestID: "prior", Content: first.Query, CreatedAt: time.Now()}, savedFirst}
	second := &types.ChatManage{}
	second.SessionID, second.Query, second.UserContent, second.MaxRounds = "session", "paraphrase", "paraphrase", 1
	second.EnableRewrite = true
	second.CitationEnabled = &noCitations
	ctx, done = newTurn(second)
	require.NoError(t, svc.KnowledgeQAByEvent(ctx, second, []types.EventType{types.LOAD_HISTORY, types.QUERY_UNDERSTAND, types.INTO_CHAT_MESSAGE, types.CHAT_COMPLETION_STREAM}))
	require.False(t, second.NeedsRetrieval(), "real rewrite intent did not take the no-search path")
	savedSecond := wait(done)
	require.Empty(t, savedSecond.KnowledgeReferences)
	require.Equal(t, proof, savedSecond.SourceLineage)
	model.mu.Lock()
	for _, message := range append(append([]chat.Message{}, model.inputs[1]...), model.inputs[2]...) {
		require.Equal(t, proof, message.SourceLineage)
	}
	require.Contains(t, model.inputs[1][1].Content, savedFirst.Content)
	require.Contains(t, model.inputs[2][2].Content, savedFirst.Content)
	model.mu.Unlock()
	revoked = true
	third := &types.ChatManage{}
	third.SessionID, third.Query, third.UserContent, third.MaxRounds = "session", "again", "again", 1
	ctx, _ = newTurn(third)
	require.ErrorIs(t, svc.KnowledgeQAByEvent(ctx, third, []types.EventType{types.LOAD_HISTORY, types.CHAT_COMPLETION_STREAM}), access.ErrNextcloudPublicationDenied)
	require.Equal(t, 3, model.calls(), "revoked historical prose reached a model")
	require.ErrorIs(t, check(context.Background(), savedFirst.SourceLineage), access.ErrNextcloudPublicationDenied)
	require.ErrorIs(t, check(context.Background(), savedSecond.SourceLineage), access.ErrNextcloudPublicationDenied)
	plain := &types.ChatManage{}
	plain.SessionID, plain.Query, plain.UserContent = "fresh", "hello", "hello"
	ctx, done = newTurn(plain)
	require.NoError(t, svc.KnowledgeQAByEvent(ctx, plain, []types.EventType{types.CHAT_COMPLETION_STREAM}))
	require.Empty(t, wait(done).SourceLineage.Sources)
}

func TestRAGSourceLineageActualModelBoundaryRejectsUncoveredInputs(t *testing.T) {
	for _, adapter := range []string{"legacy history", "memory", "quote", "image", "caption"} {
		t.Run(adapter, func(t *testing.T) {
			model := &ragEntryModel{}
			manager := chatpipeline.NewEventManager()
			chatpipeline.NewPluginChatCompletionStream(manager, ragEntryModels{model: model})
			cm := &types.ChatManage{}
			cm.Query, cm.UserContent = "hello", "hello"
			cm.EventBus = event.NewEventBus().AsEventBusInterface()
			switch adapter {
			case "legacy history":
				cm.History = []*types.History{{Query: "q", Answer: "secret"}}
			case "memory":
				cm.MemoryPrompt = "secret"
			case "quote":
				cm.QuotedContext = "secret"
			case "image":
				cm.Images = []string{"resource://secret"}
			case "caption":
				cm.ImageDescription = "secret"
			}
			ctx := types.WithRAGSourceLineage(context.Background(), types.NewRAGSourceLineage(func(context.Context, *types.SourceLineage) error { return nil }))
			err := (&sessionService{eventManager: manager}).KnowledgeQAByEvent(ctx, cm, []types.EventType{types.CHAT_COMPLETION_STREAM})
			require.Error(t, err)
			require.Equal(t, 0, model.calls())
		})
	}
}
