package service

import (
	"context"
	"errors"
	"testing"
	"time"

	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

// reproUnderstandFailModel reproduces the production signature recorded in
// r27a: the non-streaming Chat call (query rewrite) fails while ChatStream
// works. The understand failure is tolerated and the turn continues.
type reproUnderstandFailModel struct {
	streamCalls int
}

func (m *reproUnderstandFailModel) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	return nil, errors.New("rewrite endpoint 503")
}

func (m *reproUnderstandFailModel) ChatStream(
	_ context.Context, _ []chat.Message, _ *chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	m.streamCalls++
	ch := make(chan types.StreamResponse, 2)
	ch <- types.StreamResponse{ResponseType: types.ResponseTypeAnswer, Content: "fallback answer"}
	ch <- types.StreamResponse{ResponseType: types.ResponseTypeAnswer, Content: "", Done: true}
	close(ch)
	return ch, nil
}

func (m *reproUnderstandFailModel) GetModelName() string { return "repro" }
func (m *reproUnderstandFailModel) GetModelID() string   { return "repro-model" }

// reproKBService keeps the fallback document listing empty; the prompt then
// renders without a KB section, which is all the repro needs.
type reproKBService struct {
	interfaces.KnowledgeBaseService
}

func (reproKBService) GetKnowledgeBaseByID(context.Context, string) (*types.KnowledgeBase, error) {
	return nil, errors.New("no kb in repro")
}

// reproSearchNothingPlugin forces the consolidated retrieval stage to report
// ErrSearchNothing, exactly like an empty knowledge base (or an embedding
// outage) does in production.
type reproSearchNothingPlugin struct{}

func (p *reproSearchNothingPlugin) ActivationEvents() []types.EventType {
	return []types.EventType{types.CHUNK_SEARCH_PARALLEL}
}

func (p *reproSearchNothingPlugin) OnEvent(
	_ context.Context, _ types.EventType, _ *types.ChatManage, _ func() *chatpipeline.PluginError,
) *chatpipeline.PluginError {
	return chatpipeline.ErrSearchNothing
}

// TestKnowledgeQAFallbackSilenceRepro locks the r27 defect: with an
// ACL-mode lineage turn (ordinary WeKnora-authorized chat), a tolerated
// query-understand failure followed by an empty retrieval must still stream a
// marked fallback answer. Before the fix the model fallback stub returned
// without emitting anything, leaving the assistant message incomplete forever.
func TestKnowledgeQAFallbackSilenceRepro(t *testing.T) {
	model := &reproUnderstandFailModel{}
	models := &stubModelService{
		chatModel: model,
		modelsByID: map[string]*types.Model{
			"repro-chat": {ID: "repro-chat", TenantID: 7, Type: types.ModelTypeKnowledgeQA},
		},
	}
	cfg := &config.Config{Conversation: &config.ConversationConfig{
		MaxRounds:        2,
		FallbackStrategy: "model",
		FallbackPrompt:   "Answer the latest user question: {{query}}",
		Summary:          &config.SummaryConfig{Prompt: "p", ContextTemplate: "{{query}}"},
	}}

	manager := chatpipeline.NewEventManager()
	authorize := func(context.Context, *types.SourceLineage) error { return nil }
	chatpipeline.ConfigureSourceLineage(manager, nil)
	chatpipeline.NewPluginQueryUnderstand(manager, models, nil, nil, cfg)
	manager.Register(&reproSearchNothingPlugin{})
	chatpipeline.NewPluginChatCompletionStream(manager, models)

	svc := &sessionService{
		cfg:                  cfg,
		modelService:         models,
		eventManager:         manager,
		knowledgeBaseService: reproKBService{},
	}

	bus := event.NewEventBus()
	done := make(chan string, 1)
	bus.On(event.EventAgentFinalAnswer, func(_ context.Context, evt event.Event) error {
		if data, ok := evt.Data.(event.AgentFinalAnswerData); ok && data.Done {
			done <- data.Content
		}
		return nil
	})

	cm := &types.ChatManage{}
	cm.SessionID, cm.Query = "session", "设备发放登记表的用途"
	cm.ChatModelID = "repro-chat"
	cm.EnableRewrite = true
	cm.FallbackStrategy = types.FallbackStrategyModel
	cm.FallbackPrompt = cfg.Conversation.FallbackPrompt
	cm.EventBus = bus.AsEventBusInterface()
	cm.SearchTargets = types.SearchTargets{{
		Type: types.SearchTargetTypeKnowledgeBase, TenantID: 7, KnowledgeBaseID: "kb",
	}}

	ctx := types.WithRAGSourceLineage(
		context.Background(), types.NewWeKnoraACLSourceLineage(authorize),
	)

	err := svc.KnowledgeQAByEvent(ctx, cm, []types.EventType{
		types.QUERY_UNDERSTAND, types.CHUNK_SEARCH_PARALLEL,
	})
	require.NoError(t, err, "empty retrieval must fail neither closed nor open")

	select {
	case content := <-done:
		require.NotEmpty(t, content, "a marked fallback answer must reach the event bus")
	case <-time.After(3 * time.Second):
		t.Fatal("REPRODUCED: fallback emitted nothing — assistant would stay incomplete forever")
	}
}
