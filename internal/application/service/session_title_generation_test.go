package service

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type titleGenerationModel struct {
	messages []chat.Message
	content  string
}

func (m *titleGenerationModel) Chat(
	_ context.Context, messages []chat.Message, _ *chat.ChatOptions,
) (*types.ChatResponse, error) {
	m.messages = messages
	if m.content != "" {
		return &types.ChatResponse{Content: m.content}, nil
	}
	return &types.ChatResponse{Content: "海底电力电缆寿命与短路电流计算方法及其工程应用分析"}, nil
}

func (*titleGenerationModel) ChatStream(
	context.Context, []chat.Message, *chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	panic("title generation must use non-streaming chat")
}

func (*titleGenerationModel) GetModelName() string { return "configured model" }
func (*titleGenerationModel) GetModelID() string   { return "qa-model" }

type titleGenerationModels struct {
	interfaces.ModelService
	model           *titleGenerationModel
	selectedID      string
	availableModels []*types.Model
}

func (m *titleGenerationModels) ListModels(context.Context) ([]*types.Model, error) {
	if m.availableModels != nil {
		return m.availableModels, nil
	}
	return []*types.Model{{ID: "qa-model", Type: types.ModelTypeKnowledgeQA, Status: types.ModelStatusActive}}, nil
}

func (m *titleGenerationModels) GetChatModel(_ context.Context, id string) (chat.Chat, error) {
	m.selectedID = id
	return m.model, nil
}

func newTitleGenerationFixture(t *testing.T, initialTitle string) (*sessionService, *types.Session, context.Context) {
	t.Helper()
	svc, db := newTestSessionService(t)
	require.NoError(t, db.AutoMigrate(&testListSessionsIMChannelSession{}))
	svc.cfg = &config.Config{Conversation: &config.ConversationConfig{}}
	proof, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	session := &types.Session{TenantID: 7, UserID: "reader", Title: initialTitle, TitleSourceLineage: proof}
	require.NoError(t, db.Create(session).Error)
	return svc, session, ragReadContext()
}

func TestGenerateTitleSummarizesFirstUserMessageWithConfiguredModel(t *testing.T) {
	for _, requestedModel := range []string{"", "selected-model"} {
		t.Run("model="+requestedModel, func(t *testing.T) {
			model := &titleGenerationModel{}
			models := &titleGenerationModels{model: model}
			svc, session, ctx := newTitleGenerationFixture(t, "")
			svc.modelService = models
			messages := []types.Message{
				{Role: "system", Content: "system context"},
				{Role: "user", Content: "请分析海缆寿命与短路电流"},
				{Role: "assistant", Content: "previous answer"},
				{Role: "user", Content: "later question"},
			}
			title, err := svc.GenerateTitle(ctx, session, messages, requestedModel)
			require.NoError(t, err)
			assert.Equal(t, "海底电力电缆寿命与短路电流计算方法及其工程应用分", title)
			stored, err := svc.sessionRepo.Get(ctx, session.TenantID, session.UserID, session.ID)
			require.NoError(t, err)
			assert.Equal(t, title, stored.Title)
			assert.Equal(t, "reader", stored.UserID)
			require.NoError(t, stored.TitleSourceLineage.RequireComplete())
			wantModel := requestedModel
			if wantModel == "" {
				wantModel = "qa-model"
			}
			assert.Equal(t, wantModel, models.selectedID)
			require.Len(t, model.messages, 2)
			assert.Contains(t, model.messages[1].Content, messages[1].Content)
			assert.NotContains(t, model.messages[1].Content, "later question")
		})
	}
}

func TestGenerateTitlePreservesExistingTitle(t *testing.T) {
	svc, session, ctx := newTitleGenerationFixture(t, "My project")
	session.Title = "" // A stale caller must not overwrite a title that is already stored.
	title, err := svc.GenerateTitle(ctx, session, nil, "")
	require.NoError(t, err)
	assert.Equal(t, "My project", title)
}

func TestGenerateTitleAsyncUsesWorkspaceUtilityDefault(t *testing.T) {
	for _, explicit := range []string{"", "task-model"} {
		t.Run("explicit="+explicit, func(t *testing.T) {
			models := &titleGenerationModels{model: &titleGenerationModel{}, availableModels: []*types.Model{
				{ID: "chat", Type: types.ModelTypeKnowledgeQA, Status: types.ModelStatusActive, IsDefault: true},
				{ID: "utility", Type: types.ModelTypeKnowledgeQA, Status: types.ModelStatusActive},
			}}
			(&types.DefaultSettings{UtilityModelID: "utility"}).ApplyModelDefaults(models.availableModels)
			svc, session, ctx := newTitleGenerationFixture(t, "")
			svc.modelService = models
			bus := event.NewEventBus()
			emitted := make(chan event.Event, 1)
			bus.On(event.EventSessionTitle, func(_ context.Context, evt event.Event) error {
				emitted <- evt
				return nil
			})
			svc.GenerateTitleAsync(ctx, session, "hello", explicit, bus)
			select {
			case evt := <-emitted:
				assert.Equal(t, session.ID, evt.SessionID)
				assert.NotEmpty(t, evt.Data.(event.SessionTitleData).Title)
				assert.Empty(t, session.Title, "the request retains its own session snapshot")
			case <-time.After(5 * time.Second):
				t.Fatal("committed title was not published")
			}
			want := explicit
			if want == "" {
				want = "utility"
			}
			assert.Equal(t, want, models.selectedID)
		})
	}
}

func TestGenerateTitlePersistsJSONTitleValue(t *testing.T) {
	model := &titleGenerationModel{content: `{"title":"Agent search tools"}`}
	svc, session, ctx := newTitleGenerationFixture(t, "")
	svc.modelService = &titleGenerationModels{model: model}
	messages := []types.Message{{Role: "user", Content: "Original question"}}
	title, err := svc.GenerateTitle(ctx, session, messages, "qa-model")
	require.NoError(t, err)
	assert.Equal(t, "Agent search tools", title)
	stored, err := svc.sessionRepo.Get(ctx, session.TenantID, session.UserID, session.ID)
	require.NoError(t, err)
	assert.Equal(t, title, stored.Title)
	assert.Equal(t, title, session.Title)
}
