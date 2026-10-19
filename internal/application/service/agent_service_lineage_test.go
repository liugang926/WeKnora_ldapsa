package service

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/agent/tools"
	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type agentPromptActualModel struct {
	fakeAgentChatModel
	messages []chat.Message
	calls    int
}

func (m *agentPromptActualModel) ChatStream(
	ctx context.Context,
	messages []chat.Message,
	options *chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	m.calls++
	m.messages = append([]chat.Message{}, messages...)
	return m.fakeAgentChatModel.ChatStream(ctx, messages, options)
}

// Install the actual display protocol before journal callbacks are installed.
// The older history/body fixture never creates names and omits this module.
func agentPromptDisplayFixtureSchema(
	t *testing.T,
) func(*gorm.DB, *repository.KnowledgeInputProvenanceStore, context.Context) {
	t.Helper()
	return func(db *gorm.DB, _ *repository.KnowledgeInputProvenanceStore, _ context.Context) {
		if db.Migrator().HasTable("knowledge_display_versions") {
			return
		}
		ddl, err := os.ReadFile("../../../migrations/sqlite/000059_knowledge_collection_display_inputs.up.sql")
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
}

// Start a new actual request for a newly created KB. The body fixture's
// resolved old-KB capability cannot authorize creating a different resource.
func agentPromptActualRequestContext(original context.Context, t *testing.T) context.Context {
	t.Helper()
	caller := types.CallerFromContext(original)
	principal, ok := types.PrincipalFromContext(original)
	require.True(t, ok)
	ctx := types.WithExecutionTenant(
		types.WithPrincipal(types.WithCaller(context.Background(), caller), principal),
		caller.TenantID,
	)
	ctx = context.WithValue(ctx, types.UserIDContextKey, caller.UserID)
	if tenant, ok := types.TenantInfoFromContext(original); ok {
		ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
	}
	if deadline, ok := original.Deadline(); ok {
		current, cancel := context.WithDeadline(ctx, deadline)
		t.Cleanup(cancel)
		ctx = current
	}
	return ctx
}

func TestAgentPromptActualDefaultKBAndDocumentMetadata(t *testing.T) {
	messages, knowledge, store, db, ctx, session, _ := messageBodyIntegrationFixture(
		t,
		false,
		agentPromptDisplayFixtureSchema(t),
	)
	ctx = agentPromptActualRequestContext(ctx, t)
	groups := NewGroupAccessService(repository.NewGroupAccessRepository(db))
	kbs := &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db)}
	knowledge.kbService = kbs
	knowledge.groupAccess = groups
	knowledge.contentLeases = store.ContentLeaseStore()
	knowledge.knowledgeInputs = store
	kb := &types.KnowledgeBase{
		CreatorID:        types.CallerFromContext(ctx).UserID,
		ID:               "agent-prompt-kb",
		TenantID:         7,
		Name:             "Actual retained prompt KB",
		Type:             types.KnowledgeBaseTypeDocument,
		EmbeddingModelID: "embed",
		SummaryModelID:   "summary",
		IndexingStrategy: types.IndexingStrategy{VectorEnabled: true},
		Description:      "UNPROVEN_KB_DESCRIPTION",
		GeneratedProfile: &types.KnowledgeBaseProfile{Gist: "UNPROVEN_PROFILE"},
	}
	producer := repository.WithIndependentKnowledgeBaseNameInput(
		repository.WithIndependentKnowledgeInput(ctx),
		"",
		kb.Name,
	)
	require.NoError(t, store.CreateKnowledgeBaseWithInput(producer, kb))
	document := &types.Knowledge{
		ID:              "agent-prompt-doc",
		TenantID:        7,
		KnowledgeBaseID: kb.ID,
		Type:            types.KnowledgeTypeManual,
		Source:          "manual",
		Channel:         "web",
		Title:           "Actual original selected document",
		FileName:        "actual.md",
		FileType:        "md",
		CustomMetadata:  types.JSON(`{}`),
		Metadata:        types.JSON(`{"manual":{"content":"actual authored document","version":1}}`),
	}
	_, err := store.CreateKnowledgeWithInput(
		repository.WithIndependentKnowledgeInput(ctx),
		document,
		"actual authored document",
	)
	require.NoError(t, err)
	ctx = WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(ctx)
	settings := &types.AgentConfig{
		MaxIterations:  2,
		AllowedTools:   tools.DefaultAllowedTools(),
		KnowledgeBases: []string{kb.ID},
		KnowledgeIDs:   []string{document.ID},
		SearchTargets: types.SearchTargets{
			&types.SearchTarget{Type: types.SearchTargetTypeKnowledgeBase, TenantID: 7, KnowledgeBaseID: kb.ID},
		},
		MCPSelectionMode: "none",
	}
	service := &agentService{
		db:                     db,
		cfg:                    &config.Config{},
		knowledgeService:       knowledge,
		knowledgeBaseService:   kbs,
		messageService:         messages,
		ragChunkSnapshotReader: repository.NewRAGChunkSnapshotReader(db, store),
	}
	model := &agentPromptActualModel{}
	engine, err := service.CreateAgentEngine(
		ctx,
		settings,
		model,
		nil,
		event.NewEventBus(),
		session.ID,
		"agent-metadata-turn",
	)
	require.NoError(t, err)
	guarded, ok := engine.(*agentPromptInputEngine)
	require.True(t, ok)
	guarded.SetSourceLineageVerifier(func(current context.Context, lineage *types.SourceLineage) error {
		return messages.sourceLineageGuard.Check(
			current,
			lineage,
			access.LineageReadScope{Targets: settings.SearchTargets},
		)
	})
	state, err := engine.Execute(ctx, session.ID, "agent-metadata-turn", "answer using the selected knowledge", nil)
	require.NoError(t, err)
	require.True(t, state.IsComplete)
	require.Equal(t, 1, model.calls, "default KB metadata must reach the real Agent model boundary")
	var wire strings.Builder
	for _, message := range model.messages {
		wire.WriteString(message.Content)
	}
	require.Contains(t, wire.String(), kb.Name)
	require.Contains(t, wire.String(), document.Title)
	require.NotContains(t, wire.String(), "doc_count=")
	require.NotContains(t, wire.String(), "UNPROVEN_KB_DESCRIPTION")
	require.NotContains(t, wire.String(), "UNPROVEN_PROFILE")
	require.NoError(t, CheckMessageMaterialOutputBoundary(ctx))
	require.NoError(
		t,
		db.Model(&types.User{}).Where("id=?", types.CallerFromContext(ctx).UserID).Update("is_active", false).Error,
	)
	require.Error(t, CheckMessageMaterialOutputBoundary(ctx), "buffered HTTP output must recheck the current reader")
}

func TestAgentPromptActualBodyAndLineageMutationDeniedAtHTTP(t *testing.T) {
	for _, mutation := range []string{"title", "lineage"} {
		t.Run(mutation, func(t *testing.T) {
			messages, knowledge, store, db, ctx, _, _ := messageBodyIntegrationFixture(
				t,
				false,
				agentPromptDisplayFixtureSchema(t),
			)
			ctx = agentPromptActualRequestContext(ctx, t)
			kbs := &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db)}
			knowledge.kbService = kbs
			knowledge.groupAccess = NewGroupAccessService(repository.NewGroupAccessRepository(db))
			knowledge.contentLeases = store.ContentLeaseStore()
			knowledge.knowledgeInputs = store
			kb := &types.KnowledgeBase{
				CreatorID:        types.CallerFromContext(ctx).UserID,
				ID:               "agent-body-kb",
				TenantID:         7,
				Name:             "Actual body-bound KB",
				Type:             "document",
				EmbeddingModelID: "embed",
				SummaryModelID:   "summary",
				IndexingStrategy: types.IndexingStrategy{VectorEnabled: true},
			}
			producer := repository.WithIndependentKnowledgeBaseNameInput(
				repository.WithIndependentKnowledgeInput(ctx),
				"",
				kb.Name,
			)
			require.NoError(t, store.CreateKnowledgeBaseWithInput(producer, kb))
			ctx = WithMessageMaterialOutputBoundary(ctx)
			defer CloseMessageMaterialOutputBoundary(ctx)
			service := &agentService{knowledgeService: knowledge, messageService: messages}
			guard, err := service.prepareAgentPromptInputs(
				ctx,
				&types.AgentConfig{
					KnowledgeBases: []string{kb.ID},
					SearchTargets: types.SearchTargets{
						&types.SearchTarget{
							Type:            types.SearchTargetTypeKnowledgeBase,
							TenantID:        7,
							KnowledgeBaseID: kb.ID,
						},
					},
				},
			)
			require.NoError(t, err)
			held, err := holdAgentPromptInputs(ctx, guard)
			require.NoError(t, err)
			require.True(t, held)
			if mutation == "title" {
				guard.kbInfos[0].Name = "foreign current label"
			} else {
				guard.kbInfos[0].SourceLineage = types.UnknownSourceLineage()
			}
			require.Error(t, CheckMessageMaterialOutputBoundary(ctx))
		})
	}
}

func TestAgentPromptBuiltinActualFactoryAndOverride(t *testing.T) {
	messages, _, _, db, ctx, session, _ := messageBodyIntegrationFixture(t, false, agentPromptDisplayFixtureSchema(t))
	require.NoError(t, types.LoadBuiltinAgentsConfig("../../../config"))
	require.NoError(t, db.AutoMigrate(&types.CustomAgent{}))
	const original = "ACTUAL_IMAGE_OWNED_AGENT_TEMPLATE"
	cfg := &config.Config{
		PromptTemplates: &config.PromptTemplatesConfig{
			AgentSystemPrompt: []config.PromptTemplate{{ID: "data_analyst", Content: original}},
		},
	}
	builtin := types.GetBuiltinAgentWithContext(ctx, types.BuiltinDataAnalystID, 7)
	require.NotNil(t, builtin)
	actual, _ := cfg.ResolveCustomAgentPrompts(builtin)
	require.Equal(t, original, actual)
	assistant, err := messages.CreateMessage(
		ctx,
		&types.Message{
			SessionID:     session.ID,
			RequestID:     "builtin-prompt-request",
			Role:          "assistant",
			AgentID:       builtin.ID,
			AgentTenantID: 7,
		},
	)
	require.NoError(t, err)
	service := &agentService{db: db, cfg: cfg, messageService: messages}
	settings := &types.AgentConfig{UseCustomSystemPrompt: true, SystemPrompt: original}
	lineage, check, err := service.builtinAgentPromptInput(ctx, settings, session.ID, assistant.ID, original)
	require.NoError(t, err)
	require.NoError(t, lineage.RequireComplete())
	require.NoError(t, check(ctx))
	builtin.Config.SystemPrompt = "UNKNOWN_RETAINED_OVERRIDE"
	require.NoError(t, repository.NewCustomAgentRepository(db).CreateAgent(ctx, builtin))
	require.Error(t, check(ctx), "a persisted builtin-ID override cannot certify unknown retained text")
	_, _, err = service.builtinAgentPromptInput(
		ctx,
		&types.AgentConfig{UseCustomSystemPrompt: true, SystemPrompt: builtin.Config.SystemPrompt},
		session.ID,
		assistant.ID,
		builtin.Config.SystemPrompt,
	)
	require.Error(t, err)
}
