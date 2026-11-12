package service

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/agent"
	"github.com/Tencent/WeKnora/internal/agent/tools"
	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type agentPromptActualModel struct {
	fakeAgentChatModel
	messages []chat.Message
	calls    int
}

type agentPromptMCPDirectory struct {
	interfaces.MCPServiceService
}

func (d *agentPromptMCPDirectory) ListMCPServicesByIDs(
	_ context.Context, _ uint64, ids []string,
) ([]*types.MCPService, error) {
	result := make([]*types.MCPService, 0, len(ids))
	for _, id := range ids {
		result = append(result, &types.MCPService{
			ID: id, Name: "Actual prompt MCP", Description: "Server-resolved MCP metadata",
		})
	}
	return result, nil
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
		Description:      "Description of the selected knowledge base",
		GeneratedProfile: &types.KnowledgeBaseProfile{Gist: "Profile of the selected knowledge base"},
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
	_, wrapped := engine.(*agentPromptInputEngine)
	require.False(t, wrapped, "ordinary KB metadata must not enter the Nextcloud material guard")
	direct, ok := engine.(*agent.AgentEngine)
	require.True(t, ok)
	direct.SetSourceLineageVerifier(func(current context.Context, lineage *types.SourceLineage) error {
		if current == nil || current.Err() != nil {
			return access.ErrNextcloudPublicationUnavailable
		}
		return lineage.RequireComplete()
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
	require.Contains(t, wire.String(), "doc_count=")
	require.Contains(t, wire.String(), kb.Description)
	require.Contains(t, wire.String(), kb.GeneratedProfile.Gist)
}

func TestAgentPromptMaterialHoldRequiresExplicitInputMaterials(t *testing.T) {
	inputs := &agentPromptInputGuard{
		bases: []*repository.KnowledgeBasePromptInputSnapshot{{}},
	}
	check := func(context.Context) error { return nil }

	require.False(
		t,
		shouldHoldAgentPromptInputs(context.Background(), inputs, nil),
		"an ordinary KB prompt without explicit materials must use the direct engine",
	)
	require.False(
		t,
		shouldHoldAgentPromptInputs(
			withAgentExplicitInputMaterials(context.Background(), false),
			inputs,
			check,
		),
		"a server-owned prompt alone is not a Nextcloud material",
	)
	require.True(
		t,
		shouldHoldAgentPromptInputs(
			withAgentExplicitInputMaterials(context.Background(), true),
			inputs,
			nil,
		),
		"explicit materials must keep retained prompt producers held",
	)
}

func TestAgentPinnedMetadataIsSourceIndependent(t *testing.T) {
	ctx := types.WithExecutionTenant(
		context.Background(),
		7,
	)
	settings := &types.AgentConfig{
		MCPSelectionMode:    "none",
		PinnedMCPServiceIDs: []string{"mcp-prompt"},
		PinnedSkillNames:    []string{"skill-prompt"},
		TenantSkills: []*types.TenantSkillEntity{{
			Name: "skill-prompt", Description: "Server-resolved pinned skill metadata",
		}},
	}
	service := &agentService{
		mcpServiceService: &agentPromptMCPDirectory{},
	}
	lineage := sourceIndependentPromptLineage()
	require.NoError(t, lineage.RequireComplete())
	mcpServices, skills := service.resolveSourceIndependentPinnedMentions(
		ctx,
		settings,
		lineage,
	)
	require.Len(t, mcpServices, 1)
	require.Len(t, skills, 1)
	require.NoError(t, mcpServices[0].SourceLineage.RequireComplete())
	require.NoError(t, skills[0].SourceLineage.RequireComplete())
}

func TestAgentPromptQuestionOriginIsAHintNotSelectedDocumentInput(t *testing.T) {
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
		ID:               "question-origin-kb",
		TenantID:         7,
		Name:             "Question origin KB",
		Type:             types.KnowledgeBaseTypeDocument,
		EmbeddingModelID: "embed",
		SummaryModelID:   "summary",
		IndexingStrategy: types.IndexingStrategy{VectorEnabled: true},
	}
	require.NoError(t, store.CreateKnowledgeBaseWithInput(
		repository.WithIndependentKnowledgeBaseNameInput(
			repository.WithIndependentKnowledgeInput(ctx),
			"",
			kb.Name,
		),
		kb,
	))
	originDocument := &types.Knowledge{
		ID:              "question-origin-document",
		TenantID:        7,
		KnowledgeBaseID: kb.ID,
		Type:            types.KnowledgeTypeManual,
		Source:          "manual",
		Channel:         "web",
		Title:           "Suggested question source",
		FileName:        "suggested.md",
		FileType:        "md",
		CustomMetadata:  types.JSON(`{}`),
		Metadata:        types.JSON(`{"manual":{"content":"hint only","version":1}}`),
	}
	require.NoError(t, db.Create(originDocument).Error)
	service := &agentService{
		db:                   db,
		cfg:                  &config.Config{},
		knowledgeService:     knowledge,
		knowledgeBaseService: kbs,
		messageService:       messages,
	}
	configWithOrigin := &types.AgentConfig{
		KnowledgeBases: []string{kb.ID},
		KnowledgeIDs:   nil,
		SearchTargets: types.SearchTargets{
			&types.SearchTarget{
				Type:            types.SearchTargetTypeKnowledgeBase,
				TenantID:        7,
				KnowledgeBaseID: kb.ID,
			},
		},
		QuestionOrigin: &types.QuestionOrigin{
			KnowledgeBaseID: kb.ID,
			KnowledgeID:     originDocument.ID,
		},
	}

	guard, err := service.prepareAgentPromptInputs(ctx, configWithOrigin)
	require.NoError(t, err, "a suggestion origin must not require an original-material receipt")
	require.NotNil(t, guard)
	require.NotNil(t, guard.origin)
	require.NotNil(t, guard.origin.Document)
	require.Equal(t, originDocument.Title, guard.origin.Document.Title)
	require.Empty(t, guard.selectedDocs)
	require.NoError(t, guard.Close())

	configWithOrigin.KnowledgeIDs = []string{originDocument.ID}
	guard, err = service.prepareAgentPromptInputs(ctx, configWithOrigin)
	require.NoError(t, err)
	require.Len(t, guard.selectedDocs, 1)
	require.Equal(t, originDocument.ID, guard.selectedDocs[0].KnowledgeID)
	require.NoError(t, guard.Close())

	configWithOrigin.SearchTargets[0].KnowledgeBaseID = "different-kb"
	_, err = service.prepareAgentPromptInputs(ctx, configWithOrigin)
	require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
}

func TestAgentPromptActualBodyAndLineageMutationDeniedAtHTTP(t *testing.T) {
	for _, mutation := range []string{"title", "lineage"} {
		t.Run(mutation, func(t *testing.T) {
			messages, knowledge, store, db, ctx, session, _ := messageBodyIntegrationFixture(
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
			ctx = withAgentExplicitInputMaterials(ctx, true)
			custom := &types.CustomAgent{ID: "body-agent", TenantID: kb.TenantID}
			custom.Config.SystemPrompt = "Original server-selected prompt"
			require.NoError(t, db.AutoMigrate(&types.CustomAgent{}))
			require.NoError(t, db.Create(custom).Error)
			assistant, err := messages.CreateMessage(ctx, &types.Message{
				SessionID: session.ID, RequestID: "body-request", Role: "assistant",
				AgentID: custom.ID, AgentTenantID: custom.TenantID,
			})
			require.NoError(t, err)
			service := &agentService{
				db: db, cfg: &config.Config{},
				knowledgeBaseService: kbs, knowledgeService: knowledge, messageService: messages,
				ragChunkSnapshotReader: repository.NewRAGChunkSnapshotReader(db, store),
			}
			settings := &types.AgentConfig{
				UseCustomSystemPrompt: true, SystemPrompt: custom.Config.SystemPrompt,
				KnowledgeBases: []string{kb.ID}, MCPSelectionMode: "none",
				SearchTargets: types.SearchTargets{{
					Type: types.SearchTargetTypeKnowledgeBase, TenantID: kb.TenantID, KnowledgeBaseID: kb.ID,
				}},
			}
			engine, err := service.CreateAgentEngine(
				ctx, settings, &agentPromptActualModel{}, nil, event.NewEventBus(), session.ID, assistant.ID,
			)
			require.NoError(t, err)
			wrapped, ok := engine.(*agentPromptInputEngine)
			require.True(t, ok)
			guard := wrapped.inputs
			require.True(t, guard.held)
			require.NoError(t, CheckMessageMaterialOutputBoundary(ctx))
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
	_, overrideCheck, err := service.builtinAgentPromptInput(
		ctx,
		&types.AgentConfig{UseCustomSystemPrompt: true, SystemPrompt: builtin.Config.SystemPrompt},
		session.ID,
		assistant.ID,
		builtin.Config.SystemPrompt,
	)
	require.NoError(t, err)
	require.NoError(t, overrideCheck(ctx))
}
