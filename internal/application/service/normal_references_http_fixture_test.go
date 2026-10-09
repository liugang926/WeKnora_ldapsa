package service

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// This test-only bridge lets the external HTTP tests use the real signed
// machine ingestion fixture without introducing a service -> handler cycle.
type NormalReferencesHTTPFixtureForTest struct {
	DB       *gorm.DB
	Store    *repository.KnowledgeInputProvenanceStore
	Ctx      context.Context
	Session  *types.Session
	Sessions interfaces.SessionService
	Messages interfaces.MessageService
	KBs      interfaces.KnowledgeBaseService
	Models   interfaces.ModelService
	Config   *config.Config
	Lineage  *types.SourceLineage
	Revoke   func()
	Cancel   func()
}

type normalReferencesModelsForTest struct {
	interfaces.ModelService
	final chat.Chat
}

func (m normalReferencesModelsForTest) GetModelByID(context.Context, string) (*types.Model, error) {
	return &types.Model{ID: "normal-reference-model", TenantID: 7, Name: "controlled final model", Type: types.ModelTypeKnowledgeQA, Status: types.ModelStatusActive}, nil
}
func (m normalReferencesModelsForTest) ListModels(context.Context) ([]*types.Model, error) {
	return []*types.Model{}, nil
}
func (m normalReferencesModelsForTest) GetChatModel(context.Context, string) (chat.Chat, error) {
	return m.final, nil
}
func (m normalReferencesModelsForTest) GetEmbeddingModel(context.Context, string) (embedding.Embedder, error) {
	return parentChildEmbedder{}, nil
}

func NewNormalReferencesHTTPFixtureForTest(t *testing.T, pg bool, final chat.Chat, modes ...string) *NormalReferencesHTTPFixtureForTest {
	t.Helper()
	mode := "current"
	if len(modes) > 0 {
		mode = modes[0]
	}
	m := newMachinePipelineFixtureWithDocument(t, pg, "Current employee policy: read the approved record, then ask before changing another department's access.")
	k := m.ingest(t)
	for i := 0; i < len(m.f.queue.tasks); i++ {
		require.NoError(t, m.run(t, m.f.queue.tasks[i]))
	}
	var chunks []*types.Chunk
	require.NoError(t, m.f.db.Where("knowledge_id=?", k.ID).Order("chunk_index").Find(&chunks).Error)
	require.Len(t, chunks, 2, "actual signed production sync generated text and summary")
	migration := m.f.db
	directory := "sqlite"
	names := []string{"000023_message_artifacts_table.up.sql", "000026_message_artifacts_deleted_at.up.sql", "000070_message_original_materials.up.sql"}
	artifactType := "TEXT"
	if pg {
		directory, artifactType = "versioned", "JSONB"
		names = []string{"000103_message_artifacts_table.up.sql", "000107_message_artifacts_deleted_at.up.sql", "000151_message_original_materials.up.sql"}
		pool, err := m.f.db.DB()
		require.NoError(t, err)
		migration, err = gorm.Open(postgres.New(postgres.Config{Conn: pool, PreferSimpleProtocol: true}), &gorm.Config{})
		require.NoError(t, err)
		require.NoError(t, migration.Exec("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" WITH SCHEMA public").Error)
		require.NoError(t, migration.Exec("ALTER EXTENSION \"uuid-ossp\" SET SCHEMA public").Error)
		require.NoError(t, migration.Exec("CREATE FUNCTION uuid_generate_v4() RETURNS uuid LANGUAGE sql AS $$ SELECT public.uuid_generate_v4() $$").Error)
	}
	if !m.f.db.Migrator().HasColumn(&types.Message{}, "artifacts") {
		require.NoError(t, migration.Exec("ALTER TABLE messages ADD COLUMN artifacts "+artifactType).Error)
	}
	for _, name := range names {
		ddl, err := os.ReadFile("../../../migrations/" + directory + "/" + name)
		require.NoError(t, err)
		require.NoError(t, migration.Exec(string(ddl)).Error)
	}
	require.NoError(t, repository.InstallOriginalBodyRetentionTriggers(m.f.db))
	models := normalReferencesModelsForTest{final: final}
	engine := &fakeRetrieveEngineService{engineType: types.PostgresRetrieverEngineType, support: []types.RetrieverType{types.VectorRetrieverType},
		canned: []*types.IndexWithScore{{ChunkID: chunks[0].ID, KnowledgeID: k.ID, Score: 1}, {ChunkID: chunks[1].ID, KnowledgeID: k.ID, Score: .9}}}
	if mode == "empty-fixed" || mode == "empty-fixed-refuse" {
		engine.canned = nil
	}
	groups := NewGroupAccessService(repository.NewGroupAccessRepository(m.f.db))
	publication := access.NewNextcloudPublicationGuard(repository.NewDirectoryRepository(m.f.db), repository.NewDataSourceRepository(m.f.db))
	m.f.s.publicationGuard = publication
	kbs := &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(m.f.db), kgRepo: m.f.s.repo, chunkRepo: m.f.s.chunkRepo,
		contentLeases: m.f.store.ContentLeaseStore(), ragChunkSnapshots: repository.NewRAGChunkSnapshotReader(m.f.db, m.f.store), ragChunkLineageGuard: m.f.guard,
		modelService: models, retrieveEngine: parentChildRetrieveRegistry{engine: engine}, groupAccess: groups,
		publicationGuard: publication}
	cfg := &config.Config{Conversation: &config.ConversationConfig{MaxRounds: 3, FallbackStrategy: "fixed", FallbackResponse: "CURRENT FIXED SAFE FALLBACK", EmbeddingTopK: 2, RerankTopK: 2, EnableRewrite: false,
		Summary: &config.SummaryConfig{Prompt: "Answer using the admitted current sources", ContextTemplate: "{{query}}\n{{contexts}}"}}}
	tenantService := normalReferencesTenantForTest{db: m.f.db}
	messages := &messageService{messageRepo: repository.NewMessageRepository(m.f.db), sessionRepo: repository.NewSessionRepository(m.f.db), sourceLineageGuard: m.f.guard, kbService: kbs, knowService: m.f.s, modelService: models}
	ConfigureMessageMaterialProvenance(messages, m.f.store)
	repository.ConfigureMessageOriginalBodyAvailability(m.f.store, repository.CheckOriginalBodyAvailableInTx)
	manager := chatpipeline.NewEventManager()
	chatpipeline.ConfigureSourceLineage(manager, m.f.guard)
	chatpipeline.NewPluginLoadHistory(manager, messages, cfg)
	chatpipeline.NewPluginMemoryRecall(manager, nil)
	chatpipeline.NewPluginQueryUnderstand(manager, models, messages, nil, cfg)
	chatpipeline.NewPluginSearchParallel(manager, kbs, m.f.s, m.f.s.chunkService, cfg, nil, tenantService, nil, nil, nil, nil, m.f.s.chunkRepo, m.f.s.repo)
	chatpipeline.NewPluginRerank(manager, models)
	chatpipeline.NewPluginMerge(manager, m.f.s.chunkRepo, m.f.s.chunkService)
	chatpipeline.ConfigureRAGChunkMergeHydration(manager, kbs)
	chatpipeline.NewPluginFilterTopK(manager)
	into := chatpipeline.NewPluginIntoChatMessage(manager, messages, m.f.s)
	manager.Register(normalReferencesCheckpointForTest{base: into, hook: func(ctx context.Context) {
		switch mode {
		case "source-before-refs":
			m.personal.Store(false)
		case "actor-before-refs":
			require.NoError(t, m.f.db.Model(&types.User{}).Where("id=?", types.CallerFromContext(ctx).UserID).Update("is_active", false).Error)
		case "lease-before-refs":
			require.NoError(t, m.f.db.Exec("UPDATE nextcloud_content_leases SET expires_at_ms=1 WHERE kind='read' AND released_at_ms IS NULL").Error)
		}
	}})
	chatpipeline.NewPluginChatCompletionStream(manager, models)
	sessions := &sessionService{cfg: cfg, sessionRepo: repository.NewSessionRepository(m.f.db), messageRepo: repository.NewMessageRepository(m.f.db), knowledgeBaseService: kbs,
		knowledgeService: m.f.s, chunkService: m.f.s.chunkService, modelService: models, tenantService: tenantService, eventManager: manager, busyGate: NewSessionBusyGate(), groupAccess: groups}
	ConfigureMessageSourceLineage(messages, sessions, &SessionForkService{}, manager, m.f.guard)
	ctx := context.WithValue(m.f.ctx, types.UserIDContextKey, types.CallerFromContext(m.f.ctx).UserID)
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	t.Cleanup(cancel)
	var tenant types.Tenant
	require.NoError(t, m.f.db.Where("id=7").Take(&tenant).Error)
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, &tenant)
	session := &types.Session{ID: "normal-references-session", TenantID: 7, UserID: types.CallerFromContext(ctx).UserID}
	require.NoError(t, m.f.db.Create(session).Error)
	original, err := m.f.store.CaptureKnowledgeOutputInputs(ctx, types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID})
	require.NoError(t, err)
	return &NormalReferencesHTTPFixtureForTest{DB: m.f.db, Store: m.f.store, Ctx: ctx, Session: session, Sessions: normalReferencesSessionForTest{SessionService: sessions, mode: mode}, Messages: messages, KBs: kbs, Models: models, Config: cfg, Lineage: original.Proof.SourceLineage, Revoke: func() { m.personal.Store(false) }, Cancel: cancel}
}

type normalReferencesTenantForTest struct {
	interfaces.TenantService
	db *gorm.DB
}

func (s normalReferencesTenantForTest) GetTenantByID(ctx context.Context, id uint64) (*types.Tenant, error) {
	var tenant types.Tenant
	err := s.db.WithContext(ctx).Where("id=?", id).Take(&tenant).Error
	return &tenant, err
}

// The checkpoint delegates to the actual prompt producer before changing current
// authority. It does not construct an event or replace any source proof.
type normalReferencesCheckpointForTest struct {
	base *chatpipeline.PluginIntoChatMessage
	hook func(context.Context)
}

func (p normalReferencesCheckpointForTest) ActivationEvents() []types.EventType {
	return []types.EventType{types.INTO_CHAT_MESSAGE}
}
func (p normalReferencesCheckpointForTest) OnEvent(ctx context.Context, kind types.EventType, cm *types.ChatManage, next func() *chatpipeline.PluginError) *chatpipeline.PluginError {
	return p.base.OnEvent(ctx, kind, cm, func() *chatpipeline.PluginError { p.hook(ctx); return next() })
}

type normalReferencesSessionForTest struct {
	interfaces.SessionService
	mode string
}

func (s normalReferencesSessionForTest) KnowledgeQA(ctx context.Context, req *types.QARequest, bus *event.EventBus) error {
	if s.mode == "unknown-prefix" {
		// Explicit unproven prior-output negative. The real producer must never
		// clear the strict consumer's unknown prefix when its own proof arrives.
		_ = bus.Emit(ctx, event.Event{Type: event.EventAgentToolCall, SessionID: req.Session.ID, Data: event.AgentToolCallData{ToolCallID: "unproven-old-prefix", ToolName: "unproven-old-prefix"}})
	}
	if s.mode == "references-refuse" || s.mode == "thinking-refuse" {
		kind := event.EventAgentReferences
		if s.mode == "empty-fixed-refuse" {
			kind = event.EventAgentFinalAnswer
		}
		if s.mode == "thinking-refuse" {
			kind = event.EventAgentThought
		}
		bus.On(kind, func(context.Context, event.Event) error { return errors.New("controlled actual subscriber refusal") })
	}
	return s.SessionService.KnowledgeQA(ctx, req, bus)
}
