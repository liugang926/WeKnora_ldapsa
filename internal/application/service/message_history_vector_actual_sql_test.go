//go:build sqlite_fts5

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	sqlindex "github.com/Tencent/WeKnora/internal/application/repository/retriever/sqlite"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/logprivacy"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/models/rerank"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	sqlite_vec "github.com/asg017/sqlite-vec-go-bindings/cgo"
	"github.com/stretchr/testify/require"
)

var historyVectorActualVecAuto sync.Once

const historyVectorActualKB = "actual-history-search-kb"
const historyVectorActualIndexPoison = "UNPROVEN_INDEX_BODY_MUST_NEVER_REACH_MODEL_OR_HTTP"

// Only external providers are controlled. Every message, admission, child,
// chunk, index, source check and original read below uses production code.
type historyVectorActualEmbedder struct{ parentChildEmbedder }

func (historyVectorActualEmbedder) BatchEmbed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range out {
		out[i] = []float32{1}
	}
	return out, nil
}
func (e historyVectorActualEmbedder) BatchEmbedWithPool(ctx context.Context, _ embedding.Embedder, texts []string) ([][]float32, error) {
	return e.BatchEmbed(ctx, texts)
}

type historyVectorActualChat struct {
	t       *testing.T
	streams atomic.Int64
}

func (c *historyVectorActualChat) GetModelID() string   { return "actual-history-chat" }
func (c *historyVectorActualChat) GetModelName() string { return "controlled actual history chat" }
func (c *historyVectorActualChat) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	return nil, fmt.Errorf("unexpected non-streaming producer")
}
func (c *historyVectorActualChat) ChatStream(_ context.Context, input []chat.Message, _ *chat.ChatOptions) (<-chan types.StreamResponse, error) {
	var text strings.Builder
	for _, message := range input {
		text.WriteString(message.Content)
	}
	require.Contains(c.t, text.String(), "Alice knows Bob", "actual source text must reach the production prompt before a controlled completion")
	c.streams.Add(1)
	responses := make(chan types.StreamResponse, 1)
	responses <- types.StreamResponse{ResponseType: types.ResponseTypeAnswer, Content: "Bob is named in the actual source used for this controlled completion.", Done: true}
	close(responses)
	return responses, nil
}

type historyVectorActualModels struct {
	interfaces.ModelService
	t                *testing.T
	chatter          *historyVectorActualChat
	lookupHook       func(context.Context)
	lookups          atomic.Int64
	rankCalls        atomic.Int64
	documents        []string
	enrichmentGuards []*MessageMaterialGuard
}

func (m *historyVectorActualModels) GetEmbeddingModel(context.Context, string) (embedding.Embedder, error) {
	return historyVectorActualEmbedder{}, nil
}
func (m *historyVectorActualModels) GetChatModel(ctx context.Context, id string) (chat.Chat, error) {
	if id == m.chatter.GetModelID() {
		return m.chatter, nil
	}
	if guard, ok := ctx.Value(historyMaterialGuardContextKey{}).(*MessageMaterialGuard); ok {
		require.NoError(m.t, guard.Recheck(), "actual original parents must be current before an enrichment model lookup")
		m.enrichmentGuards = append(m.enrichmentGuards, guard)
	}
	return m.ModelService.GetChatModel(ctx, id)
}
func (m *historyVectorActualModels) GetRerankModel(ctx context.Context, _ string) (rerank.Reranker, error) {
	m.lookups.Add(1)
	require.True(m.t, logprivacy.ContentSuppressed(ctx))
	if m.lookupHook != nil {
		m.lookupHook(ctx)
	}
	return historyVectorActualReranker{m}, nil
}

type historyVectorActualReranker struct{ models *historyVectorActualModels }

func (r historyVectorActualReranker) GetModelID() string   { return "actual-history-rerank" }
func (r historyVectorActualReranker) GetModelName() string { return "controlled actual history rerank" }
func (r historyVectorActualReranker) Rerank(ctx context.Context, _ string, documents []string) ([]rerank.RankResult, error) {
	r.models.rankCalls.Add(1)
	require.True(r.models.t, logprivacy.ContentSuppressed(ctx))
	r.models.documents = append([]string{}, documents...)
	out := make([]rerank.RankResult, len(documents))
	for i := range documents {
		require.NotContains(r.models.t, documents[i], historyVectorActualIndexPoison)
		out[i] = rerank.RankResult{Index: i, RelevanceScore: 1 - float64(i)/1000, Document: rerank.DocumentInfo{Text: "PROVIDER_RETURNED_BODY_MUST_BE_IGNORED"}}
	}
	return out, nil
}

type historyVectorActualFixture struct {
	m                  *machinePipelineFixture
	models             *historyVectorActualModels
	base               context.Context
	messages           *messageService
	kb                 *knowledgeBaseService
	source             *types.Knowledge
	sessions           []*types.Session
	questions, answers []*types.Message
	children           []*types.CommittedHistoryKnowledgeInput
	pauseEnrichment    bool
}

type historyVectorActualOptions struct{ Questions, PauseEnrichment bool }

func newHistoryVectorActualFixture(t *testing.T, options ...historyVectorActualOptions) *historyVectorActualFixture {
	t.Helper()
	var option historyVectorActualOptions
	if len(options) > 0 {
		option = options[0]
	}
	historyVectorActualVecAuto.Do(sqlite_vec.Auto)
	m := newMachinePipelineFixtureWithDocument(t, false, "Project Cedar: Alice knows Bob. This actual connector document is used by the controlled RAG producer.", true, true)
	db := m.f.db
	installDocumentOnboardingSchema(t, db)
	collection, err := os.ReadFile("../../../migrations/sqlite/000058_knowledge_collection_checkpoints.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(collection)).Error)
	engines := types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{RetrieverType: types.VectorRetrieverType, RetrieverEngineType: types.SQLiteRetrieverEngineType}, {RetrieverType: types.KeywordsRetrieverType, RetrieverEngineType: types.SQLiteRetrieverEngineType}}}
	require.NoError(t, db.Model(&types.Tenant{}).Where("id=?", 7).Update("retriever_engines", engines).Error)
	registry := retriever.NewRetrieveEngineRegistry(nil, nil)
	require.NoError(t, registry.Register(retriever.NewKVHybridRetrieveEngine(sqlindex.NewSQLiteRetrieveEngineRepository(db), types.SQLiteRetrieverEngineType)))
	models := &historyVectorActualModels{ModelService: m.f.s.modelService, t: t, chatter: &historyVectorActualChat{t: t}}
	m.f.s.modelService = models
	m.f.s.retrieveEngine = registry
	// Install the real producer material/sidecar protocol before sessions.
	if !db.Migrator().HasColumn(&types.Message{}, "artifacts") {
		require.NoError(t, db.Exec("ALTER TABLE messages ADD COLUMN artifacts TEXT").Error)
	}
	for _, name := range []string{"000023_message_artifacts_table.up.sql", "000026_message_artifacts_deleted_at.up.sql"} {
		ddl, err := os.ReadFile("../../../migrations/sqlite/" + name)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
	require.NoError(t, repository.InstallOriginalBodyRetentionTriggers(db))
	require.NoError(t, repository.UpgradeMessageProducerAdvanceSchema(db))
	repository.ConfigureMessageOriginalBodyAvailability(m.f.store, repository.CheckOriginalBodyAvailableInTx)
	source := m.ingest(t)
	for i := 0; i < len(m.f.queue.tasks); i++ {
		require.NoError(t, m.run(t, m.f.queue.tasks[i]), m.f.queue.tasks[i].Type())
	}
	require.NoError(t, db.Where("id=?", source.ID).Take(source).Error)
	require.Equal(t, types.ParseStatusCompleted, source.ParseStatus)
	historyKB := &types.KnowledgeBase{ID: historyVectorActualKB, TenantID: 7, Name: "actual independent history KB", Type: types.KnowledgeBaseTypeDocument,
		EmbeddingModelID: "embed", SummaryModelID: "summary", ChunkingConfig: types.ChunkingConfig{ChunkSize: 512, ChunkOverlap: 0}, IndexingStrategy: types.IndexingStrategy{VectorEnabled: true}}
	if option.Questions {
		historyKB.QuestionGenerationConfig = &types.QuestionGenerationConfig{Enabled: true, QuestionCount: 1}
		m.f.model.detectQuestions = true
	}
	admin := historyVectorActualCaller(t, m, "enrich-human", types.TenantRoleAdmin)
	independent := repository.WithIndependentKnowledgeBaseNameInput(repository.WithIndependentKnowledgeInput(admin), historyKB.ID, historyKB.Name)
	require.NoError(t, m.f.store.CreateKnowledgeBaseWithInput(independent, historyKB))
	// Use a non-manager with a real restricted target grant, so withdrawing
	// only this grant is distinct from revoking original source/owner access.
	require.NoError(t, db.Model(&types.TenantMember{}).Where("tenant_id=? AND user_id=?", 7, "enrich-human").Update("role", types.TenantRoleContributor).Error)
	require.NoError(t, db.Create(&types.DirectoryGroup{ID: "history-target-group", DirectoryID: "machine-ad", ObjectGUID: "history-target-guid", DN: "cn=history-target", DisplayName: "history target", Status: types.DirectoryObjectActive, SnapshotVersion: 1}).Error)
	require.NoError(t, db.Create(&types.DirectoryGroupMembership{DirectoryID: "machine-ad", GroupID: "history-target-group", IdentityID: "machine-ad-user", Source: types.DirectoryMembershipDirect, SnapshotVersion: 1}).Error)
	require.NoError(t, db.Create(&types.ResourceAccessPolicy{ID: "history-target-policy", TenantID: 7, ResourceType: types.GroupResourceTypeKnowledgeBase, ResourceID: historyKB.ID, Mode: types.ResourceAccessRestricted}).Error)
	require.NoError(t, db.Create(&types.ResourceGroupGrant{ID: "history-target-grant", TenantID: 7, ResourceType: types.GroupResourceTypeKnowledgeBase, ResourceID: historyKB.ID, DirectoryGroupID: "history-target-group", Permission: types.ResourcePermissionEdit, Origin: types.GrantOriginManual}).Error)
	// Configure history only after producing source input; the source KB's
	// own original configuration must not be changed to set up this search.
	require.NoError(t, db.Model(&types.Tenant{}).Where("id=?", 7).Updates(map[string]any{
		"chat_history_config": types.ChatHistoryConfig{Enabled: true, EmbeddingModelID: "embed", KnowledgeBaseID: historyKB.ID},
		"retrieval_config":    types.RetrievalConfig{EmbeddingTopK: 20, VectorThreshold: .1, RerankModelID: "actual-history-rerank", RerankTopK: 20, RerankThreshold: .1},
	}).Error)
	ctx := historyVectorActualCaller(t, m, "enrich-human", types.TenantRoleContributor)
	groups := NewGroupAccessService(repository.NewGroupAccessRepository(db))
	kb := &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db), kgRepo: m.f.s.repo, chunkRepo: m.f.s.chunkRepo,
		contentLeases: m.f.store.ContentLeaseStore(), ragChunkSnapshots: repository.NewRAGChunkSnapshotReader(db, m.f.store), ragChunkLineageGuard: m.f.guard,
		modelService: models, retrieveEngine: registry, groupAccess: groups,
		publicationGuard: access.NewNextcloudPublicationGuard(repository.NewDirectoryRepository(db), repository.NewDataSourceRepository(db))}
	m.f.s.publicationGuard = kb.publicationGuard
	messages := &messageService{messageRepo: repository.NewMessageRepository(db), sessionRepo: repository.NewSessionRepository(db),
		sourceLineageGuard: m.f.guard, kbService: kb, knowService: m.f.s, modelService: models}
	ConfigureMessageMaterialProvenance(messages, m.f.store)
	repository.ConfigureKnowledgeInputHistoryVerifier(m.f.store, messages)
	ConfigureKnowledgePostProcessHistoryMaterial(m.f.post, m.f.s)
	f := &historyVectorActualFixture{m: m, models: models, base: ctx, messages: messages, kb: kb, source: source, pauseEnrichment: option.PauseEnrichment}
	for i := 0; i < 2; i++ {
		f.produce(t, i)
	}
	require.EqualValues(t, 2, models.chatter.streams.Load())
	// Corrupt only the untrusted index copy. The actual native candidate
	// query has no body projection; original hydration must still succeed.
	require.NoError(t, db.Table("lite_embeddings").Where("knowledge_base_id=?", historyKB.ID).Update("content", historyVectorActualIndexPoison).Error)
	return f
}

func historyVectorActualCaller(t *testing.T, m *machinePipelineFixture, user string, role types.TenantRole) context.Context {
	t.Helper()
	ctx := types.WithExecutionTenant(context.Background(), 7)
	ctx = types.WithCaller(ctx, types.Caller{TenantID: 7, UserID: user, Role: role})
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: user})
	ctx = context.WithValue(ctx, types.UserIDContextKey, user)
	var tenant types.Tenant
	require.NoError(t, m.f.db.Where("id=?", 7).Take(&tenant).Error)
	return context.WithValue(ctx, types.TenantInfoContextKey, &tenant)
}

// Real RAG prompt/stream/completion and real committed-history/DocumentProcess
// are the only producers of the assistant receipts and child material.
func (f *historyVectorActualFixture) produce(t *testing.T, index int) {
	t.Helper()
	session, err := f.messages.sessionRepo.Create(f.base, &types.Session{ID: fmt.Sprintf("actual-vector-session-%d", index), TenantID: 7, UserID: "enrich-human"})
	require.NoError(t, err)
	question, err := f.messages.CreateMessage(f.base, &types.Message{SessionID: session.ID, RequestID: "shared-request-across-two-sessions", Role: "user", Content: fmt.Sprintf("Original owned question %d: whom does Alice know?", index), IsCompleted: true, ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{f.source.KnowledgeBaseID}}})
	require.NoError(t, err)
	answer, err := f.messages.CreateMessage(f.base, &types.Message{SessionID: session.ID, RequestID: question.RequestID, Role: "assistant", ExecutionContext: question.ExecutionContext})
	require.NoError(t, err)
	producer := WithMessageMaterialOutputBoundary(f.base)
	defer CloseMessageMaterialOutputBoundary(producer)
	_, err = f.messages.GetMessage(producer, session.ID, question.ID)
	require.NoError(t, err)
	require.NoError(t, f.messages.bindInitialMessageMaterial(producer, answer.ID, question.ID, question.RequestID))
	producer = context.WithValue(producer, agentHistoryCurrentRequestKey{}, question.RequestID)
	manager := chatpipeline.NewEventManager()
	chatpipeline.ConfigureSourceLineage(manager, f.m.f.guard)
	chatpipeline.NewPluginMerge(manager, f.m.f.s.chunkRepo, f.m.f.s.chunkService)
	chatpipeline.ConfigureRAGChunkMergeHydration(manager, f.kb)
	chatpipeline.NewPluginIntoChatMessage(manager, f.messages, f.m.f.s)
	chatpipeline.NewPluginChatCompletionStream(manager, f.models)
	cm := &types.ChatManage{}
	cm.TenantID = 7
	cm.SessionID = session.ID
	cm.UserMessageID = question.ID
	cm.MessageID = answer.ID
	cm.Query = question.Content
	cm.UserContent = question.Content
	cm.ChatModelID = f.models.chatter.GetModelID()
	cm.SearchTargets = types.SearchTargets{&types.SearchTarget{Type: types.SearchTargetTypeKnowledgeBase, TenantID: 7, KnowledgeBaseID: f.source.KnowledgeBaseID}}
	cm.SummaryConfig = types.SummaryConfig{Prompt: "Answer from the actual document", ContextTemplate: "Question: {{query}}\nDocument: {{contexts}}", MaxTokens: 512, MaxCompletionTokens: 512}
	producer, err = manager.BeginSourceLineage(producer, cm, access.LineageReadScope{Targets: cm.SearchTargets})
	require.NoError(t, err)
	rows, err := f.kb.HybridSearch(producer, f.source.KnowledgeBaseID, types.SearchParams{QueryText: question.Content, MatchCount: 3, SkipContextEnrichment: true})
	require.NoError(t, err)
	require.NotEmpty(t, rows)
	for _, row := range rows {
		require.Equal(t, f.source.ID, row.KnowledgeID)
		require.NoError(t, row.SourceLineage.RequireComplete())
	}
	cm.SearchResult = rows
	require.Nil(t, manager.Trigger(producer, types.CHUNK_MERGE, cm))
	var streamed strings.Builder
	var finalLineage *types.SourceLineage
	done := false
	bus := event.NewEventBus()
	bus.On(event.EventAgentFinalAnswer, func(_ context.Context, evt event.Event) error {
		data := evt.Data.(event.AgentFinalAnswerData)
		streamed.WriteString(data.Content)
		if data.Done {
			done = true
			finalLineage, _ = evt.SourceLineage.(*types.SourceLineage)
		}
		return nil
	})
	cm.EventBus = bus.AsEventBusInterface()
	require.NoError(t, (&sessionService{eventManager: manager, groupAccess: f.kb.groupAccess}).KnowledgeQAByEvent(producer, cm, []types.EventType{types.INTO_CHAT_MESSAGE, types.CHAT_COMPLETION_STREAM}))
	require.True(t, done)
	require.NotNil(t, finalLineage)
	require.NoError(t, finalLineage.RequireComplete())
	require.NotEmpty(t, finalLineage.Sources)
	answer.Content = streamed.String()
	answer.SourceLineage = finalLineage
	answer.IsCompleted = true
	require.NoError(t, f.messages.UpdateMessage(producer, answer))
	require.NoError(t, CheckMessageMaterialOutputBoundary(producer))
	// This writer re-reads committed parent originals; it does not accept
	// the cached streamed body or a caller-constructed child proof.
	queueStart := len(f.m.f.queue.tasks)
	var target types.KnowledgeBase
	require.NoError(t, f.m.f.db.Where("id=? AND tenant_id=?", historyVectorActualKB, 7).Take(&target).Error)
	grant, err := access.ResolveKB(f.base, access.KBRequest{Caller: types.CallerFromContext(f.base)}, &target, types.OrgRoleEditor, nil, nil)
	require.NoError(t, err)
	child, err := f.m.f.s.CreateCommittedHistoryKnowledgeInput(grant.Context(f.base), types.CommittedHistoryKnowledgeInputRequest{TenantID: 7, KnowledgeBaseID: historyVectorActualKB, SessionID: session.ID, AssistantMessageID: answer.ID})
	require.NoError(t, err)
	require.NotNil(t, child.Proof)
	for i := queueStart; i < len(f.m.f.queue.tasks); i++ {
		task := f.m.f.queue.tasks[i]
		switch task.Type() {
		case types.TypeDocumentProcess:
			require.NoError(t, f.m.f.s.ProcessDocument(context.Background(), task))
		case types.TypeKnowledgePostProcess:
			require.NoError(t, f.m.f.post.Handle(context.Background(), task))
		case types.TypeSummaryGeneration:
			if !f.pauseEnrichment {
				require.NoError(t, f.m.f.s.ProcessSummaryGeneration(context.Background(), task))
			}
		case types.TypeQuestionGeneration:
			if !f.pauseEnrichment {
				require.NoError(t, f.m.f.s.ProcessQuestionGeneration(context.Background(), task))
			}
		default:
			t.Fatalf("unexpected history child worker task %s", task.Type())
		}
	}
	var current types.Knowledge
	require.NoError(t, f.m.f.db.Where("id=?", child.Knowledge.ID).Take(&current).Error)
	if f.pauseEnrichment {
		require.Equal(t, types.ParseStatusFinalizing, current.ParseStatus)
	} else {
		require.Equal(t, types.ParseStatusCompleted, current.ParseStatus)
	}
	require.Equal(t, "enabled", current.EnableStatus)
	var count int64
	require.NoError(t, f.m.f.db.Model(&types.Chunk{}).Where("knowledge_id=?", current.ID).Count(&count).Error)
	require.Positive(t, count)
	f.sessions = append(f.sessions, session)
	f.questions = append(f.questions, question)
	f.answers = append(f.answers, answer)
	f.children = append(f.children, child)
}

func (f *historyVectorActualFixture) activeLeases(t *testing.T, kbID string) int64 {
	t.Helper()
	var count int64
	q := f.m.f.db.Table("nextcloud_content_leases").Where("tenant_id=? AND released_at_ms IS NULL AND expires_at_ms>?", 7, time.Now().UnixMilli())
	if kbID != "" {
		q = q.Where("knowledge_base_id=?", kbID)
	}
	require.NoError(t, q.Count(&count).Error)
	return count
}

// These HTTP assertions exercise the actual service-owned output boundary and
// JSON writer. Handler/JWT router coverage belongs in package handler, which
// cannot be imported by a service-package test without an import cycle.
func (f *historyVectorActualFixture) httpSearch(t *testing.T, afterModel func(context.Context, *types.MessageSearchResult)) *httptest.ResponseRecorder {
	t.Helper()
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/messages/search", nil).WithContext(f.base)
	http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, _, check, releaseHTTP := BeginMessageMaterialHTTPBoundary(r.Context())
		defer releaseHTTP()
		producer, releaseModel, external := retainMessageMaterialProducer(ctx)
		require.True(t, external)
		result, err := f.messages.SearchMessages(producer, &types.MessageSearchParams{Query: "Whom does Alice know?", Mode: types.MessageSearchModeVector, Limit: 20})
		releaseModel()
		if err != nil {
			http.Error(w, "Search refused", http.StatusForbidden)
			return
		}
		markMessageMaterialReady(ctx)
		require.Positive(t, f.activeLeases(t, historyVectorActualKB), "child KB physical fence must outlive the model owner")
		if afterModel != nil {
			afterModel(ctx, result)
		}
		if err := check(); err != nil {
			http.Error(w, "Search output refused", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(result))
	}).ServeHTTP(response, request)
	return response
}

func TestHistoryVectorActualSQLFreshMultiSessionOriginalsAndHTTPLease(t *testing.T) {
	f := newHistoryVectorActualFixture(t)
	ids, err := f.messages.materialStore.OwnedHistoryKnowledgeIDs(f.base, 7, historyVectorActualKB, "enrich-human", nil)
	require.NoError(t, err)
	require.Len(t, ids, 2)
	f.models.lookupHook = func(ctx context.Context) {
		// Each capture requires its own exact actual parent plan. Re-reading
		// both under the model context proves the union survives KB hydration.
		for _, child := range f.children {
			snapshot, err := f.messages.materialStore.CaptureKnowledgeOutputInputs(ctx, types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: historyVectorActualKB, KnowledgeID: child.Knowledge.ID})
			require.NoError(t, err)
			require.Equal(t, child.Proof.AdmissionID, snapshot.Proof.AdmissionID)
		}
	}
	response := f.httpSearch(t, func(ctx context.Context, result *types.MessageSearchResult) {
		require.Equal(t, 2, result.Total)
		require.NoError(t, CheckMessageMaterialOutputBoundary(ctx), "actual pool of one must complete final nonrecursive checks")
	})
	require.Equal(t, http.StatusOK, response.Code)
	var result types.MessageSearchResult
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	require.Equal(t, 2, result.Total)
	bySession := map[string]*types.MessageSearchGroupItem{}
	for _, item := range result.Items {
		require.NotNil(t, item)
		bySession[item.SessionID] = item
	}
	require.Len(t, bySession, 2)
	for i, session := range f.sessions {
		item := bySession[session.ID]
		require.NotNil(t, item)
		require.Equal(t, f.questions[i].RequestID, item.RequestID)
		require.Equal(t, f.questions[i].Content, item.QueryContent)
		require.Equal(t, f.answers[i].Content, item.AnswerContent)
	}
	require.EqualValues(t, 1, f.models.lookups.Load())
	require.EqualValues(t, 1, f.models.rankCalls.Load())
	joined := strings.Join(f.models.documents, "\n")
	for _, question := range f.questions {
		require.Contains(t, joined, question.Content)
	}
	require.NotContains(t, joined, historyVectorActualIndexPoison)
	require.NotContains(t, response.Body.String(), historyVectorActualIndexPoison)
	require.NotContains(t, response.Body.String(), "PROVIDER_RETURNED_BODY_MUST_BE_IGNORED")
	require.Zero(t, f.activeLeases(t, ""), "HTTP release must close original and child broad/exact fences")
}

func TestHistoryVectorActualSQLSourceRevokedDuringModelLookupRefusesHTTP(t *testing.T) {
	f := newHistoryVectorActualFixture(t)
	f.models.lookupHook = func(context.Context) { f.m.allow.Store(false) }
	response := f.httpSearch(t, nil)
	require.Equal(t, http.StatusForbidden, response.Code)
	require.EqualValues(t, 1, f.models.lookups.Load())
	require.Zero(t, f.models.rankCalls.Load(), "revoked actual source must be rechecked after model lookup and before any Rerank documents")
	for _, answer := range f.answers {
		require.NotContains(t, response.Body.String(), answer.Content)
	}
	require.Zero(t, f.activeLeases(t, ""))
}

func TestHistoryVectorActualSQLTargetOnlyGrantRevokedAtHTTPWriter(t *testing.T) {
	f := newHistoryVectorActualFixture(t)
	response := f.httpSearch(t, func(ctx context.Context, result *types.MessageSearchResult) {
		require.Equal(t, 2, result.Total)
		require.NoError(t, f.m.f.db.Where("id=?", "history-target-grant").Delete(&types.ResourceGroupGrant{}).Error)
		require.True(t, f.m.allow.Load(), "original NC source remains authorized")
		// Re-read originals independently under their original source KB.
		// Target index policy must not make the original conversation itself
		// the proof of access to a separate history search KB.
		read := WithMessageMaterialOutputBoundary(f.base)
		defer CloseMessageMaterialOutputBoundary(read)
		for i, answer := range f.answers {
			row, err := f.messages.GetMessage(read, f.sessions[i].ID, answer.ID)
			require.NoError(t, err)
			require.Equal(t, answer.Content, row.Content)
		}
		require.NoError(t, CheckMessageMaterialOutputBoundary(read))
		require.Error(t, CheckMessageMaterialOutputBoundary(ctx), "target-only current authorization must survive through the final sender checkpoint")
	})
	require.Equal(t, http.StatusForbidden, response.Code)
	require.EqualValues(t, 1, f.models.rankCalls.Load(), "this case revokes after successful model/service work")
	for _, answer := range f.answers {
		require.NotContains(t, response.Body.String(), answer.Content)
	}
	require.Zero(t, f.activeLeases(t, ""))
}

func TestHistoryVectorActualSQLChildBodyChangedAtHTTPWriter(t *testing.T) {
	f := newHistoryVectorActualFixture(t)
	response := f.httpSearch(t, func(ctx context.Context, result *types.MessageSearchResult) {
		require.Equal(t, 2, result.Total)
		var chunk types.Chunk
		require.NoError(t, f.m.f.db.Where("knowledge_id=? AND chunk_type=?", f.children[0].Knowledge.ID, types.ChunkTypeText).Take(&chunk).Error)
		// An unadmitted SQL mutation changes the actual child's body/version,
		// while original parent messages, source grants and owner remain valid.
		require.NoError(t, f.m.f.db.Model(&types.Chunk{}).Where("id=?", chunk.ID).Update("content", "UNADMITTED CHILD REPLACEMENT").Error)
		require.True(t, f.m.allow.Load())
		require.Error(t, CheckMessageMaterialOutputBoundary(ctx), "final sender must recheck exact actual child body")
	})
	require.Equal(t, http.StatusForbidden, response.Code)
	for _, answer := range f.answers {
		require.NotContains(t, response.Body.String(), answer.Content)
	}
	require.Zero(t, f.activeLeases(t, ""))
}

func TestHistoryVectorActualSQLRetainedBridgeCannotDropInputsAtHTTPWriter(t *testing.T) {
	for _, mode := range []string{"nil-child", "deleted-child", "nil-kb-authorizer", "mutated-snapshot"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistoryVectorActualFixture(t)
			response := f.httpSearch(t, func(ctx context.Context, result *types.MessageSearchResult) {
				require.Equal(t, 2, result.Total)
				slot := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
				slot.mu.Lock()
				held := append([]*historyVectorMaterialBridge{}, slot.HistoryVectorReads...)
				slot.mu.Unlock()
				require.Len(t, held, 1)
				bridge := held[0]
				id := f.children[0].Knowledge.ID
				switch mode {
				case "nil-child":
					bridge.children[id] = nil
				case "deleted-child":
					delete(bridge.children, id)
				case "nil-kb-authorizer":
					bridge.kbCurrent = nil
				case "mutated-snapshot":
					require.NotNil(t, bridge.children[id])
					require.NotEmpty(t, bridge.children[id].Chunks)
					bridge.children[id].Chunks[0].Content = "UNADMITTED RETAINED SNAPSHOT REPLACEMENT"
				}
				require.Error(t, CheckMessageMaterialOutputBoundary(ctx), "held inputs cannot weaken into capture-phase optional inputs")
			})
			require.Equal(t, http.StatusForbidden, response.Code)
			for _, answer := range f.answers {
				require.NotContains(t, response.Body.String(), answer.Content)
			}
			require.Zero(t, f.activeLeases(t, ""))
		})
	}
}

func TestHistoryVectorActualSQLCurrentOwnedMetadataRejectsOwnerABA(t *testing.T) {
	f := newHistoryVectorActualFixture(t)
	first := f.sessions[0]
	// The same current owner string after ABA cannot replace the original
	// admitted owner epoch. Filter its child before native TopK is evaluated.
	require.NoError(t, f.m.f.db.Model(&types.Session{}).Where("id=?", first.ID).Update("user_id", "different-current-owner").Error)
	require.NoError(t, f.m.f.db.Model(&types.Session{}).Where("id=?", first.ID).Update("user_id", "enrich-human").Error)
	ids, err := f.messages.materialStore.OwnedHistoryKnowledgeIDs(f.base, 7, historyVectorActualKB, "enrich-human", nil)
	require.NoError(t, err)
	require.Equal(t, []string{f.children[1].Knowledge.ID}, ids)
	current := WithMessageMaterialOutputBoundary(f.base)
	defer CloseMessageMaterialOutputBoundary(current)
	result, err := f.messages.SearchMessages(current, &types.MessageSearchParams{Query: "Whom does Alice know?", Mode: types.MessageSearchModeVector, Limit: 20})
	require.NoError(t, err)
	require.Equal(t, 1, result.Total)
	require.Equal(t, f.sessions[1].ID, result.Items[0].SessionID)
	require.Equal(t, f.questions[1].Content, result.Items[0].QueryContent)
	require.NotContains(t, strings.Join(f.models.documents, "\n"), f.questions[0].Content)
	require.NoError(t, CheckMessageMaterialOutputBoundary(current))
}
