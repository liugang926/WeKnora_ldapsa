package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/models/vlm"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

type enrichmentTestModel struct {
	calls        int
	beforeReturn func()
	questions    bool
}

func (m *enrichmentTestModel) Chat(_ context.Context, messages []chat.Message, _ *chat.ChatOptions) (*types.ChatResponse, error) {
	m.calls++
	if m.beforeReturn != nil {
		m.beforeReturn()
	}
	text := "Actual grounded document summary"
	if m.questions {
		text = "What is the actual body of this document?\nHow does the neighboring content apply?"
	}
	return &types.ChatResponse{Content: text}, nil
}
func (m *enrichmentTestModel) ChatStream(context.Context, []chat.Message, *chat.ChatOptions) (<-chan types.StreamResponse, error) {
	return nil, nil
}
func (m *enrichmentTestModel) GetModelName() string { return "local mock" }
func (m *enrichmentTestModel) GetModelID() string   { return "summary" }

type enrichmentTestVLM struct {
	calls        int
	beforeReturn func()
}

func (m *enrichmentTestVLM) Predict(_ context.Context, images [][]byte, prompt string) (string, error) {
	m.calls++
	if m.beforeReturn != nil {
		m.beforeReturn()
	}
	return "Actual recognized document image text for retrieval", nil
}
func (m *enrichmentTestVLM) GetModelName() string { return "local mock vision" }
func (m *enrichmentTestVLM) GetModelID() string   { return "vision" }

type enrichmentTestEngine struct{ *parentChildRetrieveEngine }

func (e *enrichmentTestEngine) DeleteByChunkIDList(context.Context, []string, int, string) error {
	return nil
}

type enrichmentTestModels struct {
	interfaces.ModelService
	chat   *enrichmentTestModel
	vision *enrichmentTestVLM
}

func (m enrichmentTestModels) GetChatModel(context.Context, string) (chat.Chat, error) {
	return m.chat, nil
}
func (m enrichmentTestModels) GetVLMModel(context.Context, string) (vlm.VLM, error) {
	return m.vision, nil
}
func (m enrichmentTestModels) GetEmbeddingModel(context.Context, string) (embedding.Embedder, error) {
	return parentChildEmbedder{}, nil
}

type enrichmentTestBases struct {
	interfaces.KnowledgeBaseService
	db *gorm.DB
}

func (b enrichmentTestBases) GetKnowledgeBaseByID(ctx context.Context, id string) (*types.KnowledgeBase, error) {
	var row types.KnowledgeBase
	err := b.db.WithContext(ctx).Where("id = ?", id).Take(&row).Error
	return &row, err
}
func (b enrichmentTestBases) GetKnowledgeBaseByIDOnly(ctx context.Context, id string) (*types.KnowledgeBase, error) {
	return b.GetKnowledgeBaseByID(ctx, id)
}

type enrichmentTestChunks struct {
	interfaces.ChunkService
	repo interfaces.ChunkRepository
}

func (c enrichmentTestChunks) GetRepository() interfaces.ChunkRepository { return c.repo }
func (c enrichmentTestChunks) ListChunksByKnowledgeID(ctx context.Context, id string) ([]*types.Chunk, error) {
	return c.repo.ListChunksByKnowledgeID(ctx, 7, id)
}
func (c enrichmentTestChunks) GetChunkByIDOnly(ctx context.Context, id string) (*types.Chunk, error) {
	return c.repo.GetChunkByID(ctx, 7, id)
}

type enrichmentTestFiles struct {
	interfaces.FileService
	data []byte
}

func (f *enrichmentTestFiles) GetFile(ctx context.Context, _ string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(f.data)), nil
}

type enrichmentTestFixture struct {
	db         *gorm.DB
	store      *repository.KnowledgeInputProvenanceStore
	s          *knowledgeService
	post       *KnowledgePostProcessService
	ctx, build context.Context
	k          *types.Knowledge
	chunks     []*types.Chunk
	queue      *knowledgeInputQueue
	model      *enrichmentTestModel
	vision     *enrichmentTestVLM
	guard      *access.NextcloudSourceLineageGuard
	engine     *enrichmentTestEngine
}

func newEnrichmentTestFixture(t *testing.T, pg bool) *enrichmentTestFixture {
	t.Helper()
	store, db, ctx := autoTagActualFixture(t, pg)
	db.Config.Logger = gormlogger.Default.LogMode(gormlogger.Silent)
	require.NoError(t, db.AutoMigrate(&types.Tenant{}, &types.User{}, &types.TenantMember{}, &types.TenantAPIKey{}, &types.Model{}, &types.KnowledgeTagRelation{}, &types.Directory{}, &types.DirectoryIdentity{}, &types.DirectoryGroup{}, &types.DirectoryGroupMembership{}, &types.TenantGroupRoleGrant{}, &types.ResourceAccessPolicy{}, &types.ResourceGroupGrant{}))
	migration := db
	path := "../../../migrations/sqlite/000062_knowledge_enrichment_inputs.up.sql"
	displayPath := "../../../migrations/sqlite/000059_knowledge_collection_display_inputs.up.sql"
	if pg {
		sqlDB, e := db.DB()
		require.NoError(t, e)
		migration, e = gorm.Open(postgres.New(postgres.Config{Conn: sqlDB, PreferSimpleProtocol: true}), &gorm.Config{})
		require.NoError(t, e)
		path = "../../../migrations/versioned/000143_knowledge_enrichment_inputs.up.sql"
		displayPath = "../../../migrations/versioned/000140_knowledge_collection_display_inputs.up.sql"
	}
	for _, file := range []string{path, displayPath} {
		data, e := os.ReadFile(file)
		require.NoError(t, e)
		require.NoError(t, migration.Exec(string(data)).Error)
	}
	require.NoError(t, db.Create(&types.User{ID: "enrich-human", Username: "enrich-human", Email: "enrich@example.test", IsActive: true}).Error)
	require.NoError(t, db.Create(&types.TenantMember{TenantID: 7, UserID: "enrich-human", Role: types.TenantRoleAdmin, Status: types.TenantMemberStatusActive}).Error)
	tenantRecord := &types.Tenant{ID: 7, RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{RetrieverType: types.VectorRetrieverType, RetrieverEngineType: types.PostgresRetrieverEngineType}}}}
	require.NoError(t, db.Save(tenantRecord).Error)
	ctx = types.WithCaller(ctx, types.Caller{TenantID: 7, UserID: "enrich-human", Role: types.TenantRoleAdmin})
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "enrich-human"})
	groups := NewGroupAccessService(repository.NewGroupAccessRepository(db))
	repository.ConfigureKnowledgeInputActorGroupAccess(store, groups)
	knowledge := repository.NewKnowledgeRepository(db)
	tags := repository.NewKnowledgeTagRepository(db)
	repository.ConfigureKnowledgeDisplayInputWriters(store, tags, knowledge, func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
		actual := repository.NewKnowledgeInputProvenanceStore(tx)
		repository.ConfigureKnowledgeInputActorGroupAccess(actual, NewGroupAccessService(repository.NewGroupAccessRepository(tx)))
		return actual
	})
	repository.ConfigureKnowledgeCurrentReadActor(knowledge, store)
	kb := &types.KnowledgeBase{}
	require.NoError(t, db.Where("id='kb'").Take(kb).Error)
	kb.Type = types.KnowledgeBaseTypeDocument
	kb.SummaryModelID = "summary"
	kb.EmbeddingModelID = "embed"
	kb.IndexingStrategy = types.IndexingStrategy{VectorEnabled: true}
	kb.VLMConfig = types.VLMConfig{Enabled: true, ModelID: "vision", DescriptionLanguage: "English"}
	kb.QuestionGenerationConfig = &types.QuestionGenerationConfig{Enabled: true, QuestionCount: 2}
	require.NoError(t, store.UpdateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(ctx), kb))
	var authErr error
	ctx, authErr = store.AuthorizeCurrentKnowledgeInputWrite(ctx, kb)
	require.NoError(t, authErr)
	for _, id := range []string{"summary", "embed", "vision"} {
		require.NoError(t, db.Create(&types.Model{ID: id, TenantID: 7, Name: "local " + id, Source: types.ModelSourceRemote, Status: types.ModelStatusActive}).Error)
	}
	k := &types.Knowledge{ID: "enrichment-doc", TenantID: 7, KnowledgeBaseID: "kb", Type: types.KnowledgeTypeManual, Channel: "web", Source: "manual", Title: "actual title", Metadata: types.JSON(`{"manual":{"content":"original human full document"}}`), CustomMetadata: types.JSON(`{"project":"original context"}`), ParseStatus: types.ParseStatusFinalizing, EnableStatus: "enabled", PendingSubtasksCount: 1, SummaryStatus: types.SummaryStatusPending}
	proof, e := store.CreateKnowledgeWithInput(repository.WithIndependentKnowledgeInput(ctx), k, "original human full document")
	require.NoError(t, e)
	build := repository.WithKnowledgeBuildInputProof(ctx, store, proof)
	chunks := []*types.Chunk{}
	for i, id := range []string{"text-one", "text-two"} {
		chunks = append(chunks, &types.Chunk{ID: id, TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Content: strings.Repeat("Actual human document with enough content to summarize safely. ", 3), ChunkType: types.ChunkTypeText, ChunkIndex: i, StartAt: i * 180, EndAt: (i + 1) * 180, IsEnabled: true, IndexStatus: "ready", Metadata: types.JSON(`{}`)})
	}
	chunks[0].NextChunkID = chunks[1].ID
	chunks[1].PreChunkID = chunks[0].ID
	chunkRepo := repository.NewChunkRepository(db)
	require.NoError(t, chunkRepo.CreateChunks(build, chunks))
	model := &enrichmentTestModel{}
	vision := &enrichmentTestVLM{}
	queue := &knowledgeInputQueue{}
	engine := &enrichmentTestEngine{&parentChildRetrieveEngine{}}
	bases := enrichmentTestBases{db: db}
	tenant := &types.Tenant{ID: 7, Status: "active", RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{RetrieverType: types.VectorRetrieverType, RetrieverEngineType: types.PostgresRetrieverEngineType}}}}
	cfg := &config.Config{Conversation: &config.ConversationConfig{GenerateSummaryPrompt: "Summarize in {{language}}", GenerateQuestionsPrompt: "Generate {{question_count}} questions for {{doc_name}}: {{content}} {{context}} in {{language}}"}}
	require.NoError(t, config.BindIndependentEnrichmentPromptLiteral(cfg, "summary", "Summarize in {{language}}"))
	require.NoError(t, config.BindIndependentEnrichmentPromptLiteral(cfg, "question", "Generate {{question_count}} questions for {{doc_name}}: {{content}} {{context}} in {{language}}"))
	s := &knowledgeService{config: cfg, repo: knowledge, chunkRepo: chunkRepo, chunkService: enrichmentTestChunks{repo: chunkRepo}, kbService: bases, modelService: enrichmentTestModels{chat: model, vision: vision}, tenantRepo: knowledgeInputTenantRepo{tenant: tenant}, retrieveEngine: parentChildRetrieveRegistry{engine: engine}, task: queue, knowledgeInputs: store, knowledgeInputWriter: store}
	guard := access.NewNextcloudSourceLineageGuard(knowledge.(access.NextcloudLineageSnapshotLookup), bases, nil, nil, groups, nil)
	repository.ConfigureKnowledgeOriginalInputSourceVerifier(store, NewKnowledgeOriginalInputSourceVerifier(store, guard, nil))
	post := &KnowledgePostProcessService{chunkRepo: chunkRepo, taskEnqueuer: queue, knowledgeInputs: store, enrichmentConfig: cfg}
	return &enrichmentTestFixture{db, store, s, post, ctx, build, k, chunks, queue, model, vision, guard, engine}
}
func assertEnrichmentReadable(t *testing.T, f *enrichmentTestFixture, ids []string) {
	t.Helper()
	var k types.Knowledge
	require.NoError(t, f.db.Where("id = ?", f.k.ID).Take(&k).Error)
	if k.ParseStatus == types.ParseStatusFinalizing {
		_, _, e := f.s.repo.FinalizeSubtask(f.build, k.ID)
		require.NoError(t, e)
		require.NoError(t, f.db.Where("id = ?", k.ID).Take(&k).Error)
	}
	kg, e := NewKnowledgeOutputAuthorizer(f.store, f.guard).Begin(f.ctx, []*types.Knowledge{&k})
	require.NoError(t, e)
	require.NoError(t, kg.Check(f.ctx, []*types.Knowledge{&k}))
	cg, e := NewChunkOutputAuthorizer(f.store, f.guard).Begin(f.ctx, types.ChunkOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, ChunkIDs: ids})
	require.NoError(t, e)
	snapshot, e := cg.Snapshot()
	require.NoError(t, e)
	require.NoError(t, cg.Check(f.ctx, snapshot))
	rows, e := repository.NewRAGChunkSnapshotReader(f.db, f.store).HydrateRAGChunkSnapshots(f.ctx, []types.RAGChunkReadRequest{{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, ChunkIDs: ids}})
	require.NoError(t, e)
	for _, row := range rows {
		require.NoError(t, row.SourceLineage.RequireComplete())
	}
}
func enrichmentTask(t *testing.T, kind string, p any) *asynq.Task {
	t.Helper()
	data, e := json.Marshal(p)
	require.NoError(t, e)
	return asynq.NewTask(kind, data)
}

func runEnrichmentOriginalRoutes(t *testing.T, pg bool) {
	t.Run("summary enqueue generate refresh readable", func(t *testing.T) {
		f := newEnrichmentTestFixture(t, pg)
		require.True(t, f.post.enqueueSummaryGenerationTask(f.build, types.KnowledgePostProcessPayload{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: f.k.ID, Language: "en-US"}, 0))
		require.NoError(t, f.s.ProcessSummaryGeneration(context.Background(), f.queue.tasks[0]))
		require.Equal(t, 1, f.model.calls)
		require.NoError(t, f.s.RequestKnowledgeSummaryRefresh(f.ctx, f.k.ID))
		require.NoError(t, f.s.ProcessSummaryGeneration(context.Background(), f.queue.tasks[len(f.queue.tasks)-1]))
		require.Equal(t, 2, f.model.calls)
		rows, e := f.s.chunkRepo.ListChunksByKnowledgeID(context.Background(), 7, f.k.ID)
		require.NoError(t, e)
		ids := []string{}
		for _, row := range rows {
			ids = append(ids, row.ID)
		}
		assertEnrichmentReadable(t, f, ids)
	})
	t.Run("legacy question whole document and batch and regenerate readable", func(t *testing.T) {
		f := newEnrichmentTestFixture(t, pg)
		f.model.questions = true
		actual, e := f.store.CaptureKnowledgeEnrichmentInputs(f.build, types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: f.k.ID, ChunkIDs: enrichmentInputChunkIDs(f.chunks)})
		require.NoError(t, e)
		p := types.QuestionGenerationPayload{OriginalInput: actual.Proof, TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: f.k.ID, QuestionCount: 2, Language: "en-US"}
		p.ProducerInput, e = f.store.AdmitKnowledgeProducerInput(f.build, actual.Proof, "question", questionProducerControls(f.s.config, p))
		require.NoError(t, e)
		require.NoError(t, f.s.ProcessQuestionGeneration(context.Background(), enrichmentTask(t, types.TypeQuestionGeneration, p)))
		require.Equal(t, 2, f.model.calls)
		require.Equal(t, 1, f.post.enqueueQuestionGenerationTasks(f.build, types.KnowledgePostProcessPayload{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: f.k.ID, Language: "en-US"}, types.QuestionGenerationConfig{Enabled: true, QuestionCount: 2}, 0, f.chunks))
		require.NoError(t, f.s.ProcessQuestionGeneration(context.Background(), f.queue.tasks[len(f.queue.tasks)-1]))
		require.Equal(t, 4, f.model.calls)
		generated, e := f.s.RegenerateChunkQuestions(f.ctx, f.chunks[0].ID)
		require.NoError(t, e)
		require.Len(t, generated, 2)
		require.Equal(t, 5, f.model.calls)
		assertEnrichmentReadable(t, f, []string{f.chunks[0].ID, f.chunks[1].ID})
	})
	t.Run("image actual bytes OCR caption atomic receipt readable", func(t *testing.T) {
		f := newEnrichmentTestFixture(t, pg)
		imageBytes := []byte("actual original parser image bytes")
		d := sha256.Sum256(imageBytes)
		actual, e := f.store.CaptureKnowledgeEnrichmentInputs(f.build, types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: f.k.ID, ChunkIDs: []string{f.chunks[0].ID}})
		require.NoError(t, e)
		p := types.ImageMultimodalPayload{OriginalInput: actual.Proof, TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: f.k.ID, ChunkID: f.chunks[0].ID, ImageURL: "local://actual.png", ImageDigest: hex.EncodeToString(d[:]), EnableOCR: true, EnableCaption: true, Language: "en-US"}
		p.ProducerInput, e = f.store.AdmitKnowledgeProducerInput(f.build, actual.Proof, "image", imageProducerControls(p))
		require.NoError(t, e)
		svc := &ImageMultimodalService{knowledgeInputs: f.store, knowledgeRepo: f.s.repo, chunkService: f.s.chunkService, kbService: f.s.kbService, modelService: f.s.modelService, tenantRepo: f.s.tenantRepo, retrieveEngine: f.s.retrieveEngine, fileSvc: &enrichmentTestFiles{data: imageBytes}, taskEnqueuer: f.queue}
		require.NoError(t, svc.Handle(context.Background(), enrichmentTask(t, types.TypeImageMultimodal, p)))
		require.Equal(t, 2, f.vision.calls)
		children, e := f.s.chunkRepo.ListChunksByParentIDs(context.Background(), 7, []string{f.chunks[0].ID})
		require.NoError(t, e)
		require.Len(t, children, 2)
		require.Len(t, f.queue.tasks, 1)
		var next types.KnowledgePostProcessPayload
		require.NoError(t, json.Unmarshal(f.queue.tasks[0].Payload(), &next))
		require.Equal(t, actual.Proof.AdmissionID, next.OriginalInput.AdmissionID)
		ids := []string{}
		for _, c := range children {
			ids = append(ids, c.ID)
		}
		assertEnrichmentReadable(t, f, ids)
	})
}
func TestKnowledgeEnrichmentOriginalRoutesSQLite(t *testing.T) { runEnrichmentOriginalRoutes(t, false) }
func TestPostgresKnowledgeEnrichmentOriginalRoutes(t *testing.T) {
	runEnrichmentOriginalRoutes(t, true)
}

func runEnrichmentRejectedChanges(t *testing.T, pg bool) {
	for _, mode := range []string{"body ABA", "metadata ABA", "neighbor ABA", "enable ABA", "config ABA", "model ABA", "runtime template", "tenant routing ABA", "KB binding ABA", "generic summary ABA", "generic questions ABA", "user revoked", "role revoked", "forged language", "forged proof", "forged scope", "canceled context"} {
		t.Run(mode, func(t *testing.T) {
			f := newEnrichmentTestFixture(t, pg)
			f.model.questions = true
			require.Equal(t, 1, f.post.enqueueQuestionGenerationTasks(f.build, types.KnowledgePostProcessPayload{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: f.k.ID, Language: "en-US"}, types.QuestionGenerationConfig{Enabled: true, QuestionCount: 2}, 0, f.chunks))
			var p types.QuestionGenerationPayload
			require.NoError(t, json.Unmarshal(f.queue.tasks[0].Payload(), &p))
			mutate := func() {
				switch mode {
				case "body ABA":
					require.NoError(t, f.db.Model(&types.Chunk{}).Where("id = ?", f.chunks[1].ID).Update("content", "changed neighbor body").Error)
					require.NoError(t, f.db.Model(&types.Chunk{}).Where("id = ?", f.chunks[1].ID).Update("content", f.chunks[1].Content).Error)
				case "metadata ABA":
					require.NoError(t, f.db.Model(&types.Knowledge{}).Where("id = ?", f.k.ID).Update("custom_metadata", types.JSON(`{"project":"changed"}`)).Error)
					require.NoError(t, f.db.Model(&types.Knowledge{}).Where("id = ?", f.k.ID).Update("custom_metadata", f.k.CustomMetadata).Error)
				case "neighbor ABA":
					require.NoError(t, f.db.Model(&types.Chunk{}).Where("id = ?", f.chunks[0].ID).Update("next_chunk_id", "other").Error)
					require.NoError(t, f.db.Model(&types.Chunk{}).Where("id = ?", f.chunks[0].ID).Update("next_chunk_id", f.chunks[0].NextChunkID).Error)
				case "enable ABA":
					require.NoError(t, f.db.Model(&types.Chunk{}).Where("id = ?", f.chunks[1].ID).Update("is_enabled", false).Error)
					require.NoError(t, f.db.Model(&types.Chunk{}).Where("id = ?", f.chunks[1].ID).Update("is_enabled", true).Error)
				case "config ABA":
					require.NoError(t, f.db.Model(&types.KnowledgeBase{}).Where("id='kb'").Update("summary_model_id", "changed").Error)
					require.NoError(t, f.db.Model(&types.KnowledgeBase{}).Where("id='kb'").Update("summary_model_id", "summary").Error)
				case "model ABA":
					require.NoError(t, f.db.Model(&types.Model{}).Where("id='summary'").Update("name", "changed").Error)
					require.NoError(t, f.db.Model(&types.Model{}).Where("id='summary'").Update("name", "local summary").Error)
				case "tenant routing ABA":
					require.NoError(t, f.db.Model(&types.Tenant{}).Where("id=7").Update("status", "disabled").Error)
					require.NoError(t, f.db.Model(&types.Tenant{}).Where("id=7").Update("status", "active").Error)
				case "KB binding ABA":
					require.NoError(t, f.db.Model(&types.KnowledgeBase{}).Where("id='kb'").Update("storage_backend_id", "other").Error)
					require.NoError(t, f.db.Model(&types.KnowledgeBase{}).Where("id='kb'").Update("storage_backend_id", nil).Error)
				case "generic summary ABA":
					require.NoError(t, f.db.Model(&types.Knowledge{}).Where("id = ?", f.k.ID).Update("description", "unproved other summary").Error)
					require.NoError(t, f.db.Model(&types.Knowledge{}).Where("id = ?", f.k.ID).Update("description", "").Error)
				case "generic questions ABA":
					require.NoError(t, f.db.Model(&types.Chunk{}).Where("id = ?", f.chunks[1].ID).Update("metadata", types.JSON(`{"generated_questions":[{"id":"forged","question":"Cached unproved question"}]}`)).Error)
					require.NoError(t, f.db.Model(&types.Chunk{}).Where("id = ?", f.chunks[1].ID).Update("metadata", types.JSON(`{}`)).Error)
				case "runtime template":
					f.s.config.Conversation.GenerateQuestionsPrompt = "changed runtime prompt"
				case "user revoked":
					require.NoError(t, f.db.Model(&types.User{}).Where("id='enrich-human'").Update("is_active", false).Error)
				case "role revoked":
					require.NoError(t, f.db.Model(&types.TenantMember{}).Where("user_id='enrich-human'").Update("role", types.TenantRoleViewer).Error)
				}
			}
			ctx := context.Background()
			pre := false
			switch mode {
			case "forged language":
				p.Language = "different language"
				pre = true
			case "forged proof":
				p.ProducerInput.ID = "made-up-receipt"
				pre = true
			case "forged scope":
				p.KnowledgeBaseID = "other"
				pre = true
			case "canceled context":
				pre = true
			}
			if mode == "canceled context" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if !pre {
				f.model.beforeReturn = mutate
			}
			err := f.s.ProcessQuestionGeneration(ctx, enrichmentTask(t, types.TypeQuestionGeneration, p))
			require.Error(t, err)
			if pre {
				require.Zero(t, f.model.calls)
			} else {
				require.Equal(t, 1, f.model.calls)
			}
			for _, row := range f.chunks {
				var actual types.Chunk
				require.NoError(t, f.db.Where("id = ?", row.ID).Take(&actual).Error)
				meta, e := actual.DocumentMetadata()
				require.NoError(t, e)
				require.Empty(t, meta.GeneratedQuestions)
			}
			require.Empty(t, f.engine.indexed)
		})
	}
	for _, mode := range []string{"bytes mismatch", "image control forged", "source revoke during OCR", "cancel during OCR"} {
		t.Run(mode, func(t *testing.T) {
			f := newEnrichmentTestFixture(t, pg)
			image := []byte("actual original image bytes")
			d := sha256.Sum256(image)
			actual, e := f.store.CaptureKnowledgeEnrichmentInputs(f.build, types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: f.k.ID, ChunkIDs: []string{f.chunks[0].ID}})
			require.NoError(t, e)
			p := types.ImageMultimodalPayload{OriginalInput: actual.Proof, TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: f.k.ID, ChunkID: f.chunks[0].ID, ImageURL: "local://actual.png", ImageDigest: hex.EncodeToString(d[:]), EnableOCR: true, EnableCaption: true}
			p.ProducerInput, e = f.store.AdmitKnowledgeProducerInput(f.build, actual.Proof, "image", imageProducerControls(p))
			require.NoError(t, e)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			files := &enrichmentTestFiles{data: image}
			switch mode {
			case "bytes mismatch":
				files.data = []byte("replaced bytes behind same storage URL")
			case "image control forged":
				p.ImageSourceType = "scanned_pdf"
			case "source revoke during OCR":
				f.vision.beforeReturn = func() {
					require.NoError(t, f.db.Model(&types.User{}).Where("id='enrich-human'").Update("is_active", false).Error)
				}
			case "cancel during OCR":
				f.vision.beforeReturn = cancel
			}
			svc := &ImageMultimodalService{knowledgeInputs: f.store, knowledgeRepo: f.s.repo, chunkService: f.s.chunkService, kbService: f.s.kbService, modelService: f.s.modelService, tenantRepo: f.s.tenantRepo, retrieveEngine: f.s.retrieveEngine, fileSvc: files, taskEnqueuer: f.queue}
			require.Error(t, svc.Handle(ctx, enrichmentTask(t, types.TypeImageMultimodal, p)))
			if mode == "bytes mismatch" || mode == "image control forged" {
				require.Zero(t, f.vision.calls)
			} else {
				require.Equal(t, 1, f.vision.calls)
			}
			var count int64
			require.NoError(t, f.db.Model(&types.Chunk{}).Where("parent_chunk_id = ?", f.chunks[0].ID).Count(&count).Error)
			require.Zero(t, count)
			require.Empty(t, f.engine.indexed)
			require.Empty(t, f.queue.tasks)
		})
	}
}
func TestKnowledgeEnrichmentRejectedChangesSQLite(t *testing.T) {
	runEnrichmentRejectedChanges(t, false)
}
func TestPostgresKnowledgeEnrichmentRejectedChanges(t *testing.T) {
	runEnrichmentRejectedChanges(t, true)
}

func runEnrichmentCurrentWriters(t *testing.T, pg bool) {
	t.Run("new actor regenerates original content after old actor revoked", func(t *testing.T) {
		f := newEnrichmentTestFixture(t, pg)
		require.NoError(t, f.db.Create(&types.User{ID: "new-writer", Username: "new-writer", Email: "new@example.test", IsActive: true}).Error)
		require.NoError(t, f.db.Create(&types.TenantMember{TenantID: 7, UserID: "new-writer", Role: types.TenantRoleContributor, Status: types.TenantMemberStatusActive}).Error)
		ctx := types.WithCaller(types.WithExecutionTenant(context.Background(), 7), types.Caller{TenantID: 7, UserID: "new-writer", Role: types.TenantRoleContributor})
		ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "new-writer"})
		require.NoError(t, f.db.Model(&types.User{}).Where("id='enrich-human'").Update("is_active", false).Error)
		require.NoError(t, f.s.RequestKnowledgeSummaryRefresh(ctx, f.k.ID))
		require.NoError(t, f.s.ProcessSummaryGeneration(context.Background(), f.queue.tasks[len(f.queue.tasks)-1]))
		f.model.questions = true
		var kb types.KnowledgeBase
		require.NoError(t, f.db.Where("id='kb'").Take(&kb).Error)
		ctx, e := f.store.AuthorizeCurrentKnowledgeInputWrite(ctx, &kb)
		require.NoError(t, e)
		items, e := f.s.RegenerateChunkQuestions(ctx, f.chunks[0].ID)
		require.NoError(t, e)
		require.Len(t, items, 2)
		require.Equal(t, 2, f.model.calls)
	})
	for _, revoke := range []bool{false, true} {
		name := "ingest key private worker can generate and public read stays denied"
		if revoke {
			name = "ingest key revoked during model never writes"
		}
		t.Run(name, func(t *testing.T) {
			f := newEnrichmentTestFixture(t, pg)
			tenant := uint64(7)
			key := &types.TenantAPIKey{TenantID: &tenant, ScopeType: types.APIKeyScopeTenant, Name: "local ingest fixture", KeyHash: "fixture-not-a-real-key", KnowledgeBaseIDs: types.StringArray{"kb"}, Capabilities: types.StringArray{string(types.APIKeyCapabilityIngest)}}
			require.NoError(t, f.db.Create(key).Error)
			ctx := types.WithCaller(types.WithExecutionTenant(context.Background(), 7), types.Caller{TenantID: 7, Role: types.TenantRoleViewer})
			ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalAPITenant, ID: "7"})
			ctx = types.WithTenantAPIKeyScope(ctx, types.TenantAPIKeyScope{KeyID: key.ID, ScopeType: key.ScopeType, KnowledgeBaseIDs: key.KnowledgeBaseIDs, Capabilities: key.Capabilities})
			_, e := f.store.RefreshCurrentReadActor(ctx)
			require.Error(t, e)
			if revoke {
				f.model.beforeReturn = func() { now := time.Now().UTC(); require.NoError(t, f.db.Model(key).Update("revoked_at", now).Error) }
			}
			require.NoError(t, f.s.RequestKnowledgeSummaryRefresh(ctx, f.k.ID))
			e = f.s.ProcessSummaryGeneration(context.Background(), f.queue.tasks[len(f.queue.tasks)-1])
			if revoke {
				require.Error(t, e)
				var actual types.Knowledge
				require.NoError(t, f.db.Where("id = ?", f.k.ID).Take(&actual).Error)
				require.Empty(t, actual.Description)
				require.Empty(t, f.engine.indexed)
			} else {
				require.NoError(t, e)
			}
			require.Equal(t, 1, f.model.calls)
			_, e = f.store.RefreshCurrentReadActor(ctx)
			require.Error(t, e)
		})
	}
}
func TestKnowledgeEnrichmentCurrentWritersSQLite(t *testing.T) { runEnrichmentCurrentWriters(t, false) }
func TestPostgresKnowledgeEnrichmentCurrentWriters(t *testing.T) {
	runEnrichmentCurrentWriters(t, true)
}
