package service

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type knowledgeInputQueue struct{ tasks []*asynq.Task }

func (q *knowledgeInputQueue) Enqueue(task *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	q.tasks = append(q.tasks, task)
	return &asynq.TaskInfo{ID: "queued", Queue: "default"}, nil
}

type knowledgeInputTenantRepo struct {
	interfaces.TenantRepository
	tenant *types.Tenant
}

func (r knowledgeInputTenantRepo) GetTenantByID(context.Context, uint64) (*types.Tenant, error) {
	return r.tenant, nil
}
func (r knowledgeInputTenantRepo) AdjustStorageUsed(context.Context, uint64, int64) error { return nil }

func knowledgeInputServiceFixture(t *testing.T) (*knowledgeService, *repository.KnowledgeInputProvenanceStore, *gorm.DB, *knowledgeInputQueue, context.Context) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "input.db")), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.Chunk{}, &types.Session{}, &types.Message{}, &types.KnowledgeTag{}))
	require.NoError(t, db.Exec("CREATE TABLE nextcloud_source_versions(candidate_knowledge_id TEXT); CREATE TABLE nextcloud_source_revisions(candidate_knowledge_id TEXT); CREATE TABLE knowledge_tag_relations(knowledge_id TEXT,tag_id TEXT)").Error)
	for _, file := range []string{"000053_knowledge_input_provenance.up.sql", "000054_knowledge_configuration_history_inputs.up.sql", "000055_knowledge_actual_config_controls.up.sql"} {
		script, err := os.ReadFile(filepath.Join("../../../migrations/sqlite", file))
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(script)).Error)
	}
	store := repository.NewKnowledgeInputProvenanceStore(db)
	kb := &types.KnowledgeBase{ID: "kb", TenantID: 7, Name: "explicit human config", ChunkingConfig: types.ChunkingConfig{ChunkSize: 512, ChunkOverlap: 0}, IndexingStrategy: types.IndexingStrategy{VectorEnabled: true}, EmbeddingModelID: "embed"}
	require.NoError(t, store.CreateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(context.Background()), kb))
	tenant := &types.Tenant{ID: 7, RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{RetrieverType: types.VectorRetrieverType, RetrieverEngineType: types.PostgresRetrieverEngineType}}}}
	ctx := types.WithExecutionTenant(context.Background(), 7)
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
	queue := &knowledgeInputQueue{}
	s := &knowledgeService{repo: repository.NewKnowledgeRepository(db), chunkRepo: repository.NewChunkRepository(db), kbService: &createKnowledgeFileKBServiceStub{kb: kb}, fileSvc: &createKnowledgeFileServiceStub{}, task: queue, tenantRepo: knowledgeInputTenantRepo{tenant: tenant}, graphEngine: parentChildGraphRepo{}, retrieveEngine: parentChildRetrieveRegistry{engine: &parentChildRetrieveEngine{}}, modelService: parentChildModelService{embedder: parentChildEmbedder{}}, knowledgeInputs: store, knowledgeInputWriter: store}
	return s, store, db, queue, ctx
}

func TestKnowledgeInputManualActualEntryWorkerAndSearch(t *testing.T) {
	s, store, db, queue, ctx := knowledgeInputServiceFixture(t)
	knowledge, err := s.CreateKnowledgeFromManual(repository.WithIndependentKnowledgeInput(ctx), "kb", &types.ManualKnowledgePayload{Title: "original human title", Content: "actual human document text", Status: types.ManualKnowledgeStatusPublish}, "web")
	require.NoError(t, err)
	require.Len(t, queue.tasks, 1)
	var payload types.ManualProcessPayload
	require.NoError(t, json.Unmarshal(queue.tasks[0].Payload(), &payload))
	require.NotNil(t, payload.OriginalInput)
	require.NoError(t, repository.KnowledgeInputArtifactMatches(payload.OriginalInput, payload.Content))
	require.NoError(t, s.ProcessManualUpdate(ctx, queue.tasks[0]))
	var chunks []*types.Chunk
	require.NoError(t, db.Where("knowledge_id = ?", knowledge.ID).Find(&chunks).Error)
	require.NotEmpty(t, chunks)
	workerCtx := repository.WithKnowledgeBuildInputProof(ctx, store, payload.OriginalInput)
	require.NoError(t, db.Where("id = ?", knowledge.ID).Take(knowledge).Error)
	knowledge.ParseStatus = types.ParseStatusCompleted
	knowledge.EnableStatus = "enabled"
	require.NoError(t, s.repo.UpdateKnowledge(workerCtx, knowledge))
	reader := repository.NewRAGChunkSnapshotReader(db, store)
	rows, err := reader.HydrateRAGChunkSnapshots(ctx, []types.RAGChunkReadRequest{{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: knowledge.ID, ChunkIDs: []string{chunks[0].ID}}})
	require.NoError(t, err)
	require.NoError(t, rows[0].SourceLineage.RequireComplete())
	search := &knowledgeBaseService{kgRepo: s.repo, chunkRepo: s.chunkRepo, ragChunkSnapshots: reader, searchPublicationCheck: func(context.Context, *types.Knowledge) error { return nil }}
	searchCtx := types.WithCaller(ctx, types.Caller{TenantID: 7, UserID: "actual-human"})
	searchCtx = types.WithPrincipal(searchCtx, types.Principal{Type: types.PrincipalWebUser, ID: "actual-human"})
	searchCtx = types.WithRAGSourceLineage(searchCtx, types.NewRAGSourceLineage(func(_ context.Context, l *types.SourceLineage) error { return l.RequireComplete() }))
	result, err := search.processSearchResults(searchCtx, []*types.IndexWithScore{{KnowledgeID: knowledge.ID, ChunkID: chunks[0].ID, Content: "forged stale index text", Score: 1}}, true)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, chunks[0].Content, result[0].Content)
	require.NotEqual(t, "forged stale index text", result[0].Content)
	require.NoError(t, result[0].SourceLineage.RequireComplete())
}

func TestKnowledgeInputManualForgedJobAndConfigChangeNeverReadModel(t *testing.T) {
	for _, mode := range []string{"artifact", "config"} {
		t.Run(mode, func(t *testing.T) {
			s, _, db, queue, ctx := knowledgeInputServiceFixture(t)
			_, err := s.CreateKnowledgeFromManual(repository.WithIndependentKnowledgeInput(ctx), "kb", &types.ManualKnowledgePayload{Title: "human", Content: "original human body", Status: types.ManualKnowledgeStatusPublish}, "web")
			require.NoError(t, err)
			var payload types.ManualProcessPayload
			require.NoError(t, json.Unmarshal(queue.tasks[0].Payload(), &payload))
			if mode == "artifact" {
				payload.Content = "forged derived body"
			} else {
				require.NoError(t, db.Exec("UPDATE knowledge_bases SET summary_model_id='new' WHERE id='kb'").Error)
			}
			data, err := json.Marshal(payload)
			require.NoError(t, err)
			require.Error(t, s.ProcessManualUpdate(ctx, asynq.NewTask(types.TypeManualProcess, data)))
			var count int64
			require.NoError(t, db.Model(&types.Chunk{}).Count(&count).Error)
			require.Zero(t, count)
		})
	}
}
