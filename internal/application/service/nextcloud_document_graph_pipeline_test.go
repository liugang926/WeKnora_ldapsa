//go:build sqlite_fts5

package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	sqlindex "github.com/Tencent/WeKnora/internal/application/repository/retriever/sqlite"
	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
	sqlite_vec "github.com/asg017/sqlite-vec-go-bindings/cgo"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

type documentGraphModels struct{ machinePipelineModels }

func (documentGraphModels) GetEmbeddingModel(context.Context, string) (embedding.Embedder, error) {
	return localDerivedEmbedder{}, nil
}

// No prebuilt chunks, completed spans or published candidates: the actual sync
// and document handlers produce all of these, then enqueue the real graph task.
// HTTP source and model responses remain controlled fixtures.
func TestNextcloudDocumentToSQLIndexGraphAndCurrentState(t *testing.T) {
	localDerivedVecAuto.Do(sqlite_vec.Auto)
	m := newMachinePipelineFixture(t, false)
	db := m.f.db
	engines := types.RetrieverEngines{Engines: []types.RetrieverEngineParams{
		{RetrieverType: types.VectorRetrieverType, RetrieverEngineType: types.SQLiteRetrieverEngineType},
		{RetrieverType: types.KeywordsRetrieverType, RetrieverEngineType: types.SQLiteRetrieverEngineType},
	}}
	require.NoError(t, db.Model(&types.Tenant{}).Where("id=?", 7).Update("retriever_engines", engines).Error)
	registry := retriever.NewRetrieveEngineRegistry(nil, nil)
	require.NoError(t, registry.Register(retriever.NewKVHybridRetrieveEngine(sqlindex.NewSQLiteRetrieveEngineRepository(db), types.SQLiteRetrieverEngineType)))
	m.f.s.retrieveEngine = registry
	m.f.s.modelService = documentGraphModels{machinePipelineModels{auto: m.model, summary: m.f.model}}
	var kb types.KnowledgeBase
	require.NoError(t, db.Where("id=?", "kb").Take(&kb).Error)
	kb.IndexingStrategy = types.IndexingStrategy{VectorEnabled: true, KeywordEnabled: true, GraphEnabled: true, GraphRequired: true}
	kb.ExtractConfig = &types.ExtractConfig{Enabled: true, Tags: []string{"KNOWS"}}
	require.NoError(t, m.f.store.UpdateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(m.f.ctx), &kb))
	state := repository.NewNextcloudProcessingStateStore(db)
	provider := repository.NewScopedSQLGraphRepository(db, nil)
	m.f.s.graphEngine = provider
	base, configSHA, binding, err := repository.NextcloudEventDataSourceIdentity(m.ds.Config)
	require.NoError(t, err)
	encrypted, err := utils.EncryptAESGCM("0123456789abcdef0123456789abcdef", utils.GetAESKey())
	require.NoError(t, err)
	require.NoError(t, db.Table("nextcloud_event_connections").Create(map[string]any{"connection_id": "actual-document-graph-connection", "tenant_id": 7, "knowledge_base_id": "kb", "datasource_id": m.ds.ID, "nextcloud_instance_id": "instance", "binding_id": binding, "datasource_base_url": base, "datasource_config_sha256": configSHA, "status": "active", "current_key_id": "current", "current_secret_ciphertext": encrypted}).Error)
	require.NoError(t, state.RefreshSourceEpoch(context.Background(), "actual-document-graph-connection"))
	row := m.ingest(t)
	model := &graphEntryModel{}
	empty, _ := types.NewCompleteSourceLineage()
	template, err := types.BindGraphPromptTemplateOrigin(&types.PromptTemplateStructured{Description: "extract entities and edges %s"}, empty)
	require.NoError(t, err)
	handler := &ChunkExtractService{buildInputs: m.f.store, template: template, modelService: &graphEntryModels{model: model}, graphEngine: provider, processingLedger: state, knowledgeRepo: m.f.s.repo}
	graphTasks := 0
	for i := 0; i < len(m.f.queue.tasks); i++ {
		task := m.f.queue.tasks[i]
		if task.Type() != types.TypeChunkExtract {
			require.NoError(t, m.run(t, task), task.Type())
			continue
		}
		graphTasks++
		var payload types.ExtractChunkPayload
		require.NoError(t, json.Unmarshal(task.Payload(), &payload))
		require.Equal(t, row.ID, payload.KnowledgeID)
		require.NotNil(t, payload.OriginalInput)
		scope := repository.NextcloudContentScope{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: row.ID, DataSourceID: m.ds.ID, ExternalID: "nextcloud:instance:77"}
		leases := repository.NewNextcloudContentLeaseStore(db)
		lease, err := leases.AcquireKnowledge(context.Background(), scope, repository.NextcloudContentBuildLease, "actual-enqueued-graph", time.Minute)
		require.NoError(t, err)
		ctx := repository.WithNextcloudBuildLease(context.Background(), leases, lease, scope)
		require.NoError(t, handler.Handle(ctx, task))
		calls := model.calls
		require.NoError(t, handler.Handle(ctx, asynq.NewTask(task.Type(), task.Payload())))
		require.Equal(t, calls, model.calls, "queue redelivery must reuse durable graph ACK")
		require.NoError(t, leases.ReleaseLease(context.Background(), lease.ID))
	}
	require.NoError(t, db.Where("id=?", row.ID).Take(row).Error)
	require.Equal(t, types.ParseStatusCompleted, row.ParseStatus, row.ErrorMessage)
	require.Positive(t, graphTasks)
	require.Equal(t, graphTasks, model.calls)
	var n int64
	for _, table := range []string{"chunks", "lite_embeddings", "vec_embeddings_3", "lite_embeddings_fts", "nextcloud_graph_records", "nextcloud_status_outbox"} {
		require.NoError(t, db.Table(table).Count(&n).Error)
		require.Positive(t, n, table)
	}
	require.NoError(t, state.Capture(context.Background(), 7, "kb", m.ds.ID, "nextcloud:instance:77", true))
	var persisted struct{ PayloadJSON string }
	require.NoError(t, db.Table("nextcloud_file_states").Where("connection_id=?", "actual-document-graph-connection").Take(&persisted).Error)
	var status repository.NextcloudFileStateV2
	require.NoError(t, json.Unmarshal([]byte(persisted.PayloadJSON), &status))
	require.Equal(t, "published", status.PublicationState)
	for _, stage := range []string{"parse", "chunk", "embedding", "index"} {
		require.Equal(t, "succeeded", status.Stages[stage].State, stage)
	}
	require.Equal(t, "succeeded", status.Graph.State)
	require.True(t, status.Graph.Persisted)
	require.Equal(t, graphTasks, status.Graph.Expected)
	t.Log("actual HTTP sync -> saved original -> parser/chunker -> SQLite vector+FTS rows -> enqueued extraction -> fenced SQL graph -> published stage/outbox; controlled source and models")
}
