//go:build sqlite_fts5

package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"strconv"
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
type documentGraphLive struct {
	Pair                                        repository.NextcloudSourcePairing
	Token, ExternalID, ETag, Connection, Secret string
	Directory, GUID                             string
}

func TestNextcloudDocumentToSQLIndexGraphAndCurrentState(t *testing.T) {
	runDocumentGraphPipeline(t, "")
}
func TestNextcloudLiveDocumentToGraphAndInbox(t *testing.T) {
	path := os.Getenv("WEKNORA_DOCUMENT_GRAPH_LIVE_FIXTURE")
	if path == "" {
		t.Skip("owned Nextcloud fixture required")
	}
	runDocumentGraphPipeline(t, path)
}
func runDocumentGraphPipeline(t *testing.T, path string) {
	localDerivedVecAuto.Do(sqlite_vec.Auto)
	var live documentGraphLive
	var source *machinePipelineSource
	if path != "" {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(raw, &live))
		live.Pair.TenantID = 7
		require.NotEmpty(t, live.Pair.KnowledgeBaseID)
		source = &machinePipelineSource{Pair: live.Pair, Token: live.Token, ExternalID: live.ExternalID, ETag: live.ETag, Prepared: func(pair *repository.NextcloudSourcePairing) {
			raw, err := json.Marshal(pair)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path+".ready", raw, 0600))
			require.Eventually(t, func() bool { _, err := os.Stat(path + ".committed"); return err == nil }, 45*time.Second, 100*time.Millisecond)
		}}
	}
	m := newMachinePipelineFixtureWithSource(t, false, "Actual original document about its topic, with enough text for chunking and retrieval.", source)
	if source != nil {
		require.NotEmpty(t, live.Directory)
		require.NotEmpty(t, live.GUID)
		require.NoError(t, m.f.db.Model(&types.Directory{}).Where("id=?", "machine-ad").Update("id", live.Directory).Error)
		require.NoError(t, m.f.db.Model(&types.DirectoryIdentity{}).Where("id=?", "machine-ad-user").Updates(map[string]any{"directory_id": live.Directory, "object_guid": live.GUID}).Error)
	}
	connection, instance, secret := "actual-document-graph-connection", "instance", "0123456789abcdef0123456789abcdef"
	if source != nil {
		connection, instance, secret = live.Connection, live.Pair.InstanceID, live.Secret
	}

	db := m.f.db
	// Collection reads require the same membership/ABA checkpoint migration
	// as the normal app, before document creation starts updating the scope.
	collectionDDL, err := os.ReadFile("../../../migrations/sqlite/000058_knowledge_collection_checkpoints.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(collectionDDL)).Error)
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
	require.NoError(t, db.Where("id=?", m.ds.KnowledgeBaseID).Take(&kb).Error)
	kb.IndexingStrategy = types.IndexingStrategy{VectorEnabled: true, KeywordEnabled: true, GraphEnabled: true, GraphRequired: true}
	kb.ExtractConfig = &types.ExtractConfig{Enabled: true, Tags: []string{"KNOWS"}}
	require.NoError(t, m.f.store.UpdateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(m.f.ctx), &kb))
	state := repository.NewNextcloudProcessingStateStore(db)
	provider := repository.NewScopedSQLGraphRepository(db, nil)
	m.f.s.graphEngine = provider
	base, configSHA, binding, err := repository.NextcloudEventDataSourceIdentity(m.ds.Config)
	require.NoError(t, err)
	encrypted, err := utils.EncryptAESGCM(secret, utils.GetAESKey())
	require.NoError(t, err)
	require.NoError(t, db.Table("nextcloud_event_connections").Create(map[string]any{"connection_id": connection, "tenant_id": 7, "knowledge_base_id": m.ds.KnowledgeBaseID, "datasource_id": m.ds.ID, "nextcloud_instance_id": instance, "binding_id": binding, "datasource_base_url": base, "datasource_config_sha256": configSHA, "status": "active", "current_key_id": "current", "current_secret_ciphertext": encrypted}).Error)
	require.NoError(t, state.RefreshSourceEpoch(context.Background(), connection))
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
		scope := repository.NextcloudContentScope{TenantID: 7, KnowledgeBaseID: m.ds.KnowledgeBaseID, KnowledgeID: row.ID, DataSourceID: m.ds.ID, ExternalID: m.externalID}
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
	require.NoError(t, state.Capture(context.Background(), 7, m.ds.KnowledgeBaseID, m.ds.ID, m.externalID, true))
	var persisted struct{ PayloadJSON string }
	require.NoError(t, db.Table("nextcloud_file_states").Where("connection_id=?", connection).Take(&persisted).Error)
	var status repository.NextcloudFileStateV2
	require.NoError(t, json.Unmarshal([]byte(persisted.PayloadJSON), &status))
	require.Equal(t, "published", status.PublicationState)
	for _, stage := range []string{"parse", "chunk", "embedding", "index"} {
		require.Equal(t, "succeeded", status.Stages[stage].State, stage)
	}
	require.Equal(t, "succeeded", status.Graph.State)
	require.True(t, status.Graph.Persisted)
	require.Equal(t, graphTasks, status.Graph.Expected)
	if source != nil {
		require.Equal(t, live.ETag, status.SourceETag)
		require.Equal(t, strconv.FormatInt(live.Pair.PublicationEpoch, 10), status.PublicationEpoch)
		dispatcher := NewNextcloudStateDispatcher(state, nil)
		dispatcher.deliver(context.Background(), time.Now().UTC())
		var delivered int64
		require.NoError(t, db.Table("nextcloud_status_outbox").Where("event_id=? AND state='delivered'", status.EventID).Count(&delivered).Error)
		require.EqualValues(t, 1, delivered)
		require.NoError(t, os.WriteFile(path+".result", []byte(persisted.PayloadJSON), 0600))
		// The same derived output is read with a nominal WK admin actor whose
		// actual source identity is checked over signed HTTP against NC/LDAPS.
		output, err := NewKnowledgeOutputAuthorizer(m.f.store, m.f.guard).Begin(m.f.ctx, []*types.Knowledge{row})
		require.NoError(t, err)
		require.NoError(t, output.Check(m.f.ctx, []*types.Knowledge{row}))
		var chunk types.Chunk
		require.NoError(t, db.Where("knowledge_id=?", row.ID).Take(&chunk).Error)
		chunks, err := NewChunkOutputAuthorizer(m.f.store, m.f.guard).Begin(m.f.ctx, types.ChunkOutputReadRequest{TenantID: 7, KnowledgeBaseID: m.ds.KnowledgeBaseID, KnowledgeID: row.ID, ChunkIDs: []string{chunk.ID}})
		require.NoError(t, err)
		snapshot, err := chunks.Snapshot()
		require.NoError(t, err)
		require.NoError(t, chunks.Check(m.f.ctx, snapshot))
		if os.Getenv("WEKNORA_DOCUMENT_GRAPH_HTTP") == "1" {
			var databases []struct{ Name, File string }
			require.NoError(t, db.Raw("PRAGMA database_list").Scan(&databases).Error)
			databasePath := ""
			for _, database := range databases {
				if database.Name == "main" {
					databasePath = database.File
				}
			}
			require.NotEmpty(t, databasePath)
			// Private paths reference this exact live database and original file;
			// the handler test must not synthesize another published generation.
			original, err := m.files.GetFile(m.f.ctx, row.FilePath)
			require.NoError(t, err)
			originalBytes, err := io.ReadAll(original)
			require.NoError(t, err)
			require.NoError(t, original.Close())
			bodySHA := sha256.Sum256(originalBytes)
			descriptor := map[string]any{"database": databasePath, "file_root": m.base, "knowledge_id": row.ID,
				"knowledge_base_id": row.KnowledgeBaseID, "user_id": "enrich-human", "file_path": row.FilePath, "body_sha256": hex.EncodeToString(bodySHA[:]),
				"environment": map[string]string{}}
			environment := descriptor["environment"].(map[string]string)
			for _, key := range []string{"SYSTEM_AES_KEY", "WEKNORA_NEXTCLOUD_DEV_HTTP", "WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", "WEKNORA_ORIGINAL_BODY_JOURNAL_DIRECTORY", "WEKNORA_ORIGINAL_BODY_JOURNAL_KEY", "WEKNORA_ORIGINAL_BODY_JOURNAL_PIN"} {
				environment[key] = os.Getenv(key)
			}
			encoded, err := json.Marshal(descriptor)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path+".http-input", encoded, 0600))
		}
		require.NoError(t, os.WriteFile(path+".access-ready", []byte("ready"), 0600))
		require.Eventually(t, func() bool { _, err := os.Stat(path + ".revoked"); return err == nil }, 90*time.Second, 100*time.Millisecond)
		require.Error(t, output.Check(m.f.ctx, []*types.Knowledge{row}), "retained document output must recheck current source access")
		require.Error(t, chunks.Check(m.f.ctx, snapshot), "retained chunks must recheck current source access")
		_, err = NewKnowledgeOutputAuthorizer(m.f.store, m.f.guard).Begin(m.f.ctx, []*types.Knowledge{row})
		require.Error(t, err, "nominal WK admin must not bypass actual LDAP source revocation")
		if os.Getenv("WEKNORA_DOCUMENT_GRAPH_HTTP") == "1" {
			require.NoError(t, os.WriteFile(path+".source-checked", []byte("checked"), 0600))
			require.Eventually(t, func() bool { _, err := os.Stat(path + ".http-done"); return err == nil }, 60*time.Second, 100*time.Millisecond)
		}
		t.Log("actual LDAPS reader can read processed output before NC share removal; retained document/chunk output and new admin read denied after removal")

	}
	t.Log("actual HTTP sync -> saved original -> parser/chunker -> SQLite vector+FTS rows -> enqueued extraction -> fenced SQL graph -> published stage/outbox; controlled source and models")
}
