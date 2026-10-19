package service

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func scopedGraphFixtureDatabase(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(
		sqlite.Open("file:"+filepath.Join(t.TempDir(), "graph.db")+"?_busy_timeout=5000"),
		&gorm.Config{},
	)
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(
		t,
		db.AutoMigrate(
			&types.KnowledgeBase{},
			&types.Knowledge{},
			&types.Chunk{},
			&types.DataSource{},
			&repository.NextcloudSourcePairing{},
			&types.KnowledgeProcessingSpan{},
			&types.Message{},
		),
	)
	// AutoMigrate has no scope-index tags on Chunk. Restore the exact existing
	// production DDL so this workload measures the supported schema, rather
	// than repeated full scans caused by an incomplete test database.
	initialDDL, err := os.ReadFile("../../../migrations/sqlite/000000_init.up.sql")
	require.NoError(t, err)
	for _, name := range []string{"idx_chunks_tenant_kg", "idx_chunks_kb_tenant", "idx_chunks_knowledge_enabled"} {
		statement := ""
		for _, line := range strings.Split(string(initialDDL), "\n") {
			if strings.HasPrefix(line, "CREATE INDEX IF NOT EXISTS "+name+" ON chunks(") {
				statement = line
				break
			}
		}
		require.NotEmpty(t, statement, "existing production chunk index is required: %s", name)
		require.NoError(t, db.Exec(statement).Error)
	}
	require.NoError(
		t,
		db.Exec(
			"CREATE UNIQUE INDEX knowledge_processing_spans_scope_identity ON knowledge_processing_spans(knowledge_id,attempt,span_id)",
		).Error,
	)
	for _, name := range []string{"000031_nextcloud_source_versions", "000045_nextcloud_source_revisions", "000046_nextcloud_content_leases", "000047_nextcloud_candidate_retry", "000053_knowledge_input_provenance", "000054_knowledge_configuration_history_inputs", "000055_knowledge_actual_config_controls", "000056_knowledge_original_worker_actor", "000057_processing_span_original_inputs", "000032_nextcloud_event_inbox", "000038_nextcloud_gc", "000040_nextcloud_gc_object_claim", "000041_nextcloud_gc_derived_inventory", "000068_nextcloud_local_derived_gc", "000075_nextcloud_processing_state", "000077_nextcloud_content_revision", "000064_nextcloud_publication_recovery", "000076_nextcloud_directory_migrations", "000078_nextcloud_control_recovery", "000079_nextcloud_recovery_build_leases", "000080_nextcloud_graph_recovery_fence"} {
		script, err := os.ReadFile("../../../migrations/sqlite/" + name + ".up.sql")
		require.NoError(t, err)
		if name == "000075_nextcloud_processing_state" && db.Migrator().HasColumn(&types.DataSource{}, "sync_enabled") {
			script = []byte(strings.SplitN(string(script), "\n", 2)[1])
		}
		require.NoError(t, db.Exec(string(script)).Error)
	}
	return db
}

func scopedGraphPipelineFixture(
	t *testing.T,
	required bool,
) (*gorm.DB, *repository.KnowledgeInputProvenanceStore, *repository.NextcloudProcessingStateStore, *repository.ScopedSQLGraphRepository, context.Context, *types.KnowledgeBuildInputProof, *types.Knowledge) {
	t.Helper()
	db := scopedGraphFixtureDatabase(t)
	inputs := repository.NewKnowledgeInputProvenanceStore(db)
	kb := &types.KnowledgeBase{
		ID:               "scoped-graph-" + uuid.NewString(),
		TenantID:         7,
		Name:             "trusted source",
		SummaryModelID:   "local-graph-model",
		IndexingStrategy: types.IndexingStrategy{GraphEnabled: true, GraphRequired: required},
		ExtractConfig:    &types.ExtractConfig{Enabled: true, Tags: []string{"KNOWS"}},
	}
	require.NoError(
		t,
		inputs.CreateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(context.Background()), kb),
	)
	namespace := types.NameSpace{KnowledgeBase: kb.ID, Knowledge: "document"}
	ctx, proof, doc := graphNextcloudSource(t, db, inputs, namespace, "77", namespace.Knowledge, "etag-v1")
	require.NoError(t, db.Model(doc).Update("pending_subtasks_count", 1).Error)
	require.NoError(
		t,
		db.Create(
			&types.KnowledgeProcessingSpan{
				KnowledgeID: doc.ID,
				Attempt:     1,
				SpanID:      "root",
				Kind:        types.SpanKindRoot,
				Name:        "root",
				Status:      types.SpanStatusRunning,
			},
		).Error,
	)
	var ds types.DataSource
	require.NoError(t, db.Where("id='ds-77'").Take(&ds).Error)
	base, configSHA, binding, err := repository.NextcloudEventDataSourceIdentity(ds.Config)
	require.NoError(t, err)
	encrypted, err := utils.EncryptAESGCM("0123456789abcdef0123456789abcdef", utils.GetAESKey())
	require.NoError(t, err)
	require.NoError(
		t,
		db.Table("nextcloud_event_connections").
			Create(map[string]any{"connection_id": "pipeline-connection-0001", "tenant_id": 7, "knowledge_base_id": kb.ID, "datasource_id": ds.ID, "nextcloud_instance_id": "instance", "binding_id": binding, "datasource_base_url": base, "datasource_config_sha256": configSHA, "status": "active", "current_key_id": "current", "current_secret_ciphertext": encrypted}).
			Error,
	)
	state := repository.NewNextcloudProcessingStateStore(db)
	var chunk types.Chunk
	require.NoError(t, db.Where("knowledge_id=?", doc.ID).Take(&chunk).Error)
	require.NoError(t, state.PlanGraph(ctx, doc, 1, []*types.Chunk{&chunk}, true, required))
	return db, inputs, state, repository.NewScopedSQLGraphRepository(db, nil), ctx, proof, doc
}

func pipelineExtractTask(
	t *testing.T,
	db *gorm.DB,
	inputs *repository.KnowledgeInputProvenanceStore,
	state *repository.NextcloudProcessingStateStore,
	provider *repository.ScopedSQLGraphRepository,
	ctx context.Context,
	proof *types.KnowledgeBuildInputProof,
	doc *types.Knowledge,
	model *graphEntryModel,
) error {
	t.Helper()
	empty, _ := types.NewCompleteSourceLineage()
	template, err := types.BindGraphPromptTemplateOrigin(
		&types.PromptTemplateStructured{Description: "extract entities and edges %s"},
		empty,
	)
	require.NoError(t, err)
	handler := &ChunkExtractService{
		buildInputs:      inputs,
		template:         template,
		modelService:     &graphEntryModels{model: model},
		graphEngine:      provider,
		processingLedger: state,
		knowledgeRepo:    repository.NewKnowledgeRepository(db),
	}
	payload, err := json.Marshal(
		types.ExtractChunkPayload{
			TenantID:      7,
			KnowledgeID:   doc.ID,
			ChunkID:       doc.ID + "-chunk",
			ModelID:       "local-graph-model",
			OriginalInput: proof,
			Attempt:       1,
		},
	)
	require.NoError(t, err)
	return handler.Handle(ctx, asynq.NewTask(types.TypeChunkExtract, payload))
}

func TestNextcloudActualExtractionScopedGraphBackendAndPersistedResult(t *testing.T) {
	db, inputs, state, provider, ctx, proof, doc := scopedGraphPipelineFixture(t, true)
	model := &graphEntryModel{}
	require.NoError(t, pipelineExtractTask(t, db, inputs, state, provider, ctx, proof, doc, model))
	require.Equal(t, 1, model.calls)
	var tasks []struct {
		State     string
		Persisted bool
		Finalized bool
	}
	require.NoError(t, db.Table("nextcloud_graph_tasks").Find(&tasks).Error)
	require.Len(t, tasks, 1)
	require.Equal(t, "succeeded", tasks[0].State)
	require.True(t, tasks[0].Persisted)
	require.True(t, tasks[0].Finalized)
	var statusRow struct{ PayloadJSON string }
	require.NoError(
		t,
		db.Table("nextcloud_file_states").Where("connection_id='pipeline-connection-0001'").Take(&statusRow).Error,
	)
	var status repository.NextcloudFileStateV2
	require.NoError(t, json.Unmarshal([]byte(statusRow.PayloadJSON), &status))
	require.Equal(t, "succeeded", status.Graph.State)
	require.True(t, status.Graph.Persisted)
	var pushed int64
	require.NoError(
		t,
		db.Table("nextcloud_status_outbox").Where("connection_id='pipeline-connection-0001'").Count(&pushed).Error,
	)
	require.Positive(t, pushed)

	graph, err := provider.SearchNode(
		ctx,
		types.NameSpace{KnowledgeBase: doc.KnowledgeBaseID, Knowledge: doc.ID},
		[]string{"shared"},
	)
	require.NoError(t, err)
	require.Len(t, graph.Node, 2)
	require.Len(t, graph.Relation, 1)
	require.NoError(t, pipelineExtractTask(t, db, inputs, state, provider, ctx, proof, doc, model))
	require.Equal(t, 1, model.calls, "redelivery observes durable operation ACK without asking model again")
	var count int
	require.NoError(
		t,
		db.Model(&types.Knowledge{}).Where("id=?", doc.ID).Select("pending_subtasks_count").Scan(&count).Error,
	)
	require.Zero(t, count)
}

func TestNextcloudActualExtractionWithdrawalBeforeProviderCommitWritesNothing(t *testing.T) {
	db, inputs, state, provider, ctx, proof, doc := scopedGraphPipelineFixture(t, false)
	model := &graphEntryModel{call: func(_ []chat.Message, _ *chat.ChatOptions) {
		require.NoError(
			t,
			repository.NewNextcloudContentLeaseStore(db).
				RetireKnowledge(context.Background(), repository.NextcloudContentScope{TenantID: 7, KnowledgeBaseID: doc.KnowledgeBaseID, KnowledgeID: doc.ID, DataSourceID: "ds-77", ExternalID: "nextcloud:instance:77"}),
		)
	}}
	require.Error(t, pipelineExtractTask(t, db, inputs, state, provider, ctx, proof, doc, model))
	var count int64
	require.NoError(t, db.Table("nextcloud_graph_records").Count(&count).Error)
	require.Zero(t, count)
}
