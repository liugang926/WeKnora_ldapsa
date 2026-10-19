package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	nextcloudconnector "github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"

	"github.com/Tencent/WeKnora/internal/agent"
	"github.com/Tencent/WeKnora/internal/agent/tools"
	"github.com/Tencent/WeKnora/internal/application/readlease"
	"github.com/Tencent/WeKnora/internal/application/repository"
	graphrepo "github.com/Tencent/WeKnora/internal/application/repository/retriever/neo4j"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type graphEntryModel struct {
	call  func([]chat.Message, *chat.ChatOptions)
	calls int
}

func (m *graphEntryModel) Chat(
	_ context.Context,
	msg []chat.Message,
	options *chat.ChatOptions,
) (*types.ChatResponse, error) {
	m.calls++
	if m.call != nil {
		m.call(msg, options)
	}
	return &types.ChatResponse{
		Content: ("[{\"entity\":\"shared\",\"entity_attributes\":[\"actual graph fact\"]},{\"" +
			"entity1\":\"shared\",\"entity2\":\"target\",\"relation\":\"KNOWS\"}]"),
	}, nil
}

func (m *graphEntryModel) ChatStream(
	context.Context,
	[]chat.Message,
	*chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	panic("unused local stub")
}
func (m *graphEntryModel) GetModelID() string   { return "local-graph-model" }
func (m *graphEntryModel) GetModelName() string { return "local-graph-model" }

type graphEntryModels struct {
	interfaces.ModelService
	model *graphEntryModel
}

func (m *graphEntryModels) GetChatModel(context.Context, string) (chat.Chat, error) {
	return m.model, nil
}

type graphEntryBases struct {
	interfaces.KnowledgeBaseService
	db     *gorm.DB
	reader *KnowledgeGraphLineageReader
}

func (b *graphEntryBases) GetKnowledgeBaseByIDOnly(ctx context.Context, id string) (*types.KnowledgeBase, error) {
	var kb types.KnowledgeBase
	err := b.db.WithContext(ctx).Where("id = ?", id).Take(&kb).Error
	return &kb, err
}

func (b *graphEntryBases) ReadKnowledgeGraphSnapshot(
	ctx context.Context,
	targets types.SearchTargets,
	namespace types.NameSpace,
	names []string,
) (*types.KnowledgeGraphInputSnapshot, error) {
	return b.reader.ReadKnowledgeGraphSnapshot(ctx, targets, namespace, names)
}

type graphEntryKnowledge struct {
	interfaces.KnowledgeService
	db     *gorm.DB
	leases *repository.NextcloudContentLeaseStore
}

func (k *graphEntryKnowledge) GetKnowledgeByIDOnly(ctx context.Context, id string) (*types.Knowledge, error) {
	var doc types.Knowledge
	err := k.db.WithContext(ctx).Where("id = ?", id).Take(&doc).Error
	return &doc, err
}

func (k *graphEntryKnowledge) BeginNextcloudRead(
	ctx context.Context,
	targets types.SearchTargets,
) (*readlease.NextcloudReadGuard, error) {
	scopes := []readlease.NextcloudKBReadScope{}
	for _, target := range targets {
		scopes = append(scopes, readlease.NextcloudKBReadScope{TenantID: target.TenantID, KBID: target.KnowledgeBaseID})
	}
	return readlease.BeginNextcloudKBRead(ctx, k.leases, scopes)
}

func (k *graphEntryKnowledge) CheckNextcloudReadTargets(context.Context, types.SearchTargets) error {
	return nil
}

func (k *graphEntryKnowledge) GetKnowledgeTags(context.Context, []string) (map[string][]*types.KnowledgeTag, error) {
	return map[string][]*types.KnowledgeTag{}, nil
}

func (k *graphEntryKnowledge) ListKnowledgeIDsByTagIDs(context.Context, uint64, string, []string) ([]string, error) {
	return nil, nil
}

func graphSQLNeoFixture(
	t *testing.T,
) (
	*gorm.DB,
	*repository.KnowledgeInputProvenanceStore,
	interfaces.RetrieveGraphRepository,
	types.NameSpace,
	*graphEntryBases,
	*graphEntryKnowledge,
) {
	t.Helper()
	dsn := os.Getenv("WEKNORA_KG_TEST_NEO4J_URI")
	if dsn == "" {
		t.Skip("owned Neo4j fixture is not configured")
	}
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
			&types.Message{},
			&types.DataSource{},
			&repository.NextcloudSourcePairing{},
		),
	)
	for _, version := range []string{
		"000031_nextcloud_source_versions",
		"000045_nextcloud_source_revisions",
		"000046_nextcloud_content_leases",
		"000047_nextcloud_candidate_retry",
		"000053_knowledge_input_provenance",
		"000054_knowledge_configuration_history_inputs",
		"000055_knowledge_actual_config_controls",
	} {
		script, err := os.ReadFile("../../../migrations/sqlite/" + version + ".up.sql")
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(script)).Error)
	}
	namespace := types.NameSpace{
		KnowledgeBase: "graph_sql_" + strings.ReplaceAll(uuid.NewString(), "-", ""),
		Knowledge:     "human-doc",
	}
	kb := &types.KnowledgeBase{
		ID:             namespace.KnowledgeBase,
		TenantID:       7,
		Name:           "trusted human KB",
		SummaryModelID: "local-graph-model",
		ExtractConfig: &types.ExtractConfig{
			Enabled: true,
			Tags:    []string{"KNOWS"},
			Text:    "actual human config example",
			Nodes: []*types.GraphNode{
				{Name: "config entity", Attributes: []string{"actual human config attribute"}},
			},
			CustomInstructions: "actual human extra instruction",
		},
	}
	store := repository.NewKnowledgeInputProvenanceStore(db)
	require.NoError(
		t,
		store.CreateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(context.Background()), kb),
	)
	driver, err := neo4j.NewDriver(dsn, neo4j.NoAuth())
	require.NoError(t, err)
	require.NoError(t, driver.VerifyConnectivity(context.Background()))
	repo := graphrepo.NewNeo4jRepository(driver)
	t.Cleanup(func() {
		require.NoError(
			t,
			repo.DelGraph(context.Background(), []types.NameSpace{{KnowledgeBase: namespace.KnowledgeBase}}),
		)
		require.NoError(t, driver.Close(context.Background()))
	})
	knowledge := &graphEntryKnowledge{db: db, leases: repository.NewNextcloudContentLeaseStore(db)}
	bases := &graphEntryBases{db: db}
	bases.reader = NewKnowledgeGraphLineageReader(
		knowledge,
		bases,
		repo,
		repository.NewRAGChunkSnapshotReader(db, store),
		NewGraphOriginalInputVerifier(store),
	)
	return db, store, repo, namespace, bases, knowledge
}

func graphAdmittedInput(
	t *testing.T,
	db *gorm.DB,
	store *repository.KnowledgeInputProvenanceStore,
	namespace types.NameSpace,
) (context.Context, *types.KnowledgeBuildInputProof) {
	t.Helper()
	doc := &types.Knowledge{
		ID:              namespace.Knowledge,
		TenantID:        7,
		KnowledgeBaseID: namespace.KnowledgeBase,
		Type:            types.KnowledgeTypeManual,
		Channel:         "web",
		Source:          "manual",
		Title:           "actual SQL title",
		ParseStatus:     types.ParseStatusCompleted,
		EnableStatus:    "enabled",
		CustomMetadata:  types.JSON(`{}`),
		Metadata:        types.JSON(`{"manual":{"content":"original human body","version":1}}`),
	}
	proof, err := store.CreateKnowledgeWithInput(
		repository.WithIndependentKnowledgeInput(context.Background()),
		doc,
		"original human body",
	)
	require.NoError(t, err)
	ctx := repository.WithKnowledgeBuildInputProof(context.Background(), store, proof)
	chunk := &types.Chunk{
		ID:              namespace.Knowledge + "-chunk",
		TenantID:        7,
		KnowledgeBaseID: namespace.KnowledgeBase,
		KnowledgeID:     namespace.Knowledge,
		Content:         "actual admitted parsed SQL body",
		ChunkType:       types.ChunkTypeText,
		IsEnabled:       true,
		IndexStatus:     "ready",
		Metadata:        types.JSON(`{}`),
	}
	require.NoError(t, repository.NewChunkRepository(db).CreateChunks(ctx, []*types.Chunk{chunk}))
	snapshot, err := store.CaptureKnowledgeBuildInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{
			Projection:      types.KnowledgeBuildProjectionGraph,
			TenantID:        7,
			KnowledgeBaseID: namespace.KnowledgeBase,
			KnowledgeID:     namespace.Knowledge,
			ChunkIDs:        []string{chunk.ID},
		},
	)
	require.NoError(t, err)
	return ctx, snapshot.Proof
}

func graphExtractTask(
	ctx context.Context, t *testing.T,

	store *repository.KnowledgeInputProvenanceStore,
	repo interfaces.RetrieveGraphRepository,
	namespace types.NameSpace,
	proof *types.KnowledgeBuildInputProof,
	model *graphEntryModel,
) error {
	t.Helper()
	empty, _ := types.NewCompleteSourceLineage()
	template, err := types.BindGraphPromptTemplateOrigin(
		&types.PromptTemplateStructured{Description: "code-owned local graph protocol %s"},
		empty,
	)
	require.NoError(t, err)
	svc := &ChunkExtractService{
		buildInputs:  store,
		template:     template,
		modelService: &graphEntryModels{model: model},
		graphEngine:  repo,
	}
	payload, err := json.Marshal(
		types.ExtractChunkPayload{
			TenantID:      7,
			KnowledgeID:   namespace.Knowledge,
			ChunkID:       namespace.Knowledge + "-chunk",
			ModelID:       "local-graph-model",
			OriginalInput: proof,
		},
	)
	require.NoError(t, err)
	return svc.Handle(ctx, asynq.NewTask(types.TypeChunkExtract, payload))
}

func TestKnowledgeGraphActualSQLTaskNeo4jAgentOrdinaryEntry(t *testing.T) {
	db, store, repo, namespace, bases, knowledge := graphSQLNeoFixture(t)
	buildCtx, proof := graphAdmittedInput(t, db, store, namespace)
	model := &graphEntryModel{call: func(messages []chat.Message, _ *chat.ChatOptions) {
		require.Contains(t, messages[1].Content, "actual admitted parsed SQL body")
		require.Contains(t, messages[0].Content, "actual human config example")
		require.Contains(t, messages[0].Content, "actual human config attribute")
		require.Contains(t, messages[0].Content, "actual human extra instruction")
	}}
	require.NoError(t, graphExtractTask(buildCtx, t, store, repo, namespace, proof, model))
	targets := types.SearchTargets{
		&types.SearchTarget{
			Type:            types.SearchTargetTypeKnowledgeBase,
			TenantID:        7,
			KnowledgeBaseID: namespace.KnowledgeBase,
		},
	}
	ctx := types.WithCaller(context.Background(), types.Caller{TenantID: 3, UserID: "original-person"})
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "original-person"})
	ctx = types.WithExecutionTenant(ctx, 7)
	ctx = types.WithRAGSourceLineage(
		ctx,
		types.NewRAGSourceLineage(func(ctx context.Context, lineage *types.SourceLineage) error {
			caller, _ := ctx.Value(types.CallerContextKey).(types.Caller)
			require.Equal(t, uint64(3), caller.TenantID)
			require.Equal(t, uint64(7), types.MustTenantIDFromContext(ctx))
			return lineage.RequireComplete()
		}),
	)
	snapshot, readErr := bases.reader.ReadKnowledgeGraphSnapshot(
		ctx,
		targets,
		types.NameSpace{KnowledgeBase: namespace.KnowledgeBase},
		[]string{"shared"},
	)
	require.NoError(t, readErr, "actual reader entry")
	require.NotNil(t, snapshot)
	require.NoError(t, snapshot.Close())
	tool := tools.NewQueryKnowledgeGraphTool(bases, targets).WithKnowledgeScope(knowledge)
	registry := tools.NewToolRegistry()
	registry.RegisterTool(tool)
	args, _ := json.Marshal(
		tools.QueryKnowledgeGraphInput{KnowledgeBaseIDs: []string{namespace.KnowledgeBase}, Query: "shared"},
	)
	result, err := registry.ExecuteToolWithSourceLineage(ctx, tools.ToolQueryKnowledgeGraph, args)
	require.NoError(t, err)
	require.Contains(t, result.Output, "actual graph fact")
	require.Contains(t, result.Output, "actual admitted parsed SQL body")
	require.Contains(t, result.Output, "actual SQL title")
	require.Empty(t, result.SourceLineage.Sources)
	manager := chatpipeline.NewEventManager()
	chatpipeline.NewPluginSearchEntity(
		manager,
		repo,
		repository.NewChunkRepository(db),
		repository.NewKnowledgeRepository(db),
		bases,
	)
	manage := &types.ChatManage{
		PipelineRequest: types.PipelineRequest{SearchTargets: targets},
		PipelineState: types.PipelineState{
			Entity:      []string{"shared"},
			EntityKBIDs: []string{namespace.KnowledgeBase},
		},
	}
	require.Nil(t, manager.Trigger(ctx, types.ENTITY_SEARCH, manage))
	require.Len(t, manage.SearchResult, 1)
	require.Contains(t, manage.SearchResult[0].Content, "actual admitted parsed SQL body")
	require.NotNil(t, manage.GraphResult)
	require.Equal(t, 1, model.calls)
}

func TestKnowledgeGraphActualSQLConfigMutationAndHardDeleteDenyOriginalGraph(t *testing.T) {
	for _, mode := range []string{"config rollback", "chunk rollback", "hard delete"} {
		t.Run(mode, func(t *testing.T) {
			db, store, repo, namespace, bases, _ := graphSQLNeoFixture(t)
			ctx, proof := graphAdmittedInput(t, db, store, namespace)
			require.NoError(t, graphExtractTask(ctx, t, store, repo, namespace, proof, &graphEntryModel{}))
			switch mode {
			case "config rollback":
				require.NoError(
					t,
					db.Model(&types.KnowledgeBase{}).
						Where("id = ?", namespace.KnowledgeBase).
						Update("summary_model_id", "changed").
						Error,
				)
				require.NoError(
					t,
					db.Model(&types.KnowledgeBase{}).
						Where("id = ?", namespace.KnowledgeBase).
						Update("summary_model_id", "local-graph-model").
						Error,
				)
			case "chunk rollback":
				require.NoError(
					t,
					db.Model(&types.Chunk{}).
						Where("knowledge_id = ?", namespace.Knowledge).
						Update("content", "changed").
						Error,
				)
				require.NoError(
					t,
					db.Model(&types.Chunk{}).
						Where("knowledge_id = ?", namespace.Knowledge).
						Update("content", "actual admitted parsed SQL body").
						Error,
				)
			case "hard delete":
				require.NoError(t, db.Unscoped().Where("id = ?", namespace.Knowledge).Delete(&types.Knowledge{}).Error)
			}
			turn := types.NewRAGSourceLineage(func(context.Context, *types.SourceLineage) error { return nil })
			readCtx := types.WithRAGSourceLineage(types.WithExecutionTenant(context.Background(), 7), turn)
			snapshot, err := bases.reader.ReadKnowledgeGraphSnapshot(
				readCtx,
				types.SearchTargets{
					&types.SearchTarget{
						Type:            types.SearchTargetTypeKnowledgeBase,
						TenantID:        7,
						KnowledgeBaseID: namespace.KnowledgeBase,
					},
				},
				types.NameSpace{KnowledgeBase: namespace.KnowledgeBase},
				[]string{"shared"},
			)
			require.Error(t, err)
			require.Nil(t, snapshot)
			require.Error(t, turn.Snapshot().RequireComplete())
		})
	}
}

func TestKnowledgeGraphActualSQLModelOutputWithdrawalWritesNothing(t *testing.T) {
	db, store, repo, namespace, _, _ := graphSQLNeoFixture(t)
	ctx, proof := graphAdmittedInput(t, db, store, namespace)
	model := &graphEntryModel{call: func([]chat.Message, *chat.ChatOptions) {
		require.NoError(
			t,
			db.Model(&types.KnowledgeBase{}).
				Where("id = ?", namespace.KnowledgeBase).
				Update("summary_model_id", "withdrawn during model").
				Error,
		)
	}}
	err := graphExtractTask(ctx, t, store, repo, namespace, proof, model)
	require.Error(t, err)
	require.False(t, errors.Is(err, context.Canceled))
	graph, err := repo.SearchNode(context.Background(), namespace, []string{"shared"})
	require.NoError(t, err)
	require.Empty(t, graph.Node)
	require.Empty(t, graph.InputReceipts)
}

func (k *graphEntryKnowledge) CheckKnowledgePublication(ctx context.Context, doc *types.Knowledge) error {
	if doc.Channel != types.ConnectorTypeNextcloud {
		return nil
	}
	metadata := doc.GetMetadata()
	var ds types.DataSource
	if err := k.db.WithContext(ctx).Where("id = ?", metadata["datasource_id"]).Take(&ds).Error; err != nil {
		return err
	}
	cfg, err := ds.ParseConfig()
	if err != nil {
		return err
	}
	file, err := strconv.ParseInt(metadata["nextcloud_file_id"], 10, 64)
	if err != nil {
		return err
	}
	return nextcloudconnector.CheckCurrentPublication(
		ctx,
		cfg,
		metadata["nextcloud_instance_id"],
		metadata["nextcloud_binding_id"],
		file,
		metadata["nextcloud_etag"],
		metadata["nextcloud_path"],
	)
}

func graphNextcloudSource(
	t *testing.T,
	db *gorm.DB,
	store *repository.KnowledgeInputProvenanceStore,
	namespace types.NameSpace,
	fileID, document, etag string,
) (context.Context, *types.KnowledgeBuildInputProof, *types.Knowledge) {
	t.Helper()
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }),
	)
	t.Cleanup(server.Close)
	t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
	t.Setenv(
		"WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS",
		strings.Trim(os.Getenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS")+","+server.URL, ","),
	)
	bindingID := "binding-" + fileID
	config, err := (&types.DataSourceConfig{Type: types.ConnectorTypeNextcloud, ResourceIDs: []string{
		bindingID,
	}, Settings: map[string]interface{}{
		"base_url": server.URL,
	}, Credentials: map[string]interface{}{"token": "local-only-machine-token", "key_id": "pair_test"}}).ToJSON()
	require.NoError(t, err)
	dsID := "ds-" + fileID
	require.NoError(
		t,
		db.Create(
			&types.DataSource{
				ID:              dsID,
				TenantID:        7,
				KnowledgeBaseID: namespace.KnowledgeBase,
				Type:            types.ConnectorTypeNextcloud,
				Status:          types.DataSourceStatusActive,
				Config:          config,
			},
		).Error,
	)
	base, configSHA, binding, err := repository.NextcloudEventDataSourceIdentity(config)
	require.NoError(t, err)
	require.NoError(
		t,
		db.Create(
			&repository.NextcloudSourcePairing{
				OperationID:     uuid.NewString(),
				TenantID:        7,
				KnowledgeBaseID: namespace.KnowledgeBase,
				DataSourceID:    dsID,
				InstanceID:      "instance",
				BindingID:       binding,
				BaseURL:         base,
				ConfigSHA:       configSHA,
				State:           "active",
			},
		).Error,
	)
	metadata, err := json.Marshal(
		map[string]string{
			"datasource_id":         dsID,
			"external_id":           "nextcloud:instance:" + fileID,
			"nextcloud_instance_id": "instance",
			"nextcloud_binding_id":  bindingID,
			"nextcloud_file_id":     fileID,
			"nextcloud_etag":        "",
			"source_resource_id":    bindingID,
			"nextcloud_path":        "file.md",
		},
	)
	require.NoError(t, err)
	doc := &types.Knowledge{
		ID:              document,
		TenantID:        7,
		KnowledgeBaseID: namespace.KnowledgeBase,
		Title:           "source " + fileID,
		FileName:        "file.md",
		Type:            "file",
		Channel:         types.ConnectorTypeNextcloud,
		Metadata:        metadata,
		ParseStatus:     types.ParseStatusFinalizing,
		EnableStatus:    "disabled",
	}
	require.NoError(t, db.Create(doc).Error)
	repo := repository.NewKnowledgeRepository(db)
	require.NoError(t, repo.(interface {
		StageNextcloudVersion(context.Context, uint64, string, string, string, string, string) error
	}).StageNextcloudVersion(
		context.Background(),
		7,
		namespace.KnowledgeBase,
		dsID,
		"nextcloud:instance:"+fileID,
		etag,
		document,
	))
	require.NoError(t, db.Where("id = ?", doc.ID).Take(doc).Error)
	scope := repository.NextcloudContentScope{
		TenantID:        7,
		KnowledgeBaseID: namespace.KnowledgeBase,
		KnowledgeID:     document,
		DataSourceID:    dsID,
		ExternalID:      "nextcloud:instance:" + fileID,
	}
	leases := repository.NewNextcloudContentLeaseStore(db)
	lease, err := leases.AcquireKnowledge(
		context.Background(),
		scope,
		repository.NextcloudContentBuildLease,
		"original-build-"+fileID,
		time.Minute,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = leases.ReleaseLease(context.Background(), lease.ID) })
	ctx := repository.WithNextcloudBuildLease(context.Background(), leases, lease, scope)
	proof, err := store.AdmitNextcloudBuildInputs(ctx, []byte("actual original source artifact "+fileID))
	require.NoError(t, err)
	ctx = repository.WithKnowledgeBuildInputProof(ctx, store, proof)
	if db.Migrator().HasTable("nextcloud_derived_backend_plans") {
		require.NoError(t, repository.RecordNextcloudDerivedBackendSelection(ctx, nil))
	}
	chunk := &types.Chunk{
		ID:              document + "-chunk",
		TenantID:        7,
		KnowledgeBaseID: namespace.KnowledgeBase,
		KnowledgeID:     document,
		Content:         "actual admitted source body " + fileID,
		ChunkType:       types.ChunkTypeText,
		IsEnabled:       true,
		IndexStatus:     "ready",
		Metadata:        types.JSON(`{}`),
	}
	require.NoError(t, repository.NewChunkRepository(db).CreateChunks(ctx, []*types.Chunk{chunk}))
	snapshot, err := store.CaptureKnowledgeBuildInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{
			Projection:      types.KnowledgeBuildProjectionGraph,
			TenantID:        7,
			KnowledgeBaseID: namespace.KnowledgeBase,
			KnowledgeID:     document,
			ChunkIDs:        []string{chunk.ID},
		},
	)
	require.NoError(t, err)
	return ctx, snapshot.Proof, doc
}

func TestKnowledgeGraphActualNextcloudStagingExtractionPublicationAndWithdrawal(t *testing.T) {
	db, store, repo, namespace, bases, knowledge := graphSQLNeoFixture(t)
	ctx, proof, doc := graphNextcloudSource(t, db, store, namespace, "77", namespace.Knowledge, "original-etag")
	require.NoError(t, graphExtractTask(ctx, t, store, repo, namespace, proof, &graphEntryModel{}))
	sourceRepo := repository.NewKnowledgeRepository(db)
	doc.ParseStatus = types.ParseStatusCompleted
	doc.EnableStatus = "enabled"
	require.NoError(t, sourceRepo.UpdateKnowledge(ctx, doc))
	published, err := sourceRepo.(interface {
		PublishNextcloudVersion(context.Context, string) (bool, error)
	}).PublishNextcloudVersion(ctx, doc.ID)
	require.NoError(t, err)
	require.True(t, published)
	require.NoError(t, store.RecheckKnowledgeBuildInputs(context.Background(), proof))
	checker := sourceRepo.(interface {
		RecheckNextcloudSourceLineage(context.Context, *types.SourceLineage) (bool, error)
	})
	turn := types.NewRAGSourceLineage(func(ctx context.Context, lineage *types.SourceLineage) error {
		matches, err := checker.RecheckNextcloudSourceLineage(ctx, lineage)
		if err != nil {
			return err
		}
		if !matches {
			return types.ErrSourceLineageUnknown
		}
		return nil
	})
	readCtx := types.WithRAGSourceLineage(types.WithExecutionTenant(context.Background(), 7), turn)
	targets := types.SearchTargets{
		&types.SearchTarget{
			Type:            types.SearchTargetTypeKnowledgeBase,
			TenantID:        7,
			KnowledgeBaseID: namespace.KnowledgeBase,
		},
	}
	tool := tools.NewQueryKnowledgeGraphTool(bases, targets).WithKnowledgeScope(knowledge)
	registry := tools.NewToolRegistry()
	registry.RegisterTool(tool)
	args, _ := json.Marshal(
		tools.QueryKnowledgeGraphInput{KnowledgeBaseIDs: []string{namespace.KnowledgeBase}, Query: "shared"},
	)
	result, err := registry.ExecuteToolWithSourceLineage(readCtx, tools.ToolQueryKnowledgeGraph, args)
	require.NoError(t, err)
	require.Contains(t, result.Output, "actual admitted source body 77")
	require.Len(t, result.SourceLineage.Sources, 1)
	require.Equal(t, "original-etag", result.SourceLineage.Sources[0].ETag)
	require.Equal(t, proof.SourceLineage.Sources[0].Revision, result.SourceLineage.Sources[0].Revision)
	scope := repository.NextcloudContentScope{
		TenantID:        7,
		KnowledgeBaseID: namespace.KnowledgeBase,
		KnowledgeID:     doc.ID,
		DataSourceID:    doc.GetMetadata()["datasource_id"],
		ExternalID:      doc.GetMetadata()["external_id"],
	}
	require.NoError(t, repository.NewNextcloudContentLeaseStore(db).RetireKnowledge(context.Background(), scope))
	denied, err := registry.ExecuteToolWithSourceLineage(
		context.WithoutCancel(readCtx),
		tools.ToolQueryKnowledgeGraph,
		args,
	)
	require.Error(t, err)
	require.Nil(t, denied)
}

func publishGraphSource(ctx context.Context, t *testing.T, db *gorm.DB, doc *types.Knowledge) {
	t.Helper()
	repo := repository.NewKnowledgeRepository(db)
	doc.ParseStatus = types.ParseStatusCompleted
	doc.EnableStatus = "enabled"
	require.NoError(t, repo.UpdateKnowledge(ctx, doc))
	published, err := repo.(interface {
		PublishNextcloudVersion(context.Context, string) (bool, error)
	}).PublishNextcloudVersion(ctx, doc.ID)
	require.NoError(t, err)
	require.True(t, published)
}

func TestKnowledgeGraphActualSQLMergedDocumentsRetainEverySourceDuringRead(t *testing.T) {
	db, store, repo, namespace, bases, _ := graphSQLNeoFixture(t)
	sourceA := types.NameSpace{KnowledgeBase: namespace.KnowledgeBase, Knowledge: "source-a"}
	ctxA, proofA, docA := graphNextcloudSource(t, db, store, sourceA, "77", sourceA.Knowledge, "etag-a")
	require.NoError(t, graphExtractTask(ctxA, t, store, repo, sourceA, proofA, &graphEntryModel{}))
	publishGraphSource(ctxA, t, db, docA)
	sourceB := types.NameSpace{KnowledgeBase: namespace.KnowledgeBase, Knowledge: "source-b"}
	ctxB, proofB, docB := graphNextcloudSource(t, db, store, sourceB, "78", sourceB.Knowledge, "etag-b")
	require.NoError(t, graphExtractTask(ctxB, t, store, repo, sourceB, proofB, &graphEntryModel{}))
	publishGraphSource(ctxB, t, db, docB)
	checker := repository.NewKnowledgeRepository(db).(interface {
		RecheckNextcloudSourceLineage(context.Context, *types.SourceLineage) (bool, error)
	})
	newRead := func() context.Context {
		return types.WithRAGSourceLineage(
			types.WithExecutionTenant(context.Background(), 7),
			types.NewRAGSourceLineage(func(ctx context.Context, lineage *types.SourceLineage) error {
				ok, err := checker.RecheckNextcloudSourceLineage(ctx, lineage)
				if err != nil {
					return err
				}
				if !ok {
					return types.ErrSourceLineageUnknown
				}
				return nil
			}),
		)
	}
	targets := types.SearchTargets{
		&types.SearchTarget{
			Type:            types.SearchTargetTypeKnowledgeBase,
			TenantID:        7,
			KnowledgeBaseID: namespace.KnowledgeBase,
		},
	}
	snapshot, err := bases.reader.ReadKnowledgeGraphSnapshot(
		newRead(),
		targets,
		types.NameSpace{KnowledgeBase: namespace.KnowledgeBase},
		[]string{"shared"},
	)
	require.NoError(t, err)
	require.Len(t, snapshot.Graph.Node, 4, "equal names retain both raw originals before presentation collapse")
	require.Len(t, snapshot.SourceLineage.Sources, 2)
	require.Len(t, snapshot.Chunks, 2)
	require.NoError(t, snapshot.Close())
	wrapped := &graphReadWithdrawalRepository{RetrieveGraphRepository: repo, withdraw: func() {
		metadata := docB.GetMetadata()
		require.NoError(
			t,
			repository.NewNextcloudContentLeaseStore(db).
				RetireKnowledge(context.Background(), repository.NextcloudContentScope{
					TenantID:        7,
					KnowledgeBaseID: namespace.KnowledgeBase,
					KnowledgeID:     docB.ID,
					DataSourceID:    metadata["datasource_id"],
					ExternalID:      metadata["external_id"],
				}),
		)
	}}
	bases.reader.graph = wrapped
	withdrawn, err := bases.reader.ReadKnowledgeGraphSnapshot(
		newRead(),
		targets,
		types.NameSpace{KnowledgeBase: namespace.KnowledgeBase},
		[]string{"shared"},
	)
	require.Error(t, err)
	require.Nil(t, withdrawn)
}

type graphReadWithdrawalRepository struct {
	interfaces.RetrieveGraphRepository
	withdraw func()
}

func (r *graphReadWithdrawalRepository) SearchNode(
	ctx context.Context,
	namespace types.NameSpace,
	names []string,
) (*types.GraphData, error) {
	graph, err := r.RetrieveGraphRepository.SearchNode(ctx, namespace, names)
	if err == nil && r.withdraw != nil {
		r.withdraw()
	}
	return graph, err
}

func TestKnowledgeGraphActualSQLUnusedGeneratedDescriptionDoesNotDisableGraph(t *testing.T) {
	db, store, repo, namespace, bases, _ := graphSQLNeoFixture(t)
	ctx, proof := graphAdmittedInput(t, db, store, namespace)
	require.Equal(t, types.KnowledgeBuildProjectionGraph, proof.Projection)
	require.NoError(t, graphExtractTask(ctx, t, store, repo, namespace, proof, &graphEntryModel{}))
	var doc types.Knowledge
	require.NoError(t, db.Where("id = ?", namespace.Knowledge).Take(&doc).Error)
	doc.Description = "new generated description never used by graph model"
	require.NoError(t, repository.NewKnowledgeRepository(db).UpdateKnowledge(ctx, &doc))
	require.NoError(t, store.RecheckKnowledgeBuildInputs(context.Background(), proof))
	readCtx := types.WithRAGSourceLineage(
		types.WithExecutionTenant(context.Background(), 7),
		types.NewRAGSourceLineage(func(context.Context, *types.SourceLineage) error { return nil }),
	)
	snapshot, err := bases.reader.ReadKnowledgeGraphSnapshot(
		readCtx,
		types.SearchTargets{
			&types.SearchTarget{
				Type:            types.SearchTargetTypeKnowledgeBase,
				TenantID:        7,
				KnowledgeBaseID: namespace.KnowledgeBase,
			},
		},
		types.NameSpace{KnowledgeBase: namespace.KnowledgeBase},
		[]string{"shared"},
	)
	require.NoError(t, err)
	require.Len(t, snapshot.Chunks, 1)
	require.NoError(t, snapshot.Close())
}

func TestKnowledgeGraphActualNextcloudOldTupleCannotBorrowFreshChunkReceipt(t *testing.T) {
	db, store, repo, namespace, bases, _ := graphSQLNeoFixture(t)
	ctx, proof, doc := graphNextcloudSource(t, db, store, namespace, "77", namespace.Knowledge, "original-etag")
	require.NoError(t, graphExtractTask(ctx, t, store, repo, namespace, proof, &graphEntryModel{}))
	publishGraphSource(ctx, t, db, doc)
	require.NoError(
		t,
		db.Exec(
			("UPDATE nextcloud_content_fences SET epoch=epoch+1 WHERE tenant_id=7 AND kn"+
				"owledge_base_id=? AND knowledge_id=?"),
			namespace.KnowledgeBase,
			doc.ID,
		).Error,
	)
	versionRepo := repository.NewKnowledgeRepository(db).(interface {
		StageNextcloudVersion(context.Context, uint64, string, string, string, string, string) error
	})
	require.NoError(
		t,
		versionRepo.StageNextcloudVersion(
			context.Background(),
			7,
			namespace.KnowledgeBase,
			doc.GetMetadata()["datasource_id"],
			doc.GetMetadata()["external_id"],
			"new-etag",
			doc.ID,
		),
	)
	raw, err := repo.SearchNode(context.Background(), namespace, []string{"shared"})
	require.NoError(t, err)
	require.Equal(t, "original-etag", raw.SourceLineage.Sources[0].ETag)
	require.Equal(t, proof.SourceLineage.Sources[0].Revision, raw.SourceLineage.Sources[0].Revision)
	readCtx := types.WithRAGSourceLineage(
		types.WithExecutionTenant(context.Background(), 7),
		types.NewRAGSourceLineage(func(context.Context, *types.SourceLineage) error { return nil }),
	)
	snapshot, err := bases.reader.ReadKnowledgeGraphSnapshot(
		readCtx,
		types.SearchTargets{
			&types.SearchTarget{
				Type:            types.SearchTargetTypeKnowledgeBase,
				TenantID:        7,
				KnowledgeBaseID: namespace.KnowledgeBase,
			},
		},
		types.NameSpace{KnowledgeBase: namespace.KnowledgeBase},
		[]string{"shared"},
	)
	require.Error(t, err)
	require.Nil(t, snapshot)
}

type graphConversationModel struct {
	round   int
	useTool bool
	calls   [][]chat.Message
}

func (m *graphConversationModel) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	panic("unexpected nonstream model call")
}

func (m *graphConversationModel) ChatStream(
	_ context.Context,
	messages []chat.Message,
	_ *chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	m.calls = append(m.calls, messages)
	m.round++
	ch := make(chan types.StreamResponse, 1)
	if m.useTool && m.round == 1 {
		ch <- types.StreamResponse{ResponseType: types.ResponseTypeToolCall, ToolCalls: []types.LLMToolCall{{
			ID:   "graph-read",
			Type: "function",
			Function: types.FunctionCall{
				Name:      tools.ToolQueryKnowledgeGraph,
				Arguments: `{"knowledge_base_ids":["b1"],"query":"shared"}`,
			},
		}}, Done: true, FinishReason: "tool_calls"}
	} else {
		ch <- types.StreamResponse{
			ResponseType: types.ResponseTypeAnswer,
			Content:      "A citation-free graph conclusion.",
			Done:         true,
			FinishReason: "stop",
		}
	}
	close(ch)
	return ch, nil
}
func (m *graphConversationModel) GetModelID() string   { return "local-conversation-stub" }
func (m *graphConversationModel) GetModelName() string { return "local-conversation-stub" }
func TestKnowledgeGraphActualWriterAgentCitationFreeNextTurnWithdrawal(t *testing.T) {
	db, store, repo, namespace, bases, knowledge := graphSQLNeoFixture(t)
	buildCtx, proof, doc := graphNextcloudSource(t, db, store, namespace, "77", namespace.Knowledge, "original-etag")
	require.NoError(t, graphExtractTask(buildCtx, t, store, repo, namespace, proof, &graphEntryModel{}))
	publishGraphSource(buildCtx, t, db, doc)
	checker := repository.NewKnowledgeRepository(db).(interface {
		RecheckNextcloudSourceLineage(context.Context, *types.SourceLineage) (bool, error)
	})
	check := func(ctx context.Context, lineage *types.SourceLineage) error {
		ok, err := checker.RecheckNextcloudSourceLineage(ctx, lineage)
		if err != nil {
			return err
		}
		if !ok {
			return types.ErrSourceLineageUnknown
		}
		return nil
	}
	targets := types.SearchTargets{
		&types.SearchTarget{
			Type:            types.SearchTargetTypeKnowledgeBase,
			TenantID:        7,
			KnowledgeBaseID: namespace.KnowledgeBase,
		},
	}
	registry := tools.NewToolRegistry()
	registry.RegisterTool(tools.NewQueryKnowledgeGraphTool(bases, targets).WithKnowledgeScope(knowledge))
	empty, _ := types.NewCompleteSourceLineage()
	makeEngine := func(model *graphConversationModel) *agent.AgentEngine {
		engine := agent.NewAgentEngine(
			&types.AgentConfig{MaxIterations: 5, Temperature: .3},
			model,
			registry,
			event.NewEventBus(),
			[]*agent.KnowledgeBaseInfo{
				{
					ID:            namespace.KnowledgeBase,
					Name:          "code-owned selector",
					Type:          "document",
					Capabilities:  []string{"chunks"},
					SourceLineage: empty,
				},
			},
			nil,
			"session",
			"",
		)
		require.NotNil(t, engine)
		engine.SetSourceLineageVerifier(check)
		return engine
	}
	ctx := types.WithCaller(context.Background(), types.Caller{TenantID: 3, UserID: "original-person"})
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "original-person"})
	ctx = types.WithExecutionTenant(ctx, 7)
	firstModel := &graphConversationModel{useTool: true}
	first, err := makeEngine(firstModel).Execute(ctx, "session", "turn1", "read graph", nil)
	require.NoError(t, err)
	require.Equal(t, "A citation-free graph conclusion.", first.FinalAnswer)
	require.Equal(t, proof.SourceLineage, first.SourceLineage)
	require.Len(t, firstModel.calls, 2)
	rendered := ""
	for _, message := range firstModel.calls[1] {
		rendered += message.Content
	}
	require.Contains(t, rendered, "actual graph fact")
	require.Contains(t, rendered, "KNOWS")
	followModel := &graphConversationModel{}
	follow, err := makeEngine(
		followModel,
	).Execute(ctx, "session", "turn2", "explain again", []chat.Message{{
		Role:          "assistant",
		Content:       first.FinalAnswer,
		SourceLineage: first.SourceLineage,
	}})
	require.NoError(t, err)
	require.Equal(t, first.SourceLineage, follow.SourceLineage)
	metadata := doc.GetMetadata()
	require.NoError(
		t,
		repository.NewNextcloudContentLeaseStore(db).
			RetireKnowledge(context.Background(), repository.NextcloudContentScope{
				TenantID:        7,
				KnowledgeBaseID: namespace.KnowledgeBase,
				KnowledgeID:     doc.ID,
				DataSourceID:    metadata["datasource_id"],
				ExternalID:      metadata["external_id"],
			}),
	)
	blockedModel := &graphConversationModel{}
	_, err = makeEngine(
		blockedModel,
	).Execute(ctx, "session", "turn3", "continue", []chat.Message{{
		Role:          "assistant",
		Content:       follow.FinalAnswer,
		SourceLineage: follow.SourceLineage,
	}})
	require.ErrorIs(t, err, agent.ErrAgentSourceLineageDenied)
	require.Empty(t, blockedModel.calls)
}

func TestKnowledgeGraphActualSQLUnusedKBDescriptionDoesNotRestampOrDisableGraph(t *testing.T) {
	db, store, repo, namespace, bases, _ := graphSQLNeoFixture(t)
	ctx, proof := graphAdmittedInput(t, db, store, namespace)
	require.NoError(t, graphExtractTask(ctx, t, store, repo, namespace, proof, &graphEntryModel{}))
	require.NoError(
		t,
		db.Model(&types.KnowledgeBase{}).
			Where("id = ?", namespace.KnowledgeBase).
			Update("description", "unused generated KB description").
			Error,
	)
	require.NoError(t, store.RecheckKnowledgeBuildInputs(context.Background(), proof))
	readCtx := types.WithRAGSourceLineage(
		types.WithExecutionTenant(context.Background(), 7),
		types.NewRAGSourceLineage(func(context.Context, *types.SourceLineage) error { return nil }),
	)
	snapshot, err := bases.reader.ReadKnowledgeGraphSnapshot(
		readCtx,
		types.SearchTargets{
			&types.SearchTarget{
				Type:            types.SearchTargetTypeKnowledgeBase,
				TenantID:        7,
				KnowledgeBaseID: namespace.KnowledgeBase,
			},
		},
		types.NameSpace{KnowledgeBase: namespace.KnowledgeBase},
		[]string{"shared"},
	)
	require.NoError(t, err)
	require.Equal(t, proof.InputDigest, snapshot.Graph.Node[0].SourceReceipt.InputDigest)
	require.NoError(t, snapshot.Close())
}
