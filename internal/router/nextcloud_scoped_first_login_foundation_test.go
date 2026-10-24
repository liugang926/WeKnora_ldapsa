//go:build sqlite_fts5

package router

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	sqlindex "github.com/Tencent/WeKnora/internal/application/repository/retriever/sqlite"
	"github.com/Tencent/WeKnora/internal/application/service"
	storagefile "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/database"
	"github.com/Tencent/WeKnora/internal/datasource"
	nc "github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/handler"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/Tencent/WeKnora/internal/utils"
	sqlite_vec "github.com/asg017/sqlite-vec-go-bindings/cgo"
	"github.com/hibiken/asynq"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type scopedFirstLoginInput struct {
	Instance, Directory, OwnerGUID, ReaderGUID, OwnerUID, ReaderUID                  string
	Token, Credential, Operation, Binding, KnowledgeBaseID, DataSourceID, FileSHA256 string
	RootID, FileID                                                                   int64
	BaseURL                                                                          string
}
type scopedFirstLoginLDAP struct {
	ServerURL, ServerName, BindDN, BindPassword, BaseDN, UserBaseDN, GroupBaseDN string
	LoginIdentifier, LoginPassword                                               string
}
type scopedFirstLoginQueue struct {
	mu    sync.Mutex
	tasks []*asynq.Task
}

func (q *scopedFirstLoginQueue) Enqueue(task *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasks = append(q.tasks, task)
	return &asynq.TaskInfo{}, nil
}

func (q *scopedFirstLoginQueue) pop() *asynq.Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.tasks) == 0 {
		return nil
	}
	t := q.tasks[0]
	q.tasks = q.tasks[1:]
	return t
}

type scopedFirstLoginFoundation struct {
	db                *gorm.DB
	cfg               *config.Config
	store             *repository.KnowledgeInputProvenanceStore
	groups            interfaces.GroupAccessService
	runtime           interfaces.DirectoryRuntimeService
	users             interfaces.UserService
	tenants           interfaces.TenantService
	members           interfaces.TenantMemberService
	source            interfaces.DataSourceService
	knowledge         interfaces.KnowledgeService
	base              interfaces.KnowledgeBaseService
	queue             *scopedFirstLoginQueue
	post              interfaces.TaskHandler
	chunks            interfaces.ChunkRepository
	knowledgeRepo     interfaces.KnowledgeRepository
	public            *access.NextcloudPublicationGuard
	guard             *access.NextcloudSourceLineageGuard
	leases            *repository.NextcloudContentLeaseStore
	onboarding        *repository.NextcloudOnboardingStore
	onboardingHandler *handler.NextcloudOnboardingHandler
	knowledgeHandler  *handler.KnowledgeHandler
	baseHandler       *handler.KnowledgeBaseHandler
	ldap              scopedFirstLoginLDAP
}

func scopedFirstLoginPrivateJSON(t *testing.T, path string, target any) {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Zero(t, info.Mode().Perm()&0o077)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, target))
}

func scopedFirstLoginWrite(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0o600))
}

func newScopedFirstLoginFoundation(
	t *testing.T,
	input scopedFirstLoginInput,
	fixture string,
) *scopedFirstLoginFoundation {
	t.Helper()
	sqlite_vec.Auto()
	temp := t.TempDir()
	dbPath := filepath.Join(temp, "first-login.db")
	// Execute the normal fresh SQLite migration path. The test binary is run
	// alone in its own actor, so changing its process cwd cannot affect peers.
	cwd, err := os.Getwd()
	require.NoError(t, err)
	repoRoot, err := filepath.Abs("../..")
	require.NoError(t, err)
	require.NoError(t, os.Chdir(repoRoot))
	require.NoError(
		t,
		database.RunMigrationsWithOptions("sqlite3://"+dbPath, database.MigrationOptions{SQLiteDBPath: dbPath}),
	)
	require.NoError(t, os.Chdir(cwd))
	db, err := gorm.Open(
		sqlite.Open(dbPath+"?_busy_timeout=5000"),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)},
	)
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	journal := filepath.Join(temp, "journal")
	require.NoError(t, os.MkdirAll(journal, 0o700))
	key := make([]byte, 32)
	_, err = rand.Read(key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(journal, "key"), key, 0o600))
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_DIRECTORY", journal)
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_KEY", filepath.Join(journal, "key"))
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_PIN", filepath.Join(journal, "pin"))
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	retention := repository.NewOriginalBodyRetentionStore(db)
	require.NoError(t, retention.InitializeJournal())
	require.NoError(t, repository.InstallOriginalBodyRetentionTriggers(db))
	require.NoError(t, repository.InstallOriginalBodyRetentionCallbacks(db))
	require.NoError(t, retention.VerifyJournalReady())
	require.NoError(t, repository.UpgradeMessageProducerAdvanceSchema(db))
	require.NoError(t, repository.UpgradeMessageGeneratedFieldSchema(db))
	var ldap scopedFirstLoginLDAP
	scopedFirstLoginPrivateJSON(t, fixture+".ldap", &ldap)
	require.NotEmpty(t, ldap.BindPassword)
	require.NotEmpty(t, ldap.LoginPassword)
	require.NotEmpty(t, ldap.ServerURL)
	enabled := true
	cfg := &config.Config{
		Tenant: &config.TenantConfig{EnableRBAC: &enabled},
		Auth:   &config.AuthConfig{},
		Conversation: &config.ConversationConfig{
			GenerateSummaryPrompt:   "Summarize in {{language}}",
			GenerateQuestionsPrompt: "Generate questions from {{content}}",
		},
		Directory: &config.DirectoryConfig{
			Enabled:             true,
			ManagementSource:    config.DirectoryManagementFile,
			ID:                  input.Directory,
			ProviderDisplayName: "Owned PRD Samba AD",
			Servers: []config.DirectoryServerConfig{
				{URL: ldap.ServerURL, ServerName: ldap.ServerName, TLSMode: config.DirectoryTLSLDAPS},
			},
			BindDN:         ldap.BindDN,
			BindPassword:   ldap.BindPassword,
			BaseDN:         ldap.BaseDN,
			UserBaseDN:     ldap.UserBaseDN,
			GroupBaseDN:    ldap.GroupBaseDN,
			UserFilter:     "(&(objectCategory=person)(objectClass=user))",
			GroupFilter:    "(objectClass=group)",
			CAFile:         fixture + ".ca",
			ConnectTimeout: 5 * time.Second,
			QueryTimeout:   10 * time.Second,
			PageSize:       100,
			ResultLimit:    1000,
			SyncInterval:   5 * time.Minute,
			StaleAfter:     15 * time.Minute,
		},
	}
	require.NoError(
		t,
		config.BindIndependentEnrichmentPromptLiteral(cfg, "summary", cfg.Conversation.GenerateSummaryPrompt),
	)
	require.NoError(
		t,
		config.BindIndependentEnrichmentPromptLiteral(cfg, "question", cfg.Conversation.GenerateQuestionsPrompt),
	)
	groups := service.NewGroupAccessService(repository.NewGroupAccessRepository(db))
	directories := repository.NewDirectoryRepository(db)
	usersRepo := repository.NewUserRepository(db)
	tokens := repository.NewAuthTokenRepository(db)
	tenants := service.NewTenantService(repository.NewTenantRepository(db), nil)
	members := service.NewTenantMemberService(repository.NewTenantMemberRepository(db), nil, usersRepo, tokens)
	users := service.NewUserService(cfg, usersRepo, tokens, tenants, members, nil, directories, groups)
	runtime := service.NewDirectoryRuntimeService(
		cfg,
		service.NewDirectoryService(directories),
		directories,
		users,
		tenants,
		tokens,
		nil,
		members,
	)
	service.ConfigureGroupAccessDirectoryRuntime(groups, runtime)
	_, err = runtime.ManualSync(context.Background())
	require.NoError(t, err, "actual complete LDAPS synchronization is required; no synthetic snapshot fallback")
	identity, err := directories.GetIdentityByObjectGUID(context.Background(), input.Directory, input.ReaderGUID)
	require.NoError(t, err)
	require.NotNil(t, identity)
	require.Nil(t, identity.UserID, "reader must never have logged into this WK database")
	// This is an existing operator-owned workspace/template, not the new LDAP
	// reader. Her login must not create a workspace or ordinary membership.
	require.NoError(
		t,
		db.Create(
			&types.Tenant{
				ID:       77,
				Name:     "Existing scoped fixture workspace",
				Business: "owned-first-login",
				Status:   "active",
				RetrieverEngines: types.RetrieverEngines{
					Engines: []types.RetrieverEngineParams{
						{
							RetrieverType:       types.VectorRetrieverType,
							RetrieverEngineType: types.SQLiteRetrieverEngineType,
						},
						{
							RetrieverType:       types.KeywordsRetrieverType,
							RetrieverEngineType: types.SQLiteRetrieverEngineType,
						},
					},
				},
			},
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.User{
				ID:       "scoped-fixture-operator",
				Username: "scoped-fixture-operator",
				Email:    "scoped-fixture-operator@example.test",
				TenantID: 77,
				IsActive: true,
			},
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.TenantMember{
				TenantID: 77,
				UserID:   "scoped-fixture-operator",
				Role:     types.TenantRoleOwner,
				Status:   types.TenantMemberStatusActive,
			},
		).Error,
	)
	store := repository.NewKnowledgeInputProvenanceStore(db)
	repo := repository.NewKnowledgeRepository(db)
	chunks := repository.NewChunkRepository(db)
	bases := repository.NewKnowledgeBaseRepository(db)
	repository.ConfigureKnowledgeInputActorGroupAccess(store, groups)
	factory := func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
		actual := repository.NewKnowledgeInputProvenanceStore(tx)
		repository.ConfigureKnowledgeInputActorGroupAccess(
			actual,
			service.NewGroupAccessService(repository.NewGroupAccessRepository(tx)),
		)
		return actual
	}
	repository.ConfigureKnowledgeDisplayInputWriters(store, repository.NewKnowledgeTagRepository(db), repo, factory)
	repository.ConfigureKnowledgeCurrentReadActor(repo, store)
	sources := repository.NewDataSourceRepository(db)
	pairs := repository.NewNextcloudSourcePairingRepository(db)
	repository.ConfigureDataSourceNameInputWriters(sources, pairs, factory)
	public := access.NewNextcloudPublicationGuard(directories, sources)
	leases := repository.NewNextcloudContentLeaseStore(db)
	backend := "scoped-live-local"
	require.NoError(
		t,
		db.Create(
			&types.StorageBackend{
				ID:       backend,
				TenantID: 77,
				Name:     "Owned local scoped source",
				Provider: "local",
				Status:   types.StorageBackendStatusActive,
			},
		).Error,
	)
	catalog := service.NewResourceCatalog(repository.NewResourceRepository(db))
	// Capacity admission measures the configured storage root before any upload.
	require.NoError(t, os.MkdirAll(filepath.Join(temp, "files"), 0o700))
	files := storagefile.NewResourceCatalogFileService(
		storagefile.NewBackendScopedFileService(
			backend,
			storagefile.NewLocalFileService(filepath.Join(temp, "files"), ""),
		),
		catalog,
		public,
	)
	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	t.Cleanup(pool.Release)
	modelService := service.NewModelService(
		repository.NewModelRepository(db),
		bases,
		nil,
		nil,
		embedding.NewBatchEmbedder(pool),
		tenants,
	)
	var modelInput struct {
		SourceAESKey string      `json:"source_aes_key"`
		Embedding    types.Model `json:"embedding"`
	}
	scopedFirstLoginPrivateJSON(t, fixture+".models", &modelInput)
	model := modelInput.Embedding
	require.Equal(t, types.ModelTypeEmbedding, model.Type)
	require.NotEmpty(t, model.ID)
	require.Equal(t, types.ModelStatusActive, model.Status)
	require.Equal(t, 32, len(modelInput.SourceAESKey), "private source key length")
	require.Positive(t, model.Parameters.EmbeddingParameters.Dimension)
	model.TenantID = 77
	model.Parameters.MaxConcurrency = 1
	model.Parameters.APIKey, err = utils.DecryptAESGCM(model.Parameters.APIKey, []byte(modelInput.SourceAESKey))
	require.NoError(t, err, "private model credential could not be opened")
	require.NotEmpty(t, model.Parameters.APIKey)
	model.Parameters.AppSecret, err = utils.DecryptAESGCM(model.Parameters.AppSecret, []byte(modelInput.SourceAESKey))
	require.NoError(t, err)
	modelInput.SourceAESKey = ""
	require.NoError(t, db.Create(&model).Error)
	registry := retriever.NewRetrieveEngineRegistry(nil, nil)
	require.NoError(
		t,
		registry.Register(
			retriever.NewKVHybridRetrieveEngine(
				sqlindex.NewSQLiteRetrieveEngineRepository(db),
				types.SQLiteRetrieverEngineType,
			),
		),
	)
	queue := &scopedFirstLoginQueue{}
	base := service.NewKnowledgeBaseService(
		bases,
		repo,
		chunks,
		nil,
		nil,
		modelService,
		registry,
		nil,
		repository.NewTenantRepository(db),
		files,
		nil,
		nil,
		queue,
		nil,
		nil,
		sources,
		repository.NewSyncLogRepository(db),
		nil,
		nil,
		catalog,
		nil,
		public,
		leases,
	)
	service.ConfigureKnowledgeBaseInputProvenance(base, store)
	service.ConfigureKnowledgeBaseGroupAccess(base, groups)
	guard := access.NewNextcloudSourceLineageGuard(
		repo.(access.NextcloudLineageSnapshotLookup),
		base,
		nil,
		nil,
		groups,
		public,
		store,
	)
	repository.ConfigureKnowledgeOriginalInputSourceVerifier(
		store,
		service.NewKnowledgeOriginalInputSourceVerifier(
			store,
			guard,
			sources,
			repository.NewNextcloudOnboardingStore(db),
		),
	)
	service.ConfigureKnowledgeBaseRAGChunkHydration(base, repository.NewRAGChunkSnapshotReader(db, store), guard)
	span := service.NewSpanTracker(repository.NewKnowledgeSpanRepository(db), db)
	chunkService := service.NewChunkService(chunks, repo, bases, modelService, registry, nil, queue, span)
	knowledge, err := service.NewKnowledgeService(
		cfg,
		repo,
		nil,
		base,
		repository.NewTenantRepository(db),
		tenants,
		chunkService,
		chunks,
		repository.NewKnowledgeTagRepository(db),
		nil,
		files,
		nil,
		catalog,
		modelService,
		queue,
		nil,
		nil,
		registry,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		span,
		nil,
		public,
		leases,
	)
	require.NoError(t, err)
	service.ConfigureKnowledgeInputProvenance(knowledge, store, store)
	service.ConfigureKnowledgeGroupAccess(knowledge, groups)
	service.ConfigureChunkSummaryInputProvenance(chunkService, knowledge)
	post := service.NewKnowledgePostProcessService(repo, base, chunkService, chunks, queue, nil, nil, span)
	service.ConfigureKnowledgePostProcessInputProvenance(post, store)
	service.ConfigureKnowledgePostProcessHistoryMaterial(post, knowledge)
	service.ConfigureKnowledgeEnrichmentRuntime(post, cfg)
	service.ConfigureKnowledgeAutoTagCompletion(post, repo, store)
	service.ConfigureNextcloudPostProcessLedger(post, repository.NewNextcloudProcessingStateStore(db))
	connectors := datasource.NewConnectorRegistry()
	require.NoError(t, connectors.Register(nc.NewConnector()))
	source := service.NewDataSourceService(
		sources,
		repository.NewSyncLogRepository(db),
		knowledge,
		base,
		queue,
		connectors,
		nil,
		repository.NewTenantRepository(db),
		nil,
		nil,
		repository.NewNextcloudEventInboxRepository(db),
		pairs,
	)
	service.ConfigureDataSourceOriginalInputSources(source, store)
	service.ConfigureDataSourceCapacityAdmission(
		source,
		service.NewNextcloudCapacityAdmission(
			repository.NewNextcloudCapacityStore(db),
			func(context.Context, *types.KnowledgeBase) (interfaces.FileService, error) { return files, nil },
		),
	)
	admin := types.WithExecutionTenant(
		types.WithPrincipal(
			types.WithCaller(
				context.Background(),
				types.Caller{TenantID: 77, UserID: "scoped-fixture-operator", Role: types.TenantRoleOwner},
			),
			types.Principal{Type: types.PrincipalWebUser, ID: "scoped-fixture-operator"},
		),
		77,
	)
	template := &types.KnowledgeBase{
		ID:               "scoped-login-template",
		TenantID:         77,
		Name:             "Owned source template",
		Type:             types.KnowledgeBaseTypeDocument,
		EmbeddingModelID: model.ID,
		StorageBackendID: &backend,
		ChunkingConfig:   types.ChunkingConfig{ChunkSize: 512},
		IndexingStrategy: types.IndexingStrategy{VectorEnabled: true, KeywordEnabled: true},
	}
	require.NoError(
		t,
		store.CreateKnowledgeBaseWithInput(
			repository.WithIndependentKnowledgeBaseNameInput(
				repository.WithIndependentKnowledgeInput(admin),
				template.ID,
				template.Name,
			),
			template,
		),
	)
	unrelated := &types.KnowledgeBase{
		ID:               "scoped-login-unrelated",
		TenantID:         77,
		Name:             "Unrelated inherit KB",
		Type:             types.KnowledgeBaseTypeDocument,
		EmbeddingModelID: model.ID,
		StorageBackendID: &backend,
		IndexingStrategy: template.IndexingStrategy,
	}
	require.NoError(
		t,
		store.CreateKnowledgeBaseWithInput(
			repository.WithIndependentKnowledgeBaseNameInput(
				repository.WithIndependentKnowledgeInput(admin),
				unrelated.ID,
				unrelated.Name,
			),
			unrelated,
		),
	)
	credentialHash := sha256.Sum256([]byte(input.Token))
	require.NoError(
		t,
		db.Create(
			&types.NextcloudOnboardingCredential{
				ID:                input.Credential,
				TokenSHA256:       hex.EncodeToString(credentialHash[:]),
				InstanceID:        input.Instance,
				BaseURL:           input.BaseURL,
				TenantID:          77,
				DirectoryID:       input.Directory,
				TemplateKBID:      template.ID,
				Enabled:           true,
				MaxKnowledgeBases: 2,
			},
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.NextcloudOnboardingDelegation{
				TenantID:    77,
				DirectoryID: input.Directory,
				ObjectGUID:  input.OwnerGUID,
			},
		).Error,
	)
	onboarding := repository.NewNextcloudOnboardingStore(db)
	sourceHandler := handler.NewDataSourceHandler(source, base)
	handler.ConfigureDataSourceGroupAccess(sourceHandler, groups)
	onboardHandler := handler.NewNextcloudOnboardingHandler(
		onboarding,
		handler.NewNextcloudSourcePairingHandler(pairs, sourceHandler),
		repository.NewNextcloudEventInboxRepository(db),
		nil,
	)
	kh := handler.NewKnowledgeHandler(cfg, knowledge, base, nil, nil, queue, repository.NewKnowledgeSpanRepository(db))
	handler.ConfigureKnowledgeHandlerGroupAccess(kh, groups)
	handler.ConfigureKnowledgeHandlerPublicationGuard(kh, public)
	handler.ConfigureKnowledgeHandlerContentLeaseStore(kh, leases)
	handler.ConfigureKnowledgeHandlerOutputLineage(kh, service.NewKnowledgeOutputAuthorizer(store, guard))
	handler.ConfigureKnowledgeHandlerCollectionOutput(kh, service.NewKnowledgeCollectionOutputAuthorizer(store, guard))
	kbh := handler.NewKnowledgeBaseHandler(
		cfg,
		base,
		knowledge,
		nil,
		nil,
		queue,
		nil,
		users,
		files,
		nil,
		nil,
		groups,
		sources,
		leases,
	)
	handler.ConfigureKnowledgeBaseSourceMetadataGuard(kbh, public, pairs)
	return &scopedFirstLoginFoundation{
		db:                db,
		cfg:               cfg,
		store:             store,
		groups:            groups,
		runtime:           runtime,
		users:             users,
		tenants:           tenants,
		members:           members,
		source:            source,
		knowledge:         knowledge,
		base:              base,
		queue:             queue,
		post:              post,
		chunks:            chunks,
		knowledgeRepo:     repo,
		public:            public,
		guard:             guard,
		leases:            leases,
		onboarding:        onboarding,
		onboardingHandler: onboardHandler,
		knowledgeHandler:  kh,
		baseHandler:       kbh,
		ldap:              ldap,
	}
}

func (f *scopedFirstLoginFoundation) processSource(t *testing.T, input scopedFirstLoginInput) *types.Knowledge {
	t.Helper()
	log := &types.SyncLog{
		ID:           "scoped-first-login-sync",
		DataSourceID: input.DataSourceID,
		TenantID:     77,
		Status:       types.SyncLogStatusRunning,
	}
	require.NoError(t, repository.NewSyncLogRepository(f.db).Create(context.Background(), log))
	payload, err := json.Marshal(
		types.DataSourceSyncPayload{TenantID: 77, DataSourceID: input.DataSourceID, SyncLogID: log.ID, ForceFull: true},
	)
	require.NoError(t, err)
	_, err = f.queue.Enqueue(asynq.NewTask(types.TypeDataSourceSync, payload))
	require.NoError(t, err)
	for task := f.queue.pop(); task != nil; task = f.queue.pop() {
		var run func(context.Context, *asynq.Task) error
		switch task.Type() {
		case types.TypeDataSourceSync:
			syncErr := f.source.ProcessSync(context.Background(), task)
			if syncErr != nil {
				var recorded types.SyncLog
				require.NoError(t, f.db.Where("id=?", log.ID).Take(&recorded).Error)
				detail, parseErr := recorded.ParseResult()
				require.NoError(t, parseErr)
				if detail != nil {
					for _, item := range detail.Errors {
						t.Logf("actual sync item failure code=%s", item.Code)
					}
				}
			}
			require.NoError(t, syncErr)
			continue
		case types.TypeDocumentProcess:
			run = f.knowledge.ProcessDocument
		case types.TypeKnowledgePostProcess:
			run = f.post.Handle
		case types.TypeSummaryGeneration:
			run = f.knowledge.ProcessSummaryGeneration
		default:
			t.Fatalf("unexpected actual first-login source task %s", task.Type())
		}
		require.NoError(
			t,
			wrapNextcloudBuild(f.leases, f.knowledgeRepo, f.chunks, run)(context.Background(), task),
			"actual first-login source worker",
		)
	}
	var rows []*types.Knowledge
	require.NoError(
		t,
		f.db.Where("knowledge_base_id=? AND channel=?", input.KnowledgeBaseID, types.ConnectorTypeNextcloud).
			Find(&rows).
			Error,
	)
	require.Len(t, rows, 1)
	row := rows[0]
	require.Equal(t, types.ParseStatusCompleted, row.ParseStatus)
	require.Equal(t, "enabled", row.EnableStatus)
	require.Equal(t, fmt.Sprintf("nextcloud:%s:%d", input.Instance, input.FileID), row.GetMetadata()["external_id"])
	var chunks, indexed int64
	require.NoError(
		t,
		f.db.Model(&types.Chunk{}).
			Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", 77, input.KnowledgeBaseID, row.ID).
			Count(&chunks).
			Error,
	)
	require.Positive(t, chunks)
	require.NoError(
		t,
		f.db.Table("lite_embeddings").
			Where("knowledge_base_id=? AND knowledge_id=? AND dimension>0", input.KnowledgeBaseID, row.ID).
			Count(&indexed).
			Error,
	)
	require.Positive(t, indexed, "real configured embedding must persist its native index")
	return row
}
