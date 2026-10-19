package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	storagefile "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type machinePipelineAutoModel struct {
	calls        int
	beforeReturn func()
}

func (m *machinePipelineAutoModel) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	m.calls++
	if m.beforeReturn != nil {
		m.beforeReturn()
	}
	return &types.ChatResponse{Content: `{"matches":[{"index":2,"confidence":1}]}`}, nil
}
func (m *machinePipelineAutoModel) ChatStream(context.Context, []chat.Message, *chat.ChatOptions) (<-chan types.StreamResponse, error) {
	return nil, nil
}
func (m *machinePipelineAutoModel) GetModelName() string { return "isolated auto" }
func (m *machinePipelineAutoModel) GetModelID() string   { return "auto-model" }

type machinePipelineModels struct {
	interfaces.ModelService
	auto    *machinePipelineAutoModel
	summary *enrichmentTestModel
}

func (m machinePipelineModels) GetChatModel(_ context.Context, id string) (chat.Chat, error) {
	if id == "auto-model" {
		return m.auto, nil
	}
	return m.summary, nil
}
func (m machinePipelineModels) GetEmbeddingModel(context.Context, string) (embedding.Embedder, error) {
	return parentChildEmbedder{}, nil
}

type machinePipelineFixture struct {
	liveHistorySessionID                                                                                             string
	liveHistoryVectorKBID, liveHistoryVectorKnowledgeID, liveHistoryVectorEmbeddingID, liveHistoryVectorAnswerSHA256 string
	externalID, etag                                                                                                 string
	f                                                                                                                *enrichmentTestFixture
	source                                                                                                           *DataSourceService
	ds                                                                                                               *types.DataSource
	allow                                                                                                            *atomic.Bool
	auto                                                                                                             *KnowledgeAutoTagService
	model                                                                                                            *machinePipelineAutoModel
	files                                                                                                            interfaces.FileService
	base                                                                                                             string
	personal                                                                                                         *atomic.Bool
	authenticated                                                                                                    bool
	publicationUnavailableAfterCompletion                                                                            *atomic.Bool
	publicationFailureProbe                                                                                          *atomic.Int64
	publicationAuthorizeCalls                                                                                        *atomic.Int64
}

func newMachinePipelineFixture(t *testing.T, pg bool) *machinePipelineFixture {
	return newMachinePipelineFixtureWithDocument(t, pg, strings.Repeat("Actual original document about its topic, with enough text for chunking and retrieval. ", 4))
}

func newMachinePipelineFixtureWithDocument(t *testing.T, pg bool, artifact string, withMessageMaterials ...bool) *machinePipelineFixture {
	return newMachinePipelineFixtureWithSource(t, pg, artifact, nil, withMessageMaterials...)
}

type machinePipelineSource struct {
	Pair                    repository.NextcloudSourcePairing
	Token, ExternalID, ETag string
	Prepared                func(*repository.NextcloudSourcePairing)
	BeforePrepare           func(*enrichmentTestFixture, *types.KnowledgeBase, *repository.NextcloudSourcePairing) (context.Context, error)
	AfterActivate           func(*enrichmentTestFixture, *repository.NextcloudSourcePairing)
}

func newMachinePipelineFixtureWithSource(t *testing.T, pg bool, artifact string, live *machinePipelineSource, withMessageMaterials ...bool) *machinePipelineFixture {
	t.Helper()
	fullDirectory := len(withMessageMaterials) > 1 && withMessageMaterials[1]
	f := newEnrichmentTestFixture(t, pg, fullDirectory)
	// Remove setup-only ordinary data. Every tested NC knowledge below is
	// created by the actual production sync/file pipeline.
	require.NoError(t, f.db.Unscoped().Where("knowledge_id = ?", f.k.ID).Delete(&types.Chunk{}).Error)
	require.NoError(t, f.db.Unscoped().Where("id = ?", f.k.ID).Delete(&types.Knowledge{}).Error)
	require.NoError(t, f.db.AutoMigrate(&types.DataSource{}, &types.SyncLog{}, &types.StoredResource{}, &types.ResourceBinding{}, &types.ResourceAccessGrant{}, &types.StorageBackend{}, &types.KnowledgeProcessingSpan{}, &types.TaskPendingOp{}))
	require.NoError(t, f.db.Exec("CREATE UNIQUE INDEX machine_span_tuple ON knowledge_processing_spans(knowledge_id,attempt,span_id)").Error)
	require.NoError(t, f.db.Exec("DROP TABLE nextcloud_source_revisions; DROP TABLE nextcloud_source_versions").Error)
	migration := f.db
	directory := "sqlite"
	names := []string{"000031_nextcloud_source_versions", "000045_nextcloud_source_revisions", "000046_nextcloud_content_leases", "000047_nextcloud_candidate_retry", "000035_nextcloud_source_pairing", "000039_nextcloud_source_pairing_abort", "000057_processing_span_original_inputs", "000065_nextcloud_capacity_admission", "000067_nextcloud_capacity_write_intents", "000068_nextcloud_local_derived_gc", "000069_datasource_name_inputs", "000071_original_body_retention", "000072_knowledge_auto_completion_publication", "000082_knowledge_configuration_original_body"}
	if pg {
		directory = "versioned"
		names = []string{"000112_nextcloud_source_versions", "000126_nextcloud_source_revisions", "000127_nextcloud_content_leases", "000128_nextcloud_candidate_retry", "000116_nextcloud_source_pairing", "000120_nextcloud_source_pairing_abort", "000138_processing_span_original_inputs", "000146_nextcloud_capacity_admission", "000148_nextcloud_capacity_write_intents", "000149_nextcloud_local_derived_gc", "000150_datasource_name_inputs", "000152_original_body_retention", "000153_knowledge_auto_completion_publication", "000163_knowledge_configuration_original_body"}
		sql, e := f.db.DB()
		require.NoError(t, e)
		migration, e = gorm.Open(postgres.New(postgres.Config{Conn: sql, PreferSimpleProtocol: true}), &gorm.Config{})
		require.NoError(t, e)
	}
	if len(withMessageMaterials) > 0 && withMessageMaterials[0] {
		// This optional ordinary-HTTP fixture follows the actual material chain
		// before creating any session/message: 70→71→72→73 / 151→152→153→154.
		message, body, steer := "000070_message_original_materials", "000071_original_body_retention", "000073_steer_original_materials"
		if pg {
			message, body, steer = "000151_message_original_materials", "000152_original_body_retention", "000154_steer_original_materials"
		}
		ordered := make([]string, 0, len(names)+2)
		for _, name := range names {
			if name == body {
				ordered = append(ordered, message)
			}
			ordered = append(ordered, name)
		}
		names = append(ordered, steer)
	}
	for _, name := range names {
		script, e := os.ReadFile("../../../migrations/" + directory + "/" + name + ".up.sql")
		require.NoError(t, e)
		require.NoError(t, migration.Exec(string(script)).Error)
	}
	// This fixture exercises the current production pipeline, including the
	// processing ledger and recovery gates added by directory onboarding.
	processing := []string{"000032_nextcloud_event_inbox", "000033_nextcloud_event_dispatch", "000038_nextcloud_gc", "000040_nextcloud_gc_object_claim", "000041_nextcloud_gc_derived_inventory", "000064_nextcloud_publication_recovery", "000075_nextcloud_processing_state", "000076_nextcloud_directory_migrations", "000077_nextcloud_content_revision", "000078_nextcloud_control_recovery", "000079_nextcloud_recovery_build_leases", "000080_nextcloud_graph_recovery_fence"}
	if pg {
		processing = []string{"000113_nextcloud_event_inbox", "000114_nextcloud_event_dispatch", "000119_nextcloud_gc", "000121_nextcloud_gc_object_claim", "000122_nextcloud_gc_derived_inventory", "000145_nextcloud_publication_recovery", "000156_nextcloud_processing_state", "000157_nextcloud_directory_migrations", "000158_nextcloud_content_revision", "000159_nextcloud_control_recovery", "000160_nextcloud_recovery_build_leases", "000161_nextcloud_graph_recovery_fence"}
	}
	for _, name := range processing {
		script, err := os.ReadFile("../../../migrations/" + directory + "/" + name + ".up.sql")
		require.NoError(t, err)
		if strings.HasSuffix(name, "_nextcloud_processing_state") && f.db.Migrator().HasColumn(&types.DataSource{}, "sync_enabled") {
			script = []byte(strings.SplitN(string(script), "\n", 2)[1])
		}
		require.NoError(t, migration.Exec(string(script)).Error)
	}
	installMachineOnboardingSchema(t, f.db, migration)
	installMachineCurrentManagerSchema(t, f.db, migration)
	// Install the actual combined body protocol, bootstrap its owned external
	// journal and retain a connection pool of one thereafter.
	journalDir := t.TempDir()
	require.NoError(t, os.Chmod(journalDir, 0700))
	keyPath := filepath.Join(journalDir, "key")
	pinPath := filepath.Join(journalDir, "pin")
	require.NoError(t, os.WriteFile(keyPath, []byte(strings.Repeat("combination-owned-review-", 2)), 0600))
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_DIRECTORY", journalDir)
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_KEY", keyPath)
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_PIN", pinPath)
	require.NoError(t, repository.NewOriginalBodyRetentionStore(f.db).InitializeJournal())
	require.NoError(t, repository.InstallOriginalBodyRetentionTriggers(f.db))
	require.NoError(t, repository.InstallOriginalBodyRetentionCallbacks(f.db))
	require.NoError(t, repository.NewOriginalBodyRetentionStore(f.db).VerifyJournalReady())
	pool, e := f.db.DB()
	require.NoError(t, e)
	pool.SetMaxOpenConns(1)
	pool.SetMaxIdleConns(1)
	backend := "machine-local"
	require.NoError(t, f.db.Create(&types.StorageBackend{ID: backend, TenantID: 7, Name: "isolated actual local", Provider: "local", Status: types.StorageBackendStatusActive}).Error)
	var kb types.KnowledgeBase
	require.NoError(t, f.db.Where("id='kb'").Take(&kb).Error)
	skip := false
	kb.AutoTagConfig = &types.AutoTagConfig{Enabled: true, ModelID: "auto-model", MaxTags: 2, SkipIfTagged: &skip}
	kb.StorageBackendID = &backend
	kb.ExtractConfig = nil
	kb.VLMConfig = types.VLMConfig{}
	kb.QuestionGenerationConfig = &types.QuestionGenerationConfig{Enabled: false}
	require.NoError(t, f.store.UpdateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(f.ctx), &kb))
	require.NoError(t, f.db.Create(&types.Model{ID: "auto-model", TenantID: 7, Name: "actual local auto mock", Status: types.ModelStatusActive}).Error)
	tags := repository.NewKnowledgeTagRepository(f.db)
	factory := func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
		store := repository.NewKnowledgeInputProvenanceStore(tx)
		repository.ConfigureKnowledgeInputActorGroupAccess(store, NewGroupAccessService(repository.NewGroupAccessRepository(tx)))
		return store
	}
	repository.ConfigureKnowledgeDisplayInputWriters(f.store, tags, f.s.repo, factory)
	require.NoError(t, tags.Create(repository.WithIndependentKnowledgeTagCreateInput(f.ctx, "kb", "actual topic", "", 1), &types.KnowledgeTag{ID: "machine-topic", TenantID: 7, KnowledgeBaseID: "kb", Name: "actual topic", Color: "", SortOrder: 1}))
	allow := &atomic.Bool{}
	allow.Store(true)
	personal := &atomic.Bool{}
	personal.Store(true)
	unavailableAfterCompletion := &atomic.Bool{}
	failureProbe := &atomic.Int64{}
	completedProbes := &atomic.Int64{}
	authorizeCalls := &atomic.Int64{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer isolated-machine", r.Header.Get("Authorization"))
		require.NotEmpty(t, r.Header.Get("X-WeKnora-Signature"))
		require.NotEmpty(t, r.Header.Get("X-WeKnora-Nonce"))
		switch {
		case strings.HasSuffix(r.URL.Path, "/capabilities"):
			fmt.Fprint(w, `{"protocol_version":"1","instance_id":"instance"}`)
		case strings.HasSuffix(r.URL.Path, "/bindings"):
			fmt.Fprint(w, `{"bindings":[{"id":"binding","name":"independent configuration","root_file_id":7,"publication_state":"active","publication_epoch":1}]}`)
		case strings.HasSuffix(r.URL.Path, "/manifest"):
			fmt.Fprintf(w, `{"generation":"generation","complete":true,"next_cursor":null,"items":[{"file_id":77,"etag":"actual-etag","name":"source.md","path":"source.md","mime_type":"text/markdown","size":%d,"mtime":1700000000}]}`, len(artifact))
		case strings.HasSuffix(r.URL.Path, "/changes"):
			http.NotFound(w, r)
		case strings.HasSuffix(r.URL.Path, "/content"):
			require.Equal(t, `"actual-etag"`, r.Header.Get("If-Match"))
			w.Header().Set("ETag", `"actual-etag"`)
			w.Header().Set("Content-Type", "text/markdown")
			fmt.Fprint(w, artifact)
		case strings.HasSuffix(r.URL.Path, "/publication-check"):
			if unavailableAfterCompletion.Load() {
				var completed int64
				require.NoError(t, f.db.Model(&types.Knowledge{}).Where("channel=? AND parse_status=?", types.ConnectorTypeNextcloud, types.ParseStatusCompleted).Count(&completed).Error)
				if completed > 0 {
					probe := completedProbes.Add(1)
					target := failureProbe.Load()
					if target == 0 || probe == target {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
				}
			}
			if !allow.Load() {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			var p struct{ InstanceID, ETag, Path string }
			require.NoError(t, json.NewDecoder(r.Body).Decode(&p))
			require.Equal(t, "actual-etag", p.ETag)
			require.Equal(t, "source.md", p.Path)
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/authorize"):
			authorizeCalls.Add(1)
			if !allow.Load() || !personal.Load() {
				fmt.Fprint(w, `{"allow":false,"reason":"withdrawn","checked_at":1780000000}`)
			} else {
				fmt.Fprint(w, `{"allow":true,"reason":"authorized","policy_revision":"original","source_etag":"actual-etag","checked_at":1780000000}`)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
	t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", server.URL)
	sources := repository.NewDataSourceRepository(f.db)
	pairs := repository.NewNextcloudSourcePairingRepository(f.db)
	repository.ConfigureDataSourceNameInputWriters(sources, pairs, factory)
	proposed := repository.NextcloudSourcePairing{OperationID: "actual-machine-pair", TenantID: 7, KnowledgeBaseID: "kb", InstanceID: "instance", BindingID: "binding", BaseURL: server.URL, PublicationEpoch: 1, KeyID: "local-key"}
	token, externalID, etag := "isolated-machine", "nextcloud:instance:77", "actual-etag"
	if live != nil {
		proposed = live.Pair
		if proposed.KnowledgeBaseID != kb.ID {
			kb.ID = proposed.KnowledgeBaseID
			kb.AutoTagConfig = nil
			require.NoError(t, f.store.CreateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeBaseNameInput(repository.WithIndependentKnowledgeInput(f.ctx), kb.ID, kb.Name), &kb))
		}
		token, externalID, etag = live.Token, live.ExternalID, live.ETag
		t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", proposed.BaseURL)
	}
	prepareCtx := f.ctx
	if live != nil && live.BeforePrepare != nil {
		var err error
		prepareCtx, err = live.BeforePrepare(f, &kb, &proposed)
		require.NoError(t, err)
	}
	pair, created, e := pairs.PrepareSourcePairing(prepareCtx, proposed, token)
	require.NoError(t, e)
	require.True(t, created)
	if live != nil && live.Prepared != nil {
		live.Prepared(&pair)
	}
	require.NoError(t, pairs.ActivateSourcePairing(prepareCtx, pair))
	if live != nil && live.AfterActivate != nil {
		live.AfterActivate(f, &pair)
	}
	ds, e := sources.FindByID(f.ctx, pair.DataSourceID)
	require.NoError(t, e)
	now := time.Now().UTC()
	directoryRecord := &types.Directory{ID: "machine-ad", Protocol: types.DirectoryProtocolAD, Enabled: true, SnapshotVersion: 1, LastSuccessfulSyncAt: &now, StaleAfterSeconds: 900}
	if fullDirectory {
		directoryRecord.Name = "Actual periodic directory"
		directoryRecord.ConfigSource = types.DirectoryConfigSourceDatabase
		directoryRecord.TLSMode = types.DirectoryTLSLDAPS
		directoryRecord.BaseDN = "dc=example,dc=test"
		directoryRecord.UserBaseDN = "ou=People,dc=example,dc=test"
		directoryRecord.GroupBaseDN = "ou=Groups,dc=example,dc=test"
		directoryRecord.UserFilter = "(objectClass=user)"
		directoryRecord.GroupFilter = "(objectClass=group)"
		directoryRecord.ServiceAccountDN = "cn=fixture-reader,dc=example,dc=test"
		directoryRecord.PasswordCiphertext = "fixture-only-ciphertext"
	}
	require.NoError(t, f.db.Create(directoryRecord).Error)
	user := "enrich-human"
	require.NoError(t, f.db.Create(&types.DirectoryIdentity{ID: "machine-ad-user", DirectoryID: "machine-ad", ObjectGUID: "00112233-4455-6677-8899-aabbccddeeff", UserID: &user, Status: types.DirectoryObjectActive, SnapshotVersion: 1}).Error)
	// Use the production repository getter, including sealed worker configuration
	// projections. The general enrichment fixture mock selects full KB rows.
	f.s.kbService = &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(f.db)}
	publication := access.NewNextcloudPublicationGuard(repository.NewDirectoryRepository(f.db), sources)
	groups := NewGroupAccessService(repository.NewGroupAccessRepository(f.db))
	f.guard = access.NewNextcloudSourceLineageGuard(f.s.repo.(access.NextcloudLineageSnapshotLookup), f.s.kbService, nil, nil, groups, publication)
	repository.ConfigureKnowledgeOriginalInputSourceVerifier(f.store, NewKnowledgeOriginalInputSourceVerifier(f.store, f.guard, sources))
	catalog := NewResourceCatalog(repository.NewResourceRepository(f.db))
	base := t.TempDir()
	files := storagefile.NewResourceCatalogFileService(storagefile.NewBackendScopedFileService(backend, storagefile.NewLocalFileService(base, "")), catalog)
	f.s.fileSvc = files
	f.s.graphEngine = parentChildGraphRepo{}
	f.s.resourceCatalog = catalog
	f.s.tenantRepo = repository.NewTenantRepository(f.db)
	f.s.spanTracker = NewSpanTracker(repository.NewKnowledgeSpanRepository(f.db), f.db)
	model := &machinePipelineAutoModel{}
	models := machinePipelineModels{auto: model, summary: f.model}
	f.s.modelService = models
	auto := NewKnowledgeAutoTagService(f.s.repo, tags, f.s.kbService, f.s.chunkService, models, f.s.spanTracker).(*KnowledgeAutoTagService)
	ConfigureKnowledgeAutoTagInputProvenance(auto, f.store, f.guard)
	f.post = &KnowledgePostProcessService{knowledgeRepo: f.s.repo, kbService: f.s.kbService, chunkService: f.s.chunkService, chunkRepo: f.s.chunkRepo, taskEnqueuer: f.queue, knowledgeInputs: f.store, enrichmentConfig: f.s.config, spanTracker: f.s.spanTracker}
	ConfigureKnowledgeAutoTagCompletion(f.post, f.s.repo, f.store)
	ConfigureNextcloudPostProcessLedger(f.post, repository.NewNextcloudProcessingStateStore(f.db))
	registry := datasource.NewConnectorRegistry()
	require.NoError(t, registry.Register(nextcloud.NewConnector()))
	admission := NewNextcloudCapacityAdmission(repository.NewNextcloudCapacityStore(f.db), func(context.Context, *types.KnowledgeBase) (interfaces.FileService, error) { return files, nil })
	source := &DataSourceService{dsRepo: sources, syncLogRepo: repository.NewSyncLogRepository(f.db), knowledgeService: f.s, kbService: f.s.kbService, taskEnqueuer: f.queue, connectorRegistry: registry, tenantRepo: f.s.tenantRepo, nextcloudSourcePairings: pairs, nextcloudEventInbox: repository.NewNextcloudEventInboxRepository(f.db), capacityAdmission: admission, sourceInputs: f.store}
	return &machinePipelineFixture{externalID: externalID, etag: etag, f: f, source: source, ds: ds, allow: allow, auto: auto, model: model, files: files, base: base, personal: personal, publicationUnavailableAfterCompletion: unavailableAfterCompletion, publicationFailureProbe: failureProbe, publicationAuthorizeCalls: authorizeCalls}
}

func (m *machinePipelineFixture) ingest(t *testing.T) *types.Knowledge {
	t.Helper()
	log := &types.SyncLog{ID: "actual-machine-sync", DataSourceID: m.ds.ID, TenantID: 7, Status: types.SyncLogStatusRunning, StartedAt: time.Now().UTC()}
	require.NoError(t, m.source.syncLogRepo.Create(context.Background(), log))
	require.NoError(t, m.source.ProcessSync(context.Background(), enrichmentTask(t, types.TypeDataSourceSync, types.DataSourceSyncPayload{DataSourceID: m.ds.ID, TenantID: 7, SyncLogID: log.ID, ForceFull: true})))
	var completed types.SyncLog
	require.NoError(t, m.f.db.Where("id=?", log.ID).Take(&completed).Error)
	require.Empty(t, completed.ErrorMessage, "actual connector ingestion must not hide per-file errors")
	var row types.Knowledge
	require.NoError(t, m.f.db.Where("channel = ?", types.ConnectorTypeNextcloud).Take(&row).Error)
	require.Equal(t, types.ParseStatusPending, row.ParseStatus)
	require.NotEmpty(t, row.FilePath)
	return &row
}
func (m *machinePipelineFixture) run(t *testing.T, task *asynq.Task) error {
	t.Helper()
	var p struct {
		TenantID        uint64 `json:"tenant_id"`
		KnowledgeBaseID string `json:"knowledge_base_id"`
		KnowledgeID     string `json:"knowledge_id"`
	}
	require.NoError(t, json.Unmarshal(task.Payload(), &p))
	row, lookupErr := m.f.s.repo.GetKnowledgeByID(context.Background(), p.TenantID, p.KnowledgeID)
	if lookupErr != nil {
		return lookupErr
	}
	if row == nil || row.KnowledgeBaseID != p.KnowledgeBaseID {
		return repository.ErrKnowledgeInputChanged
	}
	scope := repository.NextcloudContentScope{TenantID: p.TenantID, KnowledgeBaseID: p.KnowledgeBaseID, KnowledgeID: p.KnowledgeID, DataSourceID: m.ds.ID, ExternalID: m.externalID}
	leases := repository.NewNextcloudContentLeaseStore(m.f.db)
	lease, e := leases.AcquireKnowledge(context.Background(), scope, repository.NextcloudContentBuildLease, "actual-asynq-"+task.Type(), time.Minute)
	if e != nil {
		return e
	}
	defer leases.ReleaseLease(context.Background(), lease.ID)
	if e = leases.WaitForCurrentSourceVersion(context.Background(), scope, lease, m.etag, time.Second); e != nil {
		return e
	}
	parent := context.Background()
	if m.authenticated {
		parent = m.f.ctx
	}
	ctx := repository.WithNextcloudBuildLease(parent, leases, lease, scope)
	switch task.Type() {
	case types.TypeDocumentProcess:
		return m.f.s.ProcessDocument(ctx, task)
	case types.TypeKnowledgePostProcess:
		return m.f.post.Handle(ctx, task)
	case types.TypeKnowledgeAutoTag:
		return m.auto.Handle(ctx, task)
	case types.TypeSummaryGeneration:
		return m.f.s.ProcessSummaryGeneration(ctx, task)
	case types.TypeQuestionGeneration:
		return m.f.s.ProcessQuestionGeneration(ctx, task)
	default:
		return fmt.Errorf("unexpected actual task %s", task.Type())
	}
}
func runKnowledgeMachineActualPipeline(t *testing.T, pg bool) {
	m := newMachinePipelineFixture(t, pg)
	row := m.ingest(t)
	require.Equal(t, "1", row.GetMetadata()["nextcloud_publication_epoch"], "observed epoch belongs in knowledge metadata without altering the sealed connector artifact")
	require.NotNil(t, repository.NewNextcloudContentLeaseStore(m.f.db))
	for i := 0; i < len(m.f.queue.tasks); i++ {
		require.NoError(t, m.run(t, m.f.queue.tasks[i]), m.f.queue.tasks[i].Type())
	}
	require.Equal(t, 1, m.model.calls)
	var originalAutoBodies, originalAutoHeads int64
	require.NoError(t, m.f.db.Table("original_body_payloads").Where("state='live'").Count(&originalAutoBodies).Error)
	require.Greater(t, originalAutoBodies, int64(0), "combined actual producers captured original bodies")
	require.NoError(t, m.f.db.Table("original_body_field_heads").Where("source_table='knowledge_auto_tag_input_manifests'").Count(&originalAutoHeads).Error)
	require.EqualValues(t, 2, originalAutoHeads, "both actual queue and actual model manifests use the installed body protocol")
	require.Equal(t, 1, m.f.model.calls, "actual NC Summary must generate under the private original purpose")
	require.NoError(t, m.f.db.Where("id = ?", row.ID).Take(row).Error)
	require.Equal(t, types.ParseStatusCompleted, row.ParseStatus)
	require.Equal(t, "enabled", row.EnableStatus)
	var chunks []*types.Chunk
	require.NoError(t, m.f.db.Where("knowledge_id = ?", row.ID).Find(&chunks).Error)
	require.NotEmpty(t, chunks)
	m.f.k = row
	m.f.chunks = chunks
	original, e := m.f.store.CaptureKnowledgeOutputInputs(m.f.ctx, types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: row.ID})
	require.NoError(t, e)
	m.f.build = repository.WithKnowledgeBuildInputProof(m.f.ctx, m.f.store, original.Proof)
	kg, e := NewKnowledgeOutputAuthorizer(m.f.store, m.f.guard).Begin(m.f.ctx, []*types.Knowledge{row})
	require.NoError(t, e)
	require.NoError(t, kg.Check(m.f.ctx, []*types.Knowledge{row}))
	cg, e := NewChunkOutputAuthorizer(m.f.store, m.f.guard).Begin(m.f.ctx, types.ChunkOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: row.ID, ChunkIDs: []string{chunks[0].ID}})
	require.NoError(t, e)
	chunkSnapshot, e := cg.Snapshot()
	require.NoError(t, e)
	require.NoError(t, cg.Check(m.f.ctx, chunkSnapshot))
	leases := repository.NewNextcloudContentLeaseStore(m.f.db)
	scope := repository.NextcloudContentScope{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: row.ID, DataSourceID: m.ds.ID, ExternalID: "nextcloud:instance:77"}
	lease, e := leases.AcquireKnowledge(m.f.ctx, scope, repository.NextcloudContentReadLease, "actual-human-read", time.Minute)
	require.NoError(t, e)
	defer leases.ReleaseLease(context.Background(), lease.ID)
	rag, e := repository.NewRAGChunkSnapshotReader(m.f.db, m.f.store).HydrateRAGChunkSnapshots(m.f.ctx, []types.RAGChunkReadRequest{{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: row.ID, ChunkIDs: []string{chunks[0].ID}, Lease: &types.RAGExactReadLease{ID: lease.ID, TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: row.ID, Kind: string(lease.Kind), Epoch: lease.Epoch, OwnerID: lease.OwnerID, ExpiresAt: lease.ExpiresAt}}})
	require.NoError(t, e)
	require.Len(t, rag, 1)
	require.NoError(t, rag[0].SourceLineage.RequireComplete())
	attached, e := m.f.s.repo.GetKnowledgeTags(m.f.ctx, []string{row.ID})
	require.NoError(t, e)
	require.Len(t, attached[row.ID], 2)
	span, e := NewSpanOutputAuthorizer(m.f.store, m.f.guard).Begin(m.f.ctx, types.SpanOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: row.ID})
	require.NoError(t, e)
	snapshot, e := span.Snapshot()
	require.NoError(t, e)
	require.NoError(t, span.Check(m.f.ctx, snapshot))
	found := false
	for _, s := range snapshot.Rows {
		if s.Name == autoTagSpanName {
			found = true
			require.Equal(t, "complete", s.ContentProvenance)
			require.NotEmpty(t, s.Output)
		}
	}
	require.True(t, found)
	m.allow.Store(false)
	require.Error(t, span.Check(m.f.ctx, snapshot))
	_, e = NewKnowledgeOutputAuthorizer(m.f.store, m.f.guard).Begin(m.f.ctx, []*types.Knowledge{row})
	require.Error(t, e)
}
func TestKnowledgeMachineActualPipelineSQLite(t *testing.T) {
	runKnowledgeMachineActualPipeline(t, false)
}
func TestPostgresKnowledgeMachineActualPipeline(t *testing.T) {
	runKnowledgeMachineActualPipeline(t, true)
}
func runKnowledgeMachineOriginalNegatives(t *testing.T, pg bool) {
	for _, mode := range []string{"source withdrawn before parse", "name ABA before parse", "config ABA before parse", "pair ABA before parse", "actual stored bytes changed", "forged document language", "cross KB job", "no private build purpose", "source withdrawn during summary", "source withdrawn during auto", "name ABA during auto", "candidate ABA during auto", "chunk body ABA during auto", "lease retired during auto"} {
		t.Run(mode, func(t *testing.T) {
			m := newMachinePipelineFixture(t, pg)
			row := m.ingest(t)
			task := m.f.queue.tasks[0]
			switch mode {
			case "source withdrawn before parse":
				m.allow.Store(false)
			case "name ABA before parse":
				old := m.ds.Name
				require.NoError(t, m.f.db.Model(&types.DataSource{}).Where("id = ?", m.ds.ID).Update("name", "changed").Error)
				require.NoError(t, m.f.db.Model(&types.DataSource{}).Where("id = ?", m.ds.ID).Update("name", old).Error)
			case "config ABA before parse":
				require.NoError(t, m.f.db.Model(&types.KnowledgeBase{}).Where("id='kb'").Update("summary_model_id", "changed").Error)
				require.NoError(t, m.f.db.Model(&types.KnowledgeBase{}).Where("id='kb'").Update("summary_model_id", "summary").Error)
			case "pair ABA before parse":
				require.NoError(t, m.f.db.Model(&repository.NextcloudSourcePairing{}).Where("datasource_id = ?", m.ds.ID).Update("state", "pending").Error)
				require.NoError(t, m.f.db.Model(&repository.NextcloudSourcePairing{}).Where("datasource_id = ?", m.ds.ID).Update("state", "active").Error)
			case "actual stored bytes changed":
				physical, _, e := m.f.s.resourceCatalog.ResolvePath(m.f.ctx, row.FilePath)
				require.NoError(t, e)
				_, inner, ok := types.ParseStorageBackendPath(physical)
				require.True(t, ok)
				require.NoError(t, os.WriteFile(filepath.Join(m.base, strings.TrimPrefix(inner, "local://")), []byte("changed actual source bytes"), 0600))
			case "forged document language", "cross KB job":
				var p types.DocumentProcessPayload
				require.NoError(t, json.Unmarshal(task.Payload(), &p))
				if mode == "forged document language" {
					p.Language = "forged"
				} else {
					p.KnowledgeBaseID = "other"
				}
				task = enrichmentTask(t, types.TypeDocumentProcess, p)
			case "no private build purpose":
				require.Error(t, m.f.s.ProcessDocument(context.Background(), task))
				require.Zero(t, m.model.calls)
				require.Zero(t, m.f.model.calls)
				return
			}
			if strings.Contains(mode, "before parse") || mode == "actual stored bytes changed" || mode == "forged document language" || mode == "cross KB job" {
				require.Error(t, m.run(t, task))
				require.Zero(t, m.model.calls)
				require.Zero(t, m.f.model.calls)
				var count int64
				require.NoError(t, m.f.db.Model(&types.Chunk{}).Where("knowledge_id = ?", row.ID).Count(&count).Error)
				require.Zero(t, count)
				return
			}
			if mode == "source withdrawn during summary" {
				m.f.model.beforeReturn = func() { m.allow.Store(false) }
			}
			m.model.beforeReturn = func() {
				switch mode {
				case "source withdrawn during auto":
					m.allow.Store(false)
				case "name ABA during auto":
					old := m.ds.Name
					require.NoError(t, m.f.db.Model(&types.DataSource{}).Where("id = ?", m.ds.ID).Update("name", "changed").Error)
					require.NoError(t, m.f.db.Model(&types.DataSource{}).Where("id = ?", m.ds.ID).Update("name", old).Error)
				case "candidate ABA during auto":
					require.NoError(t, m.f.db.Model(&types.KnowledgeTag{}).Where("id='machine-topic'").Update("name", "changed").Error)
					require.NoError(t, m.f.db.Model(&types.KnowledgeTag{}).Where("id='machine-topic'").Update("name", "actual topic").Error)
				case "chunk body ABA during auto":
					var c types.Chunk
					require.NoError(t, m.f.db.Where("knowledge_id = ? AND chunk_type = ?", row.ID, types.ChunkTypeText).Take(&c).Error)
					require.NoError(t, m.f.db.Model(&types.Chunk{}).Where("id = ?", c.ID).Update("content", "changed").Error)
					require.NoError(t, m.f.db.Model(&types.Chunk{}).Where("id = ?", c.ID).Update("content", c.Content).Error)
				case "lease retired during auto":
					require.NoError(t, repository.NewNextcloudContentLeaseStore(m.f.db).RetireKnowledge(context.Background(), repository.NextcloudContentScope{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: row.ID, DataSourceID: m.ds.ID, ExternalID: "nextcloud:instance:77"}))
				}
			}
			denied := false
			for i := 0; i < len(m.f.queue.tasks); i++ {
				err := m.run(t, m.f.queue.tasks[i])
				if err != nil {
					denied = true
					break
				}
			}
			require.True(t, denied)
			if mode == "source withdrawn during summary" {
				require.Equal(t, 1, m.f.model.calls)
				require.Zero(t, m.model.calls)
				require.NoError(t, m.f.db.Where("id = ?", row.ID).Take(row).Error)
				require.Empty(t, row.Description)
			} else {
				require.Equal(t, 1, m.f.model.calls)
				require.Equal(t, 1, m.model.calls)
			}
			var added int64
			require.NoError(t, m.f.db.Model(&types.KnowledgeTagRelation{}).Where("knowledge_id = ? AND tag_id='machine-topic'", row.ID).Count(&added).Error)
			require.Zero(t, added, "denied model output must not commit tags")
		})
	}
}
func TestKnowledgeMachineOriginalNegativesSQLite(t *testing.T) {
	runKnowledgeMachineOriginalNegatives(t, false)
}
func TestPostgresKnowledgeMachineOriginalNegatives(t *testing.T) {
	runKnowledgeMachineOriginalNegatives(t, true)
}

func runKnowledgeMachineAdditionalPublicSources(t *testing.T, pg bool) {
	for _, mode := range []string{"real actor extra published source", "machine cannot borrow extra source grant", "additional personal source withdrawn", "actual user revoked", "auto payload forged", "auto model ABA", "auto tenant ABA"} {
		t.Run(mode, func(t *testing.T) {
			m := newMachinePipelineFixture(t, pg)
			row := m.ingest(t)
			var autoTask *asynq.Task
			for i := 0; i < len(m.f.queue.tasks); i++ {
				task := m.f.queue.tasks[i]
				if task.Type() == types.TypeKnowledgeAutoTag {
					autoTask = task
					break
				}
				require.NoError(t, m.run(t, task))
			}
			require.NotNil(t, autoTask)
			original, e := m.f.store.CaptureKnowledgeOutputInputs(m.f.ctx, types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: row.ID})
			require.NoError(t, e)
			sourceCtx, e := repository.WithDerivedKnowledgeDisplayInput(m.f.ctx, original.Proof)
			require.NoError(t, e)
			tags := repository.NewKnowledgeTagRepository(m.f.db)
			factory := func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
				store := repository.NewKnowledgeInputProvenanceStore(tx)
				repository.ConfigureKnowledgeInputActorGroupAccess(store, NewGroupAccessService(repository.NewGroupAccessRepository(tx)))
				return store
			}
			repository.ConfigureKnowledgeDisplayInputWriters(m.f.store, tags, m.f.s.repo, factory)
			require.NoError(t, tags.Create(sourceCtx, &types.KnowledgeTag{ID: "sourced-unselected", TenantID: 7, KnowledgeBaseID: "kb", Name: "original published source candidate", Color: "", SortOrder: 4}))
			// The original completion intent sealed the candidate list before
			// this new sourced tag existed. It must reject that membership
			// change. A separately admitted fresh producer is required for
			// the new actual candidate list and its inherited sources.
			require.Error(t, m.run(t, autoTask))
			var old types.KnowledgeAutoTagPayload
			require.NoError(t, json.Unmarshal(autoTask.Payload(), &old))
			leases := repository.NewNextcloudContentLeaseStore(m.f.db)
			scope := repository.NextcloudContentScope{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: row.ID, DataSourceID: m.ds.ID, ExternalID: "nextcloud:instance:77"}
			lease, err := leases.AcquireKnowledge(m.f.ctx, scope, repository.NextcloudContentBuildLease, "fresh-auto-producer", time.Minute)
			require.NoError(t, err)
			freshRoot := *original.Proof
			freshRoot.BodyDigest, freshRoot.Projection = "", ""
			freshRoot.ChunkIDs = nil
			fresh := repository.WithKnowledgeBuildInputProof(repository.WithNextcloudBuildLease(m.f.ctx, leases, lease, scope), m.f.store, &freshRoot)
			require.True(t, m.f.post.enqueueAutoTagTask(fresh, types.KnowledgePostProcessPayload{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: row.ID, Language: old.Language}, old.Attempt))
			require.NoError(t, leases.ReleaseLease(context.Background(), lease.ID))
			autoTask = m.f.queue.tasks[len(m.f.queue.tasks)-1]
			m.authenticated = mode != "machine cannot borrow extra source grant"
			if mode == "auto payload forged" {
				var p types.KnowledgeAutoTagPayload
				require.NoError(t, json.Unmarshal(autoTask.Payload(), &p))
				p.Language = "forged actual input"
				autoTask = enrichmentTask(t, types.TypeKnowledgeAutoTag, p)
			}
			m.model.beforeReturn = func() {
				switch mode {
				case "additional personal source withdrawn":
					m.personal.Store(false)
				case "actual user revoked":
					require.NoError(t, m.f.db.Model(&types.User{}).Where("id='enrich-human'").Update("is_active", false).Error)
				case "auto model ABA":
					require.NoError(t, m.f.db.Model(&types.Model{}).Where("id='auto-model'").Update("name", "changed").Error)
					require.NoError(t, m.f.db.Model(&types.Model{}).Where("id='auto-model'").Update("name", "actual local auto mock").Error)
				case "auto tenant ABA":
					require.NoError(t, m.f.db.Model(&types.Tenant{}).Where("id=7").Update("status", "changed").Error)
					require.NoError(t, m.f.db.Model(&types.Tenant{}).Where("id=7").Update("status", "").Error)
				}
			}
			e = m.run(t, autoTask)
			if mode == "real actor extra published source" {
				require.NoError(t, e)
				require.Equal(t, 1, m.model.calls)
				return
			}
			require.Error(t, e)
			if mode == "machine cannot borrow extra source grant" || mode == "auto payload forged" {
				require.Zero(t, m.model.calls)
			} else {
				require.Equal(t, 1, m.model.calls)
			}
			var count int64
			require.NoError(t, m.f.db.Model(&types.KnowledgeTagRelation{}).Where("knowledge_id = ? AND tag_id='machine-topic'", row.ID).Count(&count).Error)
			require.Zero(t, count)
		})
	}
}
func TestKnowledgeMachineAdditionalPublicSourcesSQLite(t *testing.T) {
	runKnowledgeMachineAdditionalPublicSources(t, false)
}
func TestPostgresKnowledgeMachineAdditionalPublicSources(t *testing.T) {
	runKnowledgeMachineAdditionalPublicSources(t, true)
}

func runKnowledgeMachineQuestionsAndProfile(t *testing.T, pg bool) {
	m := newMachinePipelineFixture(t, pg)
	var kb types.KnowledgeBase
	require.NoError(t, m.f.db.Where("id='kb'").Take(&kb).Error)
	kb.QuestionGenerationConfig = &types.QuestionGenerationConfig{Enabled: true, QuestionCount: 2}
	require.NoError(t, m.f.store.UpdateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(m.f.ctx), &kb))
	m.f.model.detectQuestions = true
	m.f.model.response = `{"summary":"A complete generated summary","gist":"Generated original profile title","topics":["actual topic"],"doc_type":"guide","typical_question":"What is this actual topic?"}`
	row := m.ingest(t)
	for i := 0; i < len(m.f.queue.tasks); i++ {
		require.NoError(t, m.run(t, m.f.queue.tasks[i]), m.f.queue.tasks[i].Type())
	}
	require.Equal(t, 2, m.f.model.calls, "actual summary and question models both ran before auto")
	require.Equal(t, 1, m.model.calls)
	require.NoError(t, m.f.db.Where("id = ?", row.ID).Take(row).Error)
	require.Equal(t, types.ParseStatusCompleted, row.ParseStatus)
	require.Equal(t, "source.md", row.Title, "NC file title remains the actual original file title; Summary only produces profile/gist")
	require.NotNil(t, row.Profile)
	require.Equal(t, "Generated original profile title", row.Profile.Gist)
	var chunks []*types.Chunk
	require.NoError(t, m.f.db.Where("knowledge_id = ? AND chunk_type = ?", row.ID, types.ChunkTypeText).Find(&chunks).Error)
	require.NotEmpty(t, chunks)
	require.Contains(t, string(chunks[0].Metadata), "questions")
	cg, e := NewChunkOutputAuthorizer(m.f.store, m.f.guard).Begin(m.f.ctx, types.ChunkOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: row.ID, ChunkIDs: []string{chunks[0].ID}})
	require.NoError(t, e)
	snapshot, e := cg.Snapshot()
	require.NoError(t, e)
	require.NoError(t, cg.Check(m.f.ctx, snapshot))
	span, e := NewSpanOutputAuthorizer(m.f.store, m.f.guard).Begin(m.f.ctx, types.SpanOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: row.ID})
	require.NoError(t, e)
	output, e := span.Snapshot()
	require.NoError(t, e)
	found := false
	for _, r := range output.Rows {
		if r.Name == autoTagSpanName {
			found = true
			require.Equal(t, "complete", r.ContentProvenance)
		}
	}
	require.True(t, found)
}
func TestKnowledgeMachineQuestionsAndProfileSQLite(t *testing.T) {
	runKnowledgeMachineQuestionsAndProfile(t, false)
}
func TestPostgresKnowledgeMachineQuestionsAndProfile(t *testing.T) {
	runKnowledgeMachineQuestionsAndProfile(t, true)
}

func runKnowledgeDataSourceActualNameInputs(t *testing.T, pg bool) {
	for _, mode := range []string{"actual same-value human edit", "partial retains unknown", "current role revoked"} {
		t.Run(mode, func(t *testing.T) {
			m := newMachinePipelineFixture(t, pg)
			require.NoError(t, m.f.db.Model(&types.DataSource{}).Where("id = ?", m.ds.ID).Update("name", "legacy unknown data-source name").Error)
			_, e := m.f.store.ReadDataSourceNameInput(m.f.ctx, 7, "kb", m.ds.ID)
			require.Error(t, e)
			ds, e := m.source.dsRepo.FindByID(m.f.ctx, m.ds.ID)
			require.NoError(t, e)
			if mode == "partial retains unknown" {
				ds.SyncSchedule = "0 0 * * *"
				require.NoError(t, m.source.dsRepo.Update(m.f.ctx, ds))
				_, e = m.f.store.ReadDataSourceNameInput(m.f.ctx, 7, "kb", ds.ID)
				require.Error(t, e)
				return
			}
			ctx := repository.WithIndependentDataSourceNameInput(m.f.ctx, ds.Name)
			if mode == "current role revoked" {
				require.NoError(t, m.f.db.Model(&types.TenantMember{}).Where("user_id='enrich-human' AND tenant_id=7").Update("role", types.TenantRoleViewer).Error)
				require.Error(t, m.source.dsRepo.Update(ctx, ds))
				_, e = m.f.store.ReadDataSourceNameInput(m.f.ctx, 7, "kb", ds.ID)
				require.Error(t, e)
				return
			}
			require.NoError(t, m.source.dsRepo.Update(ctx, ds))
			p, e := m.f.store.ReadDataSourceNameInput(m.f.ctx, 7, "kb", ds.ID)
			require.NoError(t, e)
			require.Equal(t, ds.Name, p.Name)
			require.NoError(t, m.source.dsRepo.Update(m.f.ctx, ds))
			_, e = m.f.store.ReadDataSourceNameInput(m.f.ctx, 7, "kb", ds.ID)
			require.NoError(t, e, "unrelated edits preserve an existing actual name origin")
		})
	}
}
func TestKnowledgeDataSourceActualNameInputsSQLite(t *testing.T) {
	runKnowledgeDataSourceActualNameInputs(t, false)
}
func TestPostgresKnowledgeDataSourceActualNameInputs(t *testing.T) {
	runKnowledgeDataSourceActualNameInputs(t, true)
}
