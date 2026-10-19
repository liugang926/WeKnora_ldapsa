package service

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

const (
	scopedWorkerDirectory     = "worker-prelogin-ad"
	scopedWorkerGUID          = "fedcba98-7654-3210-fedc-ba9876543210"
	scopedWorkerSourceBinding = "binding-worker-original"
	scopedWorkerTargetBinding = "binding-worker-cloned"
)

// A controlled, signed actual HTTP source. The document bytes pass through
// the production connector, file storage, admission and processing pipeline.
// These tests do not claim enterprise LDAPS or an external model provider.
func newScopedConfigurationWorkerSource(t *testing.T, decision *atomic.Int32) *httptest.Server {
	t.Helper()
	artifact := strings.Repeat("Actual source document material for the approved processing template. ", 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bearer := r.Header.Get("Authorization")
		if (bearer != "Bearer actual-worker-source" && bearer != "Bearer actual-worker-target") ||
			r.Header.Get("X-WeKnora-Signature") == "" ||
			r.Header.Get("X-WeKnora-Nonce") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		raw, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		r.Body = io.NopCloser(bytes.NewReader(raw))
		bodySHA := sha256.Sum256(raw)
		canonical := strings.Join(
			[]string{
				"weknora-hmac-sha256-v1",
				r.Method,
				r.URL.EscapedPath(),
				r.URL.RawQuery,
				hex.EncodeToString(bodySHA[:]),
				r.Header.Get("X-WeKnora-Timestamp"),
				r.Header.Get("X-WeKnora-Nonce"),
				r.Header.Get("X-WeKnora-Key-Id"),
			},
			"\n",
		)
		key := sha256.Sum256([]byte(strings.TrimPrefix(bearer, "Bearer ")))
		mac := hmac.New(sha256.New, key[:])
		_, err = mac.Write([]byte(canonical))
		require.NoError(t, err)
		signature, err := hex.DecodeString(r.Header.Get("X-WeKnora-Signature"))
		require.NoError(t, err)
		require.True(t, hmac.Equal(mac.Sum(nil), signature), "actual request body and scope must be signed")
		binding, file, etag := scopedWorkerSourceBinding, int64(77), "actual-etag"
		if strings.Contains(r.URL.Path, scopedWorkerTargetBinding) {
			binding, file, etag = scopedWorkerTargetBinding, 88, "target-etag"
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/capabilities"):
			if _, err := fmt.Fprint(w, `{"protocol_version":"1","instance_id":"instance"}`); err != nil {
				t.Errorf("write fixture response: %v", err)
			}
		case strings.HasSuffix(r.URL.Path, "/bindings"):
			if _, err := fmt.Fprintf(
				w,
				("{\"bindings\":[{\"id\":%q,\"name\":\"Actual source root\",\"root_file_id\"" +
					":7,\"publication_state\":\"active\",\"publication_epoch\":1},{\"id\":%q,\"" +
					"name\":\"Actual cloned root\",\"root_file_id\":8,\"publication_state\":\"a" +
					"ctive\",\"publication_epoch\":1}]}"),
				scopedWorkerSourceBinding,
				scopedWorkerTargetBinding,
			); err != nil {
				t.Errorf("write fixture response: %v", err)
			}
		case strings.HasSuffix(r.URL.Path, "/manifest"):
			if _, err := fmt.Fprintf(
				w,
				("{\"generation\":\"actual-worker-generation\",\"complete\":true,\"next_curs" +
					"or\":null,\"items\":[{\"file_id\":%d,\"etag\":%q,\"name\":\"source.md\",\"" +
					"path\":\"source.md\",\"mime_type\":\"text/markdown\",\"size\":%d,\"mtime\"" +
					":1700000000}]}"),
				file,
				etag,
				len(artifact),
			); err != nil {
				t.Errorf("write fixture response: %v", err)
			}
		case strings.HasSuffix(r.URL.Path, "/changes"):
			http.NotFound(w, r)
		case strings.HasSuffix(r.URL.Path, "/content"):
			require.Equal(t, `"`+etag+`"`, r.Header.Get("If-Match"))
			w.Header().Set("ETag", `"`+etag+`"`)
			w.Header().Set("Content-Type", "text/markdown")
			if _, err := fmt.Fprint(w, artifact); err != nil {
				t.Errorf("write fixture response: %v", err)
			}
		case strings.HasSuffix(r.URL.Path, "/publication-check"):
			var input struct {
				InstanceID string `json:"instance_id"`
				ETag       string `json:"etag"`
				Path       string `json:"path"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&input))
			require.Equal(t, "instance", input.InstanceID)
			require.Equal(t, etag, input.ETag)
			require.Equal(t, "source.md", input.Path)
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/authorize-manager"):
			var scope map[string]string
			require.NoError(t, json.NewDecoder(r.Body).Decode(&scope))
			require.Len(t, scope, 9)
			require.Equal(t, "instance", scope["instance_id"])
			require.Equal(t, "7", scope["tenant_id"])
			require.Equal(t, scopedWorkerDirectory, scope["directory_id"])
			require.Equal(t, scopedWorkerGUID, scope["object_guid"])
			for _, key := range []string{
				"operation_id",
				"knowledge_base_id",
				"data_source_id",
				"processing_template_id",
			} {
				require.NotEmpty(t, scope[key])
			}
			require.Len(t, scope["purpose_sha256"], 64)
			response := map[string]any{}
			for key, value := range scope {
				response[key] = value
			}
			response["binding_id"], response["allow"], response["reason"] = binding, true, "authorized"
			response["root_file_id"], response["rule_id"], response["rule_revision"] = "7",
				"controlled-actual-rule",
				"1"
			if binding == scopedWorkerTargetBinding {
				response["root_file_id"] = "8"
			}
			response["publication_epoch"], response["policy_revision"], response["checked_at"] = "1", strings.Repeat(
				"a",
				64,
			), time.Now().
				Unix()
			require.NoError(t, json.NewEncoder(w).Encode(response))
		case strings.HasSuffix(r.URL.Path, "/authorize"):
			var input struct {
				Directory string `json:"directory_id"`
				GUID      string `json:"object_guid"`
				FileID    int64  `json:"file_id"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&input))
			require.Equal(t, file, input.FileID)
			validActor := (input.Directory == scopedWorkerDirectory && input.GUID == scopedWorkerGUID) ||
				(input.Directory == "machine-ad" && input.GUID == "00112233-4455-6677-8899-aabbccddeeff")
			if !validActor {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			// Only the original template source is revoked/unknown. The exact
			// worker's own machine publication remains current and valid.
			if binding == scopedWorkerSourceBinding && decision.Load() == 2 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			if binding == scopedWorkerSourceBinding && decision.Load() == 1 {
				if _, err := fmt.Fprintf(
					w,
					`{"allow":false,"reason":"withdrawn","checked_at":%d}`,
					time.Now().Unix(),
				); err != nil {
					t.Errorf("write fixture response: %v", err)
				}
				return
			}
			if _, err := fmt.Fprintf(
				w,
				("{\"allow\":true,\"reason\":\"authorized\",\"policy_revision\":\"actual-cur" +
					"rent-policy\",\"source_etag\":%q,\"source_content_revision\":\"0\",\"check" +
					"ed_at\":%d}"),
				etag,
				time.Now().Unix(),
			); err != nil {
				t.Errorf("write fixture response: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func scopedWorkerCredential(t *testing.T, db *gorm.DB, id, base, template string) types.NextcloudOnboardingCredential {
	t.Helper()
	hash := sha256.Sum256([]byte("owned-test-installation-credential-" + id))
	credential := types.NextcloudOnboardingCredential{
		ID:                id,
		TokenSHA256:       hex.EncodeToString(hash[:]),
		TenantID:          7,
		InstanceID:        "instance",
		BaseURL:           base,
		DirectoryID:       scopedWorkerDirectory,
		TemplateKBID:      template,
		Enabled:           true,
		MaxKnowledgeBases: 10,
	}
	require.NoError(t, db.Create(&credential).Error)
	return credential
}

func scopedWorkerActivate(
	pairCtx context.Context, t *testing.T,
	f *enrichmentTestFixture,
	credential types.NextcloudOnboardingCredential,
	op types.NextcloudOnboardingOperation,
	pair *repository.NextcloudSourcePairing,
) {
	t.Helper()
	store := repository.NewNextcloudOnboardingStore(f.db)
	require.NoError(t, store.MarkActive(context.Background(), op))
	sources := repository.NewDataSourceRepository(f.db)
	ds, err := sources.FindByID(context.Background(), pair.DataSourceID)
	require.NoError(t, err)
	cfg, err := ds.ParseConfig()
	require.NoError(t, err)
	cfg.Type = ds.Type
	live, err := nextcloud.InspectPairing(context.Background(), cfg)
	require.NoError(t, err)
	_, err = repository.NewNextcloudEventInboxRepository(f.db).
		Pair(context.Background(), repository.NextcloudEventPairingIdentity{
			TenantID:            op.TenantID,
			KnowledgeBaseID:     op.KnowledgeBaseID,
			DatasourceID:        ds.ID,
			NextcloudInstanceID: live.InstanceID,
			BindingID:           live.BindingID,
			DatasourceBaseURL:   pair.BaseURL,
			DatasourceConfigSHA: pair.ConfigSHA,
			PublicationEpoch:    live.PublicationEpoch,
			PublicationState:    live.PublicationState,
		})
	require.NoError(t, err)
	require.NoError(t, store.RefreshGrantEpoch(context.Background(), op))
	require.NoError(
		t,
		store.ApplyGrantSnapshot(
			context.Background(),
			op,
			types.NextcloudGrantSnapshot{
				Complete:          true,
				PermissionVersion: 1,
				PublicationEpoch:  1,
				Subjects: []types.NextcloudGrantSubject{
					{
						Kind:             "user",
						DirectoryID:      scopedWorkerDirectory,
						ObjectGUID:       scopedWorkerGUID,
						SourceRelationID: "actual-owned-user-mount",
					},
				},
			},
		),
	)
	var current types.NextcloudOnboardingOperation
	require.NoError(t, f.db.Where("operation_id = ?", op.OperationID).Take(&current).Error)
	require.Empty(t, current.ActorUserID)
	require.Equal(t, credential.ID, current.CredentialID)
	purpose, err := store.RecordPairManagerPurpose(pairCtx)
	require.NoError(t, err)
	require.Equal(t, op.OperationID, purpose.OperationID)
	require.Equal(t, pair.DataSourceID, purpose.DataSourceID)
	require.Equal(t, scopedWorkerGUID, purpose.ActorObjectGUID)
}

func installScopedWorkerTemplateDDL(t *testing.T, f *enrichmentTestFixture) {
	t.Helper()
	if !f.db.Migrator().HasTable("knowledge_enrichment_template_inputs") {
		data, err := os.ReadFile("../../../migrations/sqlite/000066_knowledge_enrichment_template_origins.up.sql")
		require.NoError(t, err)
		// AutoMigrate already created this exact nullable column. Keep all actual
		// counters and immutable producer-table triggers from production DDL.
		ddl := strings.Replace(string(data), "ALTER TABLE knowledge_bases ADD COLUMN enrichment_templates TEXT;", "", 1)
		require.NoError(t, f.db.Exec(ddl).Error)
		require.NoError(t, repository.InstallOriginalBodyRetentionTriggers(f.db))
	}
	if !f.db.Migrator().HasTable("nextcloud_onboarding_manager_purposes") {
		data, err := os.ReadFile("../../../migrations/sqlite/000083_nextcloud_current_manager_purpose.up.sql")
		require.NoError(t, err)
		require.NoError(t, f.db.Exec(string(data)).Error)
	}
	require.True(t, f.db.Migrator().HasTable("nextcloud_onboarding_manager_heads"))
}

func newScopedConfigurationWorkerFixture(
	t *testing.T,
	decision *atomic.Int32,
) (*machinePipelineFixture, types.NextcloudOnboardingCredential) {
	t.Helper()
	server := newScopedConfigurationWorkerSource(t, decision)
	var sourceOp types.NextcloudOnboardingOperation
	var sourceCredential types.NextcloudOnboardingCredential
	var sourcePairCtx context.Context
	live := &machinePipelineSource{
		Pair: repository.NextcloudSourcePairing{
			OperationID:      uuid.NewString(),
			TenantID:         7,
			KnowledgeBaseID:  "kb",
			InstanceID:       "instance",
			BindingID:        scopedWorkerSourceBinding,
			BaseURL:          server.URL,
			PublicationEpoch: 1,
			KeyID:            "actual_worker_source",
		},
		Token:      "actual-worker-source",
		ExternalID: "nextcloud:instance:77",
		ETag:       "actual-etag",
	}
	live.BeforePrepare = func(
		f *enrichmentTestFixture,
		kb *types.KnowledgeBase,
		proposed *repository.NextcloudSourcePairing,
	) (context.Context, error) {
		require.True(
			t,
			f.db.Migrator().HasTable("nextcloud_onboarding_operations"),
			"the actual common fixture must install complete 074/081 before pairing",
		)
		installScopedWorkerTemplateDDL(t, f)
		// AutoMigrate does not reproduce the production 028 composite unique
		// constraint required by the actual restricted-resource upsert.
		require.NoError(
			t,
			f.db.Exec(
				("CREATE UNIQUE INDEX IF NOT EXISTS scoped_worker_policy_key ON resource_acc" +
					"ess_policies(tenant_id,resource_type,resource_id)"),
			).Error,
		)
		now := time.Now().UTC()
		require.NoError(
			t,
			f.db.Create(
				&types.Directory{
					ID:                   scopedWorkerDirectory,
					Protocol:             types.DirectoryProtocolAD,
					Enabled:              true,
					SnapshotVersion:      1,
					LastSuccessfulSyncAt: &now,
					StaleAfterSeconds:    900,
				},
			).Error,
		)
		require.NoError(
			t,
			f.db.Create(
				&types.DirectoryIdentity{
					ID:              "actual-unlogged-worker-manager",
					DirectoryID:     scopedWorkerDirectory,
					ObjectGUID:      scopedWorkerGUID,
					Status:          types.DirectoryObjectActive,
					SnapshotVersion: 1,
				},
			).Error,
		)
		require.NoError(
			t,
			f.db.Create(
				&types.NextcloudOnboardingDelegation{
					TenantID:    7,
					DirectoryID: scopedWorkerDirectory,
					ObjectGUID:  scopedWorkerGUID,
				},
			).Error,
		)
		require.NoError(
			t,
			f.db.Model(&types.Model{}).
				Where("id = ?", kb.EmbeddingModelID).
				Update("type", types.ModelTypeEmbedding).
				Error,
		)
		require.NoError(
			t,
			f.db.Model(&types.Model{}).
				Where("id = ?", kb.SummaryModelID).
				Update("type", types.ModelTypeKnowledgeQA).
				Error,
		)
		sourceCredential = scopedWorkerCredential(t, f.db, "actual-source-credential", server.URL, kb.ID)
		store := repository.NewNextcloudOnboardingStore(f.db)
		var err error
		sourceOp, err = store.Reserve(
			context.Background(),
			sourceCredential,
			types.NextcloudOnboardingRequest{
				OperationID:          proposed.OperationID,
				TenantID:             7,
				InstanceID:           "instance",
				BindingID:            proposed.BindingID,
				ActorDirectoryID:     scopedWorkerDirectory,
				ActorObjectGUID:      scopedWorkerGUID,
				ProcessingTemplateID: kb.ID,
				Mode:                 "create",
				Name:                 "Actual original source root",
			},
		)
		if err != nil {
			return context.Background(), err
		}
		proposed.KnowledgeBaseID = sourceOp.KnowledgeBaseID
		var actualKB types.KnowledgeBase
		require.NoError(t, f.db.Where("id = ?", sourceOp.KnowledgeBaseID).Take(&actualKB).Error)
		*kb = actualKB
		sourcePairCtx, err = store.BeginPairProducer(
			context.Background(),
			sourceCredential,
			sourceOp,
			scopedWorkerDirectory,
			scopedWorkerGUID,
		)
		return sourcePairCtx, err
	}
	live.AfterActivate = func(f *enrichmentTestFixture, pair *repository.NextcloudSourcePairing) {
		scopedWorkerActivate(sourcePairCtx, t, f, sourceCredential, sourceOp, pair)
	}
	m := newMachinePipelineFixtureWithSource(
		t,
		false,
		"actual external HTTP content is supplied by the owned server",
		live,
		false,
		true,
	)
	source := m.ingest(t)
	for i := 0; i < len(m.f.queue.tasks); i++ {
		require.NoError(t, m.run(t, m.f.queue.tasks[i]), m.f.queue.tasks[i].Type())
	}
	require.NoError(t, m.f.db.Where("id = ?", source.ID).Take(source).Error)
	require.Equal(t, types.ParseStatusCompleted, source.ParseStatus)
	var template types.KnowledgeBase
	require.NoError(t, m.f.db.Where("id = ?", "kb").Take(&template).Error)
	template.ID, template.Name = "actual-worker-derived-template", "Actual processing template"
	template.AutoTagConfig = nil
	template.EnrichmentTemplates = nil
	template.CreatorID = "enrich-human"
	create := repository.WithIndependentKnowledgeBaseNameInput(
		repository.WithIndependentKnowledgeInput(m.f.ctx),
		template.ID,
		template.Name,
	)
	require.NoError(t, m.f.store.CreateKnowledgeBaseWithInput(create, &template))
	templates := NewKnowledgeEnrichmentTemplateService(
		m.f.store,
		m.f.s.repo,
		m.f.s.modelService,
		m.f.guard,
		repository.NewNextcloudContentLeaseStore(m.f.db),
	)
	_, err := templates.GenerateDerivedTemplate(m.f.ctx, 7, template.ID, "summary", []string{source.ID})
	require.NoError(t, err)
	_, inherited, err := m.f.store.ReadKnowledgeTemplateConfiguration(m.f.ctx, 7, template.ID)
	require.NoError(t, err)
	require.Len(t, inherited.Sources, 1)
	require.Equal(t, source.ID, inherited.Sources[0].KnowledgeID)
	credential := scopedWorkerCredential(t, m.f.db, "actual-target-credential", server.URL, template.ID)
	store := repository.NewNextcloudOnboardingStore(m.f.db)
	op, err := store.Reserve(
		context.Background(),
		credential,
		types.NextcloudOnboardingRequest{
			OperationID:          uuid.NewString(),
			TenantID:             7,
			InstanceID:           "instance",
			BindingID:            scopedWorkerTargetBinding,
			ActorDirectoryID:     scopedWorkerDirectory,
			ActorObjectGUID:      scopedWorkerGUID,
			ProcessingTemplateID: template.ID,
			Mode:                 "create",
			Name:                 "Actual cloned processing root",
		},
	)
	require.NoError(t, err)
	pairer := repository.NewNextcloudSourcePairingRepository(m.f.db)
	factory := func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
		actual := repository.NewKnowledgeInputProvenanceStore(tx)
		repository.ConfigureKnowledgeInputActorGroupAccess(
			actual,
			NewGroupAccessService(repository.NewGroupAccessRepository(tx)),
		)
		return actual
	}
	sources := repository.NewDataSourceRepository(m.f.db)
	repository.ConfigureDataSourceNameInputWriters(sources, pairer, factory)
	pairCtx, err := store.BeginPairProducer(
		context.Background(),
		credential,
		op,
		scopedWorkerDirectory,
		scopedWorkerGUID,
	)
	require.NoError(t, err)
	pair, created, err := pairer.PrepareSourcePairing(
		pairCtx,
		repository.NextcloudSourcePairing{
			OperationID:      op.OperationID,
			TenantID:         7,
			KnowledgeBaseID:  op.KnowledgeBaseID,
			InstanceID:       op.InstanceID,
			BindingID:        op.BindingID,
			BaseURL:          server.URL,
			PublicationEpoch: 1,
			KeyID:            "actual_worker_target",
		},
		"actual-worker-target",
	)
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, pairer.ActivateSourcePairing(pairCtx, pair))
	scopedWorkerActivate(pairCtx, t, m.f, credential, op, &pair)
	targetDS, err := sources.FindByID(context.Background(), pair.DataSourceID)
	require.NoError(t, err)
	target := *m
	target.ds, target.externalID, target.etag = targetDS, "nextcloud:instance:88", "target-etag"
	return &target, credential
}

type scopedConfigurationObservingChat struct {
	delegate chat.Chat
	before   func(context.Context, []chat.Message)
}

func (m *scopedConfigurationObservingChat) Chat(
	ctx context.Context,
	messages []chat.Message,
	options *chat.ChatOptions,
) (*types.ChatResponse, error) {
	m.before(ctx, messages)
	return m.delegate.Chat(ctx, messages, options)
}

func (m *scopedConfigurationObservingChat) ChatStream(
	ctx context.Context,
	messages []chat.Message,
	options *chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	return m.delegate.ChatStream(ctx, messages, options)
}
func (m *scopedConfigurationObservingChat) GetModelName() string { return m.delegate.GetModelName() }
func (m *scopedConfigurationObservingChat) GetModelID() string   { return m.delegate.GetModelID() }

type scopedConfigurationObservingModels struct {
	interfaces.ModelService
	summary chat.Chat
}

func (m scopedConfigurationObservingModels) GetChatModel(ctx context.Context, id string) (chat.Chat, error) {
	if id == "summary" {
		return m.summary, nil
	}
	return m.ModelService.GetChatModel(ctx, id)
}

func TestNextcloudSourceBoundConfigurationFreshWorkerActualSQLite(t *testing.T) {
	for _, mode := range []string{
		"positive",
		"revoked before fetch",
		"unknown before fetch",
		"revoked during model",
		"unknown during model",
	} {
		t.Run(mode, func(t *testing.T) {
			decision := &atomic.Int32{}
			m, _ := newScopedConfigurationWorkerFixture(t, decision)
			baselineCalls := m.f.model.calls
			var config types.KnowledgeBase
			require.NoError(t, m.f.db.Where("id = ?", m.ds.KnowledgeBaseID).Take(&config).Error)
			_, sources, err := m.f.store.ReadKnowledgeTemplateConfiguration(m.f.ctx, 7, config.ID)
			require.NoError(t, err)
			verifier := NewKnowledgeOriginalInputSourceVerifier(
				m.f.store,
				m.f.guard,
				repository.NewDataSourceRepository(m.f.db),
			)
			require.ErrorIs(
				t,
				verifier.(*knowledgeOriginalInputSourceVerifier).CheckKnowledgeConfigurationSources(
					context.Background(),
					7,
					config.ID,
					sources,
				),
				access.ErrNextcloudPublicationDenied,
				"a public reader cannot acquire the worker purpose from IDs or lineage",
			)
			observed := false
			m.f.s.modelService = scopedConfigurationObservingModels{
				ModelService: m.f.s.modelService,
				summary: &scopedConfigurationObservingChat{
					delegate: m.f.model,
					before: func(ctx context.Context, messages []chat.Message) {
						observed = true
						caller := types.CallerFromContext(ctx)
						require.Empty(t, caller.UserID)
						require.False(
							t,
							caller.Role.HasPermission(types.TenantRoleContributor),
							"execution scope never supplies an owner/admin/contributor role",
						)
						principal, _ := types.PrincipalFromContext(ctx)
						require.False(
							t,
							principal.Valid(),
							"actual configuration restoration must not manufacture a public principal",
						)
						var bodies int64
						require.NoError(
							t,
							m.f.db.Raw(("SELECT COUNT(*) FROM original_body_leases l JOIN original_body_field_heads"+
								" h ON h.body_id=l.body_id WHERE h.source_table='knowledge_bases' AND h.row"+
								"_key=? AND l.kind='read' AND l.released_at_ms IS NULL"), config.ID).
								Scan(&bodies).
								Error,
						)
						require.Positive(
							t,
							bodies,
							"the actual target configuration bytes retain owned body leases during the model",
						)
						var reads int64
						require.NoError(
							t,
							m.f.db.Table("nextcloud_content_leases").
								Where(
									"knowledge_id = ? AND kind = 'read' AND released_at_ms IS NULL",
									sources.Sources[0].KnowledgeID,
								).
								Count(&reads).
								Error,
						)
						require.Positive(t, reads, "the original source stays physically pinned")
						joined := ""
						for _, message := range messages {
							joined += message.Content
						}
						require.Contains(t, joined, config.EnrichmentTemplates.Content("summary"))
					},
				},
			}
			if mode == "revoked before fetch" {
				decision.Store(1)
			}
			if mode == "unknown before fetch" {
				decision.Store(2)
			}
			if mode == "revoked during model" {
				m.f.model.beforeReturn = func() { decision.Store(1) }
			}
			if mode == "unknown during model" {
				m.f.model.beforeReturn = func() { decision.Store(2) }
			}
			start := len(m.f.queue.tasks)
			log := &types.SyncLog{
				ID:           uuid.NewString(),
				DataSourceID: m.ds.ID,
				TenantID:     7,
				Status:       types.SyncLogStatusRunning,
				StartedAt:    time.Now().UTC(),
			}
			require.NoError(t, m.source.syncLogRepo.Create(context.Background(), log))
			err = m.source.ProcessSync(
				context.Background(),
				enrichmentTask(
					t,
					types.TypeDataSourceSync,
					types.DataSourceSyncPayload{DataSourceID: m.ds.ID, TenantID: 7, SyncLogID: log.ID, ForceFull: true},
				),
			)
			if strings.Contains(mode, "before fetch") {
				if decision.Load() == 1 {
					require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
				} else {
					require.ErrorIs(t, err, access.ErrNextcloudPublicationUnavailable)
				}
				require.Equal(t, baselineCalls, m.f.model.calls)
				var count int64
				require.NoError(
					t,
					m.f.db.Model(&types.Knowledge{}).Where("knowledge_base_id = ?", config.ID).Count(&count).Error,
				)
				require.Zero(t, count)
				return
			}
			require.NoError(t, err)
			var taskErr error
			for i := start; i < len(m.f.queue.tasks); i++ {
				taskErr = m.run(t, m.f.queue.tasks[i])
				if taskErr != nil {
					break
				}
			}
			var current types.Knowledge
			require.NoError(
				t,
				m.f.db.Where("knowledge_base_id = ? AND channel = ?", config.ID, types.ConnectorTypeNextcloud).
					Take(&current).
					Error,
			)
			require.True(t, observed)
			if mode == "positive" {
				require.NoError(t, taskErr)
				require.Equal(t, types.ParseStatusCompleted, current.ParseStatus)
				require.NotEmpty(t, current.Description)
			} else {
				if decision.Load() == 1 {
					require.ErrorIs(t, taskErr, access.ErrNextcloudPublicationDenied)
				} else {
					require.ErrorIs(t, taskErr, access.ErrNextcloudPublicationUnavailable)
				}
				require.Empty(t, current.Description, "no model output is committed after source access changes")
			}
			var liveReadBodies int64
			require.NoError(
				t,
				m.f.db.Table("original_body_leases").
					Where("kind = 'read' AND released_at_ms IS NULL").
					Count(&liveReadBodies).
					Error,
			)
			require.Zero(t, liveReadBodies, "each finished task releases only its owned configuration/body reads")
			var unlogged types.DirectoryIdentity
			require.NoError(t, m.f.db.Where("id = ?", "actual-unlogged-worker-manager").Take(&unlogged).Error)
			require.Nil(t, unlogged.UserID)
		})
	}
}
