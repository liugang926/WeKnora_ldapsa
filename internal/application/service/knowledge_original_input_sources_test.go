package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type sourceDerivedFixtureHook struct {
	BeforeChunk     func(*enrichmentTestFixture, context.Context)
	BeforeKnowledge func(*enrichmentTestFixture, *types.Knowledge)
	LocalDerived    bool
	Graph           bool
}

func sourceBuildFixture(
	t *testing.T,
	pg bool,
	hooks ...sourceDerivedFixtureHook,
) (*enrichmentTestFixture, context.Context, *types.KnowledgeBuildInputProof, *atomic.Bool, repository.NextcloudContentScope) {
	t.Helper()
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	f := newEnrichmentTestFixture(t, pg)
	var kb types.KnowledgeBase
	require.NoError(t, f.db.Where("id='kb'").Take(&kb).Error)
	enabledGraph := len(hooks) == 0 || !hooks[0].LocalDerived
	kb.ExtractConfig = &types.ExtractConfig{
		Enabled: enabledGraph,
		Tags:    []string{"KNOWS"},
		Text:    "actual original extraction configuration",
	}
	if len(hooks) > 0 && hooks[0].Graph {
		kb.IndexingStrategy.GraphEnabled = true
	}
	if len(hooks) > 0 && hooks[0].LocalDerived {
		kb.IndexingStrategy.GraphEnabled = false
	}
	file := "../../../migrations/sqlite/000068_nextcloud_local_derived_gc.up.sql"
	if pg {
		file = "../../../migrations/versioned/000149_nextcloud_local_derived_gc.up.sql"
	}
	data, e := os.ReadFile(file)
	require.NoError(t, e)
	require.NoError(t, f.db.Exec(string(data)).Error)
	require.NoError(t, f.store.UpdateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(f.ctx), &kb))
	require.NoError(t, f.db.AutoMigrate(&types.DataSource{}, &repository.NextcloudSourcePairing{}))
	require.NoError(t, f.db.Exec("DROP TABLE nextcloud_source_revisions").Error)
	require.NoError(t, f.db.Exec("DROP TABLE nextcloud_source_versions").Error)
	paths := []string{
		"000031_nextcloud_source_versions",
		"000045_nextcloud_source_revisions",
		"000046_nextcloud_content_leases",
		"000047_nextcloud_candidate_retry",
	}
	directory := "sqlite"
	migration := f.db
	if pg {
		paths = []string{
			"000112_nextcloud_source_versions",
			"000126_nextcloud_source_revisions",
			"000127_nextcloud_content_leases",
			"000128_nextcloud_candidate_retry",
		}
		directory = "versioned"
		sqlDB, e := f.db.DB()
		require.NoError(t, e)
		migration, e = gorm.Open(postgres.New(postgres.Config{Conn: sqlDB, PreferSimpleProtocol: true}), &gorm.Config{})
		require.NoError(t, e)
	}
	for _, path := range paths {
		data, e := os.ReadFile("../../../migrations/" + directory + "/" + path + ".up.sql")
		require.NoError(t, e)
		require.NoError(t, migration.Exec(string(data)).Error)
	}
	installSourceCurrentControlSchema(t, f.db, migration)
	allow := &atomic.Bool{}
	allow.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer local-only-source-token" ||
			r.Header.Get("X-WeKnora-Signature") == "" ||
			r.Header.Get("X-WeKnora-Nonce") == "" {
			t.Errorf("signed source request missing machine authentication")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/capabilities"):
			if _, err := fmt.Fprint(w, `{"protocol_version":"1","instance_id":"instance"}`); err != nil {
				t.Errorf("write fixture response: %v", err)
			}
		case strings.HasSuffix(r.URL.Path, "/publication-check"):
			var body struct {
				InstanceID string `json:"instance_id"`
				ETag       string `json:"etag"`
				Path       string `json:"path"`
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil || body.InstanceID != "instance" ||
				body.ETag != "original-source-etag" ||
				body.Path != "source.md" ||
				!allow.Load() {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/authorize"):
			if !allow.Load() {
				if _, err := fmt.Fprint(w, `{"allow":false,"reason":"withdrawn","checked_at":1780000000}`); err != nil {
					t.Errorf("write fixture response: %v", err)
				}
			} else {
				if _, err := fmt.Fprint(
					w,
					("{\"allow\":true,\"reason\":\"authorized\",\"policy_revision\":\"original-p" +
						"olicy\",\"source_etag\":\"original-source-etag\",\"checked_at\":1780000000" +
						"}"),
				); err != nil {
					t.Errorf("write fixture response: %v", err)
				}
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
	t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", server.URL)
	config, e := (&types.DataSourceConfig{Type: types.ConnectorTypeNextcloud, ResourceIDs: []string{"binding-source"}, Settings: map[string]interface{}{"base_url": server.URL}, Credentials: map[string]interface{}{"token": "local-only-source-token", "key_id": "local_test"}}).ToJSON()
	require.NoError(t, e)
	ds := &types.DataSource{
		ID:              "source-ds",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		Type:            types.ConnectorTypeNextcloud,
		Status:          types.DataSourceStatusActive,
		Config:          config,
	}
	require.NoError(t, f.db.Create(ds).Error)
	require.NoError(t, f.db.Where("id = ?", ds.ID).Take(ds).Error)
	base, sha, binding, e := repository.NextcloudEventDataSourceIdentity(ds.Config)
	require.NoError(t, e)
	require.NoError(
		t,
		f.db.Create(
			&repository.NextcloudSourcePairing{
				OperationID:     "original-pair",
				TenantID:        7,
				KnowledgeBaseID: "kb",
				DataSourceID:    ds.ID,
				InstanceID:      "instance",
				BindingID:       binding,
				BaseURL:         base,
				ConfigSHA:       sha,
				State:           "active",
			},
		).Error,
	)
	metadata, e := json.Marshal(
		map[string]string{
			"datasource_id":         ds.ID,
			"external_id":           "nextcloud:instance:77",
			"source_resource_id":    binding,
			"nextcloud_instance_id": "instance",
			"nextcloud_binding_id":  binding,
			"nextcloud_file_id":     "77",
			"nextcloud_etag":        "",
			"nextcloud_path":        "source.md",
		},
	)
	require.NoError(t, e)
	k := &types.Knowledge{
		ID:                   "source-candidate",
		TenantID:             7,
		KnowledgeBaseID:      "kb",
		Title:                "original source title",
		FileName:             "source.md",
		Type:                 "file",
		Channel:              types.ConnectorTypeNextcloud,
		Metadata:             metadata,
		CustomMetadata:       types.JSON(`{}`),
		ParseStatus:          types.ParseStatusFinalizing,
		EnableStatus:         "disabled",
		PendingSubtasksCount: 1,
	}
	if len(hooks) > 0 && hooks[0].BeforeKnowledge != nil {
		hooks[0].BeforeKnowledge(f, k)
	}
	require.NoError(t, f.db.Create(k).Error)
	require.NoError(t, f.s.repo.(interface {
		StageNextcloudVersion(context.Context, uint64, string, string, string, string, string) error
	}).StageNextcloudVersion(context.Background(), 7, "kb", ds.ID, "nextcloud:instance:77", "original-source-etag", k.ID))
	require.NoError(t, f.db.Where("id = ?", k.ID).Take(k).Error)
	scope := repository.NextcloudContentScope{
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     k.ID,
		DataSourceID:    ds.ID,
		ExternalID:      "nextcloud:instance:77",
	}
	leases := repository.NewNextcloudContentLeaseStore(f.db)
	lease, e := leases.AcquireKnowledge(
		context.Background(),
		scope,
		repository.NextcloudContentBuildLease,
		"actual-private-build",
		time.Minute,
	)
	require.NoError(t, e)
	t.Cleanup(func() { _ = leases.ReleaseLease(context.Background(), lease.ID) })
	ctx := repository.WithNextcloudBuildLease(context.Background(), leases, lease, scope)
	proof, e := f.store.AdmitNextcloudBuildInputs(ctx, []byte("actual downloaded original source artifact"))
	require.NoError(t, e)
	ctx = repository.WithKnowledgeBuildInputProof(ctx, f.store, proof)
	if len(hooks) == 0 || !hooks[0].LocalDerived {
		tenant, e := f.s.tenantRepo.GetTenantByID(f.ctx, 7)
		require.NoError(t, e)
		engine, e := retriever.CreateRetrieveEngineFromPayload(
			ctx,
			f.s.retrieveEngine,
			f.s.ownership,
			tenant.ID,
			tenant.GetEffectiveEngines(),
			kb.VectorStoreID,
		)
		require.NoError(t, e)
		require.NoError(t, repository.RecordNextcloudDerivedBackendSelection(ctx, engine.SelectedBackendTypes()))
	}
	directories := repository.NewDirectoryRepository(f.db)
	sources := repository.NewDataSourceRepository(f.db)
	publication := access.NewNextcloudPublicationGuard(directories, sources)
	groups := NewGroupAccessService(repository.NewGroupAccessRepository(f.db))
	f.guard = access.NewNextcloudSourceLineageGuard(
		f.s.repo.(access.NextcloudLineageSnapshotLookup),
		f.s.kbService,
		nil,
		nil,
		groups,
		publication,
	)
	repository.ConfigureKnowledgeOriginalInputSourceVerifier(
		f.store,
		NewKnowledgeOriginalInputSourceVerifier(f.store, f.guard, sources),
	)
	if len(hooks) > 0 && hooks[0].BeforeChunk != nil {
		hooks[0].BeforeChunk(f, ctx)
	}
	c := &types.Chunk{
		ID:              "source-body",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     k.ID,
		Content:         strings.Repeat("Actual text parsed from the original source artifact. ", 3),
		ChunkType:       types.ChunkTypeText,
		IsEnabled:       true,
		IndexStatus:     "ready",
		Metadata:        types.JSON(`{}`),
	}
	require.NoError(t, f.s.chunkRepo.CreateChunks(ctx, []*types.Chunk{c}))
	now := time.Now().UTC()
	require.NoError(
		t,
		f.db.Create(
			&types.Directory{
				ID:                   "local-ad-fixture",
				Name:                 "isolated AD snapshot",
				Protocol:             types.DirectoryProtocolAD,
				Enabled:              true,
				SnapshotVersion:      1,
				LastSuccessfulSyncAt: &now,
				StaleAfterSeconds:    900,
			},
		).Error,
	)
	user := "enrich-human"
	require.NoError(
		t,
		f.db.Create(
			&types.DirectoryIdentity{
				ID:              "local-linked-human",
				DirectoryID:     "local-ad-fixture",
				ObjectGUID:      "00112233-4455-6677-8899-aabbccddeeff",
				UserID:          &user,
				Status:          types.DirectoryObjectActive,
				SnapshotVersion: 1,
			},
		).Error,
	)
	f.k = k
	f.build = ctx
	f.chunks = []*types.Chunk{c}
	return f, ctx, proof, allow, scope
}

func runNextcloudOriginalBuildPurpose(t *testing.T, pg bool) {
	for _, mode := range []string{"normal build publish and guarded read", "remote source withdrawn during model", "stage epoch retired during model", "forged original artifact", "public staging read denied"} {
		t.Run(mode, func(t *testing.T) {
			f, ctx, original, allow, scope := sourceBuildFixture(t, pg)
			actual, e := f.store.CaptureKnowledgeEnrichmentInputs(
				ctx,
				types.KnowledgeBuildInputCaptureRequest{
					TenantID:        7,
					KnowledgeBaseID: "kb",
					KnowledgeID:     f.k.ID,
					ChunkIDs:        []string{f.chunks[0].ID},
					Expected:        original,
				},
			)
			require.NoError(t, e)
			// The public reader has a real current human/AD snapshot, yet staging
			// content remains unreadable. A private build lease cannot escape here.
			require.Error(
				t,
				f.guard.Check(
					f.ctx,
					actual.Proof.SourceLineage,
					access.LineageReadScope{
						Targets: types.SearchTargets{
							&types.SearchTarget{
								Type:            types.SearchTargetTypeKnowledgeBase,
								TenantID:        7,
								KnowledgeBaseID: "kb",
							},
						},
					},
				),
			)
			p := types.SummaryGenerationPayload{
				OriginalInput:   actual.Proof,
				TenantID:        7,
				KnowledgeBaseID: "kb",
				KnowledgeID:     f.k.ID,
				Language:        "en-US",
			}
			p.ProducerInput, e = f.store.AdmitKnowledgeProducerInput(
				ctx,
				actual.Proof,
				"summary",
				summaryProducerControls(f.s.config, p),
			)
			require.NoError(t, e)
			switch mode {
			case "remote source withdrawn during model":
				f.model.beforeReturn = func() { allow.Store(false) }
			case "stage epoch retired during model":
				f.model.beforeReturn = func() {
					require.NoError(
						t,
						repository.NewNextcloudContentLeaseStore(f.db).RetireKnowledge(context.Background(), scope),
					)
				}
			case "forged original artifact":
				p.OriginalInput.InputDigest = strings.Repeat("0", 64)
			case "public staging read denied":
				ctx = context.Background()
			}
			e = f.s.ProcessSummaryGeneration(ctx, enrichmentTask(t, types.TypeSummaryGeneration, p))
			if mode != "normal build publish and guarded read" {
				require.Error(t, e)
				if mode == "forged original artifact" || mode == "public staging read denied" {
					require.Zero(t, f.model.calls)
				} else {
					require.Equal(t, 1, f.model.calls)
				}
				var current types.Knowledge
				require.NoError(t, f.db.Where("id = ?", f.k.ID).Take(&current).Error)
				require.Empty(t, current.Description)
				require.Empty(t, f.engine.indexed)
				return
			}
			require.NoError(t, e)
			require.Equal(t, 1, f.model.calls)
			var current types.Knowledge
			require.NoError(t, f.db.Where("id = ?", f.k.ID).Take(&current).Error)
			current.ParseStatus = types.ParseStatusCompleted
			current.EnableStatus = "enabled"
			require.NoError(t, f.s.repo.UpdateKnowledge(f.build, &current))
			require.NoError(t, f.db.Where("id = ?", current.ID).Take(&current).Error)
			kg, e := NewKnowledgeOutputAuthorizer(f.store, f.guard).Begin(f.ctx, []*types.Knowledge{&current})
			require.NoError(t, e)
			require.NoError(t, kg.Check(f.ctx, []*types.Knowledge{&current}))
			cg, e := NewChunkOutputAuthorizer(
				f.store,
				f.guard,
			).Begin(f.ctx, types.ChunkOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: current.ID, ChunkIDs: []string{f.chunks[0].ID}})
			require.NoError(t, e)
			cs, e := cg.Snapshot()
			require.NoError(t, e)
			require.NoError(t, cg.Check(f.ctx, cs))
			leases := repository.NewNextcloudContentLeaseStore(f.db)
			lease, e := leases.AcquireKnowledge(
				f.ctx,
				scope,
				repository.NextcloudContentReadLease,
				"current-human-read",
				time.Minute,
			)
			require.NoError(t, e)
			defer func() { require.NoError(t, leases.ReleaseLease(context.Background(), lease.ID)) }()
			rows, e := repository.NewRAGChunkSnapshotReader(f.db, f.store).
				HydrateRAGChunkSnapshots(f.ctx, []types.RAGChunkReadRequest{{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: current.ID, ChunkIDs: []string{f.chunks[0].ID}, Lease: &types.RAGExactReadLease{ID: lease.ID, TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: current.ID, Kind: string(lease.Kind), Epoch: lease.Epoch, OwnerID: lease.OwnerID, ExpiresAt: lease.ExpiresAt}}})
			require.NoError(t, e)
			require.NoError(t, rows[0].SourceLineage.RequireComplete())
			allow.Store(false)
			require.Error(t, kg.Check(f.ctx, []*types.Knowledge{&current}))
			require.Error(t, cg.Check(f.ctx, cs))
		})
	}
}

func TestKnowledgeNextcloudOriginalBuildPurposeSQLite(t *testing.T) {
	runNextcloudOriginalBuildPurpose(t, false)
}

func TestPostgresKnowledgeNextcloudOriginalBuildPurpose(t *testing.T) {
	runNextcloudOriginalBuildPurpose(t, true)
}

type enrichmentBuildGraphRepository struct {
	interfaces.RetrieveGraphRepository
	graphs []*types.GraphData
}

func (r *enrichmentBuildGraphRepository) AddGraph(
	ctx context.Context,
	namespace types.NameSpace,
	graphs []*types.GraphData,
) error {
	for _, g := range graphs {
		if err := types.CheckGraphWriteAuthorization(ctx, g.SourceReceipt); err != nil {
			return err
		}
	}
	if r.RetrieveGraphRepository != nil {
		if err := r.RetrieveGraphRepository.AddGraph(ctx, namespace, graphs); err != nil {
			return err
		}
	}
	r.graphs = graphs
	return nil
}

func runNextcloudActualGraphBuildPurpose(t *testing.T, pg bool) {
	for _, withdraw := range []bool{false, true} {
		name := "actual staging graph extract worker and public original input verifier"
		if withdraw {
			name = "graph model source withdrawal denies graph commit"
		}
		t.Run(name, func(t *testing.T) {
			f, ctx, original, allow, _ := sourceBuildFixture(t, pg, sourceDerivedFixtureHook{Graph: true})
			snapshot, e := f.store.CaptureKnowledgeBuildInputs(
				ctx,
				types.KnowledgeBuildInputCaptureRequest{
					Projection:      types.KnowledgeBuildProjectionGraph,
					TenantID:        7,
					KnowledgeBaseID: "kb",
					KnowledgeID:     f.k.ID,
					ChunkIDs:        []string{f.chunks[0].ID},
					Expected:        original,
				},
			)
			require.NoError(t, e)
			empty, _ := types.NewCompleteSourceLineage()
			template, e := types.BindGraphPromptTemplateOrigin(
				&types.PromptTemplateStructured{Description: "code-owned graph protocol %s"},
				empty,
			)
			require.NoError(t, e)
			model := &graphEntryModel{}
			if withdraw {
				model.call = func(_ []chat.Message, _ *chat.ChatOptions) { allow.Store(false) }
			}
			require.NoError(
				t,
				f.db.Create(
					&types.KnowledgeProcessingSpan{
						KnowledgeID: f.k.ID,
						Attempt:     1,
						SpanID:      "root",
						Kind:        types.SpanKindRoot,
						Name:        "root",
						Status:      types.SpanStatusRunning,
					},
				).Error,
			)
			state := repository.NewNextcloudProcessingStateStore(f.db)
			require.NoError(t, state.PlanGraph(ctx, f.k, 1, f.chunks, true, false))
			graph := &enrichmentBuildGraphRepository{
				RetrieveGraphRepository: repository.NewScopedSQLGraphRepository(f.db, nil),
			}
			svc := &ChunkExtractService{
				buildInputs:      f.store,
				template:         template,
				modelService:     &graphEntryModels{model: model},
				graphEngine:      graph,
				processingLedger: state,
			}
			p := types.ExtractChunkPayload{
				TenantID:      7,
				KnowledgeID:   f.k.ID,
				ChunkID:       f.chunks[0].ID,
				ModelID:       "summary",
				OriginalInput: snapshot.Proof,
				Attempt:       1,
			}
			e = svc.Handle(ctx, enrichmentTask(t, types.TypeChunkExtract, p))
			require.Equal(t, 1, model.calls, "worker error: %v", e)
			var persisted int64
			require.NoError(
				t,
				f.db.Table("nextcloud_graph_records").
					Where("tenant_id=? AND knowledge_id=?", 7, f.k.ID).
					Count(&persisted).
					Error,
			)
			if withdraw {
				require.Zero(t, persisted)
			} else {
				require.Equal(t, int64(1), persisted)
			}
			if withdraw {
				require.Error(t, e)
				require.Empty(t, graph.graphs)
				return
			}
			require.NoError(t, e)
			require.Len(t, graph.graphs, 1)
			require.NoError(
				t,
				NewGraphOriginalInputVerifier(f.store).RecheckGraphOriginalInputs(ctx, graph.graphs[0].SourceReceipt),
			)
			require.Error(
				t,
				NewGraphOriginalInputVerifier(f.store).RecheckGraphOriginalInputs(f.ctx, graph.graphs[0].SourceReceipt),
			)
		})
	}
}

func TestKnowledgeNextcloudActualGraphBuildPurposeSQLite(t *testing.T) {
	runNextcloudActualGraphBuildPurpose(t, false)
}

func TestPostgresKnowledgeNextcloudActualGraphBuildPurpose(t *testing.T) {
	runNextcloudActualGraphBuildPurpose(t, true)
}
