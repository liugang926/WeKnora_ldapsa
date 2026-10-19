package handler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type faqLeaseObservationGroups struct {
	interfaces.GroupAccessService
	Hook func(context.Context)
}

func (g *faqLeaseObservationGroups) EffectivePermission(
	ctx context.Context,
	tenant uint64,
	resource types.ResourceType,
	id string,
	action types.ResourceAction,
	now time.Time,
) (types.EffectiveResourcePermission, error) {
	actual, err := g.GroupAccessService.EffectivePermission(ctx, tenant, resource, id, action, now)
	if err == nil && g.Hook != nil {
		g.Hook(ctx)
	}
	return actual, err
}

type faqAllLeaseFixture struct {
	h         *FAQHandler
	knowledge *KnowledgeHandler
	db        *gorm.DB
	store     *repository.KnowledgeInputProvenanceStore
	ctx       context.Context
	entry     *types.FAQEntry
	task      string
	engine    *faqHTTPEngine
	calls     *atomic.Int64
	groups    *faqLeaseObservationGroups
	guard     *access.NextcloudSourceLineageGuard
}

func faqAllReadLeaseFixture(t *testing.T, change string) *faqAllLeaseFixture {
	t.Helper()
	parent, calls := collectionNextcloudHTTPFixture(t, change)
	calls.Store(-10000)
	db := parent.kbService.(collectionHTTPKB).db
	store := repository.NewKnowledgeInputProvenanceStore(db)
	groups := service.NewGroupAccessService(repository.NewGroupAccessRepository(db))
	repository.ConfigureKnowledgeInputActorGroupAccess(store, groups)
	repository.ConfigureKnowledgeDisplayInputWriters(
		store,
		repository.NewKnowledgeTagRepository(db),
		repository.NewKnowledgeRepository(db),
		func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
			actual := repository.NewKnowledgeInputProvenanceStore(tx)
			repository.ConfigureKnowledgeInputActorGroupAccess(
				actual,
				service.NewGroupAccessService(repository.NewGroupAccessRepository(tx)),
			)
			return actual
		},
	)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	// Match production's single-connection pool. Each actual producer/read has
	// a bounded context so a nested authority lookup cannot hang the suite.
	bounded, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	for _, file := range []string{"000056_knowledge_original_worker_actor.up.sql", "000060_tag_statistics_checkpoints.up.sql", "000063_faq_original_entry_inputs.up.sql"} {
		ddl, err := os.ReadFile("../../migrations/sqlite/" + file)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
	ctx := collectionHumanInputContext(7)
	deadline, _ := bounded.Deadline()
	ctx, requestCancel := context.WithDeadline(ctx, deadline)
	t.Cleanup(requestCancel)
	kb := &types.KnowledgeBase{
		ID:               "faq-derived",
		TenantID:         7,
		Type:             types.KnowledgeBaseTypeFAQ,
		Name:             "ordinary FAQ target",
		EmbeddingModelID: "embed",
	}
	require.NoError(
		t,
		store.CreateKnowledgeBaseWithInput(
			repository.WithIndependentKnowledgeBaseNameInput(
				repository.WithIndependentKnowledgeInput(ctx),
				"",
				kb.Name,
			),
			kb,
		),
	)
	var ds types.DataSource
	require.NoError(t, db.Where("id='ds'").Take(&ds).Error)
	user := "user"
	now := time.Now().UTC()
	directory := collectionDirectory{
		identity: &types.DirectoryIdentity{
			ID:              "identity",
			DirectoryID:     "ad-1",
			ObjectGUID:      "00112233-4455-6677-8899-aabbccddeeff",
			UserID:          &user,
			Status:          types.DirectoryObjectActive,
			SnapshotVersion: 1,
		},
		directory: &types.Directory{
			ID:                   "ad-1",
			Protocol:             types.DirectoryProtocolAD,
			Enabled:              true,
			SnapshotVersion:      1,
			LastSuccessfulSyncAt: &now,
			StaleAfterSeconds:    900,
		},
	}
	publication := access.NewNextcloudPublicationGuard(directory, publicationHandlerSources{source: &ds})
	kbs := faqHTTPKB{collectionHTTPKB{db: db}}
	knowledge := repository.NewKnowledgeRepository(db)
	repository.ConfigureKnowledgeCurrentReadActor(knowledge, store)
	observedGroups := &faqLeaseObservationGroups{GroupAccessService: groups}
	guard := access.NewNextcloudSourceLineageGuard(
		knowledge.(access.NextcloudLineageSnapshotLookup),
		kbs,
		nil,
		nil,
		observedGroups,
		publication,
		store,
	)
	tenant := &types.Tenant{
		ID: 7,
		RetrieverEngines: types.RetrieverEngines{
			Engines: []types.RetrieverEngineParams{
				{RetrieverType: types.VectorRetrieverType, RetrieverEngineType: types.PostgresRetrieverEngineType},
			},
		},
	}
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
	grant, err := access.ResolveKB(
		ctx,
		access.KBRequest{Caller: types.CallerFromContext(ctx)},
		kb,
		types.OrgRoleEditor,
		nil,
		nil,
	)
	require.NoError(t, err)
	ctx = grant.Context(ctx)
	queue := &faqHTTPQueue{}
	engine := &faqHTTPEngine{}
	kg, err := service.NewKnowledgeService(
		nil,
		knowledge,
		nil,
		kbs,
		faqHTTPTenants{tenant: tenant},
		nil,
		nil,
		repository.NewChunkRepository(db),
		repository.NewKnowledgeTagRepository(db),
		nil,
		nil,
		nil,
		nil,
		faqHTTPModel{},
		queue,
		nil,
		nil,
		faqHTTPRegistry{engine: engine},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		publication,
		nil,
	)
	require.NoError(t, err)
	service.ConfigureKnowledgeInputProvenance(kg, store, store)
	service.ConfigureFAQInputProvenance(kg, store, guard)
	original, err := store.CaptureKnowledgeOutputInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     "source",
			ChunkIDs:        []string{"source-chunk"},
		},
	)
	require.NoError(t, err)
	require.Len(t, original.Proof.SourceLineage.Sources, 1)
	input := &types.FAQEntryPayload{
		StandardQuestion: "actual controlled derivative FAQ",
		Answers:          []string{"source inherited FAQ answer"},
	}
	derived, err := store.WithDerivedFAQInput(
		ctx,
		kb.ID,
		types.FAQInputOperation{Kind: "create", Entry: input},
		original.Proof,
	)
	require.NoError(t, err)
	entry, err := kg.CreateFAQEntry(derived, kb.ID, input)
	require.NoError(t, err)

	batch := &types.FAQBatchUpsertPayload{
		Mode: types.FAQBatchModeAppend,
		Entries: []types.FAQEntryPayload{
			{StandardQuestion: "all leased import question", Answers: []string{"all leased import answer"}},
		},
	}
	importCtx, err := store.WithDerivedFAQInputs(
		ctx,
		kb.ID,
		types.FAQInputOperation{Kind: "import", Batch: batch},
		[]*types.KnowledgeBuildInputProof{original.Proof},
	)
	require.NoError(t, err)
	task, err := kg.UpsertFAQEntries(importCtx, kb.ID, batch)
	require.NoError(t, err)
	require.NoError(t, kg.ProcessFAQImport(bounded, queue.tasks[0]))
	// No fixture builder may be the reason a GC claim is blocked in this test.
	var builds []struct{ LeaseID string }
	require.NoError(
		t,
		db.Table("nextcloud_content_leases").
			Select("lease_id").
			Where("kind='build' AND released_at_ms IS NULL").
			Scan(&builds).
			Error,
	)
	for _, build := range builds {
		require.NoError(t, store.ContentLeaseStore().ReleaseLease(ctx, build.LeaseID))
	}
	h := NewFAQHandler(kg, kbs)
	ConfigureFAQHandlerOutputLineage(h, service.NewFAQOutputAuthorizer(store, guard))
	knowledgeHandler := &KnowledgeHandler{kgService: kg, kbService: kbs}
	ConfigureKnowledgeHandlerOutputLineage(knowledgeHandler, service.NewKnowledgeOutputAuthorizer(store, guard))
	ConfigureKnowledgeHandlerCollectionOutput(
		knowledgeHandler,
		service.NewKnowledgeCollectionOutputAuthorizer(store, guard),
	)
	ConfigureKnowledgeHandlerContentLeaseStore(knowledgeHandler, store.ContentLeaseStore())
	calls.Store(0)
	return &faqAllLeaseFixture{h, knowledgeHandler, db, store, ctx, entry, task, engine, calls, observedGroups, guard}
}

func serveFAQKnowledgeLeaseHTTP(h *KnowledgeHandler, ctx context.Context, url string) *httptest.ResponseRecorder {
	r := documentHandlerRouter()
	r.Use(func(c *gin.Context) { c.Request = c.Request.WithContext(ctx); c.Next() })
	r.GET("/knowledge/batch", h.GetKnowledgeBatch)
	r.GET("/knowledge/:id", h.GetKnowledge)
	r.GET("/knowledge-bases/:id/knowledge", h.ListKnowledge)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, url, nil))
	return w
}

func TestFAQAllParentReadLeasesActualHTTPPositiveAndGCBarrier(t *testing.T) {
	for _, route := range []string{"list", "get", "search", "export", "progress", "knowledge", "batch", "collection", "tags"} {
		t.Run(route, func(t *testing.T) {
			f := faqAllReadLeaseFixture(t, "allowed")
			leases := f.store.ContentLeaseStore()
			old := repository.NextcloudContentScope{
				TenantID:        7,
				KnowledgeBaseID: "kb",
				KnowledgeID:     "retired-parent-neighbor",
				DataSourceID:    "old-source",
				ExternalID:      "nextcloud:instance:99",
			}
			require.NoError(t, leases.RetireKnowledge(f.ctx, old))
			var retired int64
			require.NoError(
				t,
				f.db.Table("nextcloud_content_fences").
					Where("knowledge_id=?", old.KnowledgeID).
					Select("retired_at_ms").
					Scan(&retired).
					Error,
			)
			require.NoError(
				t,
				f.db.Exec(
					`INSERT INTO nextcloud_content_lease_coverage VALUES(7,'kb',?,?,'FAQ-reader-test-contract','FAQ-builder-test-contract')`,
					retired,
					retired,
				).Error,
			)
			observed := false
			blocked := false
			f.groups.Hook = func(ctx context.Context) {
				if blocked {
					return
				}
				_, err := leases.ClaimKnowledgeGC(ctx, old, time.UnixMilli(retired), time.Minute)
				require.ErrorIs(t, err, repository.ErrNextcloudContentGCBusy)
				blocked = true
			}
			require.NoError(
				t,
				f.db.Callback().
					Query().
					Before("gorm:query").
					Register("FAQ_actual_parent_lease_observation", func(tx *gorm.DB) {
						if tx.Statement.SkipHooks || tx.Statement.Table != "chunks" {
							return
						}
						// Inspect the same transaction/connection as the actual body reader.
						actual := tx.Session(&gorm.Session{NewDB: true, SkipHooks: true})
						var count int64
						require.NoError(
							t,
							actual.Table("nextcloud_content_leases").
								Where("kind='read' AND released_at_ms IS NULL AND knowledge_id='' AND knowledge_base_id IN ('kb','faq-derived')").
								Count(&count).
								Error,
						)
						require.Equal(t, int64(2), count)
						require.NoError(
							t,
							actual.Table("nextcloud_content_leases").
								Where("kind='read' AND released_at_ms IS NULL AND knowledge_id='source'").
								Count(&count).
								Error,
						)
						require.Equal(t, int64(1), count)
						observed = true
					}),
			)
			var w *httptest.ResponseRecorder
			switch route {
			case "list":
				w = serveFAQHTTP(f.h, f.ctx, http.MethodGet, "/knowledge-bases/faq-derived/faq/entries", "")
			case "get":
				w = serveFAQHTTP(
					f.h,
					f.ctx,
					http.MethodGet,
					fmt.Sprintf("/knowledge-bases/faq-derived/faq/entries/%d", f.entry.ID),
					"",
				)
			case "search":
				w = serveFAQHTTP(
					f.h,
					f.ctx,
					http.MethodPost,
					"/knowledge-bases/faq-derived/faq/search",
					`{"query_text":"source"}`,
				)
			case "export":
				w = serveFAQHTTP(
					f.h,
					f.ctx,
					http.MethodGet,
					"/knowledge-bases/faq-derived/faq/entries/export?format=json",
					"",
				)
			case "progress":
				w = serveFAQHTTP(f.h, f.ctx, http.MethodGet, "/faq/import/progress/"+f.task, "")
			case "knowledge":
				w = serveFAQKnowledgeLeaseHTTP(f.knowledge, f.ctx, "/knowledge/"+f.entry.KnowledgeID)
			case "tags":
				tag := NewTagHandler(
					nil,
					repository.NewKnowledgeTagRepository(f.db),
					repository.NewChunkRepository(f.db),
				)
				ConfigureTagHandlerOutputLineage(tag, service.NewTagOutputAuthorizer(f.store, f.guard))
				w = serveTagHTTP(tag, f.ctx, http.MethodGet, "/knowledge-bases/faq-derived/tags", "")
			case "batch":
				w = serveFAQKnowledgeLeaseHTTP(
					f.knowledge,
					f.ctx,
					"/knowledge/batch?ids="+f.entry.KnowledgeID+"&kb_id=faq-derived",
				)
			case "collection":
				w = serveFAQKnowledgeLeaseHTTP(
					f.knowledge,
					f.ctx,
					"/knowledge-bases/faq-derived/knowledge?page_size=20",
				)
			}
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.True(t, observed)
			require.True(t, blocked)
			var count int64
			require.NoError(
				t,
				f.db.Table("nextcloud_content_leases").
					Where("kind='read' AND released_at_ms IS NULL").
					Count(&count).
					Error,
			)
			require.Zero(t, count)
			_, err := leases.ClaimKnowledgeGC(f.ctx, old, time.UnixMilli(retired), time.Minute)
			require.NoError(t, err)
		})
	}
}

func TestFAQAllParentPlansRequireRealLeasesAndRejectABA(t *testing.T) {
	f := faqAllReadLeaseFixture(t, "allowed")
	request := types.FAQInputReadRequest{TenantID: 7, KnowledgeBaseID: "faq-derived", AllEntries: true}
	plan, err := f.h.outputAuthorizer.ReadPlan(f.ctx, request)
	require.NoError(t, err)
	unsafe, err := repository.WithFAQInputReadPlan(f.ctx, plan)
	require.NoError(t, err)
	_, err = f.store.CaptureFAQInputs(unsafe, request)
	require.ErrorIs(t, err, repository.ErrNextcloudContentGCUncovered)
	real, err := acquireFAQReadPlanLeases(f.ctx, f.store.ContentLeaseStore(), plan.Scopes, plan.SourceLineage, 0)
	require.NoError(t, err)
	defer real.Close()
	marked, err := repository.WithFAQInputReadPlan(real.Context(f.ctx), plan)
	require.NoError(t, err)
	_, err = f.store.CaptureFAQInputs(marked, request)
	require.NoError(t, err)
	fake := append([]repository.NextcloudContentLease{}, real.rows...)
	fake[0].KnowledgeBaseID = "foreign-kb"
	forged, err := repository.WithFAQReadLeaseCoverage(marked, fake)
	require.NoError(t, err)
	_, err = f.store.CaptureFAQInputs(forged, request)
	require.ErrorIs(t, err, repository.ErrNextcloudContentLeaseExpired)
	require.NoError(
		t,
		f.db.Model(&types.Chunk{}).Where("knowledge_base_id='faq-derived'").Update("is_enabled", false).Error,
	)
	require.NoError(
		t,
		f.db.Model(&types.Chunk{}).Where("knowledge_base_id='faq-derived'").Update("is_enabled", true).Error,
	)
	_, err = f.store.CaptureFAQInputs(marked, request)
	require.ErrorIs(t, err, repository.ErrKnowledgeInputChanged)
}

func TestFAQAllParentHTTPSourcePermissionRevokeWithOpenLeases(t *testing.T) {
	for _, route := range []string{"list", "get", "search", "export", "progress", "knowledge", "batch", "collection", "tags"} {
		t.Run(route, func(t *testing.T) {
			f := faqAllReadLeaseFixture(t, "personal_revoke_before_write")
			observed := false
			f.groups.Hook = func(ctx context.Context) {
				observed = true
				var count int64
				require.NoError(
					t,
					f.db.Table("nextcloud_content_leases").
						Where("kind='read' AND released_at_ms IS NULL AND knowledge_id='source'").
						Count(&count).
						Error,
				)
				require.Equal(t, int64(1), count)
			}
			var w *httptest.ResponseRecorder
			switch route {
			case "list":
				w = serveFAQHTTP(f.h, f.ctx, http.MethodGet, "/knowledge-bases/faq-derived/faq/entries", "")
			case "get":
				w = serveFAQHTTP(
					f.h,
					f.ctx,
					http.MethodGet,
					fmt.Sprintf("/knowledge-bases/faq-derived/faq/entries/%d", f.entry.ID),
					"",
				)
			case "search":
				w = serveFAQHTTP(
					f.h,
					f.ctx,
					http.MethodPost,
					"/knowledge-bases/faq-derived/faq/search",
					`{"query_text":"source"}`,
				)
			case "export":
				w = serveFAQHTTP(
					f.h,
					f.ctx,
					http.MethodGet,
					"/knowledge-bases/faq-derived/faq/entries/export?format=json",
					"",
				)
			case "progress":
				w = serveFAQHTTP(f.h, f.ctx, http.MethodGet, "/faq/import/progress/"+f.task, "")
			case "knowledge":
				w = serveFAQKnowledgeLeaseHTTP(f.knowledge, f.ctx, "/knowledge/"+f.entry.KnowledgeID)
			case "tags":
				tag := NewTagHandler(
					nil,
					repository.NewKnowledgeTagRepository(f.db),
					repository.NewChunkRepository(f.db),
				)
				ConfigureTagHandlerOutputLineage(tag, service.NewTagOutputAuthorizer(f.store, f.guard))
				w = serveTagHTTP(tag, f.ctx, http.MethodGet, "/knowledge-bases/faq-derived/tags", "")
			case "batch":
				w = serveFAQKnowledgeLeaseHTTP(
					f.knowledge,
					f.ctx,
					"/knowledge/batch?ids="+f.entry.KnowledgeID+"&kb_id=faq-derived",
				)
			case "collection":
				w = serveFAQKnowledgeLeaseHTTP(
					f.knowledge,
					f.ctx,
					"/knowledge-bases/faq-derived/knowledge?page_size=20",
				)
			}
			require.NotEqual(t, http.StatusOK, w.Code, w.Body.String())
			require.True(t, observed)
			require.NotContains(t, w.Body.String(), "source inherited FAQ answer")
			require.NotContains(t, w.Body.String(), f.task)
			var state string
			require.NoError(
				t,
				f.db.Table("nextcloud_content_fences").
					Where("knowledge_id='source'").
					Select("state").
					Scan(&state).
					Error,
			)
			require.Equal(t, "open", state)
			var active int64
			require.NoError(
				t,
				f.db.Table("nextcloud_content_leases").
					Where("kind='read' AND released_at_ms IS NULL").
					Count(&active).
					Error,
			)
			require.Zero(t, active)
		})
	}
}

func configureFAQProductionSQLAuthority(db *gorm.DB, store *repository.KnowledgeInputProvenanceStore) {
	build := func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
		actual := repository.NewKnowledgeInputProvenanceStore(tx)
		repository.ConfigureKnowledgeInputActorGroupAccess(
			actual,
			service.NewGroupAccessService(repository.NewGroupAccessRepository(tx)),
		)
		repository.ConfigureKnowledgeInputActorShares(
			actual,
			service.NewKBShareService(
				repository.NewKBShareRepository(tx),
				repository.NewOrganizationRepository(tx),
				repository.NewKnowledgeBaseRepository(tx),
				repository.NewKnowledgeRepository(tx),
				repository.NewDataSourceRepository(tx),
				repository.NewChunkRepository(tx),
				nil,
			),
		)
		return actual
	}
	repository.ConfigureKnowledgeInputActorGroupAccess(
		store,
		service.NewGroupAccessService(repository.NewGroupAccessRepository(db)),
	)
	repository.ConfigureKnowledgeInputActorShares(
		store,
		service.NewKBShareService(
			repository.NewKBShareRepository(db),
			repository.NewOrganizationRepository(db),
			repository.NewKnowledgeBaseRepository(db),
			repository.NewKnowledgeRepository(db),
			repository.NewDataSourceRepository(db),
			repository.NewChunkRepository(db),
			nil,
		),
	)
	repository.ConfigureKnowledgeDisplayInputWriters(
		store,
		repository.NewKnowledgeTagRepository(db),
		repository.NewKnowledgeRepository(db),
		build,
	)
}

func TestFAQAllParentActualSharedReadSingleConnectionAndShareRevoke(t *testing.T) {
	h, _, db, store, _, _, ctx := faqActualHTTPFixture(t)
	db.Config.DisableForeignKeyConstraintWhenMigrating = true
	require.NoError(
		t,
		db.AutoMigrate(
			&types.DataSource{},
			&types.Organization{},
			&types.OrganizationTenantMember{},
			&types.KnowledgeBaseShare{},
		),
	)
	configureFAQProductionSQLAuthority(db, store)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	org := &types.Organization{ID: "actual-parent-org", Name: "actual parent share", OwnerID: "user", OwnerTenantID: 9}
	require.NoError(t, db.Create(org).Error)
	require.NoError(
		t,
		db.Create(
			&types.OrganizationTenantMember{
				ID:             "actual-source-org-member",
				OrganizationID: org.ID,
				TenantID:       9,
				Role:           types.OrgRoleAdmin,
			},
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.OrganizationTenantMember{
				ID:             "actual-recipient-org-member",
				OrganizationID: org.ID,
				TenantID:       7,
				Role:           types.OrgRoleViewer,
			},
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.KnowledgeBaseShare{
				ID:              "actual-parent-share",
				KnowledgeBaseID: "shared-kb",
				OrganizationID:  org.ID,
				SharedByUserID:  "user",
				SourceTenantID:  9,
				Permission:      types.OrgRoleViewer,
			},
		).Error,
	)
	// The authenticated reader belongs only to the recipient workspace. The
	// parent grant comes from real current organization/share records.
	require.NoError(t, db.Where("tenant_id=9 AND user_id='user'").Delete(&types.TenantMember{}).Error)
	parent, err := store.CaptureKnowledgeOutputInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{TenantID: 9, KnowledgeBaseID: "shared-kb", KnowledgeID: "shared-match"},
	)
	require.NoError(t, err)
	shares := service.NewKBShareService(
		repository.NewKBShareRepository(db),
		repository.NewOrganizationRepository(db),
		repository.NewKnowledgeBaseRepository(db),
		repository.NewKnowledgeRepository(db),
		repository.NewDataSourceRepository(db),
		repository.NewChunkRepository(db),
		nil,
	)
	groups := service.NewGroupAccessService(repository.NewGroupAccessRepository(db))
	guard := access.NewNextcloudSourceLineageGuard(nil, collectionHTTPKB{db: db}, shares, nil, groups, nil, store)
	service.ConfigureFAQInputProvenance(h.knowledgeService, store, guard)
	ConfigureFAQHandlerOutputLineage(h, service.NewFAQOutputAuthorizer(store, guard))
	input := &types.FAQEntryPayload{
		StandardQuestion: "actual shared parent FAQ",
		Answers:          []string{"actual shared parent derived answer"},
	}
	derived, err := store.WithDerivedFAQInputs(
		ctx,
		"kb",
		types.FAQInputOperation{Kind: "create", Entry: input},
		[]*types.KnowledgeBuildInputProof{parent.Proof},
	)
	require.NoError(t, err)
	_, err = h.knowledgeService.CreateFAQEntry(derived, "kb", input)
	require.NoError(t, err)
	w := serveFAQHTTP(h, ctx, http.MethodGet, "/knowledge-bases/kb/faq/entries", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "actual shared parent derived answer")
	require.NoError(t, db.Where("id='actual-parent-share'").Delete(&types.KnowledgeBaseShare{}).Error)
	w = serveFAQHTTP(h, ctx, http.MethodGet, "/knowledge-bases/kb/faq/entries", "")
	require.NotEqual(t, http.StatusOK, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "actual shared parent derived answer")
	var active int64
	require.NoError(
		t,
		db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&active).Error,
	)
	require.Zero(t, active)
}

func TestFAQAllParentHTTPCurrentActorRevokeKeepsSourceFenceOpen(t *testing.T) {
	f := faqAllReadLeaseFixture(t, "allowed")
	observed := false
	f.groups.Hook = func(ctx context.Context) {
		if observed {
			return
		}
		observed = true
		var active int64
		require.NoError(
			t,
			f.db.Table("nextcloud_content_leases").
				Where("kind='read' AND released_at_ms IS NULL AND knowledge_id='source'").
				Count(&active).
				Error,
		)
		require.Equal(t, int64(1), active)
		require.NoError(t, f.db.Model(&types.User{}).Where("id='user'").Update("is_active", false).Error)
	}
	w := serveFAQHTTP(f.h, f.ctx, http.MethodGet, "/faq/import/progress/"+f.task, "")
	require.True(t, observed)
	require.NotEqual(t, http.StatusOK, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "all leased import answer")
	var state string
	require.NoError(
		t,
		f.db.Table("nextcloud_content_fences").Where("knowledge_id='source'").Select("state").Scan(&state).Error,
	)
	require.Equal(t, "open", state)
	var active int64
	require.NoError(
		t,
		f.db.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&active).Error,
	)
	require.Zero(t, active)
}
