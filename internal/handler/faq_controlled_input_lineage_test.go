package handler

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestFAQActualControlledDerivativeHTTPAndTagCountWithdraw(t *testing.T) {
	parent, calls := collectionNextcloudHTTPFixture(t, "allowed")
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
	defer cancel()
	for _, file := range []string{"000056_knowledge_original_worker_actor.up.sql", "000060_tag_statistics_checkpoints.up.sql", "000063_faq_original_entry_inputs.up.sql"} {
		ddl, err := os.ReadFile("../../migrations/sqlite/" + file)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
	ctx := collectionHumanInputContext(7)
	deadline, _ := bounded.Deadline()
	ctx, requestCancel := context.WithDeadline(ctx, deadline)
	defer requestCancel()
	kb := &types.KnowledgeBase{
		ID:               "faq-derived",
		TenantID:         7,
		Type:             types.KnowledgeBaseTypeFAQ,
		Name:             "ordinary FAQ target",
		EmbeddingModelID: "embed",
	}
	require.NoError(t, store.CreateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(ctx), kb))
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
	guard := access.NewNextcloudSourceLineageGuard(
		knowledge.(access.NextcloudLineageSnapshotLookup),
		kbs,
		nil,
		nil,
		groups,
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
		faqHTTPRegistry{engine: &faqHTTPEngine{}},
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
	snapshot, err := store.CaptureFAQInputs(
		ctx,
		types.FAQInputReadRequest{TenantID: 7, KnowledgeBaseID: kb.ID, ChunkIDs: []string{entry.ChunkID}},
	)
	require.NoError(t, err)
	require.Len(t, snapshot.Proofs[0].SourceLineage.Sources, 1)
	require.Equal(t, original.Proof.SourceLineage.Sources, snapshot.Proofs[0].SourceLineage.Sources)
	h := NewFAQHandler(kg, kbs)
	ConfigureFAQHandlerOutputLineage(h, service.NewFAQOutputAuthorizer(store, guard))
	readPlan, err := h.outputAuthorizer.ReadPlan(
		ctx,
		types.FAQInputReadRequest{TenantID: 7, KnowledgeBaseID: kb.ID, AllEntries: true},
	)
	require.NoError(t, err)
	require.Equal(
		t,
		[]types.KnowledgeSearchScope{{TenantID: 7, KBID: kb.ID}, {TenantID: 7, KBID: "kb"}},
		readPlan.Scopes,
	)
	actualRead, err := acquireFAQReadPlanLeases(
		ctx,
		store.ContentLeaseStore(),
		readPlan.Scopes,
		readPlan.SourceLineage,
		0,
	)
	require.NoError(t, err)
	defer actualRead.Close()
	readCtx, err := repository.WithFAQInputReadPlan(actualRead.Context(ctx), readPlan)
	require.NoError(t, err)
	_, err = store.CaptureFAQInputs(
		readCtx,
		types.FAQInputReadRequest{TenantID: 7, KnowledgeBaseID: kb.ID, ChunkIDs: []string{entry.ChunkID}},
	)
	require.NoError(t, err)
	w := serveFAQHTTP(h, ctx, http.MethodGet, "/knowledge-bases/faq-derived/faq/entries", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "source inherited FAQ answer")
	require.Positive(t, calls.Load())
	tag := NewTagHandler(nil, repository.NewKnowledgeTagRepository(db), repository.NewChunkRepository(db))
	ConfigureTagHandlerOutputLineage(tag, service.NewTagOutputAuthorizer(store, guard))
	w = serveTagHTTP(tag, ctx, http.MethodGet, "/knowledge-bases/faq-derived/tags", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"chunk_count":1`)
	batch := &types.FAQBatchUpsertPayload{
		Mode: types.FAQBatchModeAppend,
		Entries: []types.FAQEntryPayload{
			{
				StandardQuestion: "controlled import original question",
				Answers:          []string{"controlled import original body"},
			},
		},
	}
	derivedImport, err := store.WithDerivedFAQInputs(
		ctx,
		kb.ID,
		types.FAQInputOperation{Kind: "import", Batch: batch},
		[]*types.KnowledgeBuildInputProof{original.Proof},
	)
	require.NoError(t, err)
	taskID, err := kg.UpsertFAQEntries(derivedImport, kb.ID, batch)
	require.NoError(t, err)
	require.Len(t, queue.tasks, 1)
	require.NoError(t, kg.ProcessFAQImport(bounded, queue.tasks[0]))
	progressPlan, err := h.outputAuthorizer.ProgressReadPlan(ctx, taskID)
	require.NoError(t, err)
	require.Equal(
		t,
		[]types.KnowledgeSearchScope{{TenantID: 7, KBID: "faq-derived"}, {TenantID: 7, KBID: "kb"}},
		progressPlan.Scopes,
	)
	w = serveFAQHTTP(h, ctx, http.MethodGet, "/faq/import/progress/"+taskID, "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "controlled import original question")
	_, err = store.CaptureFAQInputs(
		readCtx,
		types.FAQInputReadRequest{TenantID: 7, KnowledgeBaseID: kb.ID, AllEntries: true},
	)
	require.Error(t, err) // original candidate membership/head plan cannot absorb own later import
	last, err := store.CaptureFAQImportOutputInputs(
		ctx,
		types.FAQImportOutputReadRequest{
			TenantID:        7,
			KnowledgeBaseID: kb.ID,
			KnowledgeID:     entry.KnowledgeID,
			Kind:            "result",
		},
	)
	require.NoError(t, err)
	require.Len(t, last.SourceLineage.Sources, 1)
	require.NoError(
		t,
		repository.NewNextcloudContentLeaseStore(db).
			RetireKnowledge(context.Background(), repository.NextcloudContentScope{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "source", DataSourceID: "ds", ExternalID: "nextcloud:instance:77"}),
	)
	w = serveFAQHTTP(h, ctx, http.MethodGet, "/knowledge-bases/faq-derived/faq/entries", "")
	require.NotEqual(t, http.StatusOK, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "source inherited FAQ answer")
	w = serveFAQHTTP(h, ctx, http.MethodGet, "/faq/import/progress/"+taskID, "")
	require.NotEqual(t, http.StatusOK, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "controlled import original question")
	_, err = store.CaptureFAQImportOutputInputs(
		ctx,
		types.FAQImportOutputReadRequest{
			TenantID:        7,
			KnowledgeBaseID: kb.ID,
			KnowledgeID:     entry.KnowledgeID,
			Kind:            "result",
		},
	)
	require.Error(t, err)
	w = serveTagHTTP(tag, ctx, http.MethodGet, "/knowledge-bases/faq-derived/tags", "")
	require.NotEqual(t, http.StatusOK, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "chunk_count")
}
