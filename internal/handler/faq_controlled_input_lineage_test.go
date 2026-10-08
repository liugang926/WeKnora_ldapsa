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
)

func TestFAQActualControlledDerivativeHTTPAndTagCountWithdraw(t *testing.T) {
	parent, calls := collectionNextcloudHTTPFixture(t, "allowed")
	db := parent.kbService.(collectionHTTPKB).db
	store := repository.NewKnowledgeInputProvenanceStore(db)
	groups := service.NewGroupAccessService(repository.NewGroupAccessRepository(db))
	repository.ConfigureKnowledgeInputActorGroupAccess(store, groups)
	for _, file := range []string{"000056_knowledge_original_worker_actor.up.sql", "000060_tag_statistics_checkpoints.up.sql", "000063_faq_original_entry_inputs.up.sql"} {
		ddl, err := os.ReadFile("../../migrations/sqlite/" + file)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
	ctx := collectionHumanInputContext(7)
	kb := &types.KnowledgeBase{ID: "faq-derived", TenantID: 7, Type: types.KnowledgeBaseTypeFAQ, Name: "ordinary FAQ target", EmbeddingModelID: "embed"}
	require.NoError(t, store.CreateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(ctx), kb))
	var ds types.DataSource
	require.NoError(t, db.Where("id='ds'").Take(&ds).Error)
	user := "user"
	now := time.Now().UTC()
	directory := collectionDirectory{identity: &types.DirectoryIdentity{ID: "identity", DirectoryID: "ad-1", ObjectGUID: "00112233-4455-6677-8899-aabbccddeeff", UserID: &user, Status: types.DirectoryObjectActive, SnapshotVersion: 1}, directory: &types.Directory{ID: "ad-1", Protocol: types.DirectoryProtocolAD, Enabled: true, SnapshotVersion: 1, LastSuccessfulSyncAt: &now, StaleAfterSeconds: 900}}
	publication := access.NewNextcloudPublicationGuard(directory, publicationHandlerSources{source: &ds})
	kbs := faqHTTPKB{collectionHTTPKB{db: db}}
	knowledge := repository.NewKnowledgeRepository(db)
	repository.ConfigureKnowledgeCurrentReadActor(knowledge, store)
	guard := access.NewNextcloudSourceLineageGuard(knowledge.(access.NextcloudLineageSnapshotLookup), kbs, nil, nil, groups, publication, store)
	tenant := &types.Tenant{ID: 7, RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{{RetrieverType: types.VectorRetrieverType, RetrieverEngineType: types.PostgresRetrieverEngineType}}}}
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
	grant, err := access.ResolveKB(ctx, access.KBRequest{Caller: types.CallerFromContext(ctx)}, kb, types.OrgRoleEditor, nil, nil)
	require.NoError(t, err)
	ctx = grant.Context(ctx)
	kg, err := service.NewKnowledgeService(nil, knowledge, nil, kbs, faqHTTPTenants{tenant: tenant}, nil, nil, repository.NewChunkRepository(db), repository.NewKnowledgeTagRepository(db), nil, nil, nil, nil, faqHTTPModel{}, &faqHTTPQueue{}, nil, nil, faqHTTPRegistry{engine: &faqHTTPEngine{}}, nil, nil, nil, nil, nil, nil, nil, nil, nil, publication, nil)
	require.NoError(t, err)
	service.ConfigureKnowledgeInputProvenance(kg, store, store)
	service.ConfigureFAQInputProvenance(kg, store, guard)
	original, err := store.CaptureKnowledgeOutputInputs(ctx, types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "source", ChunkIDs: []string{"source-chunk"}})
	require.NoError(t, err)
	require.Len(t, original.Proof.SourceLineage.Sources, 1)
	input := &types.FAQEntryPayload{StandardQuestion: "actual controlled derivative FAQ", Answers: []string{"source inherited FAQ answer"}}
	derived, err := store.WithDerivedFAQInput(ctx, kb.ID, types.FAQInputOperation{Kind: "create", Entry: input}, original.Proof)
	require.NoError(t, err)
	entry, err := kg.CreateFAQEntry(derived, kb.ID, input)
	require.NoError(t, err)
	snapshot, err := store.CaptureFAQInputs(ctx, types.FAQInputReadRequest{TenantID: 7, KnowledgeBaseID: kb.ID, ChunkIDs: []string{entry.ChunkID}})
	require.NoError(t, err)
	require.Len(t, snapshot.Proofs[0].SourceLineage.Sources, 1)
	require.Equal(t, original.Proof.SourceLineage.Sources, snapshot.Proofs[0].SourceLineage.Sources)
	h := NewFAQHandler(kg, kbs)
	ConfigureFAQHandlerOutputLineage(h, service.NewFAQOutputAuthorizer(store, guard))
	w := serveFAQHTTP(h, ctx, http.MethodGet, "/knowledge-bases/faq-derived/faq/entries", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "source inherited FAQ answer")
	require.Positive(t, calls.Load())
	tag := NewTagHandler(nil, repository.NewKnowledgeTagRepository(db), repository.NewChunkRepository(db))
	ConfigureTagHandlerOutputLineage(tag, service.NewTagOutputAuthorizer(store, guard))
	w = serveTagHTTP(tag, ctx, http.MethodGet, "/knowledge-bases/faq-derived/tags", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"chunk_count":1`)
	require.NoError(t, repository.NewNextcloudContentLeaseStore(db).RetireKnowledge(context.Background(), repository.NextcloudContentScope{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "source", DataSourceID: "ds", ExternalID: "nextcloud:instance:77"}))
	w = serveFAQHTTP(h, ctx, http.MethodGet, "/knowledge-bases/faq-derived/faq/entries", "")
	require.NotEqual(t, http.StatusOK, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "source inherited FAQ answer")
	w = serveTagHTTP(tag, ctx, http.MethodGet, "/knowledge-bases/faq-derived/tags", "")
	require.NotEqual(t, http.StatusOK, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "chunk_count")
}
