package service

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func faqActualInputFixture(
	t *testing.T,
	kind string,
) (*knowledgeService, *repository.KnowledgeInputProvenanceStore, *gorm.DB, context.Context) {
	s, store, db, ctx, _, _, _ := historyActorIntegrationFixture(t, kind)
	for _, file := range []string{
		"000058_knowledge_collection_checkpoints.up.sql",
		"000059_knowledge_collection_display_inputs.up.sql",
		"000060_tag_statistics_checkpoints.up.sql",
		"000063_faq_original_entry_inputs.up.sql",
	} {
		ddl, err := os.ReadFile("../../../migrations/sqlite/" + file)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
	var kb types.KnowledgeBase
	require.NoError(t, db.Where("id='kb'").Take(&kb).Error)
	kb.Type = types.KnowledgeBaseTypeFAQ
	require.NoError(
		t,
		store.ReplaceKnowledgeBaseConfigurationWithInput(repository.WithIndependentKnowledgeInput(ctx), &kb),
	)
	s.kbService = &createKnowledgeFileKBServiceStub{kb: &kb}
	s.tagRepo = repository.NewKnowledgeTagRepository(db)
	s.retrieveEngine = parentChildRetrieveRegistry{engine: &faqInputEngine{}}
	tenant := &types.Tenant{
		ID: 7,
		RetrieverEngines: types.RetrieverEngines{
			Engines: []types.RetrieverEngineParams{
				{RetrieverType: types.VectorRetrieverType, RetrieverEngineType: types.PostgresRetrieverEngineType},
			},
		},
	}
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
	ctx = types.WithAPIKeyOperationRequirement(
		ctx,
		types.APIKeyOperationRequirement{Capabilities: []types.APIKeyCapability{types.APIKeyCapabilityIngest}},
	)
	grant, err := access.ResolveKB(
		ctx,
		access.KBRequest{Caller: types.CallerFromContext(ctx)},
		&kb,
		types.OrgRoleEditor,
		nil,
		nil,
	)
	require.NoError(t, err)
	ctx = grant.Context(ctx)
	groups := NewGroupAccessService(repository.NewGroupAccessRepository(db))
	kbs := &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db)}
	guard := access.NewNextcloudSourceLineageGuard(
		s.repo.(access.NextcloudLineageSnapshotLookup),
		kbs,
		nil,
		nil,
		groups,
		nil,
		store,
	)
	ConfigureFAQInputProvenance(s, store, guard)
	return s, store, db, ctx
}

type faqInputEngine struct{ parentChildRetrieveEngine }

func (*faqInputEngine) BatchUpdateChunkEnabledStatus(context.Context, map[string]bool) error {
	return nil
}
func (*faqInputEngine) BatchUpdateChunkTagID(context.Context, map[string]string) error   { return nil }
func (*faqInputEngine) DeleteByChunkIDList(context.Context, []string, int, string) error { return nil }
func faqInputContext(ctx context.Context, t *testing.T, op types.FAQInputOperation) context.Context {
	t.Helper()
	marked, err := repository.WithIndependentFAQInput(ctx, "kb", op)
	require.NoError(t, err)
	return marked
}

func TestFAQActualCreateUpdateRetainsIndependentOtherEntries(t *testing.T) {
	for _, kind := range []string{"web", "api"} {
		t.Run(kind, func(t *testing.T) {
			s, store, db, ctx := faqActualInputFixture(t, kind)
			first := &types.FAQEntryPayload{
				StandardQuestion: "first actual question",
				Answers:          []string{"first actual answer"},
			}
			a, err := s.CreateFAQEntry(
				faqInputContext(ctx, t, types.FAQInputOperation{Kind: "create", Entry: first}),
				"kb",
				first,
			)
			require.NoError(t, err)
			second := &types.FAQEntryPayload{
				StandardQuestion: "second actual question",
				Answers:          []string{"second actual answer"},
			}
			b, err := s.CreateFAQEntry(
				faqInputContext(ctx, t, types.FAQInputOperation{Kind: "create", Entry: second}),
				"kb",
				second,
			)
			require.NoError(t, err)
			original, err := store.CaptureFAQInputs(
				ctx,
				types.FAQInputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", ChunkIDs: []string{b.ChunkID}},
			)
			require.NoError(t, err)
			first.Answers = []string{"updated first answer"}
			_, err = s.UpdateFAQEntry(
				faqInputContext(ctx, t, types.FAQInputOperation{Kind: "update", Entry: first, EntrySeqID: a.ID}),
				"kb",
				a.ID,
				first,
			)
			require.NoError(t, err)
			require.NoError(t, store.RecheckFAQInputs(ctx, original))
			entries, err := s.ListFAQEntries(
				ctx,
				"kb",
				&types.Pagination{Page: 1, PageSize: 20},
				nil,
				0,
				"",
				"",
				"",
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, int64(2), entries.Total)
			var chunk types.Chunk
			require.NoError(t, db.Where("id=?", a.ChunkID).Take(&chunk).Error)
			rows, err := repository.NewRAGChunkSnapshotReader(db, store).
				HydrateRAGChunkSnapshots(ctx, []types.RAGChunkReadRequest{{
					TenantID:        7,
					KnowledgeBaseID: "kb",
					KnowledgeID:     a.KnowledgeID,
					ChunkIDs: []string{
						a.ChunkID,
					},
				}})
			require.NoError(t, err)
			require.NoError(t, rows[0].SourceLineage.RequireComplete())
		})
	}
}

func TestFAQOriginalBodyControlsAndMembershipABAReject(t *testing.T) {
	for _, field := range []string{"content", "metadata", "is_enabled", "flags", "seq_id", "deleted_at", "faq_config"} {
		t.Run(field, func(t *testing.T) {
			s, store, db, ctx := faqActualInputFixture(t, "web")
			entry := &types.FAQEntryPayload{
				StandardQuestion: "actual original question",
				Answers:          []string{"original answer"},
			}
			created, err := s.CreateFAQEntry(
				faqInputContext(ctx, t, types.FAQInputOperation{Kind: "create", Entry: entry}),
				"kb",
				entry,
			)
			require.NoError(t, err)
			original, err := store.CaptureFAQInputs(
				ctx,
				types.FAQInputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", AllEntries: true},
			)
			require.NoError(t, err)
			var chunk types.Chunk
			require.NoError(t, db.Where("id=?", created.ChunkID).Take(&chunk).Error)
			if field == "faq_config" {
				require.NoError(
					t,
					db.Model(&types.KnowledgeBase{}).
						Where("id='kb'").
						Update(field, types.JSON(`{"index_mode":"question_answer"}`)).
						Error,
				)
				require.NoError(t, db.Model(&types.KnowledgeBase{}).Where("id='kb'").Update(field, nil).Error)
			} else {
				before := map[string]any{
					"content":    chunk.Content,
					"metadata":   chunk.Metadata,
					"is_enabled": chunk.IsEnabled,
					"flags":      chunk.Flags,
					"seq_id":     chunk.SeqID,
					"deleted_at": nil,
				}[field]
				after := map[string]any{
					"content": "temporary foreign body",
					"metadata": types.JSON(
						("{\"standard_question\":\"foreign question\",\"answers\":[\"foreign answer" +
							"\"]}")),
					"is_enabled": false,
					"flags":      0,
					"seq_id":     chunk.SeqID + 123,
					"deleted_at": "2026-10-09 01:00:00",
				}[field]
				require.NoError(t, db.Model(&types.Chunk{}).Where("id=?", chunk.ID).Update(field, after).Error)
				require.NoError(
					t,
					db.Unscoped().Model(&types.Chunk{}).Where("id=?", chunk.ID).Update(field, before).Error,
				)
			}
			require.Error(t, store.RecheckFAQInputs(ctx, original))
			_, err = s.GetFAQEntry(ctx, "kb", created.ID)
			require.Error(t, err)
		})
	}
}

func TestFAQActualImportWorkerAndPartialLegacyRejection(t *testing.T) {
	s, store, db, ctx := faqActualInputFixture(t, "api")
	batch := &types.FAQBatchUpsertPayload{
		Mode: types.FAQBatchModeAppend,
		Entries: []types.FAQEntryPayload{
			{StandardQuestion: "imported first", Answers: []string{"one"}},
			{StandardQuestion: "imported second", Answers: []string{"two"}},
		},
	}
	_, err := s.UpsertFAQEntries(
		faqInputContext(ctx, t, types.FAQInputOperation{Kind: "import", Batch: batch}),
		"kb",
		batch,
	)
	require.NoError(t, err)
	queue := s.task.(*knowledgeInputQueue)
	require.Len(t, queue.tasks, 1)
	var payload types.FAQImportPayload
	require.NoError(t, json.Unmarshal(queue.tasks[0].Payload(), &payload))
	require.NotEmpty(t, payload.OriginalFAQInputID)
	require.NoError(t, s.ProcessFAQImport(context.Background(), queue.tasks[0]))
	snapshot, err := store.CaptureFAQInputs(
		ctx,
		types.FAQInputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", AllEntries: true},
	)
	require.NoError(t, err)
	require.Len(t, snapshot.Chunks, 2)
	patch := &types.FAQEntryFieldsBatchUpdate{
		ByID: map[int64]types.FAQEntryFieldsUpdate{snapshot.Chunks[0].SeqID: {IsEnabled: boolFAQ(false)}},
	}
	require.NoError(
		t,
		s.UpdateFAQEntryFieldsBatch(
			faqInputContext(ctx, t, types.FAQInputOperation{Kind: "fields", Fields: patch}),
			"kb",
			patch,
		),
	)
	legacy := &types.Chunk{
		ID:              "legacy-faq",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     snapshot.Knowledge.ID,
		ChunkType:       types.ChunkTypeFAQ,
		Content:         "unadmitted old body",
		IsEnabled:       true,
	}
	require.NoError(t, repository.NewChunkRepository(db).CreateChunks(context.Background(), []*types.Chunk{legacy}))
	patch = &types.FAQEntryFieldsBatchUpdate{
		ByID: map[int64]types.FAQEntryFieldsUpdate{legacy.SeqID: {IsEnabled: boolFAQ(false)}},
	}
	require.Error(
		t,
		s.UpdateFAQEntryFieldsBatch(
			faqInputContext(ctx, t, types.FAQInputOperation{Kind: "fields", Fields: patch}),
			"kb",
			patch,
		),
	)
	var actual types.Chunk
	require.NoError(t, db.Where("id=?", legacy.ID).Take(&actual).Error)
	require.True(t, actual.IsEnabled)
}
func boolFAQ(value bool) *bool { return &value }

func TestFAQImportCurrentActorRevokeBeforeWorker(t *testing.T) {
	s, _, db, ctx := faqActualInputFixture(t, "api")
	batch := &types.FAQBatchUpsertPayload{
		Mode:    types.FAQBatchModeAppend,
		Entries: []types.FAQEntryPayload{{StandardQuestion: "must never be written", Answers: []string{"secret"}}},
	}
	_, err := s.UpsertFAQEntries(
		faqInputContext(ctx, t, types.FAQInputOperation{Kind: "import", Batch: batch}),
		"kb",
		batch,
	)
	require.NoError(t, err)
	require.NoError(
		t,
		db.Model(&types.TenantAPIKey{}).
			Where("name='real scoped key'").
			Update("capabilities", types.StringArray{string(types.APIKeyCapabilityChat)}).
			Error,
	)
	queue := s.task.(*knowledgeInputQueue)
	require.Error(
		t,
		s.ProcessFAQImport(context.Background(), asynq.NewTask(types.TypeFAQImport, queue.tasks[0].Payload())),
	)
	var count int64
	require.NoError(t, db.Model(&types.Chunk{}).Where("chunk_type='faq'").Count(&count).Error)
	require.Zero(t, count)
}
