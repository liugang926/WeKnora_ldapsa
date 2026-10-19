package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func knowledgeInputFixture(t *testing.T) (*KnowledgeInputProvenanceStore, *gorm.DB) {
	t.Helper()
	_, db, _ := sourceLineageRepoFixture(t)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.KnowledgeBase{}, &types.Chunk{}))
	script, err := os.ReadFile("../../../migrations/sqlite/000053_knowledge_input_provenance.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	db.Config.DisableForeignKeyConstraintWhenMigrating = true
	require.NoError(t, db.AutoMigrate(&types.Session{}, &types.Message{}))
	db.Config.DisableForeignKeyConstraintWhenMigrating = false
	additional, err := os.ReadFile("../../../migrations/sqlite/000054_knowledge_configuration_history_inputs.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(additional)).Error)
	controls, err := os.ReadFile("../../../migrations/sqlite/000055_knowledge_actual_config_controls.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(controls)).Error)
	actor, err := os.ReadFile("../../../migrations/sqlite/000056_knowledge_original_worker_actor.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(actor)).Error)
	store := NewKnowledgeInputProvenanceStore(db)
	require.NoError(t, store.ReplaceKnowledgeBaseConfigurationWithInput(WithIndependentKnowledgeInput(context.Background()), &types.KnowledgeBase{ID: "kb", TenantID: 7, Name: "source", EmbeddingModelID: "embed", SummaryModelID: "summary"}))
	return store, db
}
func createAdmittedManual(t *testing.T, store *KnowledgeInputProvenanceStore, db *gorm.DB) (context.Context, *types.Knowledge, *types.Chunk, *types.KnowledgeBuildInputProof) {
	t.Helper()
	k := &types.Knowledge{ID: "manual", TenantID: 7, KnowledgeBaseID: "kb", Type: types.KnowledgeTypeManual, Source: "manual", Channel: "web", Title: "human title", Metadata: types.JSON(`{"manual":{"content":"raw human body","version":1}}`), CustomMetadata: types.JSON(`{}`), ParseStatus: types.ParseStatusPending, EnableStatus: "disabled"}
	proof, err := store.CreateKnowledgeWithInput(WithIndependentKnowledgeInput(context.Background()), k, "raw human body")
	require.NoError(t, err)
	ctx := WithKnowledgeBuildInputProof(context.Background(), store, proof)
	c := &types.Chunk{ID: "manual-chunk", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Content: "actual parsed body", ChunkType: types.ChunkTypeText, IsEnabled: true, IndexStatus: "ready", Metadata: types.JSON(`{"source":"actual"}`)}
	require.NoError(t, NewChunkRepository(db).CreateChunks(ctx, []*types.Chunk{c}))
	k.ParseStatus = types.ParseStatusCompleted
	k.EnableStatus = "enabled"
	require.NoError(t, NewKnowledgeRepository(db).UpdateKnowledge(ctx, k))
	return ctx, k, c, proof
}

func TestKnowledgeInputActualWriterHydrationAndBuildSnapshot(t *testing.T) {
	store, db := knowledgeInputFixture(t)
	ctx, k, c, proof := createAdmittedManual(t, store, db)
	rows, err := NewRAGChunkSnapshotReader(db, store).HydrateRAGChunkSnapshots(context.Background(), []RAGChunkReadRequest{{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, ChunkIDs: []string{c.ID}}})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.NoError(t, rows[0].SourceLineage.RequireComplete())
	require.Empty(t, rows[0].SourceLineage.Sources)
	require.Equal(t, "actual parsed body", rows[0].Chunk.Content)
	input, err := store.CaptureKnowledgeBuildInputs(ctx, types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, ChunkIDs: []string{c.ID}})
	require.NoError(t, err)
	require.Equal(t, c.Content, input.Chunks[0].Content)
	require.NotEmpty(t, input.Proof.BodyDigest)
	require.Equal(t, proof.InputDigest, input.Proof.InputDigest)
	require.NoError(t, store.RecheckKnowledgeBuildInputs(ctx, input.Proof))
	require.ErrorIs(t, KnowledgeInputArtifactMatches(proof, "forged text"), ErrKnowledgeInputChanged)
	require.NoError(t, KnowledgeInputArtifactMatches(proof, "raw human body"))
}

func TestKnowledgeInputClientForgeAndLegacyCannotMintOrigin(t *testing.T) {
	store, db := knowledgeInputFixture(t)
	k := &types.Knowledge{ID: "legacy", TenantID: 7, KnowledgeBaseID: "kb", Type: "passage", Channel: "web", ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled", CustomMetadata: types.JSON(`{}`), Metadata: types.JSON(`{"origin":"independent","source_lineage":{"version":1,"state":"complete","sources":[]}}`)}
	_, err := store.CreateKnowledgeWithInput(context.Background(), k, "client body")
	require.ErrorIs(t, err, types.ErrSourceLineageUnknown)
	require.NoError(t, NewKnowledgeRepository(db).CreateKnowledge(context.Background(), k))
	c := &types.Chunk{ID: "legacy-chunk", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Content: "orphan chat answer", ChunkType: types.ChunkTypeText, IsEnabled: true, IndexStatus: "ready"}
	require.NoError(t, NewChunkRepository(db).CreateChunks(context.Background(), []*types.Chunk{c}))
	rows, err := NewRAGChunkSnapshotReader(db, store).HydrateRAGChunkSnapshots(context.Background(), []RAGChunkReadRequest{{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, ChunkIDs: []string{c.ID}}})
	require.NoError(t, err)
	require.ErrorIs(t, rows[0].SourceLineage.RequireComplete(), types.ErrSourceLineageUnknown)
	_, err = store.CaptureKnowledgeBuildInputs(context.Background(), types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, ChunkIDs: []string{c.ID}})
	require.ErrorIs(t, err, types.ErrSourceLineageUnknown)
}

func TestKnowledgeInputGenericMutationAndRollbackNeverRestoreOldProof(t *testing.T) {
	for _, kind := range []string{"chunk", "title", "metadata", "config"} {
		t.Run(kind, func(t *testing.T) {
			store, db := knowledgeInputFixture(t)
			ctx, k, c, _ := createAdmittedManual(t, store, db)
			input, err := store.CaptureKnowledgeBuildInputs(ctx, types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, ChunkIDs: []string{c.ID}})
			require.NoError(t, err)
			switch kind {
			case "chunk":
				require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", c.ID).Update("content", "changed").Error)
				require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", c.ID).Update("content", c.Content).Error)
			case "title":
				require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", k.ID).Update("title", "changed").Error)
				require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", k.ID).Update("title", k.Title).Error)
			case "metadata":
				require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", k.ID).Update("metadata", types.JSON(`{"manual":{"content":"changed","version":2}}`)).Error)
				require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", k.ID).Update("metadata", k.Metadata).Error)
			case "config":
				var kb types.KnowledgeBase
				require.NoError(t, db.Where("id = 'kb'").Take(&kb).Error)
				old := kb.SummaryModelID
				require.NoError(t, db.Model(&kb).Update("summary_model_id", "changed").Error)
				require.NoError(t, db.Model(&kb).Update("summary_model_id", old).Error)
			}
			require.Error(t, store.RecheckKnowledgeBuildInputs(ctx, input.Proof))
			rows, err := NewRAGChunkSnapshotReader(db, store).HydrateRAGChunkSnapshots(context.Background(), []RAGChunkReadRequest{{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, ChunkIDs: []string{c.ID}}})
			require.NoError(t, err)
			require.ErrorIs(t, rows[0].SourceLineage.RequireComplete(), types.ErrSourceLineageUnknown)
		})
	}
}

func TestKnowledgeInputImmutableOriginAndWholeBatchRollback(t *testing.T) {
	store, db := knowledgeInputFixture(t)
	ctx, k, _, proof := createAdmittedManual(t, store, db)
	require.Error(t, db.Table("knowledge_input_admissions").Where("admission_id = ?", proof.AdmissionID).Update("source_lineage", `{"version":1,"state":"complete","sources":[]}`).Error)
	require.Error(t, db.Exec("DELETE FROM knowledge_input_admissions WHERE admission_id = ?", proof.AdmissionID).Error)
	good := &types.Chunk{ID: "good", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Content: "good", ChunkType: "text", IsEnabled: true}
	bad := &types.Chunk{ID: "bad", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "other", Content: "foreign", ChunkType: "text", IsEnabled: true}
	require.Error(t, NewChunkRepository(db).CreateChunks(ctx, []*types.Chunk{good, bad}))
	var count int64
	require.NoError(t, db.Model(&types.Chunk{}).Where("id IN ?", []string{"good", "bad"}).Count(&count).Error)
	require.Zero(t, count)
}

func TestKnowledgeInputNextcloudStagingAndActualOriginalChunkReceipt(t *testing.T) {
	store, db := knowledgeInputFixture(t)
	repo := NewKnowledgeRepository(db).(*knowledgeRepository)
	repo.nextcloudPublicationCheck = func(context.Context, *types.DataSourceConfig, string, string, int64, string, string) error {
		return nil
	}
	require.NoError(t, repo.StageNextcloudVersion(context.Background(), 7, "kb", "ds", "nextcloud:instance:77", "original-etag", "candidate"))
	var k types.Knowledge
	require.NoError(t, db.Where("id = 'candidate'").Take(&k).Error)
	require.NoError(t, db.Model(&k).Updates(map[string]any{"parse_status": types.ParseStatusFinalizing, "enable_status": "disabled"}).Error)
	scope := NextcloudContentScope{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, DataSourceID: "ds", ExternalID: "nextcloud:instance:77"}
	leases := NewNextcloudContentLeaseStore(db)
	lease, err := leases.AcquireKnowledge(context.Background(), scope, NextcloudContentBuildLease, "actual-build", time.Minute)
	require.NoError(t, err)
	ctx := WithNextcloudBuildLease(context.Background(), leases, lease, scope)
	proof, err := store.AdmitNextcloudBuildInputs(ctx, []byte("actual original artifact"))
	require.NoError(t, err)
	ctx = WithKnowledgeBuildInputProof(ctx, store, proof)
	c := &types.Chunk{ID: "receipt-source", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Content: "actual newly produced text", ChunkType: "text", IsEnabled: true, IndexStatus: "ready"}
	require.NoError(t, NewChunkRepository(db).CreateChunks(ctx, []*types.Chunk{c}))
	input, err := store.CaptureKnowledgeBuildInputs(ctx, types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, ChunkIDs: []string{c.ID}})
	require.NoError(t, err)
	require.NoError(t, store.RecheckKnowledgeBuildInputs(ctx, input.Proof))
	require.Equal(t, "original-etag", input.Proof.SourceLineage.Sources[0].ETag)
	require.NoError(t, db.Where("id = ?", k.ID).Take(&k).Error)
	k.ParseStatus = types.ParseStatusCompleted
	k.EnableStatus = "enabled"
	require.NoError(t, repo.UpdateKnowledge(ctx, &k))
	published, err := repo.PublishNextcloudVersion(ctx, k.ID)
	require.NoError(t, err)
	require.True(t, published)
	read, err := leases.AcquireKnowledge(context.Background(), scope, NextcloudContentReadLease, "source-reader", time.Minute)
	require.NoError(t, err)
	request := RAGChunkReadRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, ChunkIDs: []string{c.ID}, Lease: ragTestReadToken(read)}
	rows, err := NewRAGChunkSnapshotReader(db, store).HydrateRAGChunkSnapshots(ctx, []RAGChunkReadRequest{request})
	require.NoError(t, err)
	require.NoError(t, rows[0].SourceLineage.RequireComplete())
	// Current source IDs alone cannot authenticate a chunk without an original
	// producer receipt, even while a valid current read lease is held.
	legacy := &types.Chunk{ID: "unproved-source", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Content: "old cached database body", ChunkType: "text", IsEnabled: true, IndexStatus: "ready"}
	require.NoError(t, NewChunkRepository(db).CreateChunks(context.Background(), []*types.Chunk{legacy}))
	request.ChunkIDs = []string{legacy.ID}
	rows, err = NewRAGChunkSnapshotReader(db, store).HydrateRAGChunkSnapshots(ctx, []RAGChunkReadRequest{request})
	require.NoError(t, err)
	require.ErrorIs(t, rows[0].SourceLineage.RequireComplete(), types.ErrSourceLineageUnknown)
}

func TestKnowledgeInputDerivedSourcesPreservedAndWithdrawalClosesReads(t *testing.T) {
	store, db := knowledgeInputFixture(t)
	repo := NewKnowledgeRepository(db).(*knowledgeRepository)
	_, source, err := repo.ResolveNextcloudSourceLineage(context.Background(), 7, "kb", "candidate")
	require.NoError(t, err)
	lineage, err := types.NewCompleteSourceLineage(source)
	require.NoError(t, err)
	ctx, err := WithDerivedKnowledgeInput(context.Background(), lineage)
	require.NoError(t, err)
	k := &types.Knowledge{ID: "derived", TenantID: 7, KnowledgeBaseID: "kb", Type: "passage", Channel: "web", CustomMetadata: types.JSON(`{}`), ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled"}
	proof, err := store.CreateKnowledgeWithInput(ctx, k, []string{"derived source answer"})
	require.NoError(t, err)
	ctx = WithKnowledgeBuildInputProof(ctx, store, proof)
	c := &types.Chunk{ID: "derived-chunk", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Content: "source derived answer", ChunkType: "text", IsEnabled: true, IndexStatus: "ready"}
	require.NoError(t, NewChunkRepository(db).CreateChunks(ctx, []*types.Chunk{c}))
	input, err := store.CaptureKnowledgeBuildInputs(ctx, types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, ChunkIDs: []string{c.ID}})
	require.NoError(t, err)
	require.Equal(t, lineage, input.Proof.SourceLineage)
	require.NoError(t, NewNextcloudContentLeaseStore(db).RetireKnowledge(context.Background(), NextcloudContentScope{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: source.KnowledgeID, DataSourceID: source.DataSourceID, ExternalID: source.ExternalID}))
	require.Error(t, store.RecheckKnowledgeBuildInputs(ctx, input.Proof))
	rows, err := NewRAGChunkSnapshotReader(db, store).HydrateRAGChunkSnapshots(ctx, []RAGChunkReadRequest{{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, ChunkIDs: []string{c.ID}}})
	require.NoError(t, err)
	require.ErrorIs(t, rows[0].SourceLineage.RequireComplete(), types.ErrSourceLineageUnknown)
}

func TestKnowledgeInputGraphProjectionSurvivesUnusedSummaryButBodyRollbackCloses(t *testing.T) {
	store, db := knowledgeInputFixture(t)
	ctx, k, c, _ := createAdmittedManual(t, store, db)
	request := types.KnowledgeBuildInputCaptureRequest{Projection: types.KnowledgeBuildProjectionGraph, TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, ChunkIDs: []string{c.ID}}
	input, err := store.CaptureKnowledgeBuildInputs(ctx, request)
	require.NoError(t, err)
	k.Description = "generated summary"
	require.NoError(t, NewKnowledgeRepository(db).UpdateKnowledge(ctx, k))
	c.Metadata = types.JSON(`{"generated_questions":["new generated question"]}`)
	require.NoError(t, NewChunkRepository(db).UpdateChunk(ctx, c))
	require.NoError(t, store.RecheckKnowledgeBuildInputs(ctx, input.Proof), "unused generated display fields do not change actual graph model input")
	require.NoError(t, db.Model(&types.KnowledgeBase{}).Where("id = 'kb'").Updates(map[string]any{"description": "generated description", "generated_profile": types.JSON(`{"gist":"generated profile"}`)}).Error)
	require.NoError(t, store.RecheckKnowledgeBuildInputs(ctx, input.Proof), "unused KB description/profile cannot invalidate actual original graph controls")
	full, err := store.CaptureKnowledgeBuildInputs(ctx, types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, ChunkIDs: []string{c.ID}})
	require.NoError(t, err)
	_, err = store.CaptureKnowledgeBuildInputs(ctx, types.KnowledgeBuildInputCaptureRequest{Projection: types.KnowledgeBuildProjectionGraph, TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, ChunkIDs: []string{c.ID}, Expected: full.Proof})
	require.Error(t, err, "a captured full proof cannot be switched to a subset projection")
	oldContent := c.Content
	require.NoError(t, db.Model(c).Update("content", "new text").Error)
	require.NoError(t, db.Model(c).Update("content", oldContent).Error)
	require.Error(t, store.RecheckKnowledgeBuildInputs(ctx, input.Proof), "content ABA retains a higher graph input version")
}

func TestKnowledgeInputLegacyConfigurationCannotBeAuthenticatedByHumanDocument(t *testing.T) {
	store, db := knowledgeInputFixture(t)
	legacy := &types.KnowledgeBase{ID: "legacy-kb", TenantID: 7, Name: "legacy", ExtractConfig: &types.ExtractConfig{Text: "orphan derived example"}}
	require.NoError(t, NewKnowledgeBaseRepository(db).CreateKnowledgeBase(context.Background(), legacy))
	k := &types.Knowledge{ID: "human-on-legacy", TenantID: 7, KnowledgeBaseID: legacy.ID, Type: "manual", CustomMetadata: types.JSON(`{}`)}
	_, err := store.CreateKnowledgeWithInput(WithIndependentKnowledgeInput(context.Background()), k, "new human text")
	require.Error(t, err)
	var count int64
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", k.ID).Count(&count).Error)
	require.Zero(t, count, "origin failure rolls back the child creation")
	legacy.Name = "renamed"
	require.NoError(t, store.UpdateKnowledgeBaseWithInput(WithIndependentKnowledgeInput(context.Background()), legacy), "unused display rename preserves unknown model configuration")
	var configOrigins int64
	require.NoError(t, db.Table("knowledge_config_input_origins").Where("knowledge_base_id = ?", legacy.ID).Count(&configOrigins).Error)
	require.Zero(t, configOrigins, "display rename cannot mint an origin for retained unknown examples")
	_, err = store.CreateKnowledgeWithInput(WithIndependentKnowledgeInput(context.Background()), k, "new human text after rename")
	require.Error(t, err)
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", k.ID).Count(&count).Error)
	require.Zero(t, count)
	fresh := &types.KnowledgeBase{ID: "fresh-kb", TenantID: 7, Name: "human configuration", SummaryModelID: "explicit-model", ExtractConfig: &types.ExtractConfig{Text: "new explicitly authored example"}}
	require.NoError(t, store.CreateKnowledgeBaseWithInput(WithIndependentKnowledgeInput(context.Background()), fresh))
	k.ID = "human-on-fresh"
	k.KnowledgeBaseID = fresh.ID
	proof, err := store.CreateKnowledgeWithInput(WithIndependentKnowledgeInput(context.Background()), k, "new human text")
	require.NoError(t, err)
	require.NoError(t, proof.SourceLineage.RequireComplete())
}

func TestKnowledgeInputPartialHumanUpdatePreservesRetainedDerivedDependencies(t *testing.T) {
	for _, field := range []string{"title", "custom_metadata", "manual_content"} {
		t.Run(field, func(t *testing.T) {
			store, db := knowledgeInputFixture(t)
			repo := NewKnowledgeRepository(db).(*knowledgeRepository)
			_, source, err := repo.ResolveNextcloudSourceLineage(context.Background(), 7, "kb", "candidate")
			require.NoError(t, err)
			lineage, err := types.NewCompleteSourceLineage(source)
			require.NoError(t, err)
			ctx, err := WithDerivedKnowledgeInput(context.Background(), lineage)
			require.NoError(t, err)
			k := &types.Knowledge{ID: "retained", TenantID: 7, KnowledgeBaseID: "kb", Type: "manual", Source: "manual", Channel: "web", Title: "derived title", Description: "derived description", Profile: &types.KnowledgeProfile{Gist: "derived profile"}, CustomMetadata: types.JSON(`{"label":"derived custom metadata"}`), Metadata: types.JSON(`{"manual":{"content":"derived original body","version":1}}`), ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled"}
			_, err = store.CreateKnowledgeWithInput(ctx, k, "derived original body")
			require.NoError(t, err)
			switch field {
			case "title":
				k.Title = "new human title"
			case "custom_metadata":
				k.CustomMetadata = types.JSON(`{"label":"human replacement"}`)
			case "manual_content":
				k.Metadata = types.JSON(`{"manual":{"content":"new human text","version":2}}`)
			}
			updated, err := store.UpdateKnowledgeWithInput(WithIndependentKnowledgeInput(context.Background()), k, "new human input")
			require.NoError(t, err)
			require.Equal(t, lineage, updated.SourceLineage, "retained body/Profile/metadata cannot become independent after a partial human edit")
			require.NoError(t, NewNextcloudContentLeaseStore(db).RetireKnowledge(context.Background(), NextcloudContentScope{TenantID: source.TenantID, KnowledgeBaseID: source.KnowledgeBaseID, KnowledgeID: source.KnowledgeID, DataSourceID: source.DataSourceID, ExternalID: source.ExternalID}))
			require.Error(t, store.RecheckKnowledgeBuildInputs(context.Background(), updated))
		})
	}
}
