package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func knowledgeInputPostgresFixture(t *testing.T) (*KnowledgeInputProvenanceStore, *gorm.DB) {
	t.Helper()
	_, db, _ := ragSnapshotPostgresFixture(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	migration, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB, PreferSimpleProtocol: true}), &gorm.Config{})
	require.NoError(t, err)
	script, err := os.ReadFile("../../../migrations/versioned/000134_knowledge_input_provenance.up.sql")
	require.NoError(t, err)
	require.NoError(t, migration.Exec(string(script)).Error)
	db.DisableForeignKeyConstraintWhenMigrating = true
	require.NoError(t, db.AutoMigrate(&types.Session{}, &types.Message{}))
	db.DisableForeignKeyConstraintWhenMigrating = false
	additional, err := os.ReadFile("../../../migrations/versioned/000135_knowledge_configuration_history_inputs.up.sql")
	require.NoError(t, err)
	require.NoError(t, migration.Exec(string(additional)).Error)
	controls, err := os.ReadFile("../../../migrations/versioned/000136_knowledge_actual_config_controls.up.sql")
	require.NoError(t, err)
	require.NoError(t, migration.Exec(string(controls)).Error)
	actor, err := os.ReadFile("../../../migrations/versioned/000137_knowledge_original_worker_actor.up.sql")
	require.NoError(t, err)
	require.NoError(t, migration.Exec(string(actor)).Error)
	installKnowledgeInputCurrentControls(t, db, true)
	store := NewKnowledgeInputProvenanceStore(db)
	require.NoError(
		t,
		store.ReplaceKnowledgeBaseConfigurationWithInput(
			WithIndependentKnowledgeInput(context.Background()),
			&types.KnowledgeBase{
				ID:               "kb",
				TenantID:         7,
				Name:             "source",
				EmbeddingModelID: "embed",
				SummaryModelID:   "summary",
			},
		),
	)
	return store, db
}

type knowledgeInputSnapshotPauseKey struct{}

func TestPostgresKnowledgeInputCaptureCoherentBodyAndMutationBeforeRelease(t *testing.T) {
	store, db := knowledgeInputPostgresFixture(t)
	ctx, k, c, _ := createAdmittedManual(t, store, db)
	baseline, err := store.CaptureKnowledgeBuildInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     k.ID,
			ChunkIDs:        []string{c.ID},
		},
	)
	require.NoError(t, err)
	fired := false
	name := "knowledge_input_body_changed_after_header"
	require.NoError(t, db.Callback().Query().After("gorm:query").Register(name, func(tx *gorm.DB) {
		if fired || tx.Statement.Context.Value(knowledgeInputSnapshotPauseKey{}) != true ||
			tx.Statement.Table != "knowledges" {
			return
		}
		fired = true
		require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", c.ID).Update("content", "new generic body").Error)
	}))
	t.Cleanup(func() { _ = db.Callback().Query().Remove(name) })
	rows, err := store.CaptureKnowledgeBuildInputs(
		context.WithValue(ctx, knowledgeInputSnapshotPauseKey{}, true),
		types.KnowledgeBuildInputCaptureRequest{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     k.ID,
			ChunkIDs:        []string{c.ID},
		},
	)
	require.NoError(t, err)
	require.True(t, fired)
	require.Equal(
		t,
		c.Content,
		rows.Chunks[0].Content,
		"one RR snapshot binds actual old body and its original receipt",
	)
	require.Equal(t, baseline.Proof.BodyDigest, rows.Proof.BodyDigest)
	require.Error(
		t,
		store.RecheckKnowledgeBuildInputs(ctx, rows.Proof),
		"the model/output checkpoint rejects the committed change",
	)
	require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", c.ID).Update("content", c.Content).Error)
	require.Error(
		t,
		store.RecheckKnowledgeBuildInputs(ctx, baseline.Proof),
		"rollback of text cannot roll back monotonic original material version",
	)
}

func TestPostgresKnowledgeInputConfigRollbackAndImmutableOrigin(t *testing.T) {
	store, db := knowledgeInputPostgresFixture(t)
	ctx, k, c, proof := createAdmittedManual(t, store, db)
	input, err := store.CaptureKnowledgeBuildInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     k.ID,
			ChunkIDs:        []string{c.ID},
		},
	)
	require.NoError(t, err)
	require.Error(
		t,
		db.Table("knowledge_input_admissions").
			Where("admission_id = ?", proof.AdmissionID).
			Update("input_digest", proof.InputDigest).
			Error,
	)
	require.NoError(t, db.Exec("UPDATE knowledge_bases SET summary_model_id='changed' WHERE id='kb'").Error)
	require.NoError(t, db.Exec("UPDATE knowledge_bases SET summary_model_id='summary' WHERE id='kb'").Error)
	require.Error(t, store.RecheckKnowledgeBuildInputs(ctx, input.Proof))
	rows, err := NewRAGChunkSnapshotReader(
		db,
		store,
	).HydrateRAGChunkSnapshots(ctx, []RAGChunkReadRequest{{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, ChunkIDs: []string{c.ID}}})
	require.NoError(t, err)
	require.ErrorIs(t, rows[0].SourceLineage.RequireComplete(), types.ErrSourceLineageUnknown)
}

func TestPostgresKnowledgeInputNextcloudOriginalBuildEpochAndSourceWithdrawal(t *testing.T) {
	store, db := knowledgeInputPostgresFixture(t)
	scope := NextcloudContentScope{
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     "doc-a",
		DataSourceID:    "ds",
		ExternalID:      "nextcloud:instance:77",
	}
	leases := NewNextcloudContentLeaseStore(db)
	lease, err := leases.AcquireKnowledge(
		context.Background(),
		scope,
		NextcloudContentBuildLease,
		"build-reader",
		time.Minute,
	)
	require.NoError(t, err)
	ctx := WithNextcloudBuildLease(context.Background(), leases, lease, scope)
	proof, err := store.AdmitNextcloudBuildInputs(ctx, []byte("actual original source bytes"))
	require.NoError(t, err)
	require.Equal(t, uint64(lease.Epoch), proof.BuildGeneration)
	require.Equal(t, "etag-77", proof.SourceLineage.Sources[0].ETag)
	ctx = WithKnowledgeBuildInputProof(ctx, store, proof)
	var k types.Knowledge
	require.NoError(t, db.Where("id = 'doc-a'").Take(&k).Error)
	c := &types.Chunk{
		ID:              "actual-build-chunk",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     k.ID,
		Content:         "actual build body",
		ChunkType:       "text",
		IsEnabled:       true,
		IndexStatus:     "ready",
	}
	require.NoError(t, NewChunkRepository(db).CreateChunks(ctx, []*types.Chunk{c}))
	input, err := store.CaptureKnowledgeBuildInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     k.ID,
			ChunkIDs:        []string{c.ID},
		},
	)
	require.NoError(t, err)
	require.NoError(t, store.RecheckKnowledgeBuildInputs(ctx, input.Proof))
	require.NoError(t, leases.RetireKnowledge(context.Background(), scope))
	require.Error(t, store.RecheckKnowledgeBuildInputs(ctx, input.Proof))
	_, err = store.CaptureKnowledgeBuildInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     k.ID,
			ChunkIDs:        []string{c.ID},
			Expected:        input.Proof,
		},
	)
	require.Error(t, err)
}

func TestPostgresKnowledgeInputActualControlsIgnoreGeneratedKBDescription(t *testing.T) {
	store, db := knowledgeInputPostgresFixture(t)
	ctx, k, c, _ := createAdmittedManual(t, store, db)
	input, err := store.CaptureKnowledgeBuildInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{
			Projection:      types.KnowledgeBuildProjectionGraph,
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     k.ID,
			ChunkIDs:        []string{c.ID},
		},
	)
	require.NoError(t, err)
	require.NoError(
		t,
		db.Exec(
			("UPDATE knowledge_bases SET description='generated description',generated_p" +
				"rofile='{\"gist\":\"generated\"}' WHERE id='kb'"),
		).Error,
	)
	require.NoError(t, store.RecheckKnowledgeBuildInputs(ctx, input.Proof))
	require.NoError(t, db.Exec("UPDATE knowledge_bases SET summary_model_id='changed' WHERE id='kb'").Error)
	require.NoError(t, db.Exec("UPDATE knowledge_bases SET summary_model_id='summary' WHERE id='kb'").Error)
	require.Error(
		t,
		store.RecheckKnowledgeBuildInputs(ctx, input.Proof),
		"control ABA cannot restore an original graph model input generation",
	)
}
