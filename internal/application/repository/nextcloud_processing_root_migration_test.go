package repository

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	nextcloud "github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNextcloudRootMigrationPreservesTypedGraphOriginalInputsAndCurrentProbe(t *testing.T) {
	state, db, ctx, k, chunk, inputs := processingStateFixture(t, true)
	_, err := state.StartGraph(ctx, 7, k.ID, 1, chunk.ID)
	require.NoError(t, err)
	ctx, graph := boundProcessingGraph(ctx, t, k, chunk, inputs)
	require.NoError(
		t,
		NewScopedSQLGraphRepository(
			db,
			nil,
		).AddGraph(ctx, types.NameSpace{KnowledgeBase: "kb", Knowledge: k.ID}, []*types.GraphData{graph}),
	)
	script, err := os.ReadFile("../../../migrations/sqlite/000076_nextcloud_directory_migrations.up.sql")
	require.NoError(t, err)
	if !db.Migrator().HasTable("nextcloud_root_migrations") {
		require.NoError(t, db.Exec(string(script)).Error)
	}
	var pair NextcloudSourcePairing
	require.NoError(t, db.Where("datasource_id='ds'").Take(&pair).Error)
	proof := nextcloud.RootMigration{
		OperationID:        uuid.NewString(),
		PairOperationID:    pair.OperationID,
		BindingID:          "binding",
		InstanceID:         "instance",
		TenantID:           "7",
		KnowledgeBaseID:    "kb",
		DataSourceID:       "ds",
		RootFileID:         "66",
		PhysicalRootKey:    strings.Repeat("a", 64),
		StorageIdentity:    strings.Repeat("b", 64),
		OldRootHash:        strings.Repeat("c", 64),
		NewRootHash:        strings.Repeat("d", 64),
		OldRootPath:        "/Published",
		NewRootPath:        "/Renamed",
		OldEpoch:           "0",
		PreparedEpoch:      "1",
		ResumeEpoch:        "2",
		JournalStreamID:    "external-journal",
		JournalSequence:    "1",
		JournalChainSHA256: strings.Repeat("e", 64),
		State:              "finalized",
	}
	raw, err := json.Marshal(proof)
	require.NoError(t, err)
	require.NoError(
		t,
		db.Create(
			&NextcloudRootMigration{
				OperationID:     proof.OperationID,
				PairOperationID: pair.OperationID,
				TenantID:        7,
				KnowledgeBaseID: "kb",
				DataSourceID:    "ds",
				ProofJSON:       string(raw),
				ProofSHA256:     rootProofDigest(proof),
				State:           "finalized",
				CreatedAt:       time.Now().UTC(),
				UpdatedAt:       time.Now().UTC(),
			},
		).Error,
	)
	require.NoError(
		t,
		db.Table("nextcloud_source_status_epochs").
			Create(map[string]any{
				"connection_id":     "connection-0000001",
				"publication_epoch": 2,
				"pair_operation_id": pair.OperationID,
				"verified_at":       time.Now().Unix(),
			}).
			Error,
	)
	var original types.Knowledge
	require.NoError(t, db.Where("id=?", k.ID).Take(&original).Error)
	metadata := original.GetMetadata()
	metadata["nextcloud_etag"] = ""
	metadata["nextcloud_path"] = "file.md"
	metadata["nextcloud_generation"] = "fresh-root-generation"
	metadata["nextcloud_publication_epoch"] = "2"
	metadata["source_updated_at"] = "new-observation"
	repo := NewKnowledgeRepository(db).(*knowledgeRepository)
	require.NoError(
		t,
		repo.StageNextcloudVersionWithSource(
			ctx,
			7,
			"kb",
			"ds",
			"nextcloud:instance:77",
			"original-etag",
			k.ID,
			"file.md",
			metadata,
		),
	)
	var after types.Knowledge
	require.NoError(t, db.Where("id=?", k.ID).Take(&after).Error)
	require.Equal(
		t,
		original.Metadata,
		after.Metadata,
		"authenticated same-root movement must not rewrite original proof inputs",
	)
	require.NoError(t, inputs.RecheckKnowledgeBuildInputs(ctx, graph.SourceReceipt.OriginalInput))
	require.NoError(t, state.Capture(ctx, 7, "kb", "ds", "nextcloud:instance:77", true))
	status := loadProcessingState(t, db)
	require.Equal(t, "2", status.PublicationEpoch)
	require.Equal(t, "succeeded", status.Graph.State)
	require.True(t, status.Graph.Persisted)
	path, err := nextcloudCurrentSourcePath(ctx, db, 7, "kb", "ds", "file.md")
	require.NoError(t, err)
	require.Equal(t, "file.md", path)
	allowed, err := nextcloudRootEpochAuthorized(ctx, db, 7, "kb", "ds", "file.md", 0, 2)
	require.NoError(t, err)
	require.True(t, allowed)
	allowed, err = nextcloudRootEpochAuthorized(ctx, db, 7, "kb", "other-source", "file.md", 0, 2)
	require.NoError(t, err)
	require.False(t, allowed)
	// A changed child path is outside root migration's same-file reuse proof.
	metadata["nextcloud_path"] = "other-child.md"
	require.NoError(
		t,
		repo.StageNextcloudVersionWithSource(
			ctx,
			7,
			"kb",
			"ds",
			"nextcloud:instance:77",
			"original-etag",
			k.ID,
			"file.md",
			metadata,
		),
	)
	require.Error(t, inputs.RecheckKnowledgeBuildInputs(context.WithoutCancel(ctx), graph.SourceReceipt.OriginalInput))
}

func TestNextcloudSameETagProcessingConfigurationChangeRequiresNewCandidate(t *testing.T) {
	_, db, _, k, _, _ := processingStateFixture(t, false)
	repo := NewKnowledgeRepository(db)
	params := &types.KnowledgeCheckParams{
		Type:                "file",
		FileHash:            "test-file-hash",
		FileType:            "md",
		DataSourceID:        "ds",
		ExternalID:          "nextcloud:instance:77",
		NextcloudTargetETag: "original-etag",
	}
	exists, duplicate, err := repo.CheckKnowledgeExists(context.Background(), 7, "kb", params)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, k.ID, duplicate.ID)
	require.NoError(t, db.Exec("UPDATE knowledge_bases SET summary_model_id='new-model' WHERE id='kb'").Error)
	exists, duplicate, err = repo.CheckKnowledgeExists(context.Background(), 7, "kb", params)
	require.NoError(t, err)
	require.False(t, exists)
	require.Nil(t, duplicate, "new processing settings cannot borrow same-ETag old result")
}
