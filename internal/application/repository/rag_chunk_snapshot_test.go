package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func ragSnapshotSourceFixture(t *testing.T) (*knowledgeRepository, *gorm.DB, RAGChunkReadRequest) {
	t.Helper()
	repo, db, knowledge := sourceLineageRepoFixture(t)
	require.NoError(t, db.Model(knowledge).Update("title", "actual title").Error)
	chunk := &types.Chunk{
		ID: "actual-chunk", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: knowledge.ID,
		Content: "actual database body", ChunkType: types.ChunkTypeText, IsEnabled: true, IndexStatus: "ready",
	}
	require.NoError(t, db.Create(chunk).Error)
	lease, err := NewNextcloudContentLeaseStore(db).AcquireKnowledge(context.Background(), NextcloudContentScope{
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     knowledge.ID,
		DataSourceID:    "ds",
		ExternalID:      "nextcloud:instance:77",
	}, NextcloudContentReadLease, "rag-reader", time.Minute)
	require.NoError(t, err)
	return repo, db, RAGChunkReadRequest{
		TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: knowledge.ID,
		ChunkIDs: []string{chunk.ID}, Lease: ragTestReadToken(lease),
	}
}

func ragTestReadToken(lease NextcloudContentLease) *types.RAGExactReadLease {
	return &types.RAGExactReadLease{
		ID:              lease.ID,
		TenantID:        lease.TenantID,
		KnowledgeBaseID: lease.KnowledgeBaseID,
		KnowledgeID:     lease.KnowledgeID,
		Kind:            string(lease.Kind),
		Epoch:           lease.Epoch,
		OwnerID:         lease.OwnerID,
		ExpiresAt:       lease.ExpiresAt,
	}
}

func TestRAGChunkSnapshotBindsActualTextMetadataAndOriginalLease(t *testing.T) {
	_, db, request := ragSnapshotSourceFixture(t)
	rows, err := NewRAGChunkSnapshotReader(
		db,
		nil,
	).HydrateRAGChunkSnapshots(context.Background(), []RAGChunkReadRequest{request})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "actual database body", rows[0].Chunk.Content)
	require.Equal(t, "actual title", rows[0].Knowledge.Title)
	require.NoError(t, rows[0].SourceLineage.RequireComplete())
	require.Equal(t, "original-etag", rows[0].SourceLineage.Sources[0].ETag)
	require.Equal(t, uint64(request.Lease.Epoch), rows[0].SourceLineage.Sources[0].Revision)
	request.MetadataOnly = true
	request.ChunkIDs = nil
	metadata, err := NewRAGChunkSnapshotReader(
		db,
		nil,
	).HydrateRAGChunkSnapshots(context.Background(), []RAGChunkReadRequest{request})
	require.NoError(t, err)
	require.Len(t, metadata, 1)
	require.Nil(t, metadata[0].Chunk)
	require.Equal(t, rows[0].SourceLineage, metadata[0].SourceLineage)
	encoded, err := json.Marshal(metadata[0])
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(encoded), "the receipt cannot be sent as an unguarded public DTO")
}

func TestRAGChunkSnapshotOldPinCannotAuthenticateNewVersionOrOldCachedText(t *testing.T) {
	repo, db, request := ragSnapshotSourceFixture(t)
	reader := NewRAGChunkSnapshotReader(db, nil)
	original, err := reader.HydrateRAGChunkSnapshots(context.Background(), []RAGChunkReadRequest{request})
	require.NoError(t, err)
	oldLineage := original[0].SourceLineage
	// The retrieval artifact is now stale. Advance the admitted generation and
	// update the actual row through the fixture's trusted writer.
	require.NoError(
		t,
		repo.StageNextcloudVersion(
			context.Background(),
			7,
			"kb",
			"ds",
			"nextcloud:instance:77",
			"new-etag",
			request.KnowledgeID,
		),
	)
	require.NoError(
		t,
		db.Model(&types.Chunk{}).Where("id = ?", request.ChunkIDs[0]).Update("content", "new authoritative body").Error,
	)
	published, err := repo.PublishNextcloudVersion(context.Background(), request.KnowledgeID)
	require.NoError(t, err)
	require.True(t, published)
	rows, err := reader.HydrateRAGChunkSnapshots(context.Background(), []RAGChunkReadRequest{request})
	require.ErrorIs(t, err, ErrNextcloudContentLeaseDenied)
	require.Nil(t, rows)
	// A fresh read may capture the real new row, but cannot re-certify an old
	// history/augmentation receipt under that freshly acquired token.
	lease, err := NewNextcloudContentLeaseStore(db).AcquireKnowledge(context.Background(), NextcloudContentScope{
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     request.KnowledgeID,
		DataSourceID:    "ds",
		ExternalID:      "nextcloud:instance:77",
	}, NextcloudContentReadLease, "fresh-reader", time.Minute)
	require.NoError(t, err)
	request.Lease = ragTestReadToken(lease)
	request.ExpectedLineage = oldLineage
	rows, err = reader.HydrateRAGChunkSnapshots(context.Background(), []RAGChunkReadRequest{request})
	require.ErrorIs(t, err, ErrNextcloudLineageSnapshotDenied)
	require.Nil(t, rows)
	request.ExpectedLineage = nil
	rows, err = reader.HydrateRAGChunkSnapshots(context.Background(), []RAGChunkReadRequest{request})
	require.NoError(t, err)
	require.Equal(t, "new authoritative body", rows[0].Chunk.Content)
	require.Equal(t, "new-etag", rows[0].SourceLineage.Sources[0].ETag)
}

func TestRAGChunkSnapshotRejectsLeaseForgeryExpiryAndScopePivot(t *testing.T) {
	for _, changed := range []string{"missing", "unknown id", "owner", "expiry", "released", "wrong document", "wrong chunk scope", "rewritten source channel"} {
		t.Run(changed, func(t *testing.T) {
			_, db, request := ragSnapshotSourceFixture(t)
			switch changed {
			case "missing":
				request.Lease = nil
			case "unknown id":
				request.Lease.ID = "client-chosen"
			case "owner":
				request.Lease.OwnerID = "other-reader"
			case "expiry":
				require.NoError(t, db.Exec("UPDATE nextcloud_content_leases SET expires_at_ms=0").Error)
			case "released":
				require.NoError(t, db.Exec("UPDATE nextcloud_content_leases SET released_at_ms=1").Error)
			case "wrong document":
				request.Lease.KnowledgeID = "other-document"
			case "wrong chunk scope":
				require.NoError(
					t,
					db.Model(&types.Chunk{}).
						Where("id = ?", request.ChunkIDs[0]).
						Update("knowledge_base_id", "private-other-kb").
						Error,
				)
			case "rewritten source channel":
				require.NoError(
					t,
					db.Model(&types.Knowledge{}).
						Where("id = ?", request.KnowledgeID).
						Updates(map[string]any{"channel": "web", "metadata": types.JSON(`{"source_lineage":{"version":1,"state":"complete","sources":[]}}`)}).
						Error,
				)
			}
			rows, err := NewRAGChunkSnapshotReader(
				db,
				nil,
			).HydrateRAGChunkSnapshots(context.Background(), []RAGChunkReadRequest{request})
			require.Error(t, err)
			require.Nil(t, rows)
		})
	}
}

type ragOriginTestProof struct{}

func ragTestInputDigest(k *types.Knowledge, c *types.Chunk) string {
	payload, _ := json.Marshal(struct {
		Title    string
		Metadata types.JSON
		Chunk    *types.Chunk
	}{k.Title, k.Metadata, c})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func (ragOriginTestProof) ReadOrdinaryRAGInputLineage(
	ctx context.Context,
	tx *gorm.DB,
	k *types.Knowledge,
	c *types.Chunk,
) (*types.SourceLineage, error) {
	var proof struct{ Digest string }
	query := tx.WithContext(ctx).
		Table("rag_test_origin_proofs").
		Where("knowledge_id = ? AND chunk_id = ?", k.ID, c.ID).
		Take(&proof)
	if query.Error == gorm.ErrRecordNotFound {
		return types.UnknownSourceLineage(), nil
	}
	if query.Error != nil {
		return nil, query.Error
	}
	if proof.Digest != ragTestInputDigest(k, c) {
		return types.UnknownSourceLineage(), nil
	}
	return types.NewCompleteSourceLineage()
}

func TestRAGChunkSnapshotOrdinaryLegacyAndOrphanRemainUnknownUntilTrustedProof(t *testing.T) {
	_, db, _ := ragSnapshotSourceFixture(t)
	k := &types.Knowledge{
		ID:              "orphan-history-passage",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		Type:            "manual",
		Channel:         "web",
		ParseStatus:     types.ParseStatusCompleted,
		EnableStatus:    "enabled",
		Metadata: types.JSON(
			("{\"source_lineage\":{\"version\":1,\"state\":\"complete\",\"sources\":[]}," +
				"\"origin\":\"user_upload\",\"never_source\":true}"),
		),
	}
	c := &types.Chunk{
		ID:              "orphan-chunk",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     k.ID,
		Content:         "legacy Q&A copied into an ordinary passage",
		ChunkType:       types.ChunkTypeText,
		IsEnabled:       true,
		IndexStatus:     "ready",
	}
	require.NoError(t, db.Create(k).Error)
	require.NoError(t, db.Create(c).Error)
	request := RAGChunkReadRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, ChunkIDs: []string{c.ID}}
	reader := NewRAGChunkSnapshotReader(db, nil)
	rows, err := reader.HydrateRAGChunkSnapshots(context.Background(), []RAGChunkReadRequest{request})
	require.NoError(t, err)
	require.ErrorIs(t, rows[0].SourceLineage.RequireComplete(), types.ErrSourceLineageUnknown)
	// A separate server fact binds the exact input; metadata claiming origin
	// cannot stand in for it. This test dependency represents the future writer.
	require.NoError(
		t,
		db.Exec("CREATE TABLE rag_test_origin_proofs (knowledge_id TEXT, chunk_id TEXT, digest TEXT)").Error,
	)
	reader.ordinary = ragOriginTestProof{}
	rows, err = reader.HydrateRAGChunkSnapshots(context.Background(), []RAGChunkReadRequest{request})
	require.NoError(t, err)
	require.ErrorIs(t, rows[0].SourceLineage.RequireComplete(), types.ErrSourceLineageUnknown)
	require.NoError(
		t,
		db.Exec("INSERT INTO rag_test_origin_proofs VALUES (?, ?, ?)", k.ID, c.ID, ragTestInputDigest(k, c)).Error,
	)
	rows, err = reader.HydrateRAGChunkSnapshots(context.Background(), []RAGChunkReadRequest{request})
	require.NoError(t, err)
	require.NoError(t, rows[0].SourceLineage.RequireComplete())
	require.Empty(t, rows[0].SourceLineage.Sources)
	require.NoError(t, db.Model(c).Update("content", "changed body not covered by the proof").Error)
	rows, err = reader.HydrateRAGChunkSnapshots(context.Background(), []RAGChunkReadRequest{request})
	require.NoError(t, err)
	require.ErrorIs(t, rows[0].SourceLineage.RequireComplete(), types.ErrSourceLineageUnknown)
}
