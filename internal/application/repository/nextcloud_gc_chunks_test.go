package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNextcloudExactChunkGCIsDormantScopedAndAtomicSQLite(t *testing.T) {
	ctx := context.Background()
	store, db := nextcloudGCTestDB(t)
	require.NoError(t, db.AutoMigrate(&types.ChunkRevision{}))
	script, err := os.ReadFile("../../../migrations/sqlite/000046_nextcloud_content_leases.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	now := time.Now().UTC()
	nextcloudGCTestSource(t, db, "old", "", now.Add(-2*time.Hour))
	require.NoError(t, db.Create(&types.Chunk{
		ID: "chunk-old", TenantID: 7,
		KnowledgeBaseID: "kb", KnowledgeID: "old", Content: "old content",
	}).Error)
	require.NoError(t, db.Create(&types.ChunkRevision{
		ID: uuid.NewString(), TenantID: 7,
		KnowledgeBaseID: "kb", KnowledgeID: "old", ChunkID: "chunk-old",
		Content: "earlier content",
	}).Error)
	created, err := store.InventoryRetired(ctx, now, 10)
	require.NoError(t, err)
	require.Equal(t, 1, created)
	processed, err := store.RunDue(ctx, now, 10)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	var job nextcloudGCJob
	require.NoError(t, db.Take(&job).Error)
	require.Equal(t, "blocked", job.State)
	scope := NextcloudContentScope{
		TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "old",
		DataSourceID: "ds", ExternalID: "node",
	}
	leases := NewNextcloudContentLeaseStore(db)
	reader, err := leases.AcquireKnowledge(ctx, scope, NextcloudContentReadLease, "reader", time.Minute)
	require.NoError(t, err)
	require.NoError(t, leases.RetireKnowledge(ctx, scope))
	notBefore := leaseTestNotBefore(t, db, scope)
	_, err = leases.ClaimKnowledgeGC(ctx, scope, notBefore, time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentGCUncovered)
	leaseTestCoverKB(t, db, scope) // Test-only assertion; production migration adds no row.
	_, err = leases.ClaimKnowledgeGC(ctx, scope, notBefore, time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentGCBusy)
	require.NoError(t, leases.ReleaseLease(ctx, reader.ID))
	claim, err := leases.ClaimKnowledgeGC(ctx, scope, notBefore, time.Minute)
	require.NoError(t, err)

	// Revoking coverage after claim must stop the dormant exact delete before
	// either the physical row or its item receipt can change.
	require.NoError(t, db.Exec(`DELETE FROM nextcloud_content_lease_coverage
		WHERE tenant_id = ? AND knowledge_base_id = ?`,
		scope.TenantID, scope.KnowledgeBaseID).Error)
	require.ErrorIs(t, store.DeleteNextcloudExactChunkForGC(ctx, claim, job.ID, "chunk-old"),
		ErrNextcloudContentGCUncovered)
	var blockedChunk int64
	require.NoError(t, db.Table("chunks").Where("id = ?", "chunk-old").Count(&blockedChunk).Error)
	require.EqualValues(t, 1, blockedChunk)
	var blockedItem nextcloudGCItem
	require.NoError(t, db.Where("job_id = ? AND kind = ? AND object_ref = ?",
		job.ID, "derived_chunk", "chunk-old").Take(&blockedItem).Error)
	require.Equal(t, "blocked", blockedItem.State)
	leaseTestCoverKB(t, db, scope)

	// A forged item cannot authorize another tenant's chunk.
	require.NoError(t, db.Create(&types.Chunk{
		ID: "foreign", TenantID: 8,
		KnowledgeBaseID: "kb", KnowledgeID: "old", Content: "foreign content",
	}).Error)
	require.NoError(t, db.Create(&nextcloudGCItem{
		JobID: job.ID, Kind: "derived_chunk",
		ObjectRef: "foreign", State: "blocked",
	}).Error)
	require.ErrorIs(t, store.DeleteNextcloudExactChunkForGC(ctx, claim, job.ID, "foreign"),
		ErrNextcloudContentLeaseDenied)
	var count int64
	require.NoError(t, db.Table("chunks").Where("id = ?", "foreign").Count(&count).Error)
	require.Equal(t, int64(1), count)

	// Persisted job timing, not a caller-supplied claim time, gates the delete.
	require.NoError(t, db.Model(&nextcloudGCJob{}).Where("id = ?", job.ID).
		Update("not_before", now.Add(time.Hour)).Error)
	require.ErrorIs(t, store.DeleteNextcloudExactChunkForGC(ctx, claim, job.ID, "chunk-old"),
		ErrNextcloudContentGCBusy)
	require.NoError(t, db.Model(&nextcloudGCJob{}).Where("id = ?", job.ID).
		Update("not_before", job.NotBefore).Error)

	// A revision with a mismatched scope cannot be swept under this receipt.
	foreignRevision := uuid.NewString()
	require.NoError(t, db.Create(&types.ChunkRevision{
		ID: foreignRevision, TenantID: 8,
		KnowledgeBaseID: "kb", KnowledgeID: "old", ChunkID: "chunk-old",
		Revision: 1, Content: "foreign revision",
	}).Error)
	require.ErrorIs(t, store.DeleteNextcloudExactChunkForGC(ctx, claim, job.ID, "chunk-old"),
		ErrNextcloudContentLeaseDenied)
	require.NoError(t, db.Where("id = ?", foreignRevision).Delete(&types.ChunkRevision{}).Error)

	// The chunk, revisions and receipt share one transaction.
	require.NoError(t, db.Exec(`CREATE TRIGGER fail_chunk_receipt BEFORE UPDATE OF state ON nextcloud_gc_items
		WHEN NEW.kind = 'derived_chunk' AND NEW.state = 'collected'
		BEGIN SELECT RAISE(ABORT, 'receipt crash'); END`).Error)
	require.ErrorContains(t, store.DeleteNextcloudExactChunkForGC(ctx, claim, job.ID, "chunk-old"),
		"receipt crash")
	require.NoError(t, db.Table("chunks").Where("id = ?", "chunk-old").Count(&count).Error)
	require.Equal(t, int64(1), count)
	require.NoError(t, db.Table("chunk_revisions").Where("chunk_id = ?", "chunk-old").Count(&count).Error)
	require.Equal(t, int64(1), count)
	require.NoError(t, db.Exec(`DROP TRIGGER fail_chunk_receipt`).Error)

	require.NoError(t, store.DeleteNextcloudExactChunkForGC(ctx, claim, job.ID, "chunk-old"))
	require.NoError(t, store.DeleteNextcloudExactChunkForGC(ctx, claim, job.ID, "chunk-old"))
	require.NoError(t, db.Table("chunks").Where("id = ?", "chunk-old").Count(&count).Error)
	require.Zero(t, count)
	require.NoError(t, db.Table("chunk_revisions").Where("chunk_id = ?", "chunk-old").Count(&count).Error)
	require.Zero(t, count)
	var item nextcloudGCItem
	require.NoError(t, db.Where("job_id = ? AND kind = ? AND object_ref = ?",
		job.ID, "derived_chunk", "chunk-old").Take(&item).Error)
	require.Equal(t, "collected", item.State)
	require.Zero(t, item.ConfirmedReleasedBytes)
	require.NoError(t, db.Where("id = ?", job.ID).Take(&job).Error)
	require.Equal(t, "blocked", job.State)
	item = nextcloudGCItem{}
	require.NoError(t, db.Where("job_id = ? AND kind = ?", job.ID, "derived_index").Take(&item).Error)
	require.Equal(t, "blocked", item.State)
	require.NoError(t, leases.FinishGCClaim(ctx, claim, false))
	require.ErrorIs(t, store.DeleteNextcloudExactChunkForGC(ctx, claim, job.ID, "chunk-old"),
		ErrNextcloudContentLeaseDenied)
}

func TestNextcloudExactChunkGCRequiresInventoriedImage(t *testing.T) {
	ctx := context.Background()
	store, db := nextcloudGCTestDB(t)
	require.NoError(t, db.AutoMigrate(&types.ChunkRevision{}))
	script, err := os.ReadFile("../../../migrations/sqlite/000046_nextcloud_content_leases.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	now := time.Now().UTC()
	nextcloudGCTestSource(t, db, "old", "", now.Add(-2*time.Hour))
	require.NoError(t, db.Create(&types.Chunk{
		ID: "image-chunk", TenantID: 7,
		KnowledgeBaseID: "kb", KnowledgeID: "old", ImageInfo: `[{"url":"resource://known"}]`,
	}).Error)
	_, err = store.InventoryRetired(ctx, now, 10)
	require.NoError(t, err)
	var job nextcloudGCJob
	require.NoError(t, db.Take(&job).Error)
	scope := NextcloudContentScope{
		TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "old",
		DataSourceID: "ds", ExternalID: "node",
	}
	leases := NewNextcloudContentLeaseStore(db)
	require.NoError(t, leases.RetireKnowledge(ctx, scope))
	leaseTestCoverKB(t, db, scope)
	claim, err := leases.ClaimKnowledgeGC(ctx, scope, leaseTestNotBefore(t, db, scope), time.Minute)
	require.NoError(t, err)
	require.NoError(t, db.Where("job_id = ? AND kind = ?", job.ID, "extracted_image").
		Delete(&nextcloudGCItem{}).Error)
	require.ErrorIs(t, store.DeleteNextcloudExactChunkForGC(ctx, claim, job.ID, "image-chunk"),
		ErrNextcloudContentLeaseDenied)
	var count int64
	require.NoError(t, db.Table("chunks").Where("id = ?", "image-chunk").Count(&count).Error)
	require.Equal(t, int64(1), count)
	require.NoError(t, leases.FinishGCClaim(ctx, claim, false))
}
