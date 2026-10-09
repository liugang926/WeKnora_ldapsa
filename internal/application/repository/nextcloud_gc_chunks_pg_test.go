package repository

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func nextcloudExactChunkPGDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("WEKNORA_GC_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WEKNORA_GC_TEST_POSTGRES_DSN for a disposable PostgreSQL or ParadeDB")
	}
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	adminSQL, err := admin.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = adminSQL.Close() })
	schema := "gc_chunk_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	require.NoError(t, admin.Exec(`CREATE SCHEMA "`+schema+`"`).Error)
	t.Cleanup(func() { _ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`).Error })
	scopedDSN := dsn + " search_path=" + schema + ",public"
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		require.NoError(t, err)
		query := u.Query()
		query.Set("search_path", schema+",public")
		u.RawQuery = query.Encode()
		scopedDSN = u.String()
	}
	db, err := gorm.Open(postgres.Open(scopedDSN), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.Chunk{},
		&types.ChunkRevision{}, &types.ResourceBinding{}, &nextcloudSourceVersion{},
		&nextcloudGCJob{}, &nextcloudGCItem{}))
	script, err := os.ReadFile("../../../migrations/versioned/000127_nextcloud_content_leases.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	return db
}

func TestPostgresNextcloudExactChunkGCClaimScopeTimingAndAtomicReceipt(t *testing.T) {
	ctx := context.Background()
	db := nextcloudExactChunkPGDB(t)
	store := NewNextcloudGCStore(db)
	now := time.Now().UTC()
	nextcloudGCTestSource(t, db, "old", "", now.Add(-2*time.Hour))
	require.NoError(t, db.Create(&types.Chunk{
		ID: "chunk-old", TenantID: 7,
		KnowledgeBaseID: "kb", KnowledgeID: "old", Content: "private body",
	}).Error)
	require.NoError(t, db.Create(&types.ChunkRevision{
		ID: uuid.NewString(), TenantID: 7,
		KnowledgeBaseID: "kb", KnowledgeID: "old", ChunkID: "chunk-old",
		Content: "previous private body",
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
	reader, err := leases.AcquireKnowledge(ctx, scope, NextcloudContentReadLease, "pg-reader", time.Minute)
	require.NoError(t, err)
	require.NoError(t, leases.RetireKnowledge(ctx, scope))
	notBefore := leaseTestNotBefore(t, db, scope)
	_, err = leases.ClaimKnowledgeGC(ctx, scope, notBefore, time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentGCUncovered)
	leaseTestCoverKB(t, db, scope) // Only this disposable schema gets a marker.
	_, err = leases.ClaimKnowledgeGC(ctx, scope, notBefore, time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentGCBusy)
	require.NoError(t, leases.ReleaseLease(ctx, reader.ID))
	claim, err := leases.ClaimKnowledgeGC(ctx, scope, notBefore, time.Minute)
	require.NoError(t, err)

	// Claim scope and the persisted item must both match the exact row.
	require.NoError(t, db.Create(&types.Chunk{
		ID: "foreign", TenantID: 8,
		KnowledgeBaseID: "kb", KnowledgeID: "old", Content: "another tenant",
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

	otherJobID := uuid.NewString()
	require.NoError(t, db.Create(&nextcloudGCJob{
		ID: otherJobID, TenantID: 8,
		KnowledgeBaseID: "kb", DataSourceID: "ds", ExternalID: "node",
		KnowledgeID: "other", Reason: "retired", State: "blocked",
		NotBefore: now.Add(-time.Hour), OriginalNotBefore: now,
		NextAttemptAt: now,
	}).Error)
	require.NoError(t, db.Create(&nextcloudGCItem{
		JobID: otherJobID, Kind: "derived_chunk",
		ObjectRef: "chunk-old", State: "blocked",
	}).Error)
	require.ErrorIs(t, store.DeleteNextcloudExactChunkForGC(ctx, claim, otherJobID, "chunk-old"),
		ErrNextcloudContentLeaseDenied)

	require.NoError(t, db.Model(&nextcloudGCJob{}).Where("id = ?", job.ID).
		Update("not_before", now.Add(time.Hour)).Error)
	require.ErrorIs(t, store.DeleteNextcloudExactChunkForGC(ctx, claim, job.ID, "chunk-old"),
		ErrNextcloudContentGCBusy)
	require.NoError(t, db.Model(&nextcloudGCJob{}).Where("id = ?", job.ID).
		Update("not_before", job.NotBefore).Error)

	// A database receipt failure must roll back both content tables.
	require.NoError(t, db.Exec(`CREATE FUNCTION fail_chunk_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.kind = 'derived_chunk' AND NEW.state = 'collected' THEN
			RAISE EXCEPTION 'receipt crash'; END IF;
		RETURN NEW; END $$;
		CREATE TRIGGER fail_chunk_receipt BEFORE UPDATE ON nextcloud_gc_items
		FOR EACH ROW EXECUTE FUNCTION fail_chunk_receipt()`).Error)
	require.ErrorContains(t, store.DeleteNextcloudExactChunkForGC(ctx, claim, job.ID, "chunk-old"),
		"receipt crash")
	require.NoError(t, db.Table("chunks").Where("id = ?", "chunk-old").Count(&count).Error)
	require.Equal(t, int64(1), count)
	require.NoError(t, db.Table("chunk_revisions").Where("chunk_id = ?", "chunk-old").Count(&count).Error)
	require.Equal(t, int64(1), count)
	require.NoError(t, db.Exec(`DROP TRIGGER fail_chunk_receipt ON nextcloud_gc_items`).Error)

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
