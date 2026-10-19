package repository

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func leaseTestSQLite(t *testing.T) *gorm.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "leases.db")
	db, err := gorm.Open(sqlite.Open("file:"+path+"?_busy_timeout=5000&_journal_mode=WAL"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = sqlDB.Close() })
	script, err := os.ReadFile("../../../migrations/sqlite/000046_nextcloud_content_leases.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	return db
}

func leaseTestScope(id string) NextcloudContentScope {
	return NextcloudContentScope{
		TenantID: 7, KnowledgeBaseID: "kb-1", KnowledgeID: id,
		DataSourceID: "source-1", ExternalID: "nextcloud:instance:" + id,
	}
}

func leaseTestCoverKB(t *testing.T, db *gorm.DB, scope NextcloudContentScope) {
	t.Helper()
	nowMS, err := nextcloudLeaseNowMS(db)
	require.NoError(t, err)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_content_lease_coverage
		(tenant_id, knowledge_base_id, activated_at_ms, legacy_drained_at_ms,
		 reader_revision, builder_revision) VALUES (?, ?, ?, ?, 'test-read-v1', 'test-build-v1')`,
		scope.TenantID, scope.KnowledgeBaseID, nowMS-1000, nowMS).Error)
}

func leaseTestNotBefore(t *testing.T, db *gorm.DB, scope NextcloudContentScope) time.Time {
	t.Helper()
	var fence nextcloudContentFence
	require.NoError(t, db.Where("tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
		scope.TenantID, scope.KnowledgeBaseID, scope.KnowledgeID).Take(&fence).Error)
	require.NotNil(t, fence.RetiredAtMS)
	return time.UnixMilli(*fence.RetiredAtMS).UTC()
}

func TestNextcloudContentLeaseSQLiteSearchUpgradeRetirementAndGC(t *testing.T) {
	db := leaseTestSQLite(t)
	store := NewNextcloudContentLeaseStore(db)
	ctx := context.Background()
	scope := leaseTestScope("file-1")
	other := leaseTestScope("file-2")
	broad, err := store.AcquireKBRead(ctx, scope.TenantID, scope.KnowledgeBaseID,
		"search-1", time.Minute)
	require.NoError(t, err)
	require.Empty(t, broad.KnowledgeID)
	exact, err := store.UpgradeKBRead(ctx, broad.ID, scope, "search-1", time.Minute)
	require.NoError(t, err)
	require.Equal(t, NextcloudContentReadLease, exact.Kind)
	_, err = store.UpgradeKBRead(ctx, broad.ID, scope, "different-owner", time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentLeaseExpired)
	_, err = store.AcquireKnowledge(ctx, NextcloudContentScope{
		TenantID: scope.TenantID, KnowledgeBaseID: scope.KnowledgeBaseID,
		KnowledgeID: scope.KnowledgeID, DataSourceID: "wrong-source", ExternalID: scope.ExternalID,
	}, NextcloudContentReadLease, "wrong", time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentLeaseDenied)

	require.NoError(t, store.RetireKnowledge(ctx, scope))
	require.NoError(t, store.RetireKnowledge(ctx, scope)) // stable tombstone
	_, err = store.AcquireKnowledge(ctx, scope, NextcloudContentReadLease, "late", time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentLeaseDenied)
	_, err = store.UpgradeKBRead(ctx, broad.ID, scope, "search-1", time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentLeaseDenied)
	_, err = store.RenewLease(ctx, exact.ID, time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentLeaseDenied)
	// A different generation in the same KB may still be hydrated.
	otherLease, err := store.UpgradeKBRead(ctx, broad.ID, other, "search-1", time.Minute)
	require.NoError(t, err)
	require.NoError(t, store.ReleaseLease(ctx, otherLease.ID))
	_, err = store.ClaimKnowledgeGC(ctx, scope, leaseTestNotBefore(t, db, scope), time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentGCUncovered)
	leaseTestCoverKB(t, db, scope)
	_, err = store.ClaimKnowledgeGC(ctx, scope, leaseTestNotBefore(t, db, scope), time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentGCBusy)
	require.NoError(t, store.ReleaseLease(ctx, exact.ID))
	_, err = store.ClaimKnowledgeGC(ctx, scope, leaseTestNotBefore(t, db, scope), time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentGCBusy) // broad lease still held
	require.NoError(t, store.ReleaseLease(ctx, broad.ID))
	claim, err := store.ClaimKnowledgeGC(ctx, scope, leaseTestNotBefore(t, db, scope), time.Minute)
	require.NoError(t, err)
	require.NotEmpty(t, claim.Token)
	renewed, err := store.RenewGCClaim(ctx, claim, time.Minute)
	require.NoError(t, err)
	require.Equal(t, claim.Token, renewed.Token)
	claim = renewed
	_, err = store.ClaimKnowledgeGC(ctx, scope, leaseTestNotBefore(t, db, scope), time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentGCBusy)
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		return store.ValidateGCClaimInTx(ctx, tx, claim)
	}))
	require.NoError(t, store.FinishGCClaim(ctx, claim, false))
	claim2, err := store.ClaimKnowledgeGC(ctx, scope, leaseTestNotBefore(t, db, scope), time.Minute)
	require.NoError(t, err)
	require.NotEqual(t, claim.Token, claim2.Token)
	// A crashed claimant loses its token after expiry; a newer claimant may
	// take over without allowing a stale receipt.
	require.NoError(t, db.Exec(`UPDATE nextcloud_content_fences
		SET claim_until_ms = 1 WHERE tenant_id = ? AND knowledge_base_id = ?
		AND knowledge_id = ?`, scope.TenantID, scope.KnowledgeBaseID,
		scope.KnowledgeID).Error)
	require.ErrorIs(t, store.FinishGCClaim(ctx, claim2, true), ErrNextcloudContentLeaseDenied)
	claim3, err := store.ClaimKnowledgeGC(ctx, scope, leaseTestNotBefore(t, db, scope), time.Minute)
	require.NoError(t, err)
	require.NotEqual(t, claim2.Token, claim3.Token)
	require.ErrorIs(t, db.Transaction(func(tx *gorm.DB) error {
		return store.ValidateGCClaimInTx(ctx, tx, claim)
	}), ErrNextcloudContentLeaseDenied)
	require.NoError(t, store.FinishGCClaim(ctx, claim3, true))
	_, err = store.ClaimKnowledgeGC(ctx, scope, leaseTestNotBefore(t, db, scope), time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentLeaseDenied)
}

func TestNextcloudContentLeaseSQLiteBuildFenceAndExpiry(t *testing.T) {
	db := leaseTestSQLite(t)
	store := NewNextcloudContentLeaseStore(db)
	ctx := context.Background()
	scope := leaseTestScope("build-1")
	build, err := store.AcquireKnowledge(ctx, scope, NextcloudContentBuildLease,
		"worker-1", time.Minute)
	require.NoError(t, err)
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		return store.ValidateBuildLeaseInTx(ctx, tx, build, scope)
	}))
	require.NoError(t, store.RetireKnowledge(ctx, scope))
	require.ErrorIs(t, db.Transaction(func(tx *gorm.DB) error {
		return store.ValidateBuildLeaseInTx(ctx, tx, build, scope)
	}), ErrNextcloudContentLeaseDenied)
	leaseTestCoverKB(t, db, scope)
	_, err = store.ClaimKnowledgeGC(ctx, scope, leaseTestNotBefore(t, db, scope), time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentGCBusy)
	// Simulate a crashed worker whose persisted lease expires. Its stale token
	// cannot write, and the GC admission gate eventually opens.
	require.NoError(t, db.Exec(`UPDATE nextcloud_content_leases
		SET expires_at_ms = 1 WHERE lease_id = ?`, build.ID).Error)
	_, err = store.RenewLease(ctx, build.ID, time.Minute)
	require.True(t, errors.Is(err, ErrNextcloudContentLeaseDenied) ||
		errors.Is(err, ErrNextcloudContentLeaseExpired))
	claim, err := store.ClaimKnowledgeGC(ctx, scope, leaseTestNotBefore(t, db, scope), time.Minute)
	require.NoError(t, err)
	require.ErrorIs(t, db.Transaction(func(tx *gorm.DB) error {
		return store.ValidateBuildLeaseInTx(ctx, tx, build, scope)
	}), ErrNextcloudContentLeaseDenied)
	require.NoError(t, store.FinishGCClaim(ctx, claim, true))
}

func TestNextcloudContentLeaseSQLiteGCClaimBlocksKBRead(t *testing.T) {
	db := leaseTestSQLite(t)
	store := NewNextcloudContentLeaseStore(db)
	ctx := context.Background()
	scope := leaseTestScope("retired-file")
	require.NoError(t, store.RetireKnowledge(ctx, scope))
	leaseTestCoverKB(t, db, scope)
	notBefore := leaseTestNotBefore(t, db, scope)
	_, err := store.ClaimKnowledgeGC(ctx, scope, notBefore.Add(-time.Millisecond), time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentLeaseInvalid)
	claim, err := store.ClaimKnowledgeGC(ctx, scope, notBefore, time.Minute)
	require.NoError(t, err)
	_, err = store.AcquireKBRead(ctx, scope.TenantID, scope.KnowledgeBaseID,
		"search-during-gc", time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentLeaseDenied)
	// Releasing the claim leaves a retired exact fence but permits searches
	// over other currently published knowledge in this KB.
	require.NoError(t, store.FinishGCClaim(ctx, claim, false))
	broad, err := store.AcquireKBRead(ctx, scope.TenantID, scope.KnowledgeBaseID,
		"search-after-release", time.Minute)
	require.NoError(t, err)
	require.NoError(t, store.ReleaseLease(ctx, broad.ID))

	claim, err = store.ClaimKnowledgeGC(ctx, scope, notBefore, time.Minute)
	require.NoError(t, err)
	// Expiry does not strand the entire KB. The stale claimant can no longer
	// validate its token, and a new claim waits for the broad reader.
	require.NoError(t, db.Exec(`UPDATE nextcloud_content_fences
		SET claim_until_ms = 1 WHERE tenant_id = ? AND knowledge_base_id = ?
		AND knowledge_id = ?`, scope.TenantID, scope.KnowledgeBaseID,
		scope.KnowledgeID).Error)
	broad, err = store.AcquireKBRead(ctx, scope.TenantID, scope.KnowledgeBaseID,
		"search-after-expiry", time.Minute)
	require.NoError(t, err)
	require.ErrorIs(t, db.Transaction(func(tx *gorm.DB) error {
		return store.ValidateGCClaimInTx(ctx, tx, claim)
	}), ErrNextcloudContentLeaseDenied)
	_, err = store.ClaimKnowledgeGC(ctx, scope, notBefore, time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentGCBusy)
	require.NoError(t, store.ReleaseLease(ctx, broad.ID))
	fresh, err := store.ClaimKnowledgeGC(ctx, scope, notBefore, time.Minute)
	require.NoError(t, err)
	require.NoError(t, store.FinishGCClaim(ctx, fresh, true))
}

// A valid token is insufficient at the row-delete boundary if coverage was
// revoked or an active read/build lease appeared after claim admission.
func TestNextcloudContentLeaseSQLiteGCFinalCheckRechecksCoverageAndLeases(t *testing.T) {
	db := leaseTestSQLite(t)
	store := NewNextcloudContentLeaseStore(db)
	ctx := context.Background()
	scope := leaseTestScope("final-check")
	require.NoError(t, store.RetireKnowledge(ctx, scope))
	leaseTestCoverKB(t, db, scope)
	claim, err := store.ClaimKnowledgeGC(ctx, scope, leaseTestNotBefore(t, db, scope), time.Minute)
	require.NoError(t, err)
	validate := func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			return store.ValidateGCClaimInTx(ctx, tx, claim)
		})
	}
	require.NoError(t, validate())

	require.NoError(t, db.Exec(`DELETE FROM nextcloud_content_lease_coverage
		WHERE tenant_id = ? AND knowledge_base_id = ?`,
		scope.TenantID, scope.KnowledgeBaseID).Error)
	require.ErrorIs(t, validate(), ErrNextcloudContentGCUncovered)
	leaseTestCoverKB(t, db, scope)

	nowMS, err := nextcloudLeaseNowMS(db)
	require.NoError(t, err)
	addLateLease := func(knowledgeID string, kind NextcloudContentLeaseKind) NextcloudContentLease {
		t.Helper()
		lease, insertErr := nextcloudInsertLease(db, scope.TenantID, scope.KnowledgeBaseID,
			knowledgeID, kind, 1, "late-worker", nowMS, time.Minute.Milliseconds())
		require.NoError(t, insertErr)
		return lease
	}
	for _, candidate := range []struct {
		knowledgeID string
		kind        NextcloudContentLeaseKind
	}{
		{scope.KnowledgeID, NextcloudContentBuildLease},
		{"", NextcloudContentReadLease},
	} {
		late := addLateLease(candidate.knowledgeID, candidate.kind)
		require.ErrorIs(t, validate(), ErrNextcloudContentGCBusy)
		require.NoError(t, store.ReleaseLease(ctx, late.ID))
	}
	require.NoError(t, validate())
	require.NoError(t, store.FinishGCClaim(ctx, claim, false))
}

func TestNextcloudContentLeaseSQLiteKBFenceBlocksNewWork(t *testing.T) {
	db := leaseTestSQLite(t)
	store := NewNextcloudContentLeaseStore(db)
	ctx := context.Background()
	scope := leaseTestScope("file-1")
	lease, err := store.AcquireKnowledge(ctx, scope, NextcloudContentBuildLease,
		"worker", time.Minute)
	require.NoError(t, err)
	require.NoError(t, store.RetireKB(ctx, scope.TenantID, scope.KnowledgeBaseID))
	require.NoError(t, store.RetireKB(ctx, scope.TenantID, scope.KnowledgeBaseID))
	_, err = store.AcquireKBRead(ctx, scope.TenantID, scope.KnowledgeBaseID,
		"search", time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentLeaseDenied)
	_, err = store.AcquireKnowledge(ctx, leaseTestScope("another"),
		NextcloudContentBuildLease, "late-worker", time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentLeaseDenied)
	_, err = store.RenewLease(ctx, lease.ID, time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentLeaseDenied)
}

func TestNextcloudContentLeaseSQLiteConcurrentRetirement(t *testing.T) {
	db := leaseTestSQLite(t)
	store := NewNextcloudContentLeaseStore(db)
	ctx := context.Background()
	scope := leaseTestScope("raced")
	tx := db.Begin()
	require.NoError(t, tx.Error)
	require.NoError(t, store.RetireKnowledgeInTx(ctx, tx, scope))
	start := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(start)
		_, err := store.AcquireKnowledge(ctx, scope, NextcloudContentBuildLease,
			"late-worker", time.Minute)
		result <- err
	}()
	<-start
	// SQLite may wait for the writer or return SQLITE_BUSY. Neither outcome
	// may produce a live lease before the retirement commit.
	received := false
	select {
	case err := <-result:
		require.Error(t, err)
		received = true
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, tx.Commit().Error)
	if !received {
		select {
		case err := <-result:
			require.Error(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent acquisition did not finish")
		}
	}
	_, err := store.AcquireKnowledge(ctx, scope, NextcloudContentReadLease,
		"late-reader", time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentLeaseDenied)
	var count int64
	require.NoError(t, db.Table("nextcloud_content_leases").Where(
		"tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
		scope.TenantID, scope.KnowledgeBaseID, scope.KnowledgeID).Count(&count).Error)
	require.Zero(t, count)
}

func TestNextcloudContentLeaseSQLitePruneFinishedInBoundedBatches(t *testing.T) {
	db := leaseTestSQLite(t)
	store := NewNextcloudContentLeaseStore(db)
	ctx := context.Background()
	scope := leaseTestScope("prune-file")
	acquire := func(owner string) NextcloudContentLease {
		t.Helper()
		lease, err := store.AcquireKnowledge(ctx, scope, NextcloudContentReadLease,
			owner, time.Minute)
		require.NoError(t, err)
		return lease
	}
	oldReleasedA := acquire("old-released-a")
	oldReleasedB := acquire("old-released-b")
	oldExpired := acquire("old-expired")
	recentReleased := acquire("recent-released")
	recentExpired := acquire("recent-expired")
	active := acquire("active")

	nowMS, err := nextcloudLeaseNowMS(db)
	require.NoError(t, err)
	for _, lease := range []NextcloudContentLease{oldReleasedA, oldReleasedB} {
		require.NoError(t, store.ReleaseLease(ctx, lease.ID))
		require.NoError(t, db.Exec(`UPDATE nextcloud_content_leases
			SET released_at_ms = ? WHERE lease_id = ?`,
			nowMS-int64((2*time.Hour).Milliseconds()), lease.ID).Error)
	}
	require.NoError(t, store.ReleaseLease(ctx, recentReleased.ID))
	require.NoError(t, db.Exec(`UPDATE nextcloud_content_leases
		SET expires_at_ms = ? WHERE lease_id = ?`,
		nowMS-int64((2*time.Hour).Milliseconds()), oldExpired.ID).Error)
	require.NoError(t, db.Exec(`UPDATE nextcloud_content_leases
		SET expires_at_ms = ? WHERE lease_id = ?`,
		nowMS-int64((30*time.Minute).Milliseconds()), recentExpired.ID).Error)

	_, err = store.PruneFinishedLeases(ctx, 0, 1)
	require.ErrorIs(t, err, ErrNextcloudContentLeaseInvalid)
	_, err = store.PruneFinishedLeases(ctx, time.Hour, 0)
	require.ErrorIs(t, err, ErrNextcloudContentLeaseInvalid)

	pruned, err := store.PruneFinishedLeases(ctx, time.Hour, 2)
	require.NoError(t, err)
	require.EqualValues(t, 2, pruned)
	pruned, err = store.PruneFinishedLeases(ctx, time.Hour, 2)
	require.NoError(t, err)
	require.EqualValues(t, 1, pruned)
	pruned, err = store.PruneFinishedLeases(ctx, time.Hour, 2)
	require.NoError(t, err)
	require.Zero(t, pruned)

	var remaining []string
	require.NoError(t, db.Table("nextcloud_content_leases").
		Pluck("lease_id", &remaining).Error)
	require.ElementsMatch(t, []string{recentReleased.ID, recentExpired.ID, active.ID}, remaining)
	_, err = store.RenewLease(ctx, active.ID, time.Minute)
	require.NoError(t, err)
}
