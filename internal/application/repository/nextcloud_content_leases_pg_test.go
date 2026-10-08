package repository

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func leaseTestPostgres(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("WEKNORA_LEASE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WEKNORA_LEASE_TEST_POSTGRES_DSN for disposable PostgreSQL")
	}
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	adminSQL, err := admin.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = adminSQL.Close() })
	schema := "content_lease_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	require.NoError(t, admin.Exec(`CREATE SCHEMA "`+schema+`"`).Error)
	t.Cleanup(func() { _ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`).Error })
	scopedDSN := dsn + " search_path=" + schema
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		require.NoError(t, err)
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		scopedDSN = u.String()
	}
	db, err := gorm.Open(postgres.Open(scopedDSN), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = sqlDB.Close() })
	migrationDB, err := gorm.Open(postgres.New(postgres.Config{
		DSN: scopedDSN, PreferSimpleProtocol: true,
	}), &gorm.Config{})
	require.NoError(t, err)
	migrationSQL, err := migrationDB.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = migrationSQL.Close() })
	script, err := os.ReadFile("../../../migrations/versioned/000127_nextcloud_content_leases.up.sql")
	require.NoError(t, err)
	require.NoError(t, migrationDB.Exec(string(script)).Error)
	return db
}

func TestPostgresNextcloudContentLeaseRacesWithRetirementAndGC(t *testing.T) {
	db := leaseTestPostgres(t)
	store := NewNextcloudContentLeaseStore(db)
	ctx := context.Background()
	scope := leaseTestScope("file-1")

	// Acquire wins the KB lock. Retirement cannot commit until that lease is
	// committed, so a subsequent GC claim must observe it.
	tx := db.Begin()
	require.NoError(t, tx.Error)
	lease, err := store.AcquireKnowledgeInTx(ctx, tx, scope,
		NextcloudContentReadLease, "reader-1", time.Minute)
	require.NoError(t, err)
	started := make(chan struct{})
	retired := make(chan error, 1)
	go func() {
		close(started)
		retired <- store.RetireKnowledge(ctx, scope)
	}()
	<-started
	select {
	case err := <-retired:
		t.Fatalf("retirement passed the uncommitted acquisition fence: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, tx.Commit().Error)
	select {
	case err := <-retired:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("retirement did not resume after acquisition commit")
	}
	leaseTestCoverKB(t, db, scope)
	_, err = store.ClaimKnowledgeGC(ctx, scope, leaseTestNotBefore(t, db, scope), time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentGCBusy)
	require.NoError(t, store.ReleaseLease(ctx, lease.ID))
	claim, err := store.ClaimKnowledgeGC(ctx, scope, leaseTestNotBefore(t, db, scope), time.Minute)
	require.NoError(t, err)
	_, err = store.AcquireKnowledge(ctx, scope, NextcloudContentReadLease,
		"after-claim", time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentLeaseDenied)
	require.NoError(t, store.FinishGCClaim(ctx, claim, true))

	// Retirement wins on a different generation. Acquisition cannot create a
	// new lease after that transaction commits.
	other := leaseTestScope("file-2")
	tx2 := db.Begin()
	require.NoError(t, tx2.Error)
	require.NoError(t, store.RetireKnowledgeInTx(ctx, tx2, other))
	acquired := make(chan error, 1)
	started2 := make(chan struct{})
	go func() {
		close(started2)
		_, err := store.AcquireKnowledge(ctx, other, NextcloudContentBuildLease,
			"late-worker", time.Minute)
		acquired <- err
	}()
	<-started2
	select {
	case err := <-acquired:
		t.Fatalf("acquisition passed the uncommitted retirement fence: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, tx2.Commit().Error)
	select {
	case err := <-acquired:
		require.ErrorIs(t, err, ErrNextcloudContentLeaseDenied)
	case <-time.After(5 * time.Second):
		t.Fatal("acquisition did not resume after retirement commit")
	}
	var active int64
	require.NoError(t, db.Table("nextcloud_content_leases").Where(
		"tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
		other.TenantID, other.KnowledgeBaseID, other.KnowledgeID).Count(&active).Error)
	require.Zero(t, active)
}

func TestPostgresNextcloudContentGCClaimBlocksKBRead(t *testing.T) {
	db := leaseTestPostgres(t)
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
	require.NoError(t, store.FinishGCClaim(ctx, claim, false))
	broad, err := store.AcquireKBRead(ctx, scope.TenantID, scope.KnowledgeBaseID,
		"search-after-release", time.Minute)
	require.NoError(t, err)
	require.NoError(t, store.ReleaseLease(ctx, broad.ID))

	claim, err = store.ClaimKnowledgeGC(ctx, scope, notBefore, time.Minute)
	require.NoError(t, err)
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
