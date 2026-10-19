package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type collectionLeaseHookStore struct {
	*repository.NextcloudContentLeaseStore
	afterBroad func()
	once       sync.Once
}

func (s *collectionLeaseHookStore) AcquireKBRead(
	ctx context.Context, tenant uint64, kb, owner string, ttl time.Duration,
) (repository.NextcloudContentLease, error) {
	lease, err := s.NextcloudContentLeaseStore.AcquireKBRead(ctx, tenant, kb, owner, ttl)
	if err == nil && s.afterBroad != nil {
		s.once.Do(s.afterBroad)
	}
	return lease, err
}

type collectionLeaseFixture struct {
	handler *KnowledgeHandler
	db      *gorm.DB
	store   *repository.NextcloudContentLeaseStore
	row     *types.Knowledge
	scope   repository.NextcloudContentScope
}

func newCollectionLeaseFixture(t *testing.T) *collectionLeaseFixture {
	t.Helper()
	h, db, _, _, _ := collectionNextcloudHTTPStateFixture(t, "positive", nil)
	row := &types.Knowledge{}
	require.NoError(t, db.Where("id = ?", "source").Take(row).Error)
	store := repository.NewNextcloudContentLeaseStore(db)
	// The actual publication fixture holds its builder for its own assertions.
	// End that completed build so only the reader under test can block GC.
	var builders []struct {
		ID string `gorm:"column:lease_id"`
	}
	require.NoError(
		t,
		db.Table("nextcloud_content_leases").
			Where(
				"knowledge_id = ? AND kind = ? AND released_at_ms IS NULL",
				row.ID,
				repository.NextcloudContentBuildLease,
			).
			Find(&builders).
			Error,
	)
	for _, lease := range builders {
		require.NoError(t, store.ReleaseLease(context.Background(), lease.ID))
	}
	now := time.Now().Add(-time.Second).UnixMilli()
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_content_lease_coverage
      (tenant_id, knowledge_base_id, activated_at_ms, legacy_drained_at_ms, reader_revision, builder_revision)
      VALUES (7, 'kb', ?, ?, 'actual-collection-fixture-reader', 'actual-completed-fixture-builder')`, now, now).Error)
	return &collectionLeaseFixture{
		handler: h,
		db:      db,
		store:   store,
		row:     row,
		scope: repository.NextcloudContentScope{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     row.ID,
			DataSourceID:    "ds",
			ExternalID:      "nextcloud:instance:77",
		},
	}
}

func collectionRetiredAt(t *testing.T, f *collectionLeaseFixture) time.Time {
	t.Helper()
	var retired int64
	require.NoError(
		t,
		f.db.Table("nextcloud_content_fences").
			Select("retired_at_ms").
			Where(
				"tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
				f.scope.TenantID,
				f.scope.KnowledgeBaseID,
				f.scope.KnowledgeID,
			).
			Row().
			Scan(&retired),
	)
	return time.UnixMilli(retired)
}

func assertCollectionReadersReleased(t *testing.T, db *gorm.DB, minimum int64) {
	t.Helper()
	var total, active int64
	readers := []repository.NextcloudContentLeaseKind{repository.NextcloudContentReadLease}
	require.NoError(t, db.Table("nextcloud_content_leases").Where("kind IN ?", readers).Count(&total).Error)
	require.NoError(
		t,
		db.Table("nextcloud_content_leases").
			Where("kind IN ? AND released_at_ms IS NULL", readers).
			Count(&active).
			Error,
	)
	require.GreaterOrEqual(t, total, minimum)
	require.Zero(t, active)
}

func collectionRetirementRequest(t *testing.T, url string) *httptest.ResponseRecorder {
	t.Helper()
	f := newCollectionLeaseFixture(t)
	var retireErr, claimErr error
	f.handler.contentLeases = &collectionLeaseHookStore{NextcloudContentLeaseStore: f.store, afterBroad: func() {
		retireErr = f.store.RetireKnowledge(context.Background(), f.scope)
		if retireErr != nil {
			return
		}
		_, claimErr = f.store.ClaimKnowledgeGC(context.Background(), f.scope, collectionRetiredAt(t, f), time.Minute)
	}}
	w := serveCollectionHTTP(f.handler, url, false)
	require.NoError(t, retireErr)
	require.ErrorIs(
		t,
		claimErr,
		repository.ErrNextcloudContentGCBusy,
		"the actual current collection reader must block physical GC",
	)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), f.row.Title)
	assertCollectionReadersReleased(t, f.db, 1)
	claim, err := f.store.ClaimKnowledgeGC(context.Background(), f.scope, collectionRetiredAt(t, f), time.Minute)
	require.NoError(t, err, "the completed builder and exited reader must no longer pin this retired scope")
	require.NoError(t, f.store.FinishGCClaim(context.Background(), claim, false))
	return w
}

// Current list selection holds its real broad fence before capturing originals.
func TestListKnowledgeBroadLeaseBlocksGCBeforeSnapshotCapture(t *testing.T) {
	collectionRetirementRequest(t, "/knowledge-bases/kb/knowledge?keyword=source")
}

// Exact upgrades remain independently covered; the current collection handler
// does not promise the retired legacy list path's exact-lease count.
func TestCollectionExactLeaseUpgradeBlocksGCUntilBothReadersExit(t *testing.T) {
	f := newCollectionLeaseFixture(t)
	broad, err := f.store.AcquireKBRead(context.Background(), 7, "kb", "actual-exact-upgrade", time.Minute)
	require.NoError(t, err)
	exact, err := f.store.UpgradeKBRead(context.Background(), broad.ID, f.scope, "actual-exact-upgrade", time.Minute)
	require.NoError(t, err)
	require.NoError(t, f.store.RetireKnowledge(context.Background(), f.scope))
	_, err = f.store.ClaimKnowledgeGC(context.Background(), f.scope, collectionRetiredAt(t, f), time.Minute)
	require.ErrorIs(t, err, repository.ErrNextcloudContentGCBusy)
	require.NoError(t, f.store.ReleaseLease(context.Background(), broad.ID))
	_, err = f.store.ClaimKnowledgeGC(context.Background(), f.scope, collectionRetiredAt(t, f), time.Minute)
	require.ErrorIs(
		t,
		err,
		repository.ErrNextcloudContentGCBusy,
		"the exact reader must still hold its scope after the broad reader exits",
	)
	require.NoError(t, f.store.ReleaseLease(context.Background(), exact.ID))
	claim, err := f.store.ClaimKnowledgeGC(context.Background(), f.scope, collectionRetiredAt(t, f), time.Minute)
	require.NoError(t, err)
	require.NoError(t, f.store.FinishGCClaim(context.Background(), claim, false))
	assertCollectionReadersReleased(t, f.db, 2)
}

func TestListKnowledgeWriteBoundaryRechecksCurrentShare(t *testing.T) {
	h, db, _, groups, shares, _ := collectionHTTPFixture(t)
	revoked := false
	groups.hook = func(call int) {
		if call >= 5 {
			shares.allowed = false
			revoked = true
		}
	}
	w := serveCollectionHTTP(h, "/knowledge-bases/shared-kb/knowledge", false)
	require.True(t, revoked, "the initial read must succeed before the current share changes")
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "match shared original")
	assertCollectionReadersReleased(t, db, 1)
}

func TestListKnowledgeSourcePageReturnsUnderCurrentCollectionFence(t *testing.T) {
	f := newCollectionLeaseFixture(t)
	w := serveCollectionHTTP(f.handler, "/knowledge-bases/kb/knowledge?keyword=source", false)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), f.row.Title)
	assertCollectionReadersReleased(t, f.db, 1)
}
