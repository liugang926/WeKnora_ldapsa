package readlease

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func nextcloudReadTestStore(t *testing.T) (*repository.NextcloudContentLeaseStore, *gorm.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "read-leases.db")
	db, err := gorm.Open(sqlite.Open("file:"+path+"?_busy_timeout=5000&_journal_mode=WAL"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = sqlDB.Close() })
	script, err := os.ReadFile("../../../migrations/sqlite/000046_nextcloud_content_leases.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	return repository.NewNextcloudContentLeaseStore(db), db
}

func nextcloudReadTestKnowledge(t *testing.T) *types.Knowledge {
	t.Helper()
	metadata, err := json.Marshal(map[string]string{
		"datasource_id": "ds-1", "external_id": "nextcloud:instance:41",
		"nextcloud_file_id": "41",
	})
	require.NoError(t, err)
	return &types.Knowledge{
		ID: "doc-1", TenantID: 7, KnowledgeBaseID: "kb-1",
		Channel: types.ConnectorTypeNextcloud, Metadata: types.JSON(metadata),
	}
}

func TestNextcloudKBReadRetirementBetweenSearchAndHydration(t *testing.T) {
	store, db := nextcloudReadTestStore(t)
	guard, err := BeginNextcloudKBRead(context.Background(), store,
		[]NextcloudKBReadScope{{TenantID: 7, KBID: "kb-1"}})
	require.NoError(t, err)
	defer func() { require.NoError(t, guard.Close()) }()
	knowledge := nextcloudReadTestKnowledge(t)
	scope, marked, err := NextcloudKnowledgeLeaseScope(knowledge)
	require.NoError(t, err)
	require.True(t, marked)
	require.NoError(t, store.RetireKnowledge(context.Background(), scope))
	require.ErrorIs(t, guard.PinKnowledge(knowledge), repository.ErrNextcloudContentLeaseDenied)
	require.Error(t, guard.Verify())
	require.Error(t, guard.Context().Err())
	require.NoError(t, guard.Close())
	var active int64
	require.NoError(t, db.Table("nextcloud_content_leases").
		Where("released_at_ms IS NULL").Count(&active).Error)
	require.Zero(t, active)
}

func TestNextcloudKBReadExactLeaseStopsOutputAfterRetirement(t *testing.T) {
	store, db := nextcloudReadTestStore(t)
	guard, err := BeginNextcloudKBRead(context.Background(), store,
		[]NextcloudKBReadScope{{TenantID: 7, KBID: "kb-1"}})
	require.NoError(t, err)
	knowledge := nextcloudReadTestKnowledge(t)
	require.NoError(t, guard.PinKnowledge(knowledge))
	require.NoError(t, guard.Verify())
	scope, _, err := NextcloudKnowledgeLeaseScope(knowledge)
	require.NoError(t, err)
	require.NoError(t, store.RetireKnowledge(context.Background(), scope))
	require.ErrorIs(t, guard.Verify(), repository.ErrNextcloudContentLeaseDenied)
	require.NoError(t, guard.Close())
	var count, active int64
	require.NoError(t, db.Table("nextcloud_content_leases").Count(&count).Error)
	require.EqualValues(t, 2, count)
	require.NoError(t, db.Table("nextcloud_content_leases").
		Where("released_at_ms IS NULL").Count(&active).Error)
	require.Zero(t, active)
}

func TestNextcloudKBReadCancelledAndConcurrentClose(t *testing.T) {
	store, _ := nextcloudReadTestStore(t)
	base, cancel := context.WithCancel(context.Background())
	guard, err := BeginNextcloudKBRead(base, store,
		[]NextcloudKBReadScope{{TenantID: 7, KBID: "kb-1"}})
	require.NoError(t, err)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_ = guard.Verify()
			}
		}()
	}
	cancel()
	require.ErrorIs(t, guard.Verify(), context.Canceled)
	require.NoError(t, guard.Close())
	wg.Wait()
}

type nextcloudReadFailedRenewStore struct {
	NextcloudReadLeaseStore
	beforeReturn func()
}

func (s *nextcloudReadFailedRenewStore) RenewLease(
	_ context.Context, _ string, _ time.Duration,
) (repository.NextcloudContentLease, error) {
	if s.beforeReturn != nil {
		s.beforeReturn()
	}
	return repository.NextcloudContentLease{}, sql.ErrTxDone
}

func TestNextcloudKBReadCancelDuringFailedRenew(t *testing.T) {
	t.Run("external cancellation wins", func(t *testing.T) {
		store, _ := nextcloudReadTestStore(t)
		base, cancel := context.WithCancel(context.Background())
		fault := &nextcloudReadFailedRenewStore{
			NextcloudReadLeaseStore: store,
			beforeReturn:            cancel,
		}
		guard, err := BeginNextcloudKBRead(base, fault,
			[]NextcloudKBReadScope{{TenantID: 7, KBID: "kb-1"}})
		require.NoError(t, err)
		require.ErrorIs(t, guard.Verify(), context.Canceled)
		require.ErrorIs(t, guard.Verify(), context.Canceled)
		require.NoError(t, guard.Close())
	})
	t.Run("internal lease failure stays concrete", func(t *testing.T) {
		store, _ := nextcloudReadTestStore(t)
		fault := &nextcloudReadFailedRenewStore{NextcloudReadLeaseStore: store}
		guard, err := BeginNextcloudKBRead(context.Background(), fault,
			[]NextcloudKBReadScope{{TenantID: 7, KBID: "kb-1"}})
		require.NoError(t, err)
		require.ErrorIs(t, guard.Verify(), sql.ErrTxDone)
		require.ErrorIs(t, guard.Verify(), sql.ErrTxDone)
		require.NoError(t, guard.Close())
	})
}

func TestNextcloudKnowledgeLeaseScopeRejectsMalformedMarker(t *testing.T) {
	k := nextcloudReadTestKnowledge(t)
	k.Metadata = types.JSON(`{"nextcloud_file_id":"41"}`)
	_, marked, err := NextcloudKnowledgeLeaseScope(k)
	require.True(t, marked)
	require.Error(t, err)
	ordinary := &types.Knowledge{ID: "local", TenantID: 7, KnowledgeBaseID: "kb-1"}
	_, marked, err = NextcloudKnowledgeLeaseScope(ordinary)
	require.NoError(t, err)
	require.False(t, marked)
	require.Error(t, (*NextcloudReadGuard)(nil).PinKnowledge(k))
	require.NoError(t, (*NextcloudReadGuard)(nil).PinKnowledge(ordinary))
}
