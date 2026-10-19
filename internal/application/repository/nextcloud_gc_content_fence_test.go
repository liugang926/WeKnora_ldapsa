package repository

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	filesvc "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// Existing local-resource contract fixtures have a complete, synthetic reader
// and builder inventory. Production migrations and code insert no coverage.
func gcObjectTestLeaseSchema(t *testing.T, db *gorm.DB) {
	t.Helper()
	if !db.Migrator().HasTable("knowledge_bases") {
		require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}))
		require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7}).Error)
	}
	if !db.Migrator().HasTable("nextcloud_content_fences") {
		path := "sqlite/000046"
		if db.Name() == "postgres" {
			path = "versioned/000127"
		}
		b, e := os.ReadFile("../../../migrations/" + path + "_nextcloud_content_leases.up.sql")
		require.NoError(t, e)
		require.NoError(t, db.Exec(string(b)).Error)
	}
}

func gcActualObjectFixture(t *testing.T, pg bool) (*NextcloudGCStore, *gorm.DB, string, string, string) {
	t.Helper()
	var store *NextcloudGCStore
	var db *gorm.DB
	if pg {
		db = leaseTestPostgres(t)
		require.NoError(
			t,
			db.AutoMigrate(
				&types.KnowledgeBase{},
				&types.Knowledge{},
				&types.Chunk{},
				&nextcloudSourceVersion{},
				&nextcloudGCJob{},
				&nextcloudGCItem{},
				&types.StoredResource{},
				&types.ResourceBinding{},
				&NextcloudIndexedWithdrawal{},
			),
		)
		require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7}).Error)
		store = NewNextcloudGCStore(db)
	} else {
		store, db = nextcloudGCTestDB(t)
	}
	ref := nextcloudGCTestLocalResource(t, db, strings.Repeat("z", 22), "old")
	now := time.Now().UTC()
	nextcloudGCTestSource(t, db, "old", ref, now.Add(-8*24*time.Hour))
	_, e := store.InventoryRetired(context.Background(), now, 10)
	require.NoError(t, e)
	var job nextcloudGCJob
	require.NoError(t, db.Take(&job).Error)
	base := t.TempDir()
	path := filepath.Join(base, "7", "old", "source.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, []byte("persisted-source-object"), 0o600))
	provider := filesvc.NewBackendScopedFileService("backend", filesvc.NewLocalFileService(base, ""))
	deleter := provider.(interfaces.NextcloudPhysicalGCDeleter)
	store.ConfigureLocalDeletePreparation(
		func(context.Context, *types.StoredResource) (func(context.Context) (bool, error), error) {
			return func(ctx context.Context) (bool, error) {
				return deleter.DeleteNextcloudObject(ctx, "storage://backend/local://7/old/source.txt")
			}, nil
		},
	)
	return store, db, job.ID, ref, path
}

func gcActualRunDueContract(t *testing.T, pg bool) {
	for _, mode := range []string{
		"coverage-missing",
		"broad-reader",
		"exact-reader",
		"build-worker",
		"late-reader",
		"provider-failure",
		"crash-after-unlink",
		"restore-before-provider",
		"symlink",
		"shared",
		"lost-claim",
	} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			store, db, jobID, ref, path := gcActualObjectFixture(t, pg)
			scope := NextcloudContentScope{
				TenantID:        7,
				KnowledgeBaseID: "kb",
				KnowledgeID:     "old",
				DataSourceID:    "ds",
				ExternalID:      "node",
			}
			leases := NewNextcloudContentLeaseStore(db)
			var held NextcloudContentLease
			var err error
			calls := 0
			if mode == "coverage-missing" {
				require.NoError(
					t,
					db.Exec(
						"DELETE FROM nextcloud_content_lease_coverage WHERE tenant_id=7 AND knowledge_base_id='kb'",
					).Error,
				)
			}
			if mode == "broad-reader" {
				held, err = leases.AcquireKBRead(ctx, 7, "kb", "live-reader", time.Minute)
				require.NoError(t, err)
			}
			if mode == "exact-reader" || mode == "build-worker" {
				require.NoError(
					t,
					db.Exec(
						("UPDATE nextcloud_content_fences SET state='open' WHERE tenant_id=7 AND kno" +
							"wledge_base_id='kb' AND knowledge_id='old'"),
					).Error,
				)
				kind := NextcloudContentReadLease
				if mode == "build-worker" {
					kind = NextcloudContentBuildLease
				}
				held, err = leases.AcquireKnowledge(ctx, scope, kind, "live-exact", time.Minute)
				require.NoError(t, err)
				require.NoError(t, leases.RetireKnowledge(ctx, scope))
				var fence nextcloudContentFence
				require.NoError(
					t,
					db.Where("tenant_id=7 AND knowledge_base_id='kb' AND knowledge_id='old'").Take(&fence).Error,
				)
				due := time.UnixMilli(*fence.RetiredAtMS)
				require.NoError(
					t,
					db.Model(&nextcloudGCJob{}).
						Where("id=?", jobID).
						Updates(map[string]any{"not_before": due, "original_not_before": due, "next_attempt_at": due}).
						Error,
				)
			}
			if mode == "shared" {
				var r types.StoredResource
				require.NoError(t, db.Where("handle=?", strings.Repeat("z", 22)).Take(&r).Error)
				require.NoError(
					t,
					db.Create(
						&types.ResourceBinding{
							ResourceID: r.ID,
							TenantID:   7,
							OwnerType:  "message",
							OwnerID:    "current-shared",
							Relation:   "artifact",
						},
					).Error,
				)
			}
			if mode == "symlink" {
				require.NoError(t, os.Remove(path))
				target := filepath.Join(filepath.Dir(path), "current.txt")
				require.NoError(t, os.WriteFile(target, []byte("current bytes must survive"), 0o600))
				require.NoError(t, os.Symlink(target, path))
			}
			var resource types.StoredResource
			require.NoError(t, db.Where("handle=?", strings.Repeat("z", 22)).Take(&resource).Error)
			provider := filesvc.NewBackendScopedFileService(
				"backend",
				filesvc.NewLocalFileService(filepath.Dir(filepath.Dir(filepath.Dir(path))), ""),
			)
			deleter := provider.(interfaces.NextcloudPhysicalGCDeleter)
			store.ConfigureLocalDeletePreparation(
				func(context.Context, *types.StoredResource) (func(context.Context) (bool, error), error) {
					if mode == "late-reader" {
						_, err := leases.AcquireKBRead(ctx, 7, "kb", "post-claim", time.Minute)
						require.ErrorIs(t, err, ErrNextcloudContentLeaseDenied)
					}
					if mode == "lost-claim" {
						require.NoError(
							t,
							db.Exec(
								("UPDATE nextcloud_content_fences SET claim_token='successor-claim' WHERE te" +
									"nant_id=7 AND knowledge_base_id='kb' AND knowledge_id='old'"),
							).Error,
						)
						require.NoError(
							t,
							db.Model(&nextcloudGCItem{}).
								Where("job_id=? AND kind='source_file'", jobID).
								Update("lease_token", "successor-claim").
								Error,
						)
					}
					if mode == "restore-before-provider" {
						require.NoError(
							t,
							db.Model(&nextcloudSourceVersion{}).
								Where(("tenant_id=7 AND knowledge_base_id='kb' AND datasource_id='ds' AND external" +
									"_id='node'")).
								Updates(map[string]any{"state": "published", "candidate_knowledge_id": "old"}).
								Error,
						)
					}
					return func(ioCtx context.Context) (bool, error) {
						calls++
						if mode == "provider-failure" && calls == 1 {
							return false, errors.New("owned provider unavailable")
						}
						unlinked, e := deleter.DeleteNextcloudObject(ioCtx, resource.PhysicalPath)
						if mode == "crash-after-unlink" && calls == 1 && e == nil {
							return false, errors.New("crash after unlink before DB receipt")
						}
						return unlinked, e
					}, nil
				},
			)
			_, err = store.RunDue(ctx, time.Now().Add(time.Second), 10)
			if mode == "lost-claim" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			switch mode {
			case "coverage-missing", "broad-reader", "exact-reader", "build-worker":
				require.Zero(t, calls)
				_, err = os.Stat(path)
				require.NoError(t, err)
				if held.ID != "" {
					require.NoError(t, leases.ReleaseLease(ctx, held.ID))
				} else {
					leaseTestCoverKB(t, db, scope)
				}
				_, err = store.RunDue(ctx, time.Now().Add(2*time.Hour), 10)
				require.NoError(t, err)
				require.Equal(t, 1, calls)
				_, err = os.Stat(path)
				require.ErrorIs(t, err, os.ErrNotExist)
			case "provider-failure", "crash-after-unlink":
				require.Equal(t, 1, calls)
				var f nextcloudContentFence
				require.NoError(
					t,
					db.Where("tenant_id=7 AND knowledge_base_id='kb' AND knowledge_id='old'").Take(&f).Error,
				)
				require.Equal(t, "retired", f.State)
				_, err = store.RunDue(ctx, time.Now().Add(2*time.Hour), 10)
				require.NoError(t, err)
				require.Equal(t, 2, calls)
				_, err = os.Stat(path)
				require.ErrorIs(t, err, os.ErrNotExist)
				if mode == "crash-after-unlink" {
					status, e := store.ListStatus(ctx, 7, 10)
					require.NoError(t, e)
					require.Zero(t, status[0].ConfirmedReleasedBytes)
				}
			case "restore-before-provider":
				require.Zero(t, calls)
				_, err = os.Stat(path)
				require.NoError(t, err)
			case "lost-claim":
				require.Zero(t, calls)
				_, err = os.Stat(path)
				require.NoError(t, err)
				var f nextcloudContentFence
				require.NoError(
					t,
					db.Where("tenant_id=7 AND knowledge_base_id='kb' AND knowledge_id='old'").Take(&f).Error,
				)
				require.Equal(t, "successor-claim", f.ClaimToken)
			case "symlink":
				require.Equal(t, 1, calls)
				bytes, e := os.ReadFile(filepath.Join(filepath.Dir(path), "current.txt"))
				require.NoError(t, e)
				require.Equal(t, []byte("current bytes must survive"), bytes)
			case "shared":
				require.Zero(t, calls)
				_, err = os.Stat(path)
				require.NoError(t, err)
				var n int64
				require.NoError(
					t,
					db.Model(&types.ResourceBinding{}).Where("resource_id=?", resource.ID).Count(&n).Error,
				)
				require.Equal(t, int64(1), n)
			default:
				require.Equal(t, 1, calls)
				_, err = os.Stat(path)
				require.ErrorIs(t, err, os.ErrNotExist)
			}
			_ = ref
		})
	}
}
func TestNextcloudActualRunDueContentFenceSQLite(t *testing.T)   { gcActualRunDueContract(t, false) }
func TestPostgresNextcloudActualRunDueContentFence(t *testing.T) { gcActualRunDueContract(t, true) }
func gcObjectTestRetiredFence(t *testing.T, db *gorm.DB, id string, at time.Time) {
	t.Helper()
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		now, e := nextcloudLeaseNowMS(tx)
		if e != nil {
			return e
		}
		if _, e = nextcloudLockKBFence(tx, 7, "kb", now); e != nil {
			return e
		}
		scope := NextcloudContentScope{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     id,
			DataSourceID:    "ds",
			ExternalID:      "node",
		}
		if _, e = nextcloudLockExactFence(tx, scope, now, true); e != nil {
			return e
		}
		return tx.Exec(
			("UPDATE nextcloud_content_fences SET state='retired',retired_at_ms=?,epoch=" +
				"epoch+1 WHERE tenant_id=7 AND knowledge_base_id='kb' AND knowledge_id=?"),
			at.UnixMilli(),
			id,
		).Error
	}))
}

func TestNextcloudRunDuePhysicalCoverageAndActualReaderLease(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	store, db := nextcloudGCTestDB(t)
	ref := nextcloudGCTestLocalResource(t, db, "physicalcoverage000001", "old")
	nextcloudGCTestSource(t, db, "old", ref, now.Add(-8*24*time.Hour))
	_, e := store.InventoryRetired(ctx, now, 10)
	require.NoError(t, e)
	calls := 0
	store.ConfigureLocalDelete(func(context.Context, *types.StoredResource) (bool, error) { calls++; return true, nil })
	require.NoError(
		t,
		db.Exec("DELETE FROM nextcloud_content_lease_coverage WHERE tenant_id=7 AND knowledge_base_id='kb'").Error,
	)
	_, e = store.RunDue(ctx, now, 10)
	require.NoError(t, e)
	require.Zero(t, calls)
	status, e := store.ListStatus(ctx, 7, 10)
	require.NoError(t, e)
	require.Equal(t, "content_lease_coverage_unverified", status[0].LastErrorCode)
	scope := NextcloudContentScope{
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     "old",
		DataSourceID:    "ds",
		ExternalID:      "node",
	}
	leaseTestCoverKB(t, db, scope)
	broad, e := NewNextcloudContentLeaseStore(db).AcquireKBRead(ctx, 7, "kb", "actual-reader", time.Minute)
	require.NoError(t, e)
	_, e = store.RunDue(ctx, now.Add(time.Hour), 10)
	require.NoError(t, e)
	require.Zero(t, calls)
	status, e = store.ListStatus(ctx, 7, 10)
	require.NoError(t, e)
	require.Equal(t, "content_read_or_build_busy", status[0].LastErrorCode)
	require.NoError(t, NewNextcloudContentLeaseStore(db).ReleaseLease(ctx, broad.ID))
	_, e = store.RunDue(ctx, now.Add(2*time.Hour), 10)
	require.NoError(t, e)
	require.Equal(t, 1, calls)
	var fence nextcloudContentFence
	require.NoError(t, db.Where("tenant_id=7 AND knowledge_base_id='kb' AND knowledge_id='old'").Take(&fence).Error)
	require.Equal(t, "retired", fence.State, "one original object ACK cannot certify all derived generations deleted")
}
