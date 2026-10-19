package repository

import (
	"context"
	"encoding/json"
	filesvc "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type gcCrashDescriptor struct{ DSN, Driver, Base, Phase string }

func TestNextcloudActualGCCrashChild(t *testing.T) {
	if os.Getenv("WEKNORA_GC_CRASH_CHILD") != "1" {
		t.Skip("owned actual GC process fixture")
	}
	var d gcCrashDescriptor
	b, e := os.ReadFile(os.Getenv("WEKNORA_GC_CRASH_DESCRIPTOR"))
	require.NoError(t, e)
	require.NoError(t, json.Unmarshal(b, &d))
	var db *gorm.DB
	if d.Driver == "postgres" {
		db, e = gorm.Open(postgres.Open(d.DSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	} else {
		db, e = gorm.Open(sqlite.Open(d.DSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	}
	require.NoError(t, e)
	store := NewNextcloudGCStore(db)
	provider := filesvc.NewBackendScopedFileService("backend", filesvc.NewLocalFileService(d.Base, ""))
	deleter := provider.(interfaces.NextcloudPhysicalGCDeleter)
	store.ConfigureLocalDeletePreparation(func(context.Context, *types.StoredResource) (func(context.Context) (bool, error), error) {
		if d.Phase == "claim" {
			os.Exit(77)
		}
		return func(ctx context.Context) (bool, error) {
			_, e := deleter.DeleteNextcloudObject(ctx, "storage://backend/local://7/old/source.txt")
			require.NoError(t, e)
			os.Exit(78)
			return false, nil
		}, nil
	})
	_, e = store.RunDue(context.Background(), time.Now().Add(time.Second), 10)
	require.NoError(t, e)
	t.Fatal("crash boundary was not reached")
}
func gcActualProcessCrash(t *testing.T, pg bool) {
	for _, phase := range []string{"claim", "unlink"} {
		t.Run(phase, func(t *testing.T) {
			var store *NextcloudGCStore
			var db *gorm.DB
			var jobID, ref, path, dsn string
			driver := "sqlite"
			if pg {
				store, db, jobID, ref, path = gcActualObjectFixture(t, true)
				var schema string
				require.NoError(t, db.Raw("SELECT current_schema()").Scan(&schema).Error)
				dsn = os.Getenv("WEKNORA_LEASE_TEST_POSTGRES_DSN") + " search_path=" + schema
				driver = "postgres"
			} else {
				dsn = filepath.Join(t.TempDir(), "gc.sqlite") + "?_busy_timeout=5000&_foreign_keys=on"
				var e error
				db, e = gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
				require.NoError(t, e)
				sql, e := db.DB()
				require.NoError(t, e)
				sql.SetMaxOpenConns(1)
				t.Cleanup(func() { _ = sql.Close() })
				require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.Chunk{}, &nextcloudSourceVersion{}, &nextcloudGCJob{}, &nextcloudGCItem{}, &types.StoredResource{}, &types.ResourceBinding{}))
				gcObjectTestLeaseSchema(t, db)
				ref = nextcloudGCTestLocalResource(t, db, strings.Repeat("q", 22), "old")
				now := time.Now().UTC()
				nextcloudGCTestSource(t, db, "old", ref, now.Add(-8*24*time.Hour))
				store = NewNextcloudGCStore(db)
				_, e = store.InventoryRetired(context.Background(), now, 10)
				require.NoError(t, e)
				var job nextcloudGCJob
				require.NoError(t, db.Take(&job).Error)
				jobID = job.ID
				base := t.TempDir()
				path = filepath.Join(base, "7", "old", "source.txt")
				require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
				require.NoError(t, os.WriteFile(path, []byte("persisted-source-object"), 0600))
			}
			base := filepath.Dir(filepath.Dir(filepath.Dir(path)))
			descriptor := filepath.Join(t.TempDir(), "descriptor")
			b, e := json.Marshal(gcCrashDescriptor{DSN: dsn, Driver: driver, Base: base, Phase: phase})
			require.NoError(t, e)
			require.NoError(t, os.WriteFile(descriptor, b, 0600))
			child := exec.Command(os.Args[0], "-test.run=^TestNextcloudActualGCCrashChild$")
			child.Env = append(os.Environ(), "WEKNORA_GC_CRASH_CHILD=1", "WEKNORA_GC_CRASH_DESCRIPTOR="+descriptor)
			err := child.Run()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit)
			expected := 77
			if phase == "unlink" {
				expected = 78
			}
			require.Equal(t, expected, exit.ExitCode())
			var item nextcloudGCItem
			require.NoError(t, db.Where("job_id=? AND kind='source_file'", jobID).Take(&item).Error)
			require.Equal(t, "deleting", item.State)
			require.Zero(t, item.ConfirmedReleasedBytes)
			leases := NewNextcloudContentLeaseStore(db)
			_, e = leases.AcquireKBRead(context.Background(), 7, "kb", "late-live-claim", time.Minute)
			require.ErrorIs(t, e, ErrNextcloudContentLeaseDenied)
			require.NoError(t, db.Model(&nextcloudGCItem{}).Where("job_id=? AND kind='source_file'", jobID).Update("lease_until", time.Now().Add(-time.Second)).Error)
			require.NoError(t, db.Exec("UPDATE nextcloud_content_fences SET claim_until_ms=1 WHERE tenant_id=7 AND knowledge_base_id='kb' AND knowledge_id='old'").Error)
			reader, e := leases.AcquireKBRead(context.Background(), 7, "kb", "expired-claim-reader", time.Minute)
			require.NoError(t, e)
			provider := filesvc.NewBackendScopedFileService("backend", filesvc.NewLocalFileService(base, ""))
			deleter := provider.(interfaces.NextcloudPhysicalGCDeleter)
			calls := 0
			store.ConfigureLocalDeletePreparation(func(context.Context, *types.StoredResource) (func(context.Context) (bool, error), error) {
				return func(ctx context.Context) (bool, error) {
					calls++
					return deleter.DeleteNextcloudObject(ctx, "storage://backend/local://7/old/source.txt")
				}, nil
			})
			_, e = store.RunDue(context.Background(), time.Now().Add(time.Hour), 10)
			require.NoError(t, e)
			require.Zero(t, calls, "expired token cannot outrun a newly admitted broad reader")
			require.NoError(t, leases.ReleaseLease(context.Background(), reader.ID))
			_, e = store.RunDue(context.Background(), time.Now().Add(2*time.Hour), 10)
			require.NoError(t, e)
			require.Equal(t, 1, calls)
			require.NoError(t, db.Where("job_id=? AND kind='source_file'", jobID).Take(&item).Error)
			require.Equal(t, "collected", item.State)
			if phase == "unlink" {
				require.Zero(t, item.ConfirmedReleasedBytes)
			} else {
				require.Equal(t, int64(23), item.ConfirmedReleasedBytes)
			}
			_, e = os.Stat(path)
			require.ErrorIs(t, e, os.ErrNotExist)
			var fence nextcloudContentFence
			require.NoError(t, db.Where("tenant_id=7 AND knowledge_base_id='kb' AND knowledge_id='old'").Take(&fence).Error)
			require.Equal(t, "retired", fence.State)
			require.Empty(t, fence.ClaimToken)
			_ = ref
		})
	}
}
func TestNextcloudActualGCProcessCrashSQLite(t *testing.T)   { gcActualProcessCrash(t, false) }
func TestPostgresNextcloudActualGCProcessCrash(t *testing.T) { gcActualProcessCrash(t, true) }
