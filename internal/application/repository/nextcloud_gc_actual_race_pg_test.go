package repository

import (
	"context"
	filesvc "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"os"
	"testing"
	"time"
)

func TestPostgresNextcloudActualGCProviderTransactionSerializesRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, db, jobID, _, path := gcActualObjectFixture(t, true)
	provider := filesvc.NewBackendScopedFileService("backend", filesvc.NewLocalFileService(path[:len(path)-len("/7/old/source.txt")], ""))
	deleter := provider.(interfaces.NextcloudPhysicalGCDeleter)
	entered := make(chan struct{})
	proceed := make(chan struct{})
	calls := 0
	store.ConfigureLocalDeletePreparation(func(context.Context, *types.StoredResource) (func(context.Context) (bool, error), error) {
		return func(ioCtx context.Context) (bool, error) {
			calls++
			close(entered)
			select {
			case <-proceed:
			case <-ioCtx.Done():
				return false, ioCtx.Err()
			}
			return deleter.DeleteNextcloudObject(ioCtx, "storage://backend/local://7/old/source.txt")
		}, nil
	})
	done := make(chan error, 1)
	go func() { _, e := store.RunDue(ctx, time.Now().Add(time.Second), 10); done <- e }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("actual provider was not reached")
	}
	restore := make(chan error, 1)
	go func() {
		restore <- db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if _, e := gcLockKBContent(tx, nextcloudGCJob{TenantID: 7, KnowledgeBaseID: "kb"}); e != nil {
				return e
			}
			return tx.Model(&nextcloudSourceVersion{}).Where("tenant_id=7 AND knowledge_base_id='kb' AND datasource_id='ds' AND external_id='node'").Updates(map[string]any{"state": "published", "candidate_knowledge_id": "old"}).Error
		})
	}()
	var schema string
	require.NoError(t, db.Raw("SELECT current_schema()").Scan(&schema).Error)
	deadline := time.Now().Add(3 * time.Second)
	blocked := false
	for time.Now().Before(deadline) {
		var waiting int64
		require.NoError(t, db.Raw("SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid=l.relation JOIN pg_namespace n ON n.oid=c.relnamespace WHERE NOT l.granted AND n.nspname=?", schema).Scan(&waiting).Error)
		if waiting > 0 {
			blocked = true
			break
		}
		select {
		case e := <-restore:
			t.Fatalf("restore entered before guarded unlink: %v", e)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.True(t, blocked, "actual restore must wait on the GC KB/content transaction")
	close(proceed)
	require.NoError(t, <-done)
	require.NoError(t, <-restore)
	require.Equal(t, 1, calls)
	_, e := os.Stat(path)
	require.ErrorIs(t, e, os.ErrNotExist)
	var fence nextcloudContentFence
	require.NoError(t, db.Where("tenant_id=7 AND knowledge_base_id='kb' AND knowledge_id='old'").Take(&fence).Error)
	require.Equal(t, "retired", fence.State, "a SQL restore cannot open the immutable old content generation")
	var item nextcloudGCItem
	require.NoError(t, db.Where("job_id=? AND kind='source_file'", jobID).Take(&item).Error)
	require.Equal(t, "collected", item.State)
}
