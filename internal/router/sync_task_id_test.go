package router

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

func TestLiteExecutorPreservesExactTaskIDAndRejectsConcurrentDuplicate(t *testing.T) {
	executor := NewSyncTaskExecutor()
	started := make(chan string, 1)
	release := make(chan struct{})
	executor.RegisterHandler("fixture:task", func(ctx context.Context, _ *asynq.Task) error {
		id, ok := types.TaskExecutionIDFromContext(ctx)
		if !ok {
			started <- ""
		} else {
			started <- id
		}
		<-release
		return nil
	})
	task := asynq.NewTask("fixture:task", nil)
	info, err := executor.Enqueue(task, asynq.TaskID("dssync:one"), asynq.MaxRetry(0))
	require.NoError(t, err)
	require.Equal(t, "dssync:one", info.ID)
	select {
	case id := <-started:
		require.Equal(t, info.ID, id)
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	_, err = executor.Enqueue(task, asynq.TaskID(info.ID))
	require.ErrorIs(t, err, asynq.ErrTaskIDConflict)
	close(release)
}
