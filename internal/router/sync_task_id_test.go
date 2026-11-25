package router

import (
	"context"
	"errors"
	"sync"
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

func TestSyncTaskIDPreventsConcurrentDuplicatesAndReleasesOnExit(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[failure], func(t *testing.T) {
			executor := NewSyncTaskExecutor()
			started := make(chan struct{}, 3)
			release := make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			executor.RegisterHandler("test", func(context.Context, *asynq.Task) error {
				started <- struct{}{}
				<-release
				if failure {
					return errors.New("test failure")
				}
				return nil
			})
			task := asynq.NewTask("test", nil, asynq.TaskID("original"), asynq.MaxRetry(0))
			info, err := executor.Enqueue(task, asynq.TaskID("chosen"), asynq.Queue("wiki"),
				asynq.ProcessIn(20*time.Millisecond))
			require.NoError(t, err)
			require.Equal(t, "chosen", info.ID)
			// The reservation exists before a delayed handler starts.
			_, err = executor.Enqueue(task, asynq.TaskID("chosen"), asynq.Queue("wiki"))
			require.ErrorIs(t, err, asynq.ErrTaskIDConflict)
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("task did not start")
			}
			_, err = executor.Enqueue(task, asynq.TaskID("chosen"), asynq.Queue("other"))
			require.NoError(t, err, "task IDs are scoped to queues")
			once.Do(func() { close(release) })
			require.Eventually(t, func() bool {
				executor.mu.RLock()
				defer executor.mu.RUnlock()
				return len(executor.pendingIDs) == 0
			}, time.Second, time.Millisecond)
			_, err = executor.Enqueue(task, asynq.TaskID("chosen"), asynq.Queue("wiki"))
			require.NoError(t, err, "settled task IDs can be used again")
			require.Eventually(t, func() bool {
				executor.mu.RLock()
				defer executor.mu.RUnlock()
				return len(executor.pendingIDs) == 0
			}, time.Second, time.Millisecond)
		})
	}
}
