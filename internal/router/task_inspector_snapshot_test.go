package router

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

func TestQueuedKnowledgeSnapshotLiveStatesAndErrors(t *testing.T) {
	states := []cancellableTaskState{}
	calls := map[string]int{}
	for _, name := range []string{"pending", "scheduled", "retry", "active"} {
		name := name
		states = append(states, cancellableTaskState{
			name: name,
			list: func(_ string, _ ...asynq.ListOption) ([]*asynq.TaskInfo, error) {
				calls[name]++
				count := 1
				if name == "pending" && calls[name] == 1 {
					count = listPageSize
				}
				tasks := make([]*asynq.TaskInfo, count)
				for i := range tasks {
					tasks[i] = &asynq.TaskInfo{
						Type:    types.TypeDocumentProcess,
						Payload: []byte(fmt.Sprintf(`{"knowledge_id":"%s-%d-%d"}`, name, calls[name], i)),
					}
				}
				return tasks, nil
			},
		})
	}
	ids, err := scanQueuedKnowledgeIDs(context.Background(), []string{"default"}, states)
	require.NoError(t, err)
	require.Len(t, ids, listPageSize+4)
	require.Equal(t, 2, calls["pending"])
	for _, state := range []string{"scheduled", "retry", "active"} {
		require.Contains(t, ids, state+"-1-0")
		require.Equal(t, 1, calls[state])
	}
	boom := errors.New("redis unavailable")
	states[1].list = func(string, ...asynq.ListOption) ([]*asynq.TaskInfo, error) { return nil, boom }
	ids, err = scanQueuedKnowledgeIDs(context.Background(), []string{"default"}, states)
	require.ErrorIs(t, err, boom)
	require.Nil(t, ids, "partial snapshot must never look complete")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = scanQueuedKnowledgeIDs(ctx, []string{"default"}, states)
	require.ErrorIs(t, err, context.Canceled)
}
