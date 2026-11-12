package router

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestDingTalkQueueInspectorExactIdentityAndBackendFailure(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr: server.Addr(), MaxRetries: -1, DialTimeout: time.Second, ReadTimeout: time.Second,
	})
	t.Cleanup(func() { _ = client.Close() })
	queue := asynq.NewClientFromRedisClient(client)
	inspector := asynq.NewInspectorFromRedisClient(client)
	probe := &asynqTaskInspector{inspector: inspector, redis: client}
	ctx := context.Background()
	quiet, err := probe.DingTalkTasksQuiescent(ctx, "knowledge")
	require.NoError(t, err)
	require.True(t, quiet)
	info, err := queue.Enqueue(asynq.NewTask(types.TypeDocumentProcess, []byte(`{"knowledge_id":"knowledge"}`),
		asynq.TaskID("dingtalk:knowledge:1:1"), asynq.Queue(types.QueueDefault)))
	require.NoError(t, err)
	actual, err := probe.LookupKnowledgeQueueTask(ctx, info.ID)
	require.NoError(t, err)
	require.Equal(t, info.ID, actual.ID)
	require.Equal(t, info.Payload, actual.Payload)
	quiet, err = probe.DingTalkTasksQuiescent(ctx, "knowledge")
	require.NoError(t, err)
	require.False(t, quiet)
	require.NoError(t, inspector.DeleteTask(info.Queue, info.ID))
	quiet, err = probe.DingTalkTasksQuiescent(ctx, "knowledge")
	require.NoError(t, err)
	require.True(t, quiet)
	server.Close()
	quiet, err = probe.DingTalkTasksQuiescent(ctx, "knowledge")
	require.Error(t, err, "backend failure cannot authorize a processing retry")
	require.False(t, quiet)
}
