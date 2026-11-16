package router

import (
	"context"
	"fmt"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// One pass collects every knowledge a cancellable task references, across
// pages and states, and skips task types that are not per-knowledge.
func TestQueuedKnowledgeIDsCollectsAcrossPagesAndStates(t *testing.T) {
	server := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
	asynqClient := asynq.NewClientFromRedisClient(redisClient)
	t.Cleanup(func() {
		_ = asynqClient.Close()
		_ = redisClient.Close()
	})
	inspector := &asynqTaskInspector{inspector: asynq.NewInspectorFromRedisClient(redisClient), redis: redisClient}

	const pending = listPageSize + 5
	for i := 0; i < pending; i++ {
		enqueueTask(t, asynqClient, types.TypeDocumentProcess,
			fmt.Sprintf(`{"knowledge_id":"k-%d"}`, i), fmt.Sprintf("doc-%03d", i))
	}
	scheduleTask(t, asynqClient, types.TypeKnowledgePostProcess, `{"knowledge_id":"k-scheduled"}`, "post")
	enqueueTask(t, asynqClient, types.TypeKBClone, `{"knowledge_id":"k-kb-level"}`, "clone")

	_, err := asynqClient.Enqueue(asynq.NewTask(types.TypeImageMultimodal, []byte(`{"knowledge_id":"k-image"}`)),
		asynq.Queue(types.QueueMultimodal))
	if err != nil {
		t.Fatal(err)
	}
	enqueueTask(t, asynqClient, types.TypeKnowledgeListDelete, `{"knowledge_ids":["k-delete","k-other"]}`, "delete")
	for _, probe := range []struct {
		id      string
		hasTask func(context.Context, string) (bool, error)
	}{
		{"k-image", inspector.HasQueuedTasksForKnowledge},
		{"k-delete", inspector.HasQueuedDeleteTasksForKnowledge},
	} {
		if found, err := probe.hasTask(context.Background(), probe.id); err != nil || !found {
			t.Fatalf("%s: found %v, error %v", probe.id, found, err)
		}
	}

	ids, err := inspector.QueuedKnowledgeIDs(context.Background())
	if err != nil {
		t.Fatalf("QueuedKnowledgeIDs: %v", err)
	}
	if len(ids) != pending+2 {
		t.Fatalf("got %d ids, want %d", len(ids), pending+2)
	}
	for _, id := range []string{"k-0", fmt.Sprintf("k-%d", pending-1), "k-scheduled", "k-image"} {
		if _, ok := ids[id]; !ok {
			t.Fatalf("missing %s", id)
		}
	}
	if _, ok := ids["k-kb-level"]; ok {
		t.Fatal("KB-level task types must not count")
	}
}

// A Redis failure is an error, not an empty queue.
func TestQueuedKnowledgeIDsReportsBackendErrors(t *testing.T) {
	server := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	inspector := &asynqTaskInspector{inspector: asynq.NewInspectorFromRedisClient(redisClient), redis: redisClient}
	server.Close()

	if _, err := inspector.QueuedKnowledgeIDs(context.Background()); err == nil {
		t.Fatal("expected an error when Redis is down")
	}
	for _, probe := range []struct {
		name    string
		hasTask func(context.Context, string) (bool, error)
	}{
		{"parse", inspector.HasQueuedTasksForKnowledge},
		{"delete", inspector.HasQueuedDeleteTasksForKnowledge},
	} {
		t.Run(probe.name, func(t *testing.T) {
			if found, err := probe.hasTask(context.Background(), "k-1"); found || err == nil {
				t.Fatalf("unavailable Redis: got (%v, %v), want (false, error)", found, err)
			}
		})
	}
}

func TestQueuedKnowledgeIDsLiteHasEmptySnapshot(t *testing.T) {
	ids, err := NewNoopTaskInspector().QueuedKnowledgeIDs(context.Background())
	if err != nil || ids == nil || len(ids) != 0 {
		t.Fatalf("inline queue snapshot: got %v, error %v", ids, err)
	}
}
