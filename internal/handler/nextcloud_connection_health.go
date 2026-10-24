package handler

import (
	"context"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// nextcloudConnectionHealth combines independent observations:
//   - sync logs are diagnostic history and never become a worker heartbeat;
//   - the worker gate uses live asynq server heartbeats subscribed to the
//     datasource sync queue;
//   - dispatcher freshness remains a separate condition.
func nextcloudConnectionHealth(
	ctx context.Context,
	store *repository.NextcloudOnboardingStore,
	inspector interfaces.TaskInspector,
) (types.NextcloudConnectionHealth, error) {
	return store.ConnectionHealth(ctx, nextcloudWorkerObservation(ctx, inspector))
}

func nextcloudWorkerObservation(
	ctx context.Context,
	inspector interfaces.TaskInspector,
) *repository.NextcloudWorkerObservation {
	if inspector == nil {
		return nil
	}
	stats, supported, err := inspector.WorkerServerStats(ctx)
	if err != nil || !supported {
		return nil
	}
	for _, stat := range stats {
		if _, syncWorker := stat.Queues[types.QueueSync]; !syncWorker {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(stat.Status)) {
		case "active", "quiet", "running":
			return &repository.NextcloudWorkerObservation{
				Status:     "running",
				ObservedAt: time.Now().UTC(),
			}
		}
	}
	return nil
}
