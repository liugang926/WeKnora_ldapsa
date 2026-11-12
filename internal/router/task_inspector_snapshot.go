package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
)

var (
	_ interfaces.QueuedKnowledgeInspector = (*asynqTaskInspector)(nil)
	_ interfaces.QueuedKnowledgeInspector = noopTaskInspector{}
)

// QueuedKnowledgeIDs walks each live queue state once, not once per document.
// Unlike the legacy best-effort single-row probe, scan errors are propagated
// so housekeeping cannot mistake an unavailable Redis backend for an empty one.
func (a *asynqTaskInspector) QueuedKnowledgeIDs(ctx context.Context) (map[string]struct{}, error) {
	if a == nil || a.inspector == nil {
		return nil, errors.New("queue inspector unavailable")
	}
	return scanQueuedKnowledgeIDs(ctx, queuesScanned, a.cancellableTaskStates())
}

func scanQueuedKnowledgeIDs(
	ctx context.Context,
	queues []string,
	states []cancellableTaskState,
) (map[string]struct{}, error) {
	ids := make(map[string]struct{})
	for _, queue := range queues {
		for _, state := range states {
			for page := 1; ; page++ {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				tasks, err := state.list(queue, asynq.PageSize(listPageSize), asynq.Page(page))
				if err != nil {
					if isAsynqQueueNotFound(err) {
						break
					}
					return nil, fmt.Errorf("scan queue %s state %s: %w", queue, state.name, err)
				}
				for _, task := range tasks {
					if task == nil {
						continue
					}
					if _, ok := taskTypesForKnowledgeCancel[task.Type]; !ok {
						continue
					}
					var probe knowledgeIDProbe
					if err := json.Unmarshal(task.Payload, &probe); err != nil {
						return nil, fmt.Errorf(
							"invalid document task payload in queue %s state %s: %w", queue, state.name, err,
						)
					}
					if probe.KnowledgeID != "" {
						ids[probe.KnowledgeID] = struct{}{}
					}
				}
				if len(tasks) < listPageSize {
					break
				}
			}
		}
	}
	return ids, nil
}

func (noopTaskInspector) QueuedKnowledgeIDs(context.Context) (map[string]struct{}, error) {
	return nil, nil
}
