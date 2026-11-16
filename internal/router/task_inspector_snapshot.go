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
// A failed scan discards its partial result so housekeeping can defer recovery.
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
	_, err := scanLiveTasks(ctx, queues, states, func(task *asynq.TaskInfo) (bool, error) {
		if _, ok := taskTypesForKnowledgeCancel[task.Type]; !ok {
			return false, nil
		}
		var probe knowledgeIDProbe
		if err := json.Unmarshal(task.Payload, &probe); err != nil {
			return false, fmt.Errorf("invalid document task payload: %w", err)
		}
		if probe.KnowledgeID != "" {
			ids[probe.KnowledgeID] = struct{}{}
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// scanLiveTasks stops early when visit finds a match, or walks every page for
// a full snapshot. A missing queue has no work; an unreadable one is unknown.
func scanLiveTasks(
	ctx context.Context,
	queues []string,
	states []cancellableTaskState,
	visit func(*asynq.TaskInfo) (bool, error),
) (bool, error) {
	for _, queue := range queues {
		for _, state := range states {
			for page := 1; ; page++ {
				if err := ctx.Err(); err != nil {
					return false, err
				}
				tasks, err := state.list(queue, asynq.PageSize(listPageSize), asynq.Page(page))
				if err != nil {
					if isAsynqQueueNotFound(err) {
						break
					}
					return false, fmt.Errorf("scan queue %s state %s: %w", queue, state.name, err)
				}
				for _, task := range tasks {
					if task == nil {
						continue
					}
					found, err := visit(task)
					if err != nil {
						return false, fmt.Errorf("scan queue %s state %s: %w", queue, state.name, err)
					}
					if found {
						return true, nil
					}
				}
				if len(tasks) < listPageSize {
					break
				}
			}
		}
	}
	return false, ctx.Err()
}
