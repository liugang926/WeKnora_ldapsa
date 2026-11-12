package router

import (
	"context"
	"errors"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
)

// LookupKnowledgeQueueTask is an optional exact-ID capability. Lite mode does
// not implement it: a no-op inspector cannot prove durable enqueue acceptance.
func (a *asynqTaskInspector) LookupKnowledgeQueueTask(ctx context.Context, id string) (*asynq.TaskInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	info, err := a.inspector.GetTaskInfo(types.QueueDefault, id)
	if errors.Is(err, asynq.ErrTaskNotFound) || errors.Is(err, asynq.ErrQueueNotFound) {
		return nil, nil
	}
	return info, err
}

// DingTalkTasksQuiescent is stricter than the legacy housekeeping probe: a
// failed page is unknown, never evidence authorizing a new processing attempt.
func (a *asynqTaskInspector) DingTalkTasksQuiescent(ctx context.Context, knowledge string) (bool, error) {
	for _, queue := range queuesScanned {
		for _, state := range a.cancellableTaskStates() {
			for page := 1; ; page++ {
				if err := ctx.Err(); err != nil {
					return false, err
				}
				rows, err := state.list(queue, asynq.PageSize(listPageSize), asynq.Page(page))
				if errors.Is(err, asynq.ErrQueueNotFound) {
					break
				}
				if err != nil {
					return false, err
				}
				for _, row := range rows {
					if matchesKnowledge(row.Type, row.Payload, knowledge) {
						return false, nil
					}
				}
				if len(rows) < listPageSize {
					break
				}
			}
		}
	}
	return true, nil
}
