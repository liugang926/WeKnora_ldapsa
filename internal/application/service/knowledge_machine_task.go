package service

import (
	"context"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/hibiken/asynq"
)

// All actual producer call sites use this constructor, so the receipt's fixed
// ID is a real queue option in both Redis and Lite, not an ambient test value.
func newKnowledgeQueueTask(ctx context.Context, reader any, kind string, payload []byte,
	options ...asynq.Option,
) (*asynq.Task, error) {
	requested := ""
	for _, option := range options {
		if option.Type() == asynq.TaskIDOpt {
			requested, _ = option.Value().(string)
		}
	}
	var raw []byte
	var id string
	var err error
	if store, ok := reader.(*repository.KnowledgeInputProvenanceStore); ok && store != nil {
		raw, id, err = store.PrepareNextcloudMachineTask(ctx, kind, payload, requested)
	} else {
		raw, id, err = repository.PrepareNextcloudMachineTask(ctx, kind, payload, requested)
	}
	if err != nil {
		return nil, err
	}
	if id != "" {
		options = append(options, asynq.TaskID(id))
	}
	return asynq.NewTask(kind, raw, options...), nil
}
