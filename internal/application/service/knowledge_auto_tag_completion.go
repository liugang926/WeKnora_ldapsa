package service

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/tracing/langfuse"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"gorm.io/gorm"
)

// Completion and its immutable Auto handoff commit in one SQL transaction.
// Redis is only a wake-up channel; recovery always reuses the persisted bytes.
func ConfigureKnowledgeAutoTagCompletion(
	post interfaces.TaskHandler,
	repo interfaces.KnowledgeRepository,
	store *repository.KnowledgeInputProvenanceStore,
) {
	s, ok := post.(*KnowledgePostProcessService)
	if !ok || store == nil {
		return
	}
	repository.ConfigureKnowledgeCompletionHandoff(repo, func(ctx context.Context, tx *gorm.DB, id string) error {
		p := types.KnowledgeAutoTagPayload{Language: types.LanguageFromContextOrDefault(ctx)}
		langfuse.InjectTracing(ctx, &p)
		return store.SealKnowledgeAutoTagCompletionInTx(ctx, tx, id, p)
	})
	repository.ConfigureKnowledgeCompletionObserver(repo, func(ctx context.Context, id string) error {
		// SQL already owns the work. A Redis outage cannot undo completion or
		// remove this intent; the periodic recovery scans it again.
		_, _ = RecoverKnowledgeAutoTagCompletions(ctx, store, s.taskEnqueuer, id)
		return nil
	})
}

func RecoverKnowledgeAutoTagCompletions(
	ctx context.Context,
	store *repository.KnowledgeInputProvenanceStore,
	queue interfaces.TaskEnqueuer,
	knowledgeID string,
) (int, error) {
	if store == nil || queue == nil {
		return 0, nil
	}
	rows, err := store.PendingKnowledgeAutoTagCompletions(ctx, knowledgeID, 100)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, row := range rows {
		var p types.KnowledgeAutoTagPayload
		if json.Unmarshal(row.Payload, &p) != nil || p.TaskInput == nil || p.TaskInput.ID != row.DedupKey ||
			p.TenantID != row.TenantID ||
			p.KnowledgeID != row.ScopeID {
			return count, types.ErrSourceLineageUnknown
		}
		if err := store.VerifyKnowledgeAutoTagTaskInput(ctx, p); err != nil {
			return count, err
		}
		prepared, taskID, release, prepareErr := store.PrepareNextcloudAutoTagRecovery(ctx, row.Payload)
		if prepareErr != nil {
			return count, prepareErr
		}
		options := []asynq.Option{
			asynq.Queue(types.QueueSummary),
			asynq.MaxRetry(2),
			asynq.Timeout(2 * time.Minute),
			asynq.Unique(45 * time.Second),
		}
		if taskID != "" {
			options = append(options, asynq.TaskID(taskID))
		}
		task := asynq.NewTask(types.TypeKnowledgeAutoTag, prepared, options...)
		_, enqueueErr := queue.Enqueue(task)
		release()
		if err := enqueueErr; err != nil && !errors.Is(err, asynq.ErrTaskIDConflict) &&
			!errors.Is(err, asynq.ErrDuplicateTask) {
			return count, err
		}
		if err := store.NoteKnowledgeAutoTagCompletionDelivery(ctx, row.ID); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}
