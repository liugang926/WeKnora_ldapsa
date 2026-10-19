package router

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
)

// Only an explicitly temporary failure releases an existing receipt for the
// same queue task to retry. Permanent outcomes and exhausted retries consume it.
func retryableNextcloudMachineFailure(err error) bool {
	if err == nil || errors.Is(err, asynq.SkipRetry) || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, driver.ErrBadConn) || errors.Is(err,
		access.ErrNextcloudPublicationUnavailable) {
		return true
	}
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		return true
	}
	var postgres interface{ SQLState() string }
	if errors.As(err, &postgres) {
		switch postgres.SQLState() {
		case "40001", "40P01", "55P03":
			return true
		}
	}
	return false
}

func runNextcloudMachineTask(ctx context.Context, task *asynq.Task,
	store *repository.KnowledgeInputProvenanceStore, next asynq.Handler,
) (handled bool, result error) {
	var envelope struct {
		Receipt struct {
			ID string `json:"id"`
		} `json:"_nextcloud_machine_task"`
	}
	if json.Unmarshal(task.Payload(), &envelope) != nil || envelope.Receipt.ID == "" {
		return false, nil
	}
	if store == nil {
		return true, errors.Join(repository.ErrNextcloudMachineTaskInvalid, asynq.SkipRetry)
	}
	taskID, ok := asynq.GetTaskID(ctx)
	if !ok || taskID == "" {
		taskID, _ = types.TaskExecutionIDFromContext(ctx)
	}
	retried, maxRetry, known := types.TaskRetryMetadataFromContext(ctx)
	if !known {
		retried, _ = asynq.GetRetryCount(ctx)
		maxRetry, _ = asynq.GetMaxRetry(ctx)
	}
	reject := func(err error) error {
		if retryableNextcloudMachineFailure(err) && retried < maxRetry {
			return err
		}
		_ = store.RejectNextcloudMachineTask(ctx, task.Type(), task.Payload(), taskID)
		return errors.Join(err, asynq.SkipRetry)
	}
	current, closeConfiguration, err := store.BeginNextcloudBuildConfigurationRead(ctx)
	if err != nil {
		return true, reject(err)
	}
	defer closeConfiguration()
	current, finish, err := store.BeginNextcloudMachineTask(current, task.Type(), task.Payload(), taskID)
	if err != nil {
		return true, reject(err)
	}
	result = next.ProcessTask(current, task)
	retryable := retryableNextcloudMachineFailure(result) && retried < maxRetry
	if err := finish(result == nil, retryable); err != nil {
		result = errors.Join(result, err, asynq.SkipRetry)
	}
	if result != nil && !retryable {
		result = errors.Join(result, asynq.SkipRetry)
	}
	return true, result
}
