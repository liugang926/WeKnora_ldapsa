package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"gorm.io/gorm"
)

// The decorator embeds the concrete service so optional fork methods (including
// Nextcloud migration publication) and future upstream methods remain available.
type connectorScheduleService struct{ *DataSourceService }

func (s *DataSourceService) baseDataSourceService() *DataSourceService { return s }

type configuredDataSourceService interface{ baseDataSourceService() *DataSourceService }

// WithConnectorScheduleIdentity installs the fork-only schedule adapter on the actual service.
func WithConnectorScheduleIdentity(base interfaces.DataSourceService) interfaces.DataSourceService {
	s := base.(*DataSourceService)
	s.dsRepo = repository.WithConnectorScheduleGrants(s.dsRepo)
	return &connectorScheduleService{s}
}

func (s *connectorScheduleService) UpdateDataSource(ctx context.Context,
	ds *types.DataSource,
) (*types.DataSource, error) {
	if ds != nil && ds.Type == types.ConnectorTypeDingTalk && len(ds.Config) != 0 {
		existing, err := s.GetDataSource(ctx, ds.ID)
		if err != nil {
			return nil, err
		}
		if existing.Type == types.ConnectorTypeDingTalk && unchangedDingTalkEditorConfig(existing, ds) {
			// The core keeps the stored config when it is omitted from a PUT.
			// Re-encrypting the same credentials would advance the DB revision
			// and invalidate both the saved grant and admitted work.
			ds.Config = nil
		}
	}
	return s.DataSourceService.UpdateDataSource(repository.WithConnectorScheduleSave(ctx), ds)
}

func (s *connectorScheduleService) ResumeDataSource(ctx context.Context, id string) error {
	return s.DataSourceService.ResumeDataSource(repository.WithConnectorScheduleSave(ctx), id)
}

type connectorScheduleRead struct {
	interfaces.DataSourceRepository
	first *types.DataSource
}

func (r *connectorScheduleRead) FindByID(ctx context.Context, id string) (*types.DataSource, error) {
	if r.first == nil {
		return r.DataSourceRepository.FindByID(ctx, id)
	}
	if r.first.ID != id {
		return nil, repository.ErrConnectorScheduleRunBinding
	}
	source := r.first
	r.first = nil
	return source, nil
}

func (s *connectorScheduleService) ProcessSync(ctx context.Context, task *asynq.Task) error {
	var payload types.DataSourceSyncPayload
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		return err
	}
	taskID, _ := asynq.GetTaskID(ctx)
	if taskID == "" {
		taskID, _ = types.TaskExecutionIDFromContext(ctx)
	}
	if payload.Trigger != "schedule" && !strings.HasPrefix(taskID, "dssync:") {
		if s.sourceInputs != nil && payload.SyncLogID != "" {
			scheduled, err := s.sourceInputs.IsOrdinaryScheduledRun(ctx, payload.SyncLogID)
			if err != nil {
				return err
			}
			if scheduled {
				return fmt.Errorf("%w: %w", asynq.SkipRetry, repository.ErrConnectorScheduleRunBinding)
			}
		}
		return s.DataSourceService.ProcessSync(ctx, task)
	}
	if s.sourceInputs == nil {
		return types.ErrSourceLineageUnknown
	}
	attempt, maxRetry := nextcloudSyncRetryMetadata(ctx)
	if attempt < 0 || attempt > maxRetry {
		return fmt.Errorf("%w: %w", asynq.SkipRetry, repository.ErrConnectorScheduleRunBinding)
	}
	admitted, err := s.sourceInputs.RestoreConnectorSchedule(ctx, payload, taskID, attempt)
	if errors.Is(err, gorm.ErrRecordNotFound) && payload.Initiator == (types.TaskInitiator{}) {
		// The ordinary service closes a deleted source's log after verifying its binding.
		// A mismatched source tenant must never receive a saved identity here.
		if missing, lookup := s.dsRepo.FindByID(ctx, payload.DataSourceID); lookup != nil && missing == nil {
			return s.DataSourceService.ProcessSync(ctx, task)
		}
	}
	if errors.Is(err, repository.ErrConnectorScheduleGrantRequired) ||
		errors.Is(err, repository.ErrConnectorScheduleGrantRevoked) ||
		errors.Is(err, repository.ErrConnectorScheduleRunBinding) || errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("%w: %w", asynq.SkipRetry, err)
	}
	if err != nil {
		return err
	}
	if admitted == nil {
		return s.DataSourceService.ProcessSync(ctx, task)
	}
	return s.processConnectorSchedule(ctx, task.Type(), payload, admitted, attempt, maxRetry)
}

func (s *connectorScheduleService) processConnectorSchedule(ctx context.Context, taskType string,
	payload types.DataSourceSyncPayload, admitted *repository.ConnectorScheduleExecution, attempt, maxRetry int,
) (err error) {
	runCtx, cancelRun := admitted.Context(ctx)
	defer cancelRun()
	defer func() {
		crash := recover()
		if crash == nil && err == nil {
			return
		}
		finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		retryable := attempt < maxRetry && !errors.Is(err, asynq.SkipRetry) && !errors.Is(err, asynq.RevokeTask)
		finishErr := s.sourceInputs.FinishConnectorScheduleError(finishCtx, admitted, attempt, retryable)
		if crash != nil {
			// Preserve the queue's panic handling after recording the failed attempt.
			panic(crash)
		}
		if finishErr != nil {
			err = errors.Join(err, finishErr)
		}
	}()
	// Only this in-memory invocation receives the actor. Keep persisted queue
	// bytes actor-free and resolve the current grant again on every delivery.
	payload.Initiator = admitted.Initiator
	body, err := json.Marshal(&payload)
	if err != nil {
		return err
	}
	run := *s.DataSourceService
	run.dsRepo = &connectorScheduleRead{run.dsRepo, admitted.Source}
	return run.ProcessSync(runCtx, asynq.NewTask(taskType, body))
}
