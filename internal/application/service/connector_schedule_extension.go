package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

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
	admitted, err := s.sourceInputs.RestoreConnectorSchedule(ctx, payload, taskID)
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
	// Only this in-memory invocation receives the actor. Keep persisted queue
	// bytes actor-free and resolve the current grant again on every delivery.
	payload.Initiator = admitted.Initiator
	body, err := json.Marshal(&payload)
	if err != nil {
		return err
	}
	run := *s.DataSourceService
	run.dsRepo = &connectorScheduleRead{run.dsRepo, admitted.Source}
	return run.ProcessSync(admitted.Context(ctx), asynq.NewTask(task.Type(), body))
}
