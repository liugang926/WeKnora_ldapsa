package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/textproto"
	"reflect"
	"slices"
	"strings"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/application/access"
	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/tracing/langfuse"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	secutils "github.com/Tencent/WeKnora/internal/utils"
	"github.com/hibiken/asynq"
)

// DataSourceService implements the DataSourceService interface
type DataSourceService struct {
	dsRepo                  interfaces.DataSourceRepository
	syncLogRepo             interfaces.SyncLogRepository
	knowledgeService        interfaces.KnowledgeService
	kbService               interfaces.KnowledgeBaseService
	taskEnqueuer            interfaces.TaskEnqueuer
	connectorRegistry       *datasource.ConnectorRegistry
	scheduler               *datasource.Scheduler
	tenantRepo              interfaces.TenantRepository
	tagService              interfaces.KnowledgeTagService
	audit                   interfaces.AuditLogService
	nextcloudEventInbox     *apprepo.NextcloudEventInboxRepository
	nextcloudSourcePairings *apprepo.NextcloudSourcePairingRepository
}

// NewDataSourceService creates a new data source service
func NewDataSourceService(
	dsRepo interfaces.DataSourceRepository,
	syncLogRepo interfaces.SyncLogRepository,
	knowledgeService interfaces.KnowledgeService,
	kbService interfaces.KnowledgeBaseService,
	taskEnqueuer interfaces.TaskEnqueuer,
	connectorRegistry *datasource.ConnectorRegistry,
	scheduler *datasource.Scheduler,
	tenantRepo interfaces.TenantRepository,
	tagService interfaces.KnowledgeTagService,
	audit interfaces.AuditLogService,
	nextcloudEventInbox *apprepo.NextcloudEventInboxRepository,
	nextcloudSourcePairings *apprepo.NextcloudSourcePairingRepository,
) interfaces.DataSourceService {
	return &DataSourceService{
		dsRepo:                  dsRepo,
		syncLogRepo:             syncLogRepo,
		knowledgeService:        knowledgeService,
		kbService:               kbService,
		taskEnqueuer:            taskEnqueuer,
		connectorRegistry:       connectorRegistry,
		scheduler:               scheduler,
		tenantRepo:              tenantRepo,
		tagService:              tagService,
		audit:                   audit,
		nextcloudEventInbox:     nextcloudEventInbox,
		nextcloudSourcePairings: nextcloudSourcePairings,
	}
}

// CreateDataSource creates a new data source configuration
func (s *DataSourceService) CreateDataSource(ctx context.Context, ds *types.DataSource) (*types.DataSource, error) {
	if ds == nil {
		return nil, datasource.ErrDataSourceInvalid
	}
	if ds.Type == types.ConnectorTypeNextcloud {
		return nil, apperrors.NewProtocolError(errors.New(
			"nextcloud sources require the source-pairing endpoint",
		), "Nextcloud sources require the source-pairing endpoint")
	}

	// Validate knowledge base exists
	kb, err := s.kbService.GetKnowledgeBaseByID(ctx, ds.KnowledgeBaseID)
	if err != nil || kb == nil {
		return nil, datasource.ErrKnowledgeBaseNotFound
	}
	if kb.TenantID != ds.TenantID {
		return nil, datasource.ErrKnowledgeBaseNotFound
	}

	// Validate connector type
	_, err = s.connectorRegistry.Get(ds.Type)
	if err != nil {
		return nil, err
	}

	// Validate configuration
	if cfg, err := ds.ParseConfig(); err == nil && cfg != nil {
		cfg.StripNonSecretCredentials(ds.Type)
		if blob, err := cfg.ToJSON(); err == nil {
			ds.Config = blob
		}
	}
	if err := s.validateDataSourceConfig(ctx, ds); err != nil {
		return nil, err
	}

	// Create in database
	if err := s.dsRepo.Create(ctx, ds); err != nil {
		logger.Errorf(ctx, "failed to create data source: %v", err)
		return nil, err
	}

	// Register cron schedule if configured
	if ds.SyncSchedule != "" && ds.Status == types.DataSourceStatusActive {
		if err := s.scheduler.AddOrUpdate(ds); err != nil {
			logger.Warnf(ctx, "failed to register cron for ds=%s: %v", ds.ID, err)
		}
	}

	logger.Infof(ctx, "data source created: id=%s type=%s kb=%s", ds.ID, ds.Type, ds.KnowledgeBaseID)
	recordKBActivity(ctx, s.audit, ds.TenantID, ds.KnowledgeBaseID, types.AuditActionDataSourceCreated,
		"data_source", ds.ID, types.AuditOutcomeSuccess,
		map[string]any{"name": ds.Name, "type": ds.Type})
	return ds, nil
}

// GetDataSource retrieves a data source by ID
func (s *DataSourceService) GetDataSource(ctx context.Context, id string) (*types.DataSource, error) {
	ds, err := s.dsRepo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	return ds, nil
}

// ListDataSources lists all data sources for a knowledge base
func (s *DataSourceService) ListDataSources(ctx context.Context, kbID string) ([]*types.DataSource, error) {
	dataSources, err := s.dsRepo.FindByKnowledgeBase(ctx, kbID)
	if err != nil {
		logger.Errorf(ctx, "failed to list data sources: %v", err)
		return nil, err
	}

	// Attach latest sync log to each data source
	for _, ds := range dataSources {
		log, _ := s.syncLogRepo.FindLatest(ctx, ds.ID)
		if log != nil {
			ds.LatestSyncLog = log
		}
	}

	return dataSources, nil
}

// UpdateDataSource updates an existing data source
func (s *DataSourceService) UpdateDataSource(ctx context.Context, ds *types.DataSource) (*types.DataSource, error) {
	if ds == nil || ds.ID == "" {
		return nil, datasource.ErrDataSourceInvalid
	}

	// Verify data source exists
	existing, err := s.dsRepo.FindByID(ctx, ds.ID)
	if err != nil {
		return nil, err
	}
	// Reject a Nextcloud type transition before validating any proposed
	// connector. The repository also rejects it, but validation makes an
	// outbound request with the preserved credential before that DB check.
	if (existing.Type == types.ConnectorTypeNextcloud || ds.Type == types.ConnectorTypeNextcloud) &&
		ds.Type != existing.Type {
		return nil, apperrors.NewProtocolError(errors.New(
			"nextcloud data source type cannot change",
		), "Nextcloud data source type cannot change")
	}
	if existing.Type == types.ConnectorTypeNextcloud && len(ds.Config) > 0 {
		stored, storedErr := existing.ParseConfig()
		proposed, proposedErr := ds.ParseConfig()
		if storedErr != nil || proposedErr != nil || stored == nil || proposed == nil {
			return nil, datasource.ErrInvalidConfig
		}
		// PUT /datasource retains the stored secret. Never probe a newly
		// supplied address with that secret, even when the eventual repository
		// write would reject the update. Exact URL equality is deliberate: a
		// seemingly equivalent spelling may resolve to a different endpoint.
		if stored.HasConfiguredCredentials(existing.Type) {
			storedURL, storedOK := stored.Settings["base_url"].(string)
			proposedURL, proposedOK := proposed.Settings["base_url"].(string)
			if !storedOK || !proposedOK || storedURL != proposedURL {
				return nil, apperrors.NewProtocolError(
					errors.New(
						("nextcloud base URL cannot change while credentials are " +
							"configured")), "Nextcloud base URL cannot change while credentials are configured")
			}
		}
	}

	if ds.KnowledgeBaseID == "" {
		ds.KnowledgeBaseID = existing.KnowledgeBaseID
	}
	if ds.KnowledgeBaseID != existing.KnowledgeBaseID {
		return nil, fmt.Errorf("changing knowledge base is not allowed")
	}

	if ds.TenantID == 0 {
		ds.TenantID = existing.TenantID
	}
	if ds.TenantID != existing.TenantID {
		return nil, datasource.ErrDataSourceInvalid
	}

	// Credentials NEVER flow through this endpoint — they live behind the
	// /credentials subresource. Force-preserve the stored credentials map
	// regardless of what the body says. Log a warning if a stale caller
	// passes one so we can spot them and migrate later. Non-credential
	// fields of Config (Type / ResourceIDs / Settings) flow through.
	var mergedCfg, existingParsedCfg *types.DataSourceConfig
	if len(ds.Config) > 0 {
		incomingCfg, parseIncErr := ds.ParseConfig()
		existingCfg, parseExErr := existing.ParseConfig()
		if parseIncErr == nil && parseExErr == nil && incomingCfg != nil {
			if incomingCfg.HasCredentials() {
				logger.Warnf(ctx,
					"deprecated: credentials in PUT /datasource/%s body are ignored; use PUT /credentials instead",
					secutils.SanitizeForLog(ds.ID))
			}
			merged := *incomingCfg
			if existingCfg != nil {
				merged.Credentials = existingCfg.Credentials
			} else {
				merged.Credentials = nil
			}
			merged.StripNonSecretCredentials(ds.Type)
			if blob, err := merged.ToJSON(); err == nil {
				ds.Config = blob
			}
			mergedCfg = &merged
			existingParsedCfg = existingCfg
		}
	}

	// Validate new configuration if non-credential fields changed. Skip
	// when there are no stored credentials yet (validators would fail with
	// no token to call the live API) and when the parsed config is
	// structurally identical.
	configActuallyChanged := true
	if mergedCfg != nil && existingParsedCfg != nil {
		configActuallyChanged = !reflect.DeepEqual(*mergedCfg, *existingParsedCfg)
	}
	hasCreds := mergedCfg != nil && mergedCfg.HasConfiguredCredentials(ds.Type)
	if hasCreds && (ds.Type != existing.Type || configActuallyChanged) {
		if err := s.validateDataSourceConfig(ctx, ds); err != nil {
			return nil, err
		}
	}

	if err := s.dsRepo.Update(ctx, ds); err != nil {
		logger.Errorf(ctx, "failed to update data source: %v", err)
		return nil, err
	}

	// Update cron schedule
	if err := s.scheduler.AddOrUpdate(ds); err != nil {
		logger.Warnf(ctx, "failed to update cron for ds=%s: %v", ds.ID, err)
	}

	logger.Infof(ctx, "data source updated: id=%s", ds.ID)
	recordKBActivity(ctx, s.audit, ds.TenantID, ds.KnowledgeBaseID, types.AuditActionDataSourceUpdated,
		"data_source", ds.ID, types.AuditOutcomeSuccess,
		map[string]any{"name": ds.Name, "type": ds.Type, "changed_fields": []string{"settings"}})
	return ds, nil
}

// UpdateDataSourceCredentials replaces the connector credential map. This is
// a single atomic write; the previous credential set is discarded entirely
// (callers cannot patch individual keys because half-configured connector
// auth is meaningless). After persisting, the live connection is validated
// so the caller learns immediately if the new credentials are wrong.
func (s *DataSourceService) UpdateDataSourceCredentials(
	ctx context.Context, id string, credentials map[string]interface{},
) (*types.DataSource, error) {
	if id == "" {
		return nil, datasource.ErrDataSourceInvalid
	}
	existing, err := s.dsRepo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	parsed, err := existing.ParseConfig()
	if err != nil {
		return nil, err
	}
	if parsed == nil {
		parsed = &types.DataSourceConfig{Type: existing.Type}
	}
	parsed.Credentials = credentials
	parsed.StripNonSecretCredentials(existing.Type)
	blob, err := parsed.ToJSON()
	if err != nil {
		return nil, err
	}
	existing.Config = blob

	// Run live validation now that the credentials are in place — surfaces
	// "wrong token" feedback immediately to the user instead of waiting for
	// the next scheduled sync.
	if err := s.validateDataSourceConfig(ctx, existing); err != nil {
		return nil, err
	}
	if err := s.dsRepo.Update(apprepo.WithNextcloudCredentialWrite(ctx), existing); err != nil {
		return nil, err
	}
	logger.Infof(ctx, "DataSource credentials updated: id=%s", secutils.SanitizeForLog(id))
	recordKBActivity(ctx, s.audit, existing.TenantID, existing.KnowledgeBaseID, types.AuditActionDataSourceUpdated,
		"data_source", existing.ID, types.AuditOutcomeSuccess,
		map[string]any{"name": existing.Name, "type": existing.Type, "changed_fields": []string{"credentials"}})
	return existing, nil
}

// ClearDataSourceCredentials wipes the connector credential map without
// touching any other config field. Idempotent.
func (s *DataSourceService) ClearDataSourceCredentials(ctx context.Context, id string) error {
	if id == "" {
		return datasource.ErrDataSourceInvalid
	}
	existing, err := s.dsRepo.FindByID(ctx, id)
	if err != nil {
		return err
	}
	parsed, err := existing.ParseConfig()
	if err != nil {
		return err
	}
	if parsed == nil {
		return nil
	}
	parsed.StripNonSecretCredentials(existing.Type)
	if !parsed.HasConfiguredCredentials(existing.Type) {
		blob, err := parsed.ToJSON()
		if err != nil {
			return err
		}
		existing.Config = blob
		return s.dsRepo.Update(apprepo.WithNextcloudCredentialWrite(ctx), existing)
	}
	parsed.Credentials = nil
	blob, err := parsed.ToJSON()
	if err != nil {
		return err
	}
	existing.Config = blob
	if err := s.dsRepo.Update(apprepo.WithNextcloudCredentialWrite(ctx), existing); err != nil {
		return err
	}
	logger.Infof(ctx, "DataSource credentials cleared by user: id=%s", secutils.SanitizeForLog(id))
	recordKBActivity(ctx, s.audit, existing.TenantID, existing.KnowledgeBaseID, types.AuditActionDataSourceUpdated,
		"data_source", existing.ID, types.AuditOutcomeSuccess,
		map[string]any{"name": existing.Name, "type": existing.Type, "changed_fields": []string{"credentials"}})
	return nil
}

// DeleteDataSource deletes a data source (soft delete)
func (s *DataSourceService) DeleteDataSource(ctx context.Context, id string) error {
	// Verify data source exists
	existing, err := s.dsRepo.FindByID(ctx, id)
	if err != nil {
		return err
	}

	if err := s.dsRepo.Delete(ctx, id); err != nil {
		logger.Errorf(ctx, "failed to delete data source: %v", err)
		return err
	}

	// Remove cron schedule
	s.scheduler.Remove(id)

	// Cancel any pending/running sync logs so queued asynq tasks won't retry
	if err := s.syncLogRepo.CancelPendingByDataSource(ctx, id); err != nil {
		logger.Warnf(ctx, "failed to cancel pending sync logs for ds=%s: %v", id, err)
	}

	logger.Infof(ctx, "data source deleted: id=%s", id)
	recordKBActivity(ctx, s.audit, existing.TenantID, existing.KnowledgeBaseID, types.AuditActionDataSourceDeleted,
		"data_source", existing.ID, types.AuditOutcomeSuccess,
		map[string]any{"name": existing.Name, "type": existing.Type})
	return nil
}

// ValidateConnection tests the connection to an external data source
func (s *DataSourceService) ValidateConnection(ctx context.Context, dsID string) error {
	ds, err := s.GetDataSource(ctx, dsID)
	if err != nil {
		return err
	}

	// Get connector
	connector, err := s.connectorRegistry.Get(ds.Type)
	if err != nil {
		return err
	}

	// Parse configuration
	config, err := ds.ParseConfig()
	if err != nil {
		return datasource.ErrInvalidConfig
	}

	// Validate connection
	if err := connector.Validate(ctx, config); err != nil {
		// Update data source with error
		ds.Status = types.DataSourceStatusError
		ds.ErrorMessage = apperrors.PublicMessage(err)
		_ = s.dsRepo.Update(ctx, ds)
		return err
	}

	// Clear error if it was previously in error state
	if ds.Status == types.DataSourceStatusError {
		ds.Status = types.DataSourceStatusActive
		ds.ErrorMessage = ""
		_ = s.dsRepo.Update(ctx, ds)
	}

	return nil
}

// ListAvailableResources lists resources available for sync in the external system.
// parentID enables lazy (on-demand) loading of hierarchical resources: pass "" to
// list the top level, or a resource's ExternalID to list only its direct children.
func (s *DataSourceService) ListAvailableResources(
	ctx context.Context, dsID string, parentID string,
) ([]types.Resource, error) {
	ds, err := s.GetDataSource(ctx, dsID)
	if err != nil {
		return nil, err
	}

	// Get connector
	connector, err := s.connectorRegistry.Get(ds.Type)
	if err != nil {
		return nil, err
	}

	// Parse configuration
	config, err := ds.ParseConfig()
	if err != nil {
		return nil, datasource.ErrInvalidConfig
	}

	// List resources
	resources, err := connector.ListResources(ctx, config, parentID)
	if err != nil {
		logger.Errorf(ctx, "failed to list resources: %v", err)
		return nil, err
	}

	return resources, nil
}

// ResolveResourceAncestors resolves the ancestor ExternalIDs needed to reveal the
// given resources in a lazily-loaded picker (see the connector method for details).
func (s *DataSourceService) ResolveResourceAncestors(
	ctx context.Context, dsID string, resourceIDs []string,
) ([]string, error) {
	if len(resourceIDs) == 0 {
		return []string{}, nil
	}

	ds, err := s.GetDataSource(ctx, dsID)
	if err != nil {
		return nil, err
	}

	connector, err := s.connectorRegistry.Get(ds.Type)
	if err != nil {
		return nil, err
	}

	config, err := ds.ParseConfig()
	if err != nil {
		return nil, datasource.ErrInvalidConfig
	}

	ancestors, err := connector.ResolveResourceAncestors(ctx, config, resourceIDs)
	if err != nil {
		logger.Errorf(ctx, "failed to resolve resource ancestors: %v", err)
		return nil, err
	}

	return ancestors, nil
}

// ManualSync triggers an immediate sync for a data source
func (s *DataSourceService) ManualSync(ctx context.Context, dsID string) (*types.SyncLog, error) {
	ds, err := s.GetDataSource(ctx, dsID)
	if err != nil {
		return nil, err
	}
	if ds.Type == types.ConnectorTypeNextcloud && s.nextcloudSourcePairings != nil {
		active, pairErr := s.nextcloudSourcePairings.HasActiveSourcePairing(ctx, ds)
		if pairErr != nil {
			return nil, pairErr
		}
		if !active {
			return nil, apprepo.ErrNextcloudSourcePairingConflict
		}
	}

	if ds.Status != types.DataSourceStatusActive &&
		ds.Status != types.DataSourceStatusError &&
		ds.Status != types.DataSourceStatusPaused {
		return nil, datasource.ErrDataSourceNotActive
	}

	// Create sync log
	syncLog := &types.SyncLog{
		DataSourceID: dsID,
		TenantID:     ds.TenantID,
		Status:       types.SyncLogStatusRunning,
		StartedAt:    time.Now().UTC(),
	}
	if ds.Type == types.ConnectorTypeNextcloud {
		syncLog.RecoveryVersion = 1
		syncLog.RecoveryTrigger = "manual"
	}

	if err := s.syncLogRepo.Create(ctx, syncLog); err != nil {
		logger.Errorf(ctx, "failed to create sync log: %v", err)
		return nil, err
	}

	// Enqueue sync task
	payload := &types.DataSourceSyncPayload{
		DataSourceID: dsID,
		TenantID:     ds.TenantID,
		SyncLogID:    syncLog.ID,
		ForceFull:    false,
		Initiator:    types.TaskInitiatorFromContext(ctx),
		Trigger:      "manual",
	}
	langfuse.InjectTracing(ctx, payload)

	payloadJSON, _ := json.Marshal(payload)
	task := asynq.NewTask(types.TypeDataSourceSync, payloadJSON,
		asynq.Queue(types.QueueSync), asynq.MaxRetry(5), asynq.Timeout(2*time.Hour))

	options := []asynq.Option{}
	if ds.Type == types.ConnectorTypeNextcloud {
		options = append(options, asynq.TaskID(datasource.NextcloudSyncTaskID(syncLog.ID)))
	}
	info, err := s.taskEnqueuer.Enqueue(task, options...)
	if err != nil {
		logger.Errorf(ctx, "failed to enqueue sync task: %v", err)
		if ds.Type == types.ConnectorTypeNextcloud {
			// A queue may accept the task and lose its reply. Do not write a
			// stale running snapshot over a worker that already finished.
			marked, markErr := s.syncLogRepo.MarkNextcloudEnqueueUncertain(ctx,
				syncLog.ID, dsID, ds.TenantID, "manual", datasource.NextcloudSyncTaskID(syncLog.ID))
			if marked {
				syncLog.ErrorMessage = datasource.NextcloudSyncEnqueueUncertain
			}
			return syncLog, errors.Join(datasource.ErrSyncEnqueueUncertain, markErr)
		}
		syncLog.Status = types.SyncLogStatusFailed
		syncLog.FinishedAt = timePtr(time.Now().UTC())
		syncLog.ErrorMessage = apperrors.PublicMessage(err)
		_ = s.syncLogRepo.Update(ctx, syncLog)
		if ds.Status != types.DataSourceStatusPaused {
			ds.Status = types.DataSourceStatusError
		}
		ds.ErrorMessage = fmt.Sprintf("Failed to enqueue sync: %v", err)
		_ = s.dsRepo.Update(ctx, ds)
		recordKBActivity(ctx, s.audit, ds.TenantID, ds.KnowledgeBaseID, types.AuditActionDataSourceSyncFailed,
			"data_source", ds.ID, types.AuditOutcomeFailed,
			map[string]any{"name": ds.Name, "type": ds.Type, "sync_log_id": syncLog.ID, "trigger": "manual"})
		return nil, err
	}

	if ds.Type == types.ConnectorTypeNextcloud {
		stored, readErr := s.syncLogRepo.FindByID(ctx, syncLog.ID)
		returnedID := ""
		if info != nil {
			returnedID = info.ID
		}
		if readErr != nil || !datasource.NextcloudSyncReceiptAccepted(stored,
			dsID, ds.TenantID, "manual", returnedID) {
			// A late queue acceptance cannot turn a recovered failed log into
			// a successful API response. Unknown receipts remain fail-closed.
			_, markErr := s.syncLogRepo.MarkNextcloudEnqueueUncertain(ctx,
				syncLog.ID, dsID, ds.TenantID, "manual", datasource.NextcloudSyncTaskID(syncLog.ID))
			if stored != nil {
				syncLog = stored
			}
			return syncLog, errors.Join(datasource.ErrSyncEnqueueUncertain, readErr, markErr)
		}
		syncLog = stored
	}

	logger.Infof(ctx, "sync task enqueued: ds=%s syncLog=%s", dsID, syncLog.ID)
	recordKBActivity(ctx, s.audit, ds.TenantID, ds.KnowledgeBaseID, types.AuditActionDataSourceSyncStarted,
		"data_source", ds.ID, types.AuditOutcomeAccepted,
		map[string]any{
			"name": ds.Name, "type": ds.Type, "sync_log_id": syncLog.ID,
			"task_id": info.ID, "trigger": "manual", "processing_status": "pending",
		})
	return syncLog, nil
}

// PauseDataSource pauses a data source's scheduled syncs
func (s *DataSourceService) PauseDataSource(ctx context.Context, id string) error {
	ds, err := s.GetDataSource(ctx, id)
	if err != nil {
		return err
	}

	ds.Status = types.DataSourceStatusPaused
	if err := s.dsRepo.Update(ctx, ds); err != nil {
		logger.Errorf(ctx, "failed to pause data source: %v", err)
		return err
	}

	// Remove cron schedule
	s.scheduler.Remove(id)

	logger.Infof(ctx, "data source paused: id=%s", id)
	recordKBActivity(ctx, s.audit, ds.TenantID, ds.KnowledgeBaseID, types.AuditActionDataSourcePaused,
		"data_source", ds.ID, types.AuditOutcomeSuccess, map[string]any{"name": ds.Name, "type": ds.Type})
	return nil
}

// ResumeDataSource resumes a paused data source
func (s *DataSourceService) ResumeDataSource(ctx context.Context, id string) error {
	ds, err := s.GetDataSource(ctx, id)
	if err != nil {
		return err
	}
	if ds.Type == types.ConnectorTypeNextcloud && s.nextcloudSourcePairings != nil {
		active, pairErr := s.nextcloudSourcePairings.HasActiveSourcePairing(ctx, ds)
		if pairErr != nil {
			return pairErr
		}
		if !active {
			return apprepo.ErrNextcloudSourcePairingConflict
		}
	}

	ds.Status = types.DataSourceStatusActive
	if err := s.dsRepo.Update(ctx, ds); err != nil {
		logger.Errorf(ctx, "failed to resume data source: %v", err)
		return err
	}

	// Re-register cron schedule
	if err := s.scheduler.AddOrUpdate(ds); err != nil {
		logger.Warnf(ctx, "failed to re-register cron for ds=%s: %v", ds.ID, err)
	}

	logger.Infof(ctx, "data source resumed: id=%s", id)
	recordKBActivity(ctx, s.audit, ds.TenantID, ds.KnowledgeBaseID, types.AuditActionDataSourceResumed,
		"data_source", ds.ID, types.AuditOutcomeSuccess, map[string]any{"name": ds.Name, "type": ds.Type})
	return nil
}

// GetSyncLogs retrieves sync history for a data source
func (s *DataSourceService) GetSyncLogs(ctx context.Context, dsID string, limit int, offset int) ([]*types.SyncLog, error) {
	logs, err := s.syncLogRepo.FindByDataSource(ctx, dsID, limit, offset)
	if err != nil {
		logger.Errorf(ctx, "failed to get sync logs: %v", err)
		return nil, err
	}
	return logs, nil
}

// GetSyncLog retrieves a specific sync log entry
func (s *DataSourceService) GetSyncLog(ctx context.Context, syncLogID string) (*types.SyncLog, error) {
	log, err := s.syncLogRepo.FindByID(ctx, syncLogID)
	if err != nil {
		return nil, err
	}
	return log, nil
}

// recordPreStreamSyncFailure also covers connector/configuration errors before
// the streaming handler exists. A file retry failure belongs to its sync log;
// the paired source remains active for the next durable backoff claim.
func (s *DataSourceService) recordPreStreamSyncFailure(ctx context.Context,
	ds *types.DataSource, syncLog *types.SyncLog, message string, wasPaused bool,
) {
	syncLog.Status = types.SyncLogStatusFailed
	syncLog.FinishedAt = timePtr(time.Now().UTC())
	syncLog.ErrorMessage = message
	if ds.Type == types.ConnectorTypeNextcloud {
		if isNextcloudCandidateRetryRun(ctx) {
			_ = s.syncLogRepo.Update(ctx, syncLog)
			return
		}
		expectedStatus := ds.Status
		if !wasPaused {
			ds.Status = types.DataSourceStatusError
		}
		ds.ErrorMessage = message
		if _, err := s.dsRepo.UpdateNextcloudSyncStateCAS(ctx, ds, expectedStatus); err != nil {
			// Keep the running log as the admission fence if the source
			// state could not be written. A CAS miss is safe to release.
			logger.Errorf(ctx, "failed to update Nextcloud source after pre-stream error: %v", err)
			return
		}
		if err := s.syncLogRepo.UpdateResult(ctx, syncLog); err != nil {
			logger.Errorf(ctx, "failed to update Nextcloud pre-stream sync log: %v", err)
		}
		return
	}
	_ = s.syncLogRepo.Update(ctx, syncLog)
	if !wasPaused {
		ds.Status = types.DataSourceStatusError
	}
	ds.ErrorMessage = message
	_ = s.dsRepo.Update(ctx, ds)
}

type (
	nextcloudCandidateRetryRunKey struct{}
	nextcloudEventRunKey          struct{}
	nextcloudEventFailureKey      struct{}
)

func isNextcloudCandidateRetryRun(ctx context.Context) bool {
	retry, _ := ctx.Value(nextcloudCandidateRetryRunKey{}).(bool)
	return retry
}

func isNextcloudEventRun(ctx context.Context) bool {
	event, _ := ctx.Value(nextcloudEventRunKey{}).(bool)
	return event
}

func isNextcloudRetryableEventFailure(ctx context.Context) bool {
	if !isNextcloudEventRun(ctx) {
		return false
	}
	cause, _ := ctx.Value(nextcloudEventFailureKey{}).(error)
	return errors.Is(cause, datasource.ErrRetryableSource)
}

func nextcloudSyncRetryMetadata(ctx context.Context) (retried, maxRetry int) {
	if n, ok := asynq.GetRetryCount(ctx); ok {
		retried = n
		maxRetry, _ = asynq.GetMaxRetry(ctx)
		return retried, maxRetry
	}
	if n, maxAttempts, ok := types.TaskRetryMetadataFromContext(ctx); ok {
		return n, maxAttempts
	}
	return 0, 0 // Missing worker metadata cannot authorize a retry.
}

// ProcessSync handles the actual sync operation (called by asynq task)
func (s *DataSourceService) ProcessSync(ctx context.Context, task *asynq.Task) (runErr error) {
	var payload types.DataSourceSyncPayload
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		logger.Errorf(ctx, "failed to unmarshal sync payload: %v", err)
		return err
	}
	switch payload.Trigger {
	case "nextcloud_candidate_retry":
		ctx = context.WithValue(ctx, nextcloudCandidateRetryRunKey{}, true)
	case "nextcloud_event":
		ctx = context.WithValue(ctx, nextcloudEventRunKey{}, true)
	}
	if payload.Trigger == "nextcloud_event" || payload.Trigger == "nextcloud_candidate_retry" {
		if payload.Trigger == "nextcloud_candidate_retry" {
			cancelled, err := s.checkNextcloudCandidateRetry(ctx, payload, true)
			if cancelled || err != nil {
				return err
			}
		}
		if payload.Trigger == "nextcloud_event" {
			cancelled, err := s.checkNextcloudEventSync(ctx, payload)
			if cancelled || err != nil {
				return err
			}
		}
		// The dispatcher only persists an intent. A running log exists only
		// after a task actually starts, so a pre-enqueue crash is recoverable.
		log := &types.SyncLog{
			ID: payload.SyncLogID, DataSourceID: payload.DataSourceID,
			TenantID: payload.TenantID, Status: types.SyncLogStatusRunning,
			StartedAt: time.Now().UTC(),
		}
		if err := s.syncLogRepo.Create(ctx, log); err != nil {
			// An at-least-once queue can redeliver the same task. The first run
			// owns this log; never execute a second copy concurrently.
			if existing, findErr := s.syncLogRepo.FindByID(ctx, payload.SyncLogID); findErr == nil && existing != nil {
				return nil
			}
			return err
		}
		if payload.Trigger == "nextcloud_event" {
			cancelled, err := s.checkNextcloudEventSync(ctx, payload)
			if cancelled || err != nil {
				return err
			}
		} else {
			cancelled, err := s.checkNextcloudCandidateRetry(ctx, payload, true)
			if cancelled || err != nil {
				return err
			}
		}
	}
	ctx = payload.Initiator.Apply(ctx)
	taskID, _ := asynq.GetTaskID(ctx)
	ctx = withKBActivityTask(ctx, taskID, payload.Trigger)

	logger.Infof(ctx, "processing data source sync: ds=%s syncLog=%s", payload.DataSourceID, payload.SyncLogID)

	// Get data source
	ds, err := s.GetDataSource(ctx, payload.DataSourceID)
	if err != nil {
		logger.Warnf(ctx, "data source not found (likely deleted), cancelling sync: ds=%s err=%v", payload.DataSourceID, err)
		if syncLog, slErr := s.syncLogRepo.FindByID(ctx, payload.SyncLogID); slErr == nil && syncLog != nil {
			syncLog.Status = types.SyncLogStatusCanceled
			syncLog.FinishedAt = timePtr(time.Now().UTC())
			syncLog.ErrorMessage = "data source has been deleted"
			_ = s.syncLogRepo.Update(ctx, syncLog)
		}
		return nil
	}

	// Get sync log
	syncLog, err := s.syncLogRepo.FindByID(ctx, payload.SyncLogID)
	if err != nil {
		logger.Errorf(ctx, "failed to get sync log: %v", err)
		return nil
	}
	if ds.Type == types.ConnectorTypeNextcloud && syncLog.Status != types.SyncLogStatusRunning {
		return fmt.Errorf("%w: Nextcloud sync log is no longer running", asynq.SkipRetry)
	}
	if ds.Type == types.ConnectorTypeNextcloud &&
		payload.Trigger != "nextcloud_event" && payload.Trigger != "nextcloud_candidate_retry" {
		switch syncLog.RecoveryVersion {
		case 0:
			// Rows created before the recovery migration retain their original
			// execution path, including older payloads with an empty trigger.
			// No absence-based recovery is ever allowed for them.
		case 1:
			if payload.Trigger != "manual" && payload.Trigger != "schedule" {
				return fmt.Errorf("%w: unknown Nextcloud sync trigger", asynq.SkipRetry)
			}
			if taskID == "" {
				taskID, _ = types.TaskExecutionIDFromContext(ctx)
			}
			retried, maxRetry := nextcloudSyncRetryMetadata(ctx)
			token, claimErr := s.syncLogRepo.ClaimNextcloudSyncStart(ctx, syncLog.ID,
				payload.DataSourceID, payload.TenantID, payload.Trigger, taskID, retried)
			if claimErr != nil {
				return claimErr
			}
			if token == "" {
				return fmt.Errorf("%w: Nextcloud sync worker claim rejected", asynq.SkipRetry)
			}
			syncLog.ErrorMessage = ""
			defer func() {
				// A canceled worker context must not prevent its durable handoff
				// to the same Asynq task's next attempt.
				finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
				defer cancel()
				terminal := runErr == nil || errors.Is(runErr, asynq.SkipRetry) || retried >= maxRetry
				message := "sync_attempt_retry_pending"
				if terminal {
					message = "sync_attempt_stopped_before_terminal_status"
				}
				changed, finishErr := s.syncLogRepo.FinishNextcloudSyncAttempt(finishCtx,
					syncLog.ID, payload.DataSourceID, payload.TenantID, payload.Trigger,
					taskID, token, terminal, message)
				if finishErr != nil {
					runErr = errors.Join(runErr, fmt.Errorf("finish Nextcloud sync attempt: %w", finishErr))
				} else if runErr == nil && changed {
					runErr = datasource.ErrSyncFailed
				}
			}()
		default:
			return fmt.Errorf("%w: unknown Nextcloud sync recovery version", asynq.SkipRetry)
		}
	}
	// An old queued task may survive an upgrade from a generic, unpaired
	// Nextcloud source. Deny it before connector construction or any remote
	// Fetch, including the legacy batch fallback below.
	if ds.Type == types.ConnectorTypeNextcloud && s.nextcloudSourcePairings != nil {
		active, pairErr := s.nextcloudSourcePairings.HasActiveSourcePairing(ctx, ds)
		if pairErr != nil {
			return pairErr
		}
		if !active {
			syncLog.Status = types.SyncLogStatusCanceled
			syncLog.FinishedAt = timePtr(time.Now().UTC())
			syncLog.ErrorMessage = "Nextcloud source is not paired"
			if updateErr := s.syncLogRepo.Update(ctx, syncLog); updateErr != nil {
				return updateErr
			}
			return fmt.Errorf("%w: Nextcloud source is not paired", asynq.SkipRetry)
		}
	}

	kb, kbErr := s.kbService.GetKnowledgeBaseByID(ctx, ds.KnowledgeBaseID)
	if kbErr != nil {
		logger.Warnf(ctx, "knowledge base not found (likely deleted), cancelling sync: kb=%s ds=%s err=%v",
			ds.KnowledgeBaseID, payload.DataSourceID, kbErr)
		syncLog.Status = types.SyncLogStatusCanceled
		syncLog.FinishedAt = timePtr(time.Now().UTC())
		syncLog.ErrorMessage = "knowledge base has been deleted"
		_ = s.syncLogRepo.Update(ctx, syncLog)
		return nil
	}

	ctx, err = access.WithKBTaskWrite(ctx, kb, ds.TenantID)
	if err != nil {
		return fmt.Errorf("%w: data source KB does not belong to its tenant", asynq.SkipRetry)
	}
	wasPaused := ds.Status == types.DataSourceStatusPaused

	// Get connector
	connector, err := s.connectorRegistry.Get(ds.Type)
	if err != nil {
		logger.Errorf(ctx, "connector not found: type=%s", ds.Type)
		s.recordPreStreamSyncFailure(ctx, ds, syncLog, fmt.Sprintf("Connector not found: %s", ds.Type), wasPaused)
		return err
	}

	// Parse configuration
	config, err := ds.ParseConfig()
	if err != nil {
		logger.Errorf(ctx, "failed to parse config: %v", err)
		s.recordPreStreamSyncFailure(ctx, ds, syncLog, fmt.Sprintf("Invalid configuration: %v", err), wasPaused)
		return err
	}
	// Surface the KB's multimodal/VLM state to the connector so it only extracts
	// embedded images for OCR when the KB can actually ingest them (never persisted).
	config.MultimodalEnabled = kb.IsMultimodalEnabled()
	if ds.Type == types.ConnectorTypeNextcloud {
		if s.nextcloudEventInbox == nil {
			return apperrors.NewProtocolError(fmt.Errorf(
				"nextcloud source baseline verifier is unavailable",
			), "Nextcloud source baseline verifier is unavailable")
		}
		if err := s.nextcloudEventInbox.ValidateNextcloudSourceBaseline(ctx, ds); err != nil {
			s.updateSyncRunResult(ctx, ds, syncLog, &types.SyncResult{}, nil,
				types.SyncLogStatusFailed, apperrors.PublicMessage(err), wasPaused)
			return err
		}
	}

	// Streaming path: connectors that support it interleave fetch→ingest→
	// checkpoint so a large sync bounds memory and resumes after a timeout
	// instead of restarting (Tencent/WeKnora#2136). Others fall back below.
	if sc, ok := connector.(datasource.StreamingConnector); ok {
		return s.processSyncStreaming(ctx, sc, ds, syncLog, config, payload, wasPaused)
	}

	// Fetch items based on sync mode
	var items []types.FetchedItem
	var nextCursor *types.SyncCursor
	var fetchErr error

	if payload.ForceFull || ds.SyncMode == types.SyncModeFull {
		if full, ok := connector.(datasource.FullSyncWithCursor); ok {
			cursor, cursorErr := ds.ParseSyncCursor()
			if ds.Type == types.ConnectorTypeNextcloud && cursorErr != nil {
				fetchErr = fmt.Errorf("invalid Nextcloud sync cursor: %w", cursorErr)
			} else {
				items, nextCursor, fetchErr = full.FetchAllFromCursor(ctx, config, config.ResourceIDs, cursor)
			}
		} else {
			items, fetchErr = connector.FetchAll(ctx, config, config.ResourceIDs)
		}
		logger.Infof(ctx, "full sync fetched %d items", len(items))
	} else {
		// Incremental sync
		cursor, cursorErr := ds.ParseSyncCursor()
		if ds.Type == types.ConnectorTypeNextcloud && cursorErr != nil {
			fetchErr = fmt.Errorf("invalid Nextcloud sync cursor: %w", cursorErr)
		} else {
			items, nextCursor, fetchErr = connector.FetchIncremental(ctx, config, cursor)
		}
		logger.Infof(ctx, "incremental sync fetched %d items", len(items))
	}

	var fetchWarnings []string
	var partialFetch *datasource.PartialFetchError
	if errors.As(fetchErr, &partialFetch) {
		fetchWarnings = partialFetch.Details
		fetchErr = nil
	}

	if fetchErr != nil {
		// Persist connector cursor even when fetch failed so transient outages
		// (e.g. RSS feed downtime) do not force a full re-ingest on recovery.
		if nextCursor != nil && ds.Type != types.ConnectorTypeNextcloud {
			if cursorJSON, cerr := nextCursor.ToJSON(); cerr == nil {
				ds.LastSyncCursor = cursorJSON
				if uerr := s.dsRepo.UpdateSyncState(ctx, ds); uerr != nil {
					logger.Warnf(ctx, "failed to persist sync cursor after fetch error: %v", uerr)
				}
			}
		}
		logger.Errorf(ctx, "fetch operation failed: %v", fetchErr)
		s.recordPreStreamSyncFailure(
			ctx, ds, syncLog, fmt.Sprintf("Fetch failed: %v", apperrors.PublicMessage(fetchErr)), wasPaused,
		)
		return fetchErr
	}
	if ds.Type == types.ConnectorTypeNextcloud {
		observedInstanceID := ""
		if nextCursor != nil && nextCursor.ConnectorCursor != nil {
			observedInstanceID, _ = nextCursor.ConnectorCursor["instance_id"].(string)
		}
		cancelled, guardErr := s.checkNextcloudSyncActive(ctx, payload, observedInstanceID)
		if cancelled || guardErr != nil {
			return guardErr
		}
	}

	// Process fetched items and write to knowledge base
	result := &types.SyncResult{
		Total: len(items),
	}

	// Set tenant context so KnowledgeService can resolve tenant info correctly
	ctx = context.WithValue(ctx, types.TenantIDContextKey, ds.TenantID)

	tenant, err := s.tenantRepo.GetTenantByID(ctx, ds.TenantID)
	if err != nil {
		logger.Errorf(ctx, "failed to get tenant info: %v", err)
		s.recordPreStreamSyncFailure(ctx, ds, syncLog, fmt.Sprintf("Failed to get tenant info: %v", err), wasPaused)
		return err
	}
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)

	// Auto-tag: find or create a tag for this data source so synced items are easily identifiable
	autoTagIDs := s.resolveAutoTagIDs(ctx, ds)

	for _, item := range items {
		if ds.Type == types.ConnectorTypeNextcloud {
			cancelled, guardErr := s.checkNextcloudSyncActive(ctx, payload)
			if cancelled || guardErr != nil {
				return guardErr
			}
		}
		item := item
		s.applyFetchedItem(withKBActivitySuppressed(ctx), ds, &item, autoTagIDs, result)
	}
	if ds.Type == types.ConnectorTypeNextcloud {
		cancelled, guardErr := s.checkNextcloudSyncActive(ctx, payload)
		if cancelled || guardErr != nil {
			return guardErr
		}
	}

	resultJSON, _ := result.ToJSON()
	if err := allFetchedItemsFailedError(result); err != nil {
		logger.Errorf(ctx, "data source sync failed while processing fetched items: %v", err)
		s.updateSyncRunResult(
			ctx, ds, syncLog, result, resultJSON, types.SyncLogStatusFailed, apperrors.PublicMessage(err), wasPaused,
		)
		return err
	}

	// A Nextcloud cursor includes its confirmed source inventory. Advancing it
	// after any failed ingest or deletion would acknowledge source changes that
	// were never applied and could permanently skip a tombstone. Retain the
	// previous cursor so the next run retries those changes. Other connectors
	// retain their existing partial-sync checkpoint behavior.
	if nextCursor != nil && (ds.Type != types.ConnectorTypeNextcloud || result.Failed == 0) {
		cursorJSON, _ := nextCursor.ToJSON()
		ds.LastSyncCursor = cursorJSON
	}

	ds.LastSyncAt = timePtr(time.Now().UTC())
	syncStatus := types.SyncLogStatusSuccess
	syncErrorMessage := ""
	if len(fetchWarnings) > 0 {
		syncStatus = types.SyncLogStatusPartial
		syncErrorMessage = fmt.Sprintf("Some feeds failed: %s", strings.Join(fetchWarnings, "; "))
		for _, w := range fetchWarnings {
			result.Errors = append(result.Errors, types.SyncItemError{Message: w})
		}
		resultJSON, _ = result.ToJSON()
	}
	if result.Failed > 0 {
		// Per-document failures flip the sync to partial so the drawer shows
		// which docs didn't make it (mirrors the streaming path). Deletion
		// failures additionally only retry on a later full sync.
		syncStatus = types.SyncLogStatusPartial
		if syncErrorMessage != "" {
			syncErrorMessage += "; "
		}
		syncErrorMessage += fmt.Sprintf("%d document(s) failed to sync", result.Failed)
		if result.DeletionFailed > 0 {
			syncErrorMessage += fmt.Sprintf(
				"; %d deletion failure(s) will only retry on the next full sync", result.DeletionFailed)
		}
	}
	s.updateSyncRunResult(ctx, ds, syncLog, result, resultJSON, syncStatus, syncErrorMessage, wasPaused)

	logger.Infof(ctx, "data source sync completed: ds=%s created=%d updated=%d deleted=%d",
		payload.DataSourceID, syncLog.ItemsCreated, syncLog.ItemsUpdated, syncLog.ItemsDeleted)

	return nil
}

func (
	s *DataSourceService,
) validateNextcloudEventSync(
	ctx context.Context,
	payload types.DataSourceSyncPayload,
	observedInstanceID ...string,
) error {
	if s.nextcloudEventInbox == nil || payload.NextcloudEventConnectionID == "" ||
		payload.NextcloudEventConfigSHA256 == "" || payload.DataSourceID == "" || payload.TenantID == 0 {
		return apprepo.ErrNextcloudEventScope
	}
	return s.nextcloudEventInbox.ValidateEventSyncTask(ctx, payload.NextcloudEventConnectionID,

		payload.DataSourceID,
		payload.TenantID,
		payload.NextcloudEventConfigSHA256,
		payload.SyncLogID,
		observedInstanceID...)
}

func nextcloudCandidateRetryClaim(payload types.DataSourceSyncPayload) apprepo.NextcloudCandidateRetryClaim {
	return apprepo.NextcloudCandidateRetryClaim{
		TenantID:        payload.TenantID,
		KnowledgeBaseID: payload.NextcloudRetryKnowledgeBaseID,
		DataSourceID:    payload.DataSourceID, ExternalID: payload.NextcloudRetryExternalID,
		DesiredETag:       payload.NextcloudRetryETag,
		FailedCandidateID: payload.NextcloudRetryCandidateID,
		InstanceID:        payload.NextcloudRetryInstanceID, BindingID: payload.NextcloudRetryBindingID,
		ConfigSHA:       payload.NextcloudRetryConfigSHA256,
		PairOperationID: payload.NextcloudRetryPairOperationID,
		PairingEpoch:    payload.NextcloudRetryPairingEpoch,
		LeaseToken:      payload.NextcloudRetryLeaseToken, SyncLogID: payload.SyncLogID,
	}
}

func (s *DataSourceService) validateNextcloudCandidateRetry(ctx context.Context,
	payload types.DataSourceSyncPayload, requireFailed bool,
) error {
	if s.nextcloudEventInbox == nil || payload.Trigger != "nextcloud_candidate_retry" {
		return apprepo.ErrNextcloudEventScope
	}
	return s.nextcloudEventInbox.ValidateCandidateRetryTask(ctx,
		nextcloudCandidateRetryClaim(payload), requireFailed)
}

func (s *DataSourceService) checkNextcloudCandidateRetry(ctx context.Context,
	payload types.DataSourceSyncPayload, requireFailed bool,
) (bool, error) {
	err := s.validateNextcloudCandidateRetry(ctx, payload, requireFailed)
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, apprepo.ErrNextcloudEventScope) {
		return false, err
	}
	log, lookupErr := s.syncLogRepo.FindByID(ctx, payload.SyncLogID)
	if lookupErr == nil && log != nil {
		log.Status = types.SyncLogStatusCanceled
		log.FinishedAt = timePtr(time.Now().UTC())
		log.ErrorMessage = "Nextcloud candidate retry superseded or source changed"
		if updateErr := s.syncLogRepo.Update(ctx, log); updateErr != nil {
			return false, updateErr
		}
	}
	return true, nil
}

func (
	s *DataSourceService,
) checkNextcloudEventSync(ctx context.Context, payload types.DataSourceSyncPayload, observedInstanceID ...string) (
	bool,
	error,
) {
	err := s.validateNextcloudEventSync(ctx, payload, observedInstanceID...)
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, apprepo.ErrNextcloudEventScope) {
		return false, err
	}
	log, lookupErr := s.syncLogRepo.FindByID(ctx, payload.SyncLogID)
	if lookupErr != nil {
		// A queued task revoked before it starts has no sync log yet.
		return true, nil
	}
	if log != nil {
		log.Status = types.SyncLogStatusCanceled
		log.FinishedAt = timePtr(time.Now().UTC())
		log.ErrorMessage = "event connection revoked or source changed"
		if updateErr := s.syncLogRepo.Update(ctx, log); updateErr != nil {
			return false, updateErr
		}
	}
	return true, nil
}

// A terminal log may have released source admission while an old queued task
// was still alive. Recheck before every knowledge write and before committing
// the cursor; event tasks also recheck their pinned connection.
func (
	s *DataSourceService,
) checkNextcloudSyncActive(ctx context.Context, payload types.DataSourceSyncPayload, observedInstanceID ...string) (
	bool,
	error,
) {
	if s.nextcloudSourcePairings != nil {
		active, err := s.nextcloudSourcePairings.ActiveForSync(
			ctx,
			payload.DataSourceID,
			payload.TenantID,
			observedInstanceID...,
		)
		if err != nil {
			return false, err
		}
		if !active {
			return true, fmt.Errorf("%w: Nextcloud source is not paired", asynq.SkipRetry)
		}
	}
	if payload.Trigger == "nextcloud_event" {
		cancelled, err := s.checkNextcloudEventSync(ctx, payload, observedInstanceID...)
		if cancelled || err != nil {
			return cancelled, err
		}
	}
	if payload.Trigger == "nextcloud_candidate_retry" {
		if len(observedInstanceID) > 0 && observedInstanceID[0] != payload.NextcloudRetryInstanceID {
			return true, fmt.Errorf("%w: Nextcloud retry instance changed", asynq.SkipRetry)
		}
		cancelled, err := s.checkNextcloudCandidateRetry(ctx, payload, false)
		if cancelled || err != nil {
			return cancelled, err
		}
	}
	log, err := s.syncLogRepo.FindByID(ctx, payload.SyncLogID)
	if err != nil {
		return false, err
	}
	if log.Status != types.SyncLogStatusRunning {
		return true, fmt.Errorf("%w: Nextcloud sync log is no longer running", asynq.SkipRetry)
	}
	return false, nil
}

// resolveAutoTagIDs finds or creates the per-data-source tag applied to every
// synced item so results are identifiable in the KB. A tag failure is
// non-fatal: the sync proceeds untagged.
func (s *DataSourceService) resolveAutoTagIDs(ctx context.Context, ds *types.DataSource) []string {
	autoTagIDs := []string{}
	if autoTag, tagErr := s.tagService.FindOrCreateTagByName(ctx, ds.KnowledgeBaseID, ds.Name); tagErr != nil {
		logger.Warnf(ctx, "failed to find/create auto-tag %q: %v (proceeding without tag)", ds.Name, tagErr)
	} else if autoTag != nil {
		autoTagIDs = append(autoTagIDs, autoTag.ID)
		logger.Infof(ctx, "using auto-tag %q (id=%s) for data source sync", ds.Name, autoTag.ID)
	}
	return autoTagIDs
}

// maxSyncResultErrors bounds the per-item error sample retained in
// SyncResult.Errors. That slice is persisted as jsonb and returned in every
// sync-log list response, so an unbounded list on a sync that fails thousands of
// documents means multi-MB DB rows and payloads. The accurate failure count
// lives in SyncResult.Failed (a bounded int); this list only keeps a sample for
// display (Tencent/WeKnora#2136 / #1262).
const maxSyncResultErrors = 100

// recordSyncError appends an error sample to result.Errors, capped at
// maxSyncResultErrors. Callers still increment result.Failed for the exact count.
func recordSyncError(result *types.SyncResult, item types.SyncItemError) {
	if len(result.Errors) < maxSyncResultErrors {
		result.Errors = append(result.Errors, item)
	}
}

// fetchFailureSyncError maps a connector error item into a structured, user-
// facing sample. Connectors that classify their errors (Feishu) provide a stable
// i18n code + params via metadata so the frontend localises it to the viewer's
// language; the raw status/body/log_id never leaves the server logs. Connectors
// without codes keep the raw text as a Message fallback. Best practice per
// Airbyte/Fivetran/Onyx: humanised, actionable, localised UI; raw detail in logs.
func fetchFailureSyncError(item *types.FetchedItem, rawMsg string) types.SyncItemError {
	e := types.SyncItemError{Title: item.Title}
	if code := item.Metadata["error_reason_code"]; code != "" {
		e.Code = code
		if v := item.Metadata["error_reason_code_value"]; v != "" {
			e.Params = map[string]string{"code": v}
		}
		e.Message = item.Metadata["error_reason"] // fallback if the client lacks the key
	} else {
		e.Message = rawMsg
	}
	return e
}

// applyFetchedItem writes a single fetched item into the knowledge base and
// updates result counters. It is the shared core of the batch loop and the
// streaming handler so item classification (deleted / empty / ingest outcome)
// stays identical across both fetch paths.
func (s *DataSourceService) applyFetchedItem(
	ctx context.Context, ds *types.DataSource, item *types.FetchedItem,
	tagIDs []string, result *types.SyncResult,
) {
	if item.IsDeleted {
		if !ds.SyncDeletions && ds.Type != types.ConnectorTypeNextcloud {
			// Other connectors may disable sync deletion. Nextcloud must
			// withdraw a confirmed deletion regardless of that option.
			return
		}
		if item.ExternalID == "" {
			logger.Warnf(ctx, "skipping deletion for item %q: empty external_id", item.Title)
			result.Skipped++
			return
		}
		if ds.Type == types.ConnectorTypeNextcloud {
			versions, ok := s.knowledgeService.GetRepository().(nextcloudVersionStore)
			if !ok {
				result.Failed++
				result.DeletionFailed++
				recordSyncError(result, types.SyncItemError{
					Title:   item.Title,
					Code:    "deletion_failed",
					Message: "Nextcloud version store unavailable",
				})
				return
			}
			if err := versions.TombstoneNextcloudVersion(
				ctx,
				ds.TenantID,
				ds.KnowledgeBaseID,
				ds.ID,
				item.ExternalID,
			); err !=
				nil {
				result.Failed++
				result.DeletionFailed++
				logger.Errorf(ctx, "failed to tombstone Nextcloud item %s: %v", item.ExternalID, err)
				recordSyncError(result, types.SyncItemError{
					Title:   item.Title,
					Code:    "deletion_failed",
					Message: "Nextcloud tombstone failed; see server logs",
				})
				return
			}
			result.Deleted++
			return
		}
		// Perform real KB deletion, scoped to items owned by this data source
		// so identical external IDs from different data sources cannot collide.
		repo := s.knowledgeService.GetRepository()
		existing, lookupErr := repo.FindByDataSourceExternalID(
			ctx, ds.TenantID, ds.KnowledgeBaseID, ds.ID, item.ExternalID,
		)
		if lookupErr != nil {
			logger.Errorf(ctx, "failed to find deleted knowledge for external_id=%s (ds=%s, kb=%s): %v",
				item.ExternalID, ds.ID, ds.KnowledgeBaseID, lookupErr)
			result.Failed++
			result.DeletionFailed++
			recordSyncError(result, types.SyncItemError{
				Title:   item.Title,
				Code:    "deletion_lookup_failed",
				Message: "Failed to look up the item before deletion; see server logs",
			})
			return
		}
		if existing == nil {
			// Deletion is idempotent: the source item may already have been
			// removed manually or by an earlier sync.
			result.Skipped++
			return
		}
		if deleteErr := s.knowledgeService.DeleteKnowledge(ctx, existing.ID); deleteErr != nil {
			// The cursor is already past this item, so a failed deletion normally
			// retries only on a later full sync. Counted separately so the
			// sync-log message can warn the operator about this gap.
			result.Failed++
			result.DeletionFailed++
			logger.Errorf(ctx, "failed to delete knowledge %s for external_id=%s (ds=%s): %v",
				existing.ID, item.ExternalID, ds.ID, deleteErr)
			recordSyncError(result, types.SyncItemError{
				Title:   item.Title,
				Code:    "deletion_failed",
				Message: "Deletion failed; see server logs",
			})
			return
		}
		if herr := repo.HardDeleteKnowledge(ctx, ds.TenantID, existing.ID); herr != nil {
			result.Failed++
			result.DeletionFailed++
			logger.Errorf(ctx, "failed to hard-delete knowledge %s for external_id=%s (ds=%s): %v",
				existing.ID, item.ExternalID, ds.ID, herr)
			recordSyncError(result, types.SyncItemError{
				Title:   item.Title,
				Code:    "deletion_failed",
				Message: "Deletion failed; see server logs",
			})
			return
		}
		result.Deleted++
		return
	}

	if len(item.Content) == 0 && item.URL == "" {
		// Check if this is an error item from the connector (failed to fetch content)
		if errMsg, hasErr := item.Metadata["error"]; hasErr {
			logger.Warnf(ctx, "item %q (external_id=%s) fetch failed: %s", item.Title, item.ExternalID, errMsg)
			result.Failed++
			recordSyncError(result, fetchFailureSyncError(item, errMsg))
		} else {
			logger.Infof(ctx, "skipping item %q (external_id=%s): no content or URL", item.Title, item.ExternalID)
			result.Skipped++
		}
		return
	}

	isUpdate, err := s.ingestItem(ctx, ds, item, tagIDs)
	if err != nil {
		var dupErr *types.DuplicateKnowledgeError
		switch {
		case errors.Is(err, apprepo.ErrNextcloudCandidateRetryNotDue),
			errors.Is(err, apprepo.ErrNextcloudCandidateRetryManual):
			// The durable per-file retry job owns unchanged failed ETags.
			result.Skipped++
		case errors.As(err, &dupErr):
			// Duplicate file/URL is not a failure — count as skipped.
			logger.Infof(ctx, "item %q (external_id=%s) already exists, skipping", item.Title, item.ExternalID)
			result.Skipped++
		case item.Metadata["embedded_image"] == "true":
			// An image extracted from a document for OCR is a best-effort
			// enrichment, not the document itself. If the KB cannot ingest it
			// (VLM/object-storage not configured for images, or a transient error),
			// skip it rather than failing the whole sync: the doc body already
			// synced, and the image stays in SubtreeKeep for a later retry once the
			// KB is configured.
			logger.Infof(ctx, "skipping embedded image %q (external_id=%s), not ingested: %v",
				item.Title, item.ExternalID, err)
			result.Skipped++
		default:
			logger.Warnf(ctx, "failed to ingest item %q (external_id=%s): %v", item.Title, item.ExternalID, err)
			result.Failed++
			recordSyncError(result, types.SyncItemError{
				Title:   item.Title,
				Code:    "ingest_failed",
				Message: "Ingest failed; see server logs",
			})
		}
	} else if isUpdate {
		result.Updated++
	} else {
		result.Created++
	}
}

// streamStartCursor decides which cursor a streaming fetch should resume from.
// A user-triggered full sync on its first attempt drops the cursor so every
// item is re-fetched; a retried full sync (attempt > 0) and every incremental
// sync resume from the last persisted checkpoint so a timed-out run converges
// instead of restarting from scratch.
func streamStartCursor(ds *types.DataSource, forceFull bool, attempt int) (*types.SyncCursor, error) {
	if forceFull && attempt == 0 {
		return nil, nil
	}
	return ds.ParseSyncCursor()
}

// streamSyncHandler adapts a streaming fetch to the knowledge-base ingest path.
// Emit ingests each item as it arrives (bounding memory) and Checkpoint persists
// the connector cursor plus live progress counts at page boundaries.
type streamSyncHandler struct {
	svc                   *DataSourceService
	ds                    *types.DataSource
	tagIDs                []string
	result                *types.SyncResult
	syncLog               *types.SyncLog
	payload               types.DataSourceSyncPayload
	nextcloudInstanceID   string
	nextcloudBindingID    string
	nextcloudObserved     bool
	nextcloudGuardStopped bool
	nextcloudFailedETags  map[string]string
}

var errNextcloudStreamCanceled = apperrors.NewProtocolError(errors.New(
	"nextcloud stream canceled",
), "Nextcloud stream canceled")

// ObserveNextcloudIdentity is called by the Nextcloud connector after its
// signed capabilities and live binding checks, before it emits any item.
func (h *streamSyncHandler) ObserveNextcloudIdentity(ctx context.Context, instanceID, bindingID string) error {
	if h.ds.Type != types.ConnectorTypeNextcloud || instanceID == "" || bindingID == "" {
		return fmt.Errorf("invalid Nextcloud stream identity")
	}
	if h.nextcloudObserved && (h.nextcloudInstanceID != instanceID || h.nextcloudBindingID != bindingID) {
		return apperrors.NewProtocolError(fmt.Errorf(
			"nextcloud stream identity changed",
		), "Nextcloud stream identity changed")
	}
	h.nextcloudInstanceID = instanceID
	h.nextcloudBindingID = bindingID
	h.nextcloudObserved = true
	if err := h.checkNextcloudActive(ctx); err != nil {
		return err
	}
	if h.svc.knowledgeService == nil {
		if h.payload.Trigger == "nextcloud_candidate_retry" {
			return apperrors.NewProtocolError(errors.New(
				"nextcloud failed-candidate retry store unavailable",
			), "Nextcloud failed-candidate retry store unavailable")
		}
		return nil
	}
	store, ok := h.svc.knowledgeService.GetRepository().(interface {
		FailedNextcloudCandidateETags(context.Context, uint64, string, string, string, string, time.Time, string) (
			map[string]string,
			error,
		)
	})
	if !ok {
		if h.payload.Trigger == "nextcloud_candidate_retry" {
			return apperrors.NewProtocolError(errors.New(
				"nextcloud failed-candidate retry store unavailable",
			), "Nextcloud failed-candidate retry store unavailable")
		}
		return nil
	}
	failed, err := store.FailedNextcloudCandidateETags(ctx, h.ds.TenantID,
		h.ds.KnowledgeBaseID, h.ds.ID, instanceID, bindingID, time.Now().UTC(),
		h.payload.NextcloudRetryLeaseToken)
	if err != nil {
		return fmt.Errorf("read Nextcloud failed candidates: %w", err)
	}
	h.nextcloudFailedETags = failed
	if h.payload.Trigger == "nextcloud_candidate_retry" {
		fileID := strings.TrimPrefix(h.payload.NextcloudRetryExternalID, "nextcloud:"+instanceID+":")
		if h.payload.NextcloudRetryKnowledgeBaseID != h.ds.KnowledgeBaseID ||
			h.payload.NextcloudRetryBindingID != bindingID ||
			fileID == h.payload.NextcloudRetryExternalID || failed[fileID] != h.payload.NextcloudRetryETag {
			return fmt.Errorf("%w: Nextcloud retry target is no longer current", asynq.SkipRetry)
		}
		// A retry task may not opportunistically retry another failed file.
		h.nextcloudFailedETags = map[string]string{fileID: failed[fileID]}
	}
	return nil
}

func (h *streamSyncHandler) FailedNextcloudCandidateETags() map[string]string {
	return h.nextcloudFailedETags
}

func (h *streamSyncHandler) NextcloudRetryExternalID() string {
	if h.payload.Trigger == "nextcloud_candidate_retry" {
		return h.payload.NextcloudRetryExternalID
	}
	return ""
}

func (h *streamSyncHandler) NextcloudRequireManifest() bool {
	return h.payload.Trigger == "nextcloud_event"
}

func (h *streamSyncHandler) checkNextcloudActive(ctx context.Context) error {
	if h.ds.Type != types.ConnectorTypeNextcloud {
		return nil
	}
	if !h.nextcloudObserved || h.nextcloudInstanceID == "" || h.nextcloudBindingID == "" {
		return apperrors.NewProtocolError(fmt.Errorf(
			"nextcloud stream source identity was not observed",
		), "Nextcloud stream source identity was not observed")
	}
	cancelled, err := h.svc.checkNextcloudSyncActive(ctx, h.payload, h.nextcloudInstanceID)
	if cancelled || err != nil {
		// A revoked event task may have been marked canceled by the guard.
		// Do not let the normal fetch-error path overwrite that terminal log.
		h.nextcloudGuardStopped = true
		if err != nil {
			return err
		}
		return errNextcloudStreamCanceled
	}
	return nil
}

func (h *streamSyncHandler) validateNextcloudCursor(cursor *types.SyncCursor) error {
	if !h.nextcloudObserved || cursor == nil || cursor.ConnectorCursor == nil {
		return apperrors.NewProtocolError(fmt.Errorf(
			"nextcloud stream did not return a complete cursor",
		), "Nextcloud stream did not return a complete cursor")
	}
	instanceID, _ := cursor.ConnectorCursor["instance_id"].(string)
	files, ok := cursor.ConnectorCursor["files"].(map[string]interface{})
	if !ok || instanceID != h.nextcloudInstanceID || files[h.nextcloudBindingID] == nil {
		return apperrors.NewProtocolError(fmt.Errorf(("nextcloud stream cursor identity or inventory differs f" +
			"rom observed source")), "Nextcloud stream cursor identity or inventory differs from observed source")
	}
	return nil
}

// Emit ingests one streamed item. A canceled context aborts the stream so the
// connector stops fetching; per-item ingest failures are recorded in result and
// do NOT abort (matching the batch loop, which never fails the whole sync for
// one bad document).
func (h *streamSyncHandler) Emit(ctx context.Context, item types.FetchedItem) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if h.ds.Type == types.ConnectorTypeNextcloud {
		fileID := item.Metadata["nextcloud_file_id"]
		if fileID == "" || item.Metadata["nextcloud_instance_id"] != h.nextcloudInstanceID ||
			item.Metadata["nextcloud_binding_id"] != h.nextcloudBindingID ||
			item.SourceResourceID != h.nextcloudBindingID ||
			item.ExternalID != "nextcloud:"+h.nextcloudInstanceID+":"+fileID {
			return apperrors.NewProtocolError(fmt.Errorf(("nextcloud streamed item identity differs from observed " +
				"source")), "Nextcloud streamed item identity differs from observed source")
		}
		if h.payload.Trigger == "nextcloud_candidate_retry" &&
			item.ExternalID != h.payload.NextcloudRetryExternalID {
			// This run owns one failed file only. Its complete manifest can reveal
			// another changed file; leave that file for an ordinary source sync.
			return nil
		}
		if err := h.checkNextcloudActive(ctx); err != nil {
			return err
		}
	}
	beforeFailed := h.result.Failed
	h.result.Total++
	ingestCtx := withKBActivitySuppressed(ctx)
	if h.ds.Type == types.ConnectorTypeNextcloud && h.payload.Trigger == "nextcloud_candidate_retry" {
		ingestCtx = apprepo.WithNextcloudCandidateRetryClaim(ingestCtx, nextcloudCandidateRetryClaim(h.payload))
	}
	h.svc.applyFetchedItem(ingestCtx, h.ds, &item, h.tagIDs, h.result)
	if h.ds.Type == types.ConnectorTypeNextcloud && h.result.Failed > beforeFailed {
		return apperrors.NewProtocolError(fmt.Errorf(
			"nextcloud item %s could not be processed",
			item.ExternalID,
		), fmt.Sprintf("Nextcloud item %s could not be processed", item.ExternalID))
	}
	return nil
}

// Checkpoint persists the connector cursor onto the data source and mirrors the
// running counts into the sync log so progress survives a crash and the UI can
// reflect a long sync mid-flight instead of jumping from 0 to done.
func (h *streamSyncHandler) Checkpoint(ctx context.Context, cursor *types.SyncCursor) error {
	if h.ds.Type == types.ConnectorTypeNextcloud {
		if err := h.checkNextcloudActive(ctx); err != nil {
			return err
		}
		return apperrors.NewProtocolError(fmt.Errorf(
			"nextcloud stream has no resumable partial cursor",
		), "Nextcloud stream has no resumable partial cursor")
	}
	if cursor == nil {
		return nil
	}
	cursorJSON, err := cursor.ToJSON()
	if err != nil {
		return err
	}
	h.ds.LastSyncCursor = cursorJSON
	if err := h.svc.dsRepo.UpdateSyncState(ctx, h.ds); err != nil {
		return err
	}

	// Best-effort live progress; a failure here must not abort the sync.
	h.syncLog.ItemsTotal = h.result.Total
	h.syncLog.ItemsCreated = h.result.Created
	h.syncLog.ItemsUpdated = h.result.Updated
	h.syncLog.ItemsDeleted = h.result.Deleted
	h.syncLog.ItemsSkipped = h.result.Skipped
	h.syncLog.ItemsFailed = h.result.Failed
	if err := h.svc.syncLogRepo.UpdateResult(ctx, h.syncLog); err != nil {
		logger.Warnf(ctx, "failed to persist sync log progress at checkpoint: %v", err)
	}
	return nil
}

// streamingFetch dispatches to FetchFullStream when a connector can re-fetch
// every item while keeping the stored cursor as the deletion baseline. Other
// streaming connectors keep FetchStream, including force-full runs that drop
// the cursor on the first attempt via streamStartCursor.
func streamingFetch(
	ctx context.Context,
	sc datasource.StreamingConnector,
	config *types.DataSourceConfig,
	forceFull bool,
	startCursor, fullBaseline *types.SyncCursor,
	h datasource.StreamHandler,
) (*types.SyncCursor, error) {
	if forceFull {
		if full, ok := sc.(datasource.FullStreamingConnector); ok {
			return full.FetchFullStream(ctx, config, fullBaseline, h)
		}
	}
	return sc.FetchStream(ctx, config, startCursor, h)
}

// processSyncStreaming runs a sync through a StreamingConnector, ingesting each
// item as it arrives and checkpointing progress so the run is memory-bounded and
// resumable after a timeout.
func (s *DataSourceService) processSyncStreaming(
	ctx context.Context, sc datasource.StreamingConnector,
	ds *types.DataSource, syncLog *types.SyncLog,
	config *types.DataSourceConfig, payload types.DataSourceSyncPayload, wasPaused bool,
) error {
	if ds.Type == types.ConnectorTypeNextcloud && payload.Trigger == "nextcloud_candidate_retry" {
		ctx = context.WithValue(ctx, nextcloudCandidateRetryRunKey{}, true)
	} else if ds.Type == types.ConnectorTypeNextcloud && payload.Trigger == "nextcloud_event" {
		ctx = context.WithValue(ctx, nextcloudEventRunKey{}, true)
	}
	// Tenant + auto-tag setup must precede fetching because the stream ingests
	// each item on the fly.
	ctx = context.WithValue(ctx, types.TenantIDContextKey, ds.TenantID)
	tenant, err := s.tenantRepo.GetTenantByID(ctx, ds.TenantID)
	if err != nil {
		logger.Errorf(ctx, "failed to get tenant info: %v", err)
		s.updateSyncRunResult(ctx, ds, syncLog, &types.SyncResult{}, nil,
			types.SyncLogStatusFailed, fmt.Sprintf("Failed to get tenant info: %v", err), wasPaused)
		return err
	}
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)

	autoTagIDs := s.resolveAutoTagIDs(ctx, ds)

	forceFull := payload.ForceFull || ds.SyncMode == types.SyncModeFull
	attempt, _ := asynq.GetRetryCount(ctx)
	startCursor, err := streamStartCursor(ds, forceFull, attempt)
	if err != nil {
		logger.Errorf(ctx, "failed to parse sync cursor: %v", err)
		s.updateSyncRunResult(ctx, ds, syncLog, &types.SyncResult{}, nil,
			types.SyncLogStatusFailed, fmt.Sprintf("Invalid cursor: %v", err), wasPaused)
		return err
	}

	result := &types.SyncResult{}
	handler := &streamSyncHandler{
		svc: s, ds: ds, tagIDs: autoTagIDs, result: result,
		syncLog: syncLog, payload: payload,
	}

	fullBaseline := startCursor
	if forceFull {
		if _, ok := sc.(datasource.FullStreamingConnector); ok {
			baseline, cursorErr := ds.ParseSyncCursor()
			if cursorErr != nil {
				logger.Errorf(ctx, "failed to parse full-sync cursor: %v", cursorErr)
				s.updateSyncRunResult(ctx, ds, syncLog, result, nil,
					types.SyncLogStatusFailed, fmt.Sprintf("Invalid cursor: %v", cursorErr), wasPaused)
				return cursorErr
			}
			fullBaseline = baseline
		}
	}

	nextCursor, fetchErr := streamingFetch(ctx, sc, config, forceFull, startCursor, fullBaseline, handler)
	if handler.nextcloudGuardStopped {
		if errors.Is(fetchErr, errNextcloudStreamCanceled) {
			return nil
		}
		return fetchErr
	}
	if fetchErr != nil {
		// Progress so far is already checkpointed onto ds.LastSyncCursor; leave
		// it in place so the Asynq retry resumes from there. Persist counts.
		logger.Errorf(ctx, "streaming fetch failed: %v", fetchErr)
		if ds.Type == types.ConnectorTypeNextcloud && payload.Trigger == "nextcloud_event" {
			ctx = context.WithValue(ctx, nextcloudEventFailureKey{}, fetchErr)
		}
		resultJSON, _ := result.ToJSON()
		s.updateSyncRunResult(ctx, ds, syncLog, result, resultJSON,
			types.SyncLogStatusFailed, fmt.Sprintf("Fetch failed: %v", apperrors.PublicMessage(fetchErr)), wasPaused)
		return fetchErr
	}
	if ds.Type == types.ConnectorTypeNextcloud {
		if err := handler.validateNextcloudCursor(nextCursor); err != nil {
			s.updateSyncRunResult(
				ctx, ds, syncLog, result, nil, types.SyncLogStatusFailed, apperrors.PublicMessage(err), wasPaused,
			)
			return err
		}
		if err := handler.checkNextcloudActive(ctx); err != nil {
			if errors.Is(err, errNextcloudStreamCanceled) {
				return nil
			}
			return err
		}
		if result.Failed > 0 {
			err := fmt.Errorf("%d Nextcloud item(s) failed; source cursor retained", result.Failed)
			resultJSON, _ := result.ToJSON()
			s.updateSyncRunResult(ctx, ds, syncLog, result, resultJSON,
				types.SyncLogStatusPartial, apperrors.PublicMessage(err), wasPaused)
			return nil
		}
	}

	resultJSON, _ := result.ToJSON()
	if err := allFetchedItemsFailedError(result); err != nil {
		logger.Errorf(ctx, "streaming sync failed while processing fetched items: %v", err)
		s.updateSyncRunResult(
			ctx, ds, syncLog, result, resultJSON, types.SyncLogStatusFailed, apperrors.PublicMessage(err), wasPaused,
		)
		return err
	}

	if err := storeCompletedStreamCursor(ds, payload, nextCursor); err != nil {
		s.updateSyncRunResult(ctx, ds, syncLog, result, resultJSON,
			types.SyncLogStatusFailed, fmt.Sprintf("Invalid Nextcloud cursor: %v", err), wasPaused)
		return err
	}

	// Surface per-document failures as a partial sync (not silent success), so
	// the sync-log drawer's failure detail explains which docs didn't make it —
	// the visibility gap behind "status normal but not everything syncs"
	// (Tencent/WeKnora#2136). Fetch failures abort the stream before the failed
	// page is checkpointed, so the next run retries them; deletion failures are
	// past the cursor and only retry on a full sync in the normal case (see
	// applyFetchedItem).
	status := types.SyncLogStatusSuccess
	errMsg := ""
	if result.Failed > 0 {
		status = types.SyncLogStatusPartial
		errMsg = fmt.Sprintf("%d document(s) failed to sync", result.Failed)
		if result.DeletionFailed > 0 {
			errMsg += fmt.Sprintf("; %d deletion failure(s) will only retry on the next full sync", result.DeletionFailed)
		}
	}
	s.updateSyncRunResult(ctx, ds, syncLog, result, resultJSON, status, errMsg, wasPaused)
	logger.Infof(ctx, "streaming sync completed: ds=%s created=%d updated=%d deleted=%d skipped=%d failed=%d",
		payload.DataSourceID, result.Created, result.Updated, result.Deleted, result.Skipped, result.Failed)
	return nil
}

// A single-file retry observes a full manifest to verify A but does not
// process neighboring changes. Retain the source cursor so the next normal
// sync still sees any changed or deleted B.
func storeCompletedStreamCursor(ds *types.DataSource, payload types.DataSourceSyncPayload,
	nextCursor *types.SyncCursor,
) error {
	if ds.Type == types.ConnectorTypeNextcloud && payload.Trigger == "nextcloud_candidate_retry" {
		return nil
	}
	if nextCursor != nil {
		if cursorJSON, err := nextCursor.ToJSON(); err == nil {
			ds.LastSyncCursor = cursorJSON
		} else if ds.Type == types.ConnectorTypeNextcloud {
			return err
		}
	}
	ds.LastSyncAt = timePtr(time.Now().UTC())
	return nil
}

func (s *DataSourceService) updateSyncRunResult(
	ctx context.Context,
	ds *types.DataSource,
	syncLog *types.SyncLog,
	result *types.SyncResult,
	resultJSON types.JSON,
	status string,
	errorMessage string,
	wasPaused bool,
) {
	syncLog.ItemsTotal = result.Total
	syncLog.ItemsCreated = result.Created
	syncLog.ItemsUpdated = result.Updated
	syncLog.ItemsDeleted = result.Deleted
	syncLog.ItemsSkipped = result.Skipped
	syncLog.ItemsFailed = result.Failed
	syncLog.Status = status
	syncLog.FinishedAt = timePtr(time.Now().UTC())
	syncLog.ErrorMessage = errorMessage
	syncLog.Result = resultJSON
	if ds.Type == types.ConnectorTypeNextcloud && isNextcloudCandidateRetryRun(ctx) {
		// A transient failure of one failed file is a retry-job outcome, not a
		// failure of the active source. The log releases admission for backoff.
		if err := s.syncLogRepo.UpdateResult(ctx, syncLog); err != nil {
			logger.Errorf(ctx, "failed to update Nextcloud candidate retry log: %v", err)
		}
		return
	}
	// For Nextcloud, the running log owns the source admission slot. Keep it
	// running until its cursor/state is durable; otherwise another producer
	// could start from the old cursor and race this final write.
	if ds.Type != types.ConnectorTypeNextcloud {
		if err := s.syncLogRepo.UpdateResult(ctx, syncLog); err != nil {
			logger.Errorf(ctx, "failed to update sync log: %v", err)
		}
	}

	expectedStatus := ds.Status
	if status == types.SyncLogStatusFailed {
		if !wasPaused && (ds.Type != types.ConnectorTypeNextcloud || !isNextcloudRetryableEventFailure(ctx)) {
			ds.Status = types.DataSourceStatusError
		}
	} else if wasPaused {
		ds.Status = types.DataSourceStatusPaused
	} else {
		ds.Status = types.DataSourceStatusActive
	}
	ds.ErrorMessage = errorMessage
	ds.LastSyncResult = resultJSON
	var stateErr error
	if ds.Type == types.ConnectorTypeNextcloud && isNextcloudRetryableEventFailure(ctx) {
		// A pause or deletion may commit after this worker loaded ds. This
		// guarded write records the error without restoring its stale status.
		_, stateErr = s.dsRepo.UpdateNextcloudRetryableEventFailure(ctx, ds)
	} else if ds.Type == types.ConnectorTypeNextcloud {
		updated, err := s.dsRepo.UpdateNextcloudSyncStateCAS(ctx, ds, expectedStatus)
		stateErr = err
		if err == nil && !updated && status != types.SyncLogStatusFailed {
			// A late pause/resume/deletion wins over this worker's snapshot.
			// The cursor was not committed, so an event must not be ACKed.
			status = types.SyncLogStatusCanceled
			syncLog.Status = status
			syncLog.ErrorMessage = "Nextcloud source status changed before sync completion; cursor not committed"
		}
	} else {
		stateErr = s.dsRepo.UpdateSyncState(ctx, ds)
	}
	if stateErr != nil {
		logger.Errorf(ctx, "failed to update data source: %v", stateErr)
		if ds.Type == types.ConnectorTypeNextcloud {
			// The log stays running for manual review. Releasing admission
			// would allow a second scan to start with stale deletion evidence.
			return
		}
	}
	if ds.Type == types.ConnectorTypeNextcloud {
		if err := s.syncLogRepo.UpdateResult(ctx, syncLog); err != nil {
			logger.Errorf(ctx, "failed to update Nextcloud sync log: %v", err)
		}
	}
	action := types.AuditActionDataSourceSyncCompleted
	outcome := types.AuditOutcomeSuccess
	if status == types.SyncLogStatusFailed {
		action = types.AuditActionDataSourceSyncFailed
		outcome = types.AuditOutcomeFailed
	} else if status == types.SyncLogStatusPartial {
		outcome = types.AuditOutcomePartial
	} else if status == types.SyncLogStatusCanceled {
		outcome = types.AuditOutcomeCanceled
	}
	recordKBActivity(ctx, s.audit, ds.TenantID, ds.KnowledgeBaseID, action,
		"data_source", ds.ID, outcome,
		map[string]any{
			"name": ds.Name, "type": ds.Type, "sync_log_id": syncLog.ID,
			"total": result.Total, "created": result.Created, "updated": result.Updated,
			"deleted": result.Deleted, "skipped": result.Skipped, "failed": result.Failed,
		})
}

func allFetchedItemsFailedError(result *types.SyncResult) error {
	if result == nil || result.Total == 0 {
		return nil
	}
	if result.Failed != result.Total || result.Created != 0 || result.Updated != 0 ||
		result.Deleted != 0 || result.Skipped != 0 {
		return nil
	}

	detail := ""
	if len(result.Errors) > 0 {
		detail = result.Errors[0].Display()
		const maxDetailLen = 500
		if len(detail) > maxDetailLen {
			detail = detail[:maxDetailLen] + "..."
		}
	}
	if detail == "" {
		return fmt.Errorf("all fetched items failed during sync (%d/%d)", result.Failed, result.Total)
	}
	return fmt.Errorf("all fetched items failed during sync (%d/%d): %s", result.Failed, result.Total, detail)
}

// ValidateCredentials tests connectivity using raw credentials without persisting anything.
func (s *DataSourceService) ValidateCredentials(ctx context.Context, connectorType string, credentials map[string]interface{}) error {
	connector, err := s.connectorRegistry.Get(connectorType)
	if err != nil {
		return err
	}
	config := &types.DataSourceConfig{
		Type:        connectorType,
		Credentials: credentials,
	}
	if err := connector.Validate(ctx, config); err != nil {
		return err
	}

	return nil
}

// Helper functions

func (s *DataSourceService) validateDataSourceConfig(ctx context.Context, ds *types.DataSource) error {
	connector, err := s.connectorRegistry.Get(ds.Type)
	if err != nil {
		return err
	}

	config, err := ds.ParseConfig()
	if err != nil {
		return datasource.ErrInvalidConfig
	}

	return connector.Validate(ctx, config)
}

// ingestItem writes a single FetchedItem into the knowledge base.
// If a knowledge item with the same external_id already exists, it is deleted first (update = delete + re-create).
//
// Routing logic:
//   - Has Content bytes → CreateKnowledgeFromFile (走完整的文档解析 pipeline)
//   - Has URL only      → CreateKnowledgeFromURL  (让 WeKnora 下载并解析)
//
// Returns (isUpdate, error) — isUpdate is true when an existing item was replaced.
func (s *DataSourceService) ingestItem(ctx context.Context, ds *types.DataSource, item *types.FetchedItem, tagIDs []string) (bool, error) {
	// Channel decides the knowledge "source" label shown in the UI. Prefer the
	// connector-supplied metadata["channel"] (e.g. Feishu Drive sets it to
	// "feishu" so Drive docs share the wiki's "飞书" label instead of showing
	// "unknown" for the raw ds.Type "feishu_drive"). Fall back to ds.Type so
	// connectors that don't set metadata.channel still get a meaningful label.
	channel := ds.Type // e.g. "feishu", "notion"
	// A source connector cannot relabel a Nextcloud document as ordinary
	// knowledge. Resource provenance is attached before the knowledge row is
	// persisted, so the data source type is the trusted authority here.
	if ds.Type != types.ConnectorTypeNextcloud && item.Metadata != nil {
		if mc, ok := item.Metadata["channel"]; ok && mc != "" {
			channel = mc
		}
	}

	metadata := map[string]string{
		"external_id":        item.ExternalID,
		"source_resource_id": item.SourceResourceID,
		"datasource_id":      ds.ID,
	}
	// The source system's own last-modified time, when the connector supplied
	// one. The knowledge row's UpdatedAt moves on every re-parse, so this is
	// the only record of how old the document itself is.
	if !item.UpdatedAt.IsZero() {
		metadata["source_updated_at"] = item.UpdatedAt.UTC().Format(time.RFC3339)
	}
	if !item.CreatedAt.IsZero() {
		metadata["source_created_at"] = item.CreatedAt.UTC().Format(time.RFC3339)
	}
	for k, v := range item.Metadata {
		metadata[k] = v
	}
	if ds.Type == types.ConnectorTypeNextcloud {
		// Connector metadata may not override the trusted data-source identity.
		metadata["datasource_id"] = ds.ID
		metadata["external_id"] = item.ExternalID
		metadata["source_resource_id"] = item.SourceResourceID
		return s.ingestNextcloudItem(ctx, ds, item, metadata, tagIDs)
	}

	// Check if a knowledge item with this external_id already exists → delete it first (update)
	isUpdate := false
	if item.ExternalID != "" {
		repo := s.knowledgeService.GetRepository()
		// Scope the lookup to items owned by this data source so identical
		// external IDs from two data sources cannot collide or overwrite each
		// other during updates.
		existing, err := repo.FindByDataSourceExternalID(ctx, ds.TenantID, ds.KnowledgeBaseID, ds.ID, item.ExternalID)
		if err != nil {
			if ds.Type == types.ConnectorTypeNextcloud {
				return false, fmt.Errorf(("check existing Nextcloud knowledge for external_id=%s: " +
					"%w"), item.ExternalID, err)
			}
			logger.Warnf(ctx, "failed to check existing knowledge for external_id=%s: %v", item.ExternalID, err)
			// Non-fatal: proceed with creation (may produce duplicate)
		} else if existing != nil {
			logger.Infof(ctx, "found existing knowledge %s for external_id=%s, deleting for update", existing.ID, item.ExternalID)
			if err := s.knowledgeService.DeleteKnowledge(ctx, existing.ID); err != nil {
				if ds.Type == types.ConnectorTypeNextcloud {
					return false, fmt.Errorf("delete replaced Nextcloud knowledge %s: %w", existing.ID, err)
				}
				logger.Warnf(ctx, "failed to delete existing knowledge %s: %v", existing.ID, err)
			} else {
				if herr := repo.HardDeleteKnowledge(ctx, ds.TenantID, existing.ID); herr != nil {
					if ds.Type == types.ConnectorTypeNextcloud {
						return false, fmt.Errorf("hard-delete replaced Nextcloud knowledge %s: %w", existing.ID, herr)
					}
					logger.Warnf(ctx, "failed to hard-delete replaced knowledge %s: %v", existing.ID, herr)
				}
				isUpdate = true
			}
		}
	}

	// Case 1: content already fetched → build a FileHeader from bytes and call CreateKnowledgeFromFile
	if len(item.Content) > 0 {
		fh, err := bytesToFileHeader(item.Content, item.FileName)
		if err != nil {
			return isUpdate, fmt.Errorf("build file header: %w", err)
		}
		if _, err := s.knowledgeService.CreateKnowledgeFromFile(
			ctx,
			ds.KnowledgeBaseID,
			fh,
			metadata,
			nil,           // use KB default for multimodal
			item.FileName, // customFileName — must include extension for file-type validation
			tagIDs,        // auto-tag from data source
			channel,
			nil,
		); err != nil {
			var dupErr *types.DuplicateKnowledgeError
			if errors.As(err, &dupErr) && dupIsSameNode(dupErr, item) {
				// Identical content is already present in the KB under THIS node's
				// own external_id, so the parent effectively exists — reconcile the
				// subtree so children removed from the doc do not linger.
				s.sweepStaleSubtree(ctx, ds, item)
			}
			return isUpdate, err
		}
		s.sweepStaleSubtree(ctx, ds, item)
		return isUpdate, nil
	}

	// Case 2: only a remote URL — let WeKnora handle downloading and parsing
	if item.URL != "" {
		created, err := s.knowledgeService.CreateKnowledgeFromURL(
			ctx,
			ds.KnowledgeBaseID,
			item.URL,
			item.FileName,
			"",  // auto-detect file type
			nil, // use KB default for multimodal
			item.Title,
			tagIDs, // auto-tag from data source
			channel,
			nil,
		)
		if err != nil {
			var dupErr *types.DuplicateKnowledgeError
			if errors.As(err, &dupErr) && dupIsSameNode(dupErr, item) {
				// Identical content is already present in the KB under THIS node's
				// own external_id, so the parent effectively exists — reconcile the
				// subtree so children removed from the doc do not linger.
				s.sweepStaleSubtree(ctx, ds, item)
			}
			return isUpdate, err
		}
		// URL-created knowledge has no metadata, so a later deletion could
		// never find it. Attach the datasource keys on fresh creation only;
		// the duplicate path reuses an existing row that must not be re-tagged.
		if created != nil {
			metadataBytes, mErr := json.Marshal(metadata)
			if mErr != nil {
				return isUpdate, fmt.Errorf("marshal datasource metadata: %w", mErr)
			}
			created.Metadata = types.JSON(metadataBytes)
			if uErr := s.knowledgeService.GetRepository().UpdateKnowledge(ctx, created); uErr != nil {
				return isUpdate, fmt.Errorf("attach datasource metadata: %w", uErr)
			}
		}
		s.sweepStaleSubtree(ctx, ds, item)
		return isUpdate, nil
	}

	return isUpdate, fmt.Errorf("item has neither content nor URL")
}

// nextcloudVersionStore is implemented by the SQL knowledge repository. It is
// narrow so other connectors and existing KnowledgeRepository users do not
// need a version workflow.
type nextcloudVersionStore interface {
	StageNextcloudVersion(context.Context, uint64, string, string, string, string, string) error
	StageNextcloudVersionWithSource(
		context.Context,
		uint64,
		string,
		string,
		string,
		string,
		string,
		string,
		map[string]string,
	) error
	AdmitNextcloudCandidateRetry(context.Context, uint64, string, string, string, string) error
	PublishNextcloudVersion(context.Context, string) (bool, error)
	TombstoneNextcloudVersion(context.Context, uint64, string, string, string) error
}

func (s *DataSourceService) ingestNextcloudItem(
	ctx context.Context, ds *types.DataSource, item *types.FetchedItem,
	metadata map[string]string, tagIDs []string,
) (bool, error) {
	versions, ok := s.knowledgeService.GetRepository().(nextcloudVersionStore)
	if !ok {
		return false, apperrors.NewProtocolError(errors.New(
			"nextcloud version store unavailable",
		), "Nextcloud version store unavailable")
	}
	etag := metadata["nextcloud_etag"]
	fileName, validName := secutils.ValidateInput(item.FileName)
	if ds.TenantID == 0 || ds.ID == "" || ds.KnowledgeBaseID == "" ||
		item.ExternalID == "" || strings.TrimSpace(etag) == "" || len(item.Content) == 0 ||
		!validName || fileName == "" || fileName != item.FileName || strings.ContainsAny(fileName, `/\`) {
		return false, errors.New("incomplete Nextcloud file version")
	}
	if err := versions.AdmitNextcloudCandidateRetry(ctx, ds.TenantID,
		ds.KnowledgeBaseID, ds.ID, item.ExternalID, etag); err != nil {
		return false, err
	}
	repo := s.knowledgeService.GetRepository()
	existing, err := repo.FindByDataSourceExternalID(ctx, ds.TenantID, ds.KnowledgeBaseID, ds.ID, item.ExternalID)
	if err != nil {
		return false, fmt.Errorf("check existing Nextcloud knowledge: %w", err)
	}
	isUpdate := existing != nil
	// The source guard rejects empty ETags. Keep a newly created row hidden
	// throughout parsing, including the interval before Stage commits.
	metadata["nextcloud_target_etag"] = etag
	metadata["nextcloud_etag"] = ""
	fh, err := bytesToFileHeader(item.Content, item.FileName)
	if err != nil {
		return isUpdate, fmt.Errorf("build Nextcloud file header: %w", err)
	}
	candidate, createErr := s.knowledgeService.CreateKnowledgeFromFile(
		ctx, ds.KnowledgeBaseID, fh, metadata, nil, item.FileName,
		tagIDs, types.ConnectorTypeNextcloud, nil,
	)
	if createErr != nil {
		var duplicate *types.DuplicateKnowledgeError
		if !errors.As(createErr, &duplicate) || !dupIsSameNode(duplicate, item) {
			return isUpdate, createErr
		}
		candidate = duplicate.Knowledge
	}
	if candidate == nil || candidate.ID == "" || candidate.ParseStatus == types.ParseStatusFailed {
		return isUpdate, apperrors.NewProtocolError(errors.New(
			"nextcloud candidate could not be queued for parsing",
		), "Nextcloud candidate could not be queued for parsing")
	}
	if err := versions.StageNextcloudVersionWithSource(ctx, ds.TenantID, ds.KnowledgeBaseID,
		ds.ID, item.ExternalID, etag, candidate.ID, fileName, metadata); err != nil {
		return isUpdate, apperrors.NewProtocolError(fmt.Errorf(
			"stage Nextcloud version: %w",
			err,
		), fmt.Sprintf("stage Nextcloud version: %s", apperrors.PublicMessage(
			err,
		)))
	}
	// A duplicate with identical bytes may already be fully parsed. The
	// repository also invokes this after asynchronous completion.
	if _, err := versions.PublishNextcloudVersion(ctx, candidate.ID); err != nil {
		return isUpdate, apperrors.NewProtocolError(fmt.Errorf(
			"publish completed Nextcloud version: %w",
			err,
		), fmt.Sprintf("publish completed Nextcloud version: %s", apperrors.PublicMessage(
			err,
		)))
	}
	return isUpdate, nil
}

// dupIsSameNode reports whether a duplicate-content error means the parent still
// exists in the KB *under this item's own external_id* — i.e. a content-dedup hit
// against this same node, so reconciling its subtree is safe. File deduplication
// keys on file_hash plus file_type (CheckKnowledgeExists), so an updated node whose rebuilt body
// happens to hash-collide with a DIFFERENT knowledge item (another node, or a
// manually-uploaded file with no external_id) would otherwise sweep this node's
// children even though its own parent row was just deleted for the update and
// never recreated — deleting those children with no parent to replace them. In
// that case the matched row's external_id differs (or is absent), so we skip the
// sweep and leave the children intact.
func dupIsSameNode(dupErr *types.DuplicateKnowledgeError, item *types.FetchedItem) bool {
	return dupErr != nil && dupErr.Knowledge != nil &&
		dupErr.Knowledge.GetMetadata()["external_id"] == item.ExternalID
}

// sweepStaleSubtree deletes STALE sub-items of item — knowledge whose external_id
// is prefixed with "<item.ExternalID>#" (e.g. attachment children of a docx node)
// that is NOT listed in item.SubtreeKeep, i.e. no longer present in the source.
//
// It runs only AFTER the parent item exists in the KB (freshly (re)created, or
// confirmed present via a duplicate-hash error), so a genuinely failed parent
// write never destroys existing children. Children still present in the source
// are preserved via SubtreeKeep even when they could not be re-ingested this
// cycle (e.g. a transient attachment download failure), so a still-present
// attachment never loses its previously-synced good copy. The "<id>#" prefix
// never matches the parent's own "<id>" external_id, so the parent is never
// self-swept.
func (s *DataSourceService) sweepStaleSubtree(ctx context.Context, ds *types.DataSource, item *types.FetchedItem) {
	if !item.ReplacesSubtree || item.ExternalID == "" {
		return
	}
	repo := s.knowledgeService.GetRepository()
	children, err := repo.FindByMetadataKeyPrefix(ctx, ds.TenantID, ds.KnowledgeBaseID, "external_id", types.SubtreeChildPrefix(item.ExternalID))
	if err != nil {
		logger.Warnf(ctx, "failed to list subtree of external_id=%s: %v", item.ExternalID, err)
		return
	}
	if len(children) == 0 {
		return
	}
	ids := make([]string, 0, len(children))
	for _, child := range children {
		// Scope to this data source so identical external_id prefixes from
		// another connector in the same KB cannot be swept.
		if child.GetMetadata()["datasource_id"] != ds.ID {
			continue
		}
		// A child still present in the source is preserved even if it could not be
		// re-ingested this sync; only children that vanished from the source are
		// stale and swept. Every child here was selected by the external_id-prefix
		// query, so its external_id is guaranteed present and readable (a malformed
		// row could not have matched the SQL predicate), and GetMetadata resolves
		// it identically to the keep-set entries the connector built. SubtreeKeep
		// holds one entry per still-present sub-item of this node (a small set), so
		// a linear scan is cheaper than materializing a lookup map.
		if slices.Contains(item.SubtreeKeep, child.GetMetadata()["external_id"]) {
			continue
		}
		ids = append(ids, child.ID)
	}
	if len(ids) == 0 {
		return
	}
	// Batch the deletion so a node whose attachment set shrank from N pays one
	// round of the delete fan-out rather than N sequential ones.
	if derr := s.knowledgeService.DeleteKnowledgeList(ctx, ids); derr != nil {
		logger.Warnf(ctx, "failed to delete %d stale sub-item(s) of external_id=%s: %v",
			len(ids), item.ExternalID, derr)
	} else if herr := repo.HardDeleteKnowledgeList(ctx, ds.TenantID, ids); herr != nil {
		logger.Warnf(ctx, "failed to hard-delete %d stale sub-item(s) of external_id=%s: %v",
			len(ids), item.ExternalID, herr)
	}
}

// bytesToFileHeader wraps a []byte into a *multipart.FileHeader so it can be
// consumed by KnowledgeService.CreateKnowledgeFromFile.
func bytesToFileHeader(data []byte, filename string) (*multipart.FileHeader, error) {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	// Create a form file part
	partHeader := make(textproto.MIMEHeader)
	partHeader.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, filename))
	partHeader.Set("Content-Type", "application/octet-stream")

	part, err := writer.CreatePart(partHeader)
	if err != nil {
		return nil, fmt.Errorf("create multipart part: %w", err)
	}

	if _, err := part.Write(data); err != nil {
		return nil, fmt.Errorf("write data to part: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close multipart writer: %w", err)
	}

	// Parse the multipart data to get a FileHeader
	reader := multipart.NewReader(&buf, writer.Boundary())
	form, err := reader.ReadForm(int64(len(data)) + 1024)
	if err != nil {
		return nil, fmt.Errorf("read multipart form: %w", err)
	}

	files := form.File["file"]
	if len(files) == 0 {
		return nil, fmt.Errorf("no file in multipart form")
	}

	return files[0], nil
}

func timePtr(t time.Time) *time.Time {
	utc := t.UTC()
	return &utc
}
