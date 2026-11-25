package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

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
	capacityAdmission       *NextcloudCapacityAdmission
	sourceInputs            *apprepo.KnowledgeInputProvenanceStore
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
	if err := datasource.ValidateSyncSchedule(ds.SyncSchedule); err != nil {
		return nil, err
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
		if ds.Type == types.ConnectorTypeDingTalk {
			if cfg.Settings == nil {
				cfg.Settings = make(map[string]interface{})
			}
			if _, set := cfg.Settings["include_uploaded_files"]; !set {
				cfg.Settings["include_uploaded_files"] = false
			}
		}
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
	return s.updateDataSource(ctx, ds, nil, nil)
}

// PatchDataSourceSyncSettings preserves worker state and settings omitted by the caller.
func (s *DataSourceService) PatchDataSourceSyncSettings(
	ctx context.Context, before, after *types.DataSource, fields []string,
) (*types.DataSource, error) {
	if before == nil || after == nil || before.ID != after.ID || len(fields) == 0 {
		return nil, datasource.ErrDataSourceInvalid
	}
	return s.updateDataSource(ctx, after, before, fields)
}

func (s *DataSourceService) updateDataSource(
	ctx context.Context, ds, before *types.DataSource, fields []string,
) (*types.DataSource, error) {
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
	if err := datasource.ValidateSyncSchedule(ds.SyncSchedule); err != nil {
		return nil, err
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

	if before != nil {
		err = s.dsRepo.PatchSyncSettings(ctx, ds, before.Config, fields)
	} else {
		err = s.dsRepo.Update(ctx, ds)
	}
	if err != nil {
		logger.Errorf(ctx, "failed to update data source: %v", err)
		return nil, err
	}

	if before != nil {
		// Schedule from current persisted status, never the handler's stale snapshot.
		ds, err = s.dsRepo.FindByID(ctx, ds.ID)
		if err != nil {
			return nil, err
		}
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
	if err := datasource.ValidateSyncSchedule(ds.SyncSchedule); err != nil {
		return err
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
func (s *DataSourceService) GetSyncLogs(
	ctx context.Context,
	dsID string,
	limit int,
	offset int,
) ([]*types.SyncLog, error) {
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

// finishNextcloudPreStreamRun closes a running event/candidate log when a
// worker exits before stream setup. It intentionally does not change datasource
// status; the dispatcher owns the durable retry proof.
func (s *DataSourceService) finishNextcloudPreStreamRun(
	ctx context.Context, syncLogID string, cause error,
) {
	if s.syncLogRepo == nil || syncLogID == "" {
		return
	}
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	syncLog, err := s.syncLogRepo.FindByID(finishCtx, syncLogID)
	if err != nil || syncLog == nil || syncLog.Status != types.SyncLogStatusRunning {
		return
	}
	syncLog.Status = types.SyncLogStatusFailed
	syncLog.FinishedAt = timePtr(time.Now().UTC())
	syncLog.ErrorMessage = "nextcloud_pre_stream_failed"
	if errors.Is(cause, apprepo.ErrKnowledgeInputChanged) {
		syncLog.ErrorMessage = "source_lineage_changed"
	}
	_ = s.syncLogRepo.Update(finishCtx, syncLog)
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
	_ = s.dsRepo.UpdateSyncState(ctx, ds)
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

// syncCompletionPersisted reports whether the data-source completion write for
// this round finished. updateSyncRunResult writes the terminal log first, so the
// log alone cannot prove completion. LastSyncAt must belong to this round, and
// a pending resync marker still requires the post-completion cleanup write.
func syncCompletionPersisted(ds *types.DataSource, syncLog *types.SyncLog) bool {
	if ds.LastSyncAt == nil || ds.LastSyncAt.Before(syncLog.StartedAt) {
		return false
	}
	config, err := ds.ParseConfig()
	if err != nil {
		return false
	}
	return !resyncRequired(config)
}

// ProcessSync handles the actual sync operation (called by asynq task)
func (s *DataSourceService) ProcessSync(ctx context.Context, task *asynq.Task) (runErr error) {
	var payload types.DataSourceSyncPayload
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		logger.Errorf(ctx, "failed to unmarshal sync payload: %v", err)
		return err
	}
	// Restore the admitted actor and execution tenant before any task-scoped
	// repository checks, including Nextcloud event and candidate-retry fences.
	ctx = types.WithTaskAuthorization(ctx, payload.TenantID, payload.Initiator)
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

	// Get sync log. It is loaded before the data source so that a redelivery of
	// an already finished round is recognised even when the data source has been
	// deleted in the meantime, instead of having its log rewritten to canceled.
	syncLog, err := s.syncLogRepo.FindByID(ctx, payload.SyncLogID)
	if errors.Is(err, datasource.ErrSyncLogNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read sync log: %w", err)
	}

	if syncLog == nil {
		return fmt.Errorf("read sync log: missing row")
	}

	// Canceled rounds were stopped on purpose and have no completion write to
	// verify. Keep recognizing them before the data-source lookup so deletion
	// cannot rewrite the log.
	if syncLog.Status == types.SyncLogStatusCanceled {
		logger.Infof(ctx, "sync log already finished, skipping replay: ds=%s syncLog=%s status=%s",
			payload.DataSourceID, payload.SyncLogID, syncLog.Status)
		return nil
	}

	// Get data source
	ds, err := s.GetDataSource(ctx, payload.DataSourceID)
	sourceMissing := errors.Is(err, datasource.ErrDataSourceNotFound)
	if err != nil && !sourceMissing {
		return fmt.Errorf("read data source for sync: %w", err)
	}
	terminalResult := syncLog.Status == types.SyncLogStatusSuccess || syncLog.Status == types.SyncLogStatusPartial
	if sourceMissing {
		if terminalResult {
			// The source is gone, so there is no completion write left to repair.
			// Preserve the finished log rather than rewriting it as canceled.
			logger.Infof(ctx, "sync log already finished, skipping replay: ds=%s syncLog=%s status=%s",
				payload.DataSourceID, payload.SyncLogID, syncLog.Status)
			return nil
		}
		if s.sourceInputs != nil {
			dingTalk, identifyErr := s.sourceInputs.IsDingTalkSourceTask(ctx, payload.TenantID, payload.DataSourceID)
			if identifyErr != nil {
				return identifyErr
			}
			if dingTalk {
				return fmt.Errorf("%w: DingTalk source unavailable before run claim", asynq.SkipRetry)
			}
		}
		if syncLog.TenantID != payload.TenantID || syncLog.DataSourceID != payload.DataSourceID {
			return fmt.Errorf("%w: source task sync-log binding changed", asynq.SkipRetry)
		}
		logger.Warnf(ctx, "data source not found, cancelling sync: ds=%s", payload.DataSourceID)
		return s.cancelSyncLog(ctx, syncLog, "data source has been deleted")
	}

	if ds == nil || ds.TenantID != payload.TenantID {
		return fmt.Errorf("data source task tenant binding changed: %w", asynq.SkipRetry)
	}

	// A terminal log without its source completion must fall through so an
	// asynq retry can finish the writes. failed and running also fall through:
	// failed resumes from its cursor, and running is the attempt to recover.
	if terminalResult && syncCompletionPersisted(ds, syncLog) {
		logger.Infof(ctx, "sync log already finished, skipping replay: ds=%s syncLog=%s status=%s",
			payload.DataSourceID, payload.SyncLogID, syncLog.Status)
		return nil
	}
	if ds.Type == types.ConnectorTypeNextcloud && ds.SyncEnabled != nil && !*ds.SyncEnabled {
		syncLog.Status = types.SyncLogStatusCanceled
		syncLog.FinishedAt = timePtr(time.Now().UTC())
		syncLog.ErrorMessage = "content_sync_paused"
		_ = s.syncLogRepo.Update(ctx, syncLog)
		return fmt.Errorf("%w: content sync paused", asynq.SkipRetry)
	}
	if ds.Type == types.ConnectorTypeNextcloud && syncLog.Status != types.SyncLogStatusRunning {
		return fmt.Errorf("%w: Nextcloud sync log is no longer running", asynq.SkipRetry)
	}
	if ds.Type == types.ConnectorTypeNextcloud &&
		(payload.Trigger == "nextcloud_event" || payload.Trigger == "nextcloud_candidate_retry") {
		// Event and candidate tasks have no Asynq retries. A pre-stream error
		// must still close the exact running log so dispatch can retry rather
		// than treating an archived task as a 150-minute running sync.
		defer func() {
			if runErr != nil {
				s.finishNextcloudPreStreamRun(ctx, payload.SyncLogID, runErr)
			}
		}()
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

	if ds.Type == types.ConnectorTypeNextcloud {
		if s.sourceInputs == nil {
			return types.ErrSourceLineageUnknown
		}
		var closeConfiguration func()
		ctx, closeConfiguration, err = s.sourceInputs.BeginNextcloudOriginalFetchConfigurationRead(ctx, ds)
		if err != nil {
			return fmt.Errorf("begin Nextcloud configuration read: %w", err)
		}
		defer closeConfiguration()
	}

	kb, kbErr := s.kbService.GetKnowledgeBaseByID(ctx, ds.KnowledgeBaseID)
	if kbErr != nil {
		if ds.Type == types.ConnectorTypeDingTalk {
			return kbErr
		}
		if ds.Type == types.ConnectorTypeNextcloud {
			// A failed current configuration/source check is not proof that
			// the KB was deleted. Keep the failure retryable and preserve it.
			s.recordPreStreamSyncFailure(
				ctx,
				ds,
				syncLog,
				"Nextcloud configuration unavailable",
				ds.Status == types.DataSourceStatusPaused,
			)
			return fmt.Errorf("read Nextcloud configuration projection: %w", kbErr)
		}
		if !errors.Is(kbErr, apprepo.ErrKnowledgeBaseNotFound) {
			return fmt.Errorf("read knowledge base for sync: %w", kbErr)
		}
		logger.Warnf(ctx, "knowledge base not found, cancelling sync: kb=%s ds=%s",
			ds.KnowledgeBaseID, payload.DataSourceID)
		return s.cancelSyncLog(ctx, syncLog, "knowledge base has been deleted")
	}

	if ds.Type == types.ConnectorTypeDingTalk &&
		(payload.Trigger == "schedule" || syncLog.RecoveryTrigger == "schedule") {
		if payload.Trigger != "schedule" || payload.ForceFull || payload.Initiator != (types.TaskInitiator{}) ||
			syncLog.TenantID != ds.TenantID || syncLog.DataSourceID != ds.ID || kb == nil ||
			kb.TenantID != ds.TenantID || kb.ID != ds.KnowledgeBaseID {
			return fmt.Errorf("%w: DingTalk scheduled sync binding changed", asynq.SkipRetry)
		}
		return s.processDingTalkSync(ctx, ds, kb, payload)
	}
	automaticSource, automaticErr := s.authorizeNextcloudAutomaticSync(ctx, ds, payload)
	if automaticErr != nil {
		failureCode := nextcloudAutomaticAuthorizationFailureCode(automaticErr)
		s.recordPreStreamSyncFailure(
			ctx,
			ds,
			syncLog,
			"Nextcloud automatic source authorization unavailable ("+failureCode+")",
			ds.Status == types.DataSourceStatusPaused,
		)
		return automaticErr
	}
	if knowledge, ok := s.knowledgeService.(*knowledgeService); ok && !automaticSource {
		if err := knowledge.revalidateBackgroundKBAccess(ctx, ds.TenantID, ds.KnowledgeBaseID,
			types.ResourceActionEdit); err != nil {
			if errors.Is(err, ErrResourceAccessDenied) {
				if ds.Type != types.ConnectorTypeDingTalk {
					syncLog.Status = types.SyncLogStatusFailed
					syncLog.FinishedAt = timePtr(time.Now().UTC())
					syncLog.ErrorMessage = "processing permission revoked"
					_ = s.syncLogRepo.Update(ctx, syncLog)
				}
				return fmt.Errorf("data source processing authorization revoked: %w", asynq.SkipRetry)
			}
			return err
		}
	}
	if automaticSource {
		// This private marker preserves the exact admitted operation only. It
		// never supplies a user/role or a grant to another knowledge base.
		ctx = context.WithValue(ctx, nextcloudAutomaticSyncKey{}, nextcloudAutomaticSyncScope{
			tenant: ds.TenantID, kb: ds.KnowledgeBaseID, source: ds.ID,
			log: payload.SyncLogID, trigger: payload.Trigger,
		})
	}
	ctx, err = access.WithKBTaskWrite(ctx, kb, ds.TenantID)
	if err != nil {
		return fmt.Errorf("%w: data source KB does not belong to its tenant", asynq.SkipRetry)
	}
	if ds.Type == types.ConnectorTypeDingTalk {
		return s.processDingTalkSync(ctx, ds, kb, payload)
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
	config.ProcessingPolicyDigest = types.GraphDigest(struct {
		Process types.EffectiveProcessConfig
		Index   types.IndexingStrategy
		Summary string
	}{ResolveProcessConfig(kb, nil), kb.IndexingStrategy, kb.SummaryModelID})
	if ds.Type == types.ConnectorTypeNextcloud {
		if s.nextcloudEventInbox == nil {
			return apperrors.NewProtocolError(
				fmt.Errorf("nextcloud source baseline verifier is unavailable"),
				"Nextcloud source baseline verifier is unavailable",
			)
		}
		if err := s.nextcloudEventInbox.ValidateNextcloudSourceBaseline(ctx, ds); err != nil {
			s.updateSyncRunResult(ctx, ds, syncLog, &types.SyncResult{}, nil,
				types.SyncLogStatusFailed, apperrors.PublicMessage(err), wasPaused)
			return err
		}
		if s.sourceInputs == nil {
			return types.ErrSourceLineageUnknown
		}
		ctx, err = s.sourceInputs.BindNextcloudOriginalFetch(ctx, kb, ds)
		if err != nil {
			return fmt.Errorf("bind Nextcloud original fetch: %w", err)
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

	var ingestErr error
	nonBlockingFailures := 0
	for _, item := range items {
		if ds.Type == types.ConnectorTypeNextcloud {
			cancelled, guardErr := s.checkNextcloudSyncActive(ctx, payload)
			if cancelled || guardErr != nil {
				return guardErr
			}
		}
		item := item
		ingestCtx := bindFetchedConnectorItem(withKBActivitySuppressed(ctx), connector, ds, &item)
		nonBlocking, err := s.applyFetchedItem(ingestCtx, ds, &item, autoTagIDs, result)
		if nonBlocking || errors.Is(err, ErrInvalidFileType) {
			nonBlockingFailures++
		} else if err != nil && ingestErr == nil {
			ingestErr = err
		}
	}
	if ds.Type == types.ConnectorTypeNextcloud {
		cancelled, guardErr := s.checkNextcloudSyncActive(ctx, payload)
		if cancelled || guardErr != nil {
			return guardErr
		}
	}

	resultJSON, _ := result.ToJSON()
	if ingestErr != nil {
		// Keep the previous cursor so a retry can recover items the sink did not accept.
		s.updateSyncRunResult(ctx, ds, syncLog, result, resultJSON,
			types.SyncLogStatusFailed, apperrors.PublicMessage(ingestErr), wasPaused)
		return ingestErr
	}
	if err := allFetchedItemsFailedError(result, nonBlockingFailures); err != nil {
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
	if err := s.updateSyncRunResult(
		ctx, ds, syncLog, result, resultJSON, syncStatus, syncErrorMessage, wasPaused,
	); err != nil {
		return err
	}

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
func (s *DataSourceService) checkNextcloudSyncActive(
	ctx context.Context,
	payload types.DataSourceSyncPayload,
	observedInstanceID ...string,
) (bool, error) {
	if s.dsRepo == nil || payload.DataSourceID == "" {
		return false, errors.New("nextcloud current data source unavailable")
	}
	currentSource, err := s.GetDataSource(ctx, payload.DataSourceID)
	if err != nil {
		return false, err
	}
	if currentSource == nil || currentSource.TenantID != payload.TenantID ||
		currentSource.Type != types.ConnectorTypeNextcloud {
		return false, errors.New("nextcloud current data source scope changed")
	}
	if currentSource.Status == types.DataSourceStatusPaused {
		return true, fmt.Errorf("%w: source admission paused", asynq.SkipRetry)
	}
	if currentSource.SyncEnabled != nil && !*currentSource.SyncEnabled {
		return true, fmt.Errorf("%w: content sync paused", asynq.SkipRetry)
	}
	if s.nextcloudSourcePairings != nil {
		active, err := s.nextcloudSourcePairings.ActiveForSync(
			ctx,
			payload.DataSourceID,
			payload.TenantID,
			observedInstanceID...)
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
	if admitted, present := ctx.Value(nextcloudAutomaticSyncKey{}).(nextcloudAutomaticSyncScope); present {
		if admitted.tenant != currentSource.TenantID || admitted.kb != currentSource.KnowledgeBaseID ||
			admitted.source != currentSource.ID ||
			admitted.log != payload.SyncLogID || admitted.trigger != payload.Trigger ||
			s.sourceInputs == nil {
			return true, apprepo.ErrNextcloudMachineTaskInvalid
		}
		// Independent configuration reads protect bytes, but do not by
		// themselves prove a current Nextcloud manager. Recheck that separate
		// authority at every item and cursor commit, including during long scans.
		if err := s.sourceInputs.CheckNextcloudAutomaticSyncManagerPurpose(
			ctx,
			admitted.tenant,
			admitted.kb,
			admitted.source,
		); err != nil {
			return true, err
		}
	}
	return false, nil
}

type (
	nextcloudAutomaticSyncKey   struct{}
	nextcloudAutomaticSyncScope struct {
		tenant                   uint64
		kb, source, log, trigger string
	}
)

// Ordinary human/API/manual/scheduled tasks retain their existing live ACL.
// Only a real zero-retry automatic dispatch may consume the managed source's
// exact private configuration read and fresh manager purpose instead.
func (s *DataSourceService) authorizeNextcloudAutomaticSync(
	ctx context.Context,
	ds *types.DataSource,
	payload types.DataSourceSyncPayload,
) (bool, error) {
	if ds == nil || ds.Type != types.ConnectorTypeNextcloud ||
		!reflect.DeepEqual(payload.Initiator, types.TaskInitiator{}) ||
		(payload.Trigger != "nextcloud_event" && payload.Trigger != "nextcloud_candidate_retry") {
		return false, nil
	}
	principal, present := types.PrincipalFromContext(ctx)
	if !present || principal.Type != "background_unverified" || types.CallerFromContext(ctx).UserID != "" {
		return false, nil
	}
	if _, api := types.TenantAPIKeyScopeFromContext(ctx); api {
		return false, nil
	}
	taskID, _ := asynq.GetTaskID(ctx)
	if taskID == "" {
		taskID, _ = types.TaskExecutionIDFromContext(ctx)
	}
	retried, maxRetry, metadata := 0, 0, false
	if retry, ok := asynq.GetRetryCount(ctx); ok {
		var maxOK bool
		retried = retry
		maxRetry, maxOK = asynq.GetMaxRetry(ctx)
		metadata = maxOK
	} else {
		retried, maxRetry, metadata = types.TaskRetryMetadataFromContext(ctx)
	}
	expectedID := "ncevent:" + payload.NextcloudEventConnectionID + ":" + payload.SyncLogID
	if payload.Trigger == "nextcloud_candidate_retry" {
		expectedID = "nccandidate:" + payload.SyncLogID
		if payload.NextcloudRetryKnowledgeBaseID != ds.KnowledgeBaseID {
			return false, apprepo.ErrNextcloudMachineTaskInvalid
		}
	}
	if taskID != expectedID || !metadata || retried != 0 || maxRetry != 0 {
		return false, nil
	}
	if ds.TenantID != payload.TenantID || ds.ID != payload.DataSourceID || s.sourceInputs == nil {
		return false, apprepo.ErrNextcloudMachineTaskInvalid
	}
	store := s.sourceInputs.NextcloudOnboardingConfigurationStore()
	if store == nil {
		return false, apprepo.ErrNextcloudMachineTaskInvalid
	}
	managed, err := store.ManagedKB(ctx, ds.TenantID, ds.KnowledgeBaseID)
	if err != nil || !managed {
		return false, err
	}
	if cancelled, err := s.checkNextcloudSyncActive(ctx, payload); cancelled || err != nil {
		if err == nil {
			err = apprepo.ErrNextcloudMachineTaskInvalid
		}
		return false, err
	}
	if payload.Trigger == "nextcloud_candidate_retry" {
		if err := s.validateNextcloudCandidateRetry(ctx, payload, true); err != nil {
			return false, err
		}
	}
	if err := s.sourceInputs.CheckNextcloudAutomaticSyncManagerPurpose(
		ctx,
		ds.TenantID,
		ds.KnowledgeBaseID,
		ds.ID,
	); err != nil {
		return false, err
	}
	return true, nil
}

func nextcloudAutomaticAuthorizationFailureCode(err error) string {
	switch {
	case errors.Is(err, access.ErrNextcloudPublicationDenied):
		return "manager_denied"
	case errors.Is(err, access.ErrNextcloudPublicationUnavailable):
		return "manager_unavailable"
	case errors.Is(err, apprepo.ErrNextcloudOnboardingDenied):
		return "local_authority_denied"
	case errors.Is(err, apprepo.ErrNextcloudMachineTaskInvalid):
		return "machine_task_invalid"
	case errors.Is(err, apprepo.ErrKnowledgeInputChanged):
		return "scope_changed"
	default:
		return "authorization_unavailable"
	}
}

// resolveAutoTagIDs finds or creates the per-data-source tag applied to every
// synced item so results are identifiable in the KB. A tag failure is
// non-fatal: the sync proceeds untagged.
func (s *DataSourceService) resolveAutoTagIDs(ctx context.Context, ds *types.DataSource) []string {
	if ds.Type == types.ConnectorTypeNextcloud {
		// Creating or assigning this name's grouping tag is deferred to the
		// actual original upload worker after Stage and a private build lease.
		return nil
	}
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

// recordSyncDeletedItem appends a source-deleted item sample to
// result.DeletedItems (capped at maxSyncResultErrors and deduplicated),
// resolving a human-readable title from the item, KB row, or prior sync result.
func recordSyncDeletedItem(
	result *types.SyncResult, ds *types.DataSource,
	existing *types.Knowledge, item *types.FetchedItem,
) {
	if result == nil || item == nil || len(result.DeletedItems) >= maxSyncResultErrors {
		return
	}
	for _, d := range result.DeletedItems {
		if d.ExternalID == item.ExternalID && d.SourceResourceID == item.SourceResourceID {
			return
		}
	}
	result.DeletedItems = append(result.DeletedItems, types.SyncDeletedItem{
		ExternalID:       item.ExternalID,
		SourceResourceID: item.SourceResourceID,
		Title:            resolveDeletedItemTitle(ds, existing, item),
	})
}

func resolveDeletedItemTitle(
	ds *types.DataSource, existing *types.Knowledge, item *types.FetchedItem,
) string {
	if t := strings.TrimSpace(item.Title); t != "" {
		return t
	}
	if existing != nil {
		if t := strings.TrimSpace(existing.Title); t != "" {
			return t
		}
		if t := strings.TrimSpace(existing.FileName); t != "" {
			return t
		}
	}
	if ds != nil && len(ds.LastSyncResult) > 0 {
		var prev types.SyncResult
		if err := json.Unmarshal(ds.LastSyncResult, &prev); err == nil {
			if item.ExternalID != "" {
				for _, d := range prev.DeletedItems {
					if d.Title != "" && d.ExternalID == item.ExternalID {
						return d.Title
					}
				}
			}
			if item.SourceResourceID != "" {
				matchedTitle := ""
				ambiguous := false
				for _, d := range prev.DeletedItems {
					if d.Title == "" || d.SourceResourceID != item.SourceResourceID {
						continue
					}
					if matchedTitle == "" {
						matchedTitle = d.Title
					} else if matchedTitle != d.Title {
						ambiguous = true
						break
					}
				}
				if matchedTitle != "" && !ambiguous {
					return matchedTitle
				}
			}
		}
	}
	return ""
}

// syncErrorItemLabel formats a user-facing item label for sync error samples.
// When FileName carries a folder path (e.g. Feishu Drive/Wiki path-qualified
// names) and Title is a bare name, combine them so operators can immediately
// locate the failing file.
func syncErrorItemLabel(item *types.FetchedItem) string {
	if item == nil {
		return ""
	}
	title := strings.TrimSpace(item.Title)
	fileName := strings.TrimSpace(item.FileName)
	if fileName != "" {
		folder, base := types.SplitKnowledgeRelativePath(fileName)
		if folder != "" {
			if title == "" {
				if base != "" {
					return folder + "/" + base
				}
				return fileName
			}
			if !strings.Contains(title, "/") {
				return folder + "/" + title
			}
		}
	}
	if title != "" {
		return title
	}
	return fileName
}

// isGlobalIngestAbortError reports whether an ingestion error reflects a
// workspace/context-level failure that affects all remaining items and must
// abort streaming immediately rather than isolating per item.
func isGlobalIngestAbortError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var quotaErr *types.StorageQuotaExceededError
	return errors.As(err, &quotaErr)
}

// fetchFailureSyncError maps a connector error item into a structured, user-
// facing sample. Connectors that classify their errors (Feishu) provide a stable
// i18n code + params via metadata so the frontend localises it to the viewer's
// language; the raw status/body/log_id never leaves the server logs. Connectors
// without codes keep the raw text as a Message fallback. Best practice per
// Airbyte/Fivetran/Onyx: humanised, actionable, localised UI; raw detail in logs.
func fetchFailureSyncError(item *types.FetchedItem, rawMsg string) types.SyncItemError {
	e := types.SyncItemError{Title: syncErrorItemLabel(item)}
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
// stays identical across both fetch paths. Unexpected ingestion errors are
// returned so callers do not acknowledge a cursor beyond an unaccepted item.
// The boolean marks oversized downloads that should not disable scheduled syncs.
func (s *DataSourceService) applyFetchedItem(
	ctx context.Context, ds *types.DataSource, item *types.FetchedItem,
	tagIDs []string, result *types.SyncResult,
) (nonBlockingFailure bool, ingestErr error) {
	if item.IsDeleted {
		selectedDeleted := item.Metadata[types.MetaKeySelectedResourceDeleted] == "true"
		if !ds.SyncDeletions && ds.Type != types.ConnectorTypeNextcloud {
			if selectedDeleted {
				var existing *types.Knowledge
				if s.knowledgeService != nil && s.knowledgeService.GetRepository() != nil && item.ExternalID != "" {
					existing, _ = s.knowledgeService.GetRepository().FindByDataSourceExternalID(
						ctx, ds.TenantID, ds.KnowledgeBaseID, ds.ID, item.ExternalID,
					)
				}
				result.Skipped++
				recordSyncDeletedItem(result, ds, existing, item)
			}
			// Sync deletion disabled: neither count nor delete.
			return false, nil
		}
		if item.ExternalID == "" {
			logger.Warnf(ctx, "skipping deletion for item %q: empty external_id", item.Title)
			result.Skipped++
			return false, nil
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
			return false, nil
		}
		if existing == nil {
			// Deletion is idempotent: the source item may already have been
			// removed manually or by an earlier sync.
			s.sweepStaleSubtree(ctx, ds, item)
			result.Skipped++
			if selectedDeleted {
				recordSyncDeletedItem(result, ds, nil, item)
			}
			return false, nil
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
			return false, nil
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
			return false, nil
		}
		s.sweepStaleSubtree(ctx, ds, item)
		result.Deleted++
		recordSyncDeletedItem(result, ds, existing, item)
		return false, nil
	}

	if len(item.Content) == 0 && item.URL == "" {
		// Check if this is an error item from the connector (failed to fetch content)
		if errMsg, hasErr := item.Metadata["error"]; hasErr {
			logger.Warnf(ctx, "item %q (external_id=%s) fetch failed: %s", item.Title, item.ExternalID, errMsg)
			result.Failed++
			recordSyncError(result, fetchFailureSyncError(item, errMsg))
			return item.Metadata["error_reason_code"] == "dingtalk_document_too_large", nil
		} else {
			logger.Infof(ctx, "skipping item %q (external_id=%s): no content or URL", item.Title, item.ExternalID)
			result.Skipped++
		}
		return false, nil
	}

	isUpdate, err := s.ingestItem(ctx, ds, item, tagIDs)
	if err != nil {
		itemLabel := syncErrorItemLabel(item)
		var dupErr *types.DuplicateKnowledgeError
		var appErr *apperrors.AppError
		var quotaErr *types.StorageQuotaExceededError
		switch {
		case errors.Is(err, apprepo.ErrNextcloudCandidateRetryNotDue),
			errors.Is(err, apprepo.ErrNextcloudCandidateRetryManual):
			// The durable per-file retry job owns unchanged failed ETags.
			result.Skipped++
		case errors.As(err, &dupErr):
			// Duplicate file/URL is not a failure — count as skipped.
			logger.Infof(ctx, "item %q (external_id=%s) already exists, skipping", item.Title, item.ExternalID)
			result.Skipped++
		case errors.Is(err, ErrInvalidFileType):
			result.Failed++
			recordSyncError(result, types.SyncItemError{
				Title: itemLabel, Code: "unsupported_file_type", Message: "Unsupported file type",
			})
			return false, err
		case errors.Is(err, ErrInvalidURL):
			result.Failed++
			recordSyncError(result, types.SyncItemError{
				Title: itemLabel, Code: "invalid_url", Message: "Invalid or unsafe URL",
			})
			return true, nil
		case errors.Is(err, errInvalidJSONContent):
			result.Failed++
			recordSyncError(result, types.SyncItemError{
				Title:   itemLabel,
				Code:    "invalid_json_content",
				Message: "Invalid or empty JSON file content",
			})
			return true, nil
		case errors.As(err, &appErr) && appErr.Code == apperrors.ErrImportPrerequisiteMissing:
			// A missing VLM/ASR prerequisite can never succeed on replay:
			// the file waits for the operator to configure a model.
			// Classify it as a permanent rejection — visible in the sync
			// log, stream keeps going, cursor advances past it — so one
			// image cannot abort the round and force every later sync to
			// re-fetch the same items again (Tencent/WeKnora#4712).
			result.Failed++
			recordSyncError(result, types.SyncItemError{
				Title:   itemLabel,
				Code:    "prerequisite_not_configured",
				Message: appErr.Message,
			})
			return true, nil
		case errors.As(err, &appErr) && appErr.Code == apperrors.ErrValidation:
			// Filename or folder-path validation errors cannot succeed until
			// the source item is renamed; keep them visible without blocking
			// unrelated files or newly added directories.
			result.Failed++
			recordSyncError(result, types.SyncItemError{
				Title:   itemLabel,
				Code:    "invalid_item_name",
				Message: "File name or folder path contains invalid characters",
			})
			return true, nil
		case errors.As(err, &quotaErr):
			logger.Warnf(ctx, "failed to ingest item %q (external_id=%s): %v", item.Title, item.ExternalID, err)
			result.Failed++
			recordSyncError(result, types.SyncItemError{
				Title:   itemLabel,
				Code:    "storage_quota_exceeded",
				Message: "Storage quota exceeded",
			})
			return false, &dataSourceIngestError{cause: err}
		default:
			logger.Warnf(ctx, "failed to ingest item %q (external_id=%s): %v", item.Title, item.ExternalID, err)
			result.Failed++
			recordSyncError(result, types.SyncItemError{
				Title:   itemLabel,
				Code:    "ingest_failed",
				Message: "Ingest failed; see server logs",
			})
			return false, &dataSourceIngestError{cause: err}
		}
	} else if isUpdate {
		result.Updated++
	} else {
		result.Created++
	}
	return false, nil
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

// feishuSettingResyncRequired is the one-shot upgrade marker key. Legacy
// feishu/lark data sources are stamped by migration 000122 (sqlite 000040).
const feishuSettingResyncRequired = "resync_required"

// resyncRequired reads the one-shot upgrade marker out of parsed config
// Settings. Tolerates both bool and string encodings. The marker is stamped
// onto legacy feishu/lark data sources by migration 000122 (sqlite 000040)
// when the per-source parse_mode shipped; it upgrades their next sync — even
// a scheduled incremental one — to a full pass so existing documents re-ingest
// with folder paths and block-level parsing.
func resyncRequired(cfg *types.DataSourceConfig) bool {
	if cfg == nil {
		return false
	}
	switch v := cfg.Settings[feishuSettingResyncRequired].(type) {
	case bool:
		return v
	case string:
		return v == "true"
	default:
		return false
	}
}

// clearResyncMarker removes the one-shot upgrade marker after the upgraded sync
// succeeded so it never fires again. Streaming path only: the feishu/lark
// connectors the migration tags all implement StreamingConnector.
func (s *DataSourceService) clearResyncMarker(ctx context.Context, ds *types.DataSource) {
	cfg, err := ds.ParseConfig()
	if err == nil && cfg.Settings[feishuSettingResyncRequired] == nil {
		return
	}
	// Re-read the row before writing: the upgraded sync may have run long, and
	// a full-row Update from the sync-start snapshot would silently revert
	// concurrent user edits (settings, resource_ids, schedule…). Only the
	// marker removal is ours to persist.
	fresh, err := s.dsRepo.FindByID(ctx, ds.ID)
	if err != nil {
		logger.Warnf(ctx, "failed to clear resync_required marker, re-read failed: ds=%s err=%v", ds.ID, err)
		return
	}
	cfg, err = fresh.ParseConfig()
	if err != nil {
		logger.Warnf(ctx, "failed to clear resync_required marker, config unreadable: ds=%s err=%v", ds.ID, err)
		return
	}
	if cfg.Settings[feishuSettingResyncRequired] == nil {
		return
	}
	delete(cfg.Settings, feishuSettingResyncRequired)
	blob, err := cfg.ToJSON()
	if err != nil {
		logger.Warnf(ctx, "failed to clear resync_required marker, config re-encode failed: ds=%s err=%v", ds.ID, err)
		return
	}
	fresh.Config = blob
	if err := s.dsRepo.Update(ctx, fresh); err != nil {
		logger.Warnf(ctx, "failed to clear resync_required marker: ds=%s err=%v", ds.ID, err)
	}
}

// maxConsecutiveIsolatedIngestFailures caps how many consecutive unexpected
// item ingest failures an isolating streaming connector may skip before the
// sync aborts as a systemic storage/DB outage instead of burning remote API
// budget across the remaining tree.
const maxConsecutiveIsolatedIngestFailures = 10

// streamSyncHandler adapts a streaming fetch to the knowledge-base ingest path.
// Emit ingests each item as it arrives (bounding memory) and Checkpoint persists
// the connector cursor plus live progress counts at page boundaries.
type streamSyncHandler struct {
	connector datasource.Connector
	ingestErr error
	// Count separately from the capped error samples; unsupported and oversized
	// files must not disable future syncs, including runs with both failure types.
	nonBlockingFailures int
	// isolateItemFailures is true when the connector tracks per-item cursors
	// independently and honors datasource.ErrItemIngestFailed by leaving only
	// the failed item unadvanced instead of aborting the whole round.
	isolateItemFailures       bool
	consecutiveIngestFailures int
	// unchangedItems counts in-scope items skipped by the connector's cursor
	// fast-path as already up to date during this run.
	unchangedItems            int
	svc                       *DataSourceService
	ds                        *types.DataSource
	tagIDs                    []string
	result                    *types.SyncResult
	syncLog                   *types.SyncLog
	payload                   types.DataSourceSyncPayload
	nextcloudInstanceID       string
	nextcloudBindingID        string
	nextcloudObserved         bool
	nextcloudPublicationEpoch *int64
	nextcloudGuardStopped     bool
	nextcloudFailedETags      map[string]string
}

var errNextcloudStreamCanceled = apperrors.NewProtocolError(errors.New(
	"nextcloud stream canceled",
), "Nextcloud stream canceled")

func (h *streamSyncHandler) ObserveNextcloudPublicationEpoch(
	_ context.Context,
	instance, binding string,
	epoch int64,
) error {
	if epoch < 0 || h.ds.Type != types.ConnectorTypeNextcloud {
		return errNextcloudStreamCanceled
	}
	if h.nextcloudObserved && (h.nextcloudInstanceID != instance || h.nextcloudBindingID != binding) {
		return errNextcloudStreamCanceled
	}
	h.nextcloudPublicationEpoch = &epoch
	return nil
}

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

var _ datasource.UnchangedItemRecorder = (*streamSyncHandler)(nil)

// RecordUnchanged marks one in-scope item as verified unchanged via cursor.
func (h *streamSyncHandler) RecordUnchanged() {
	h.unchangedItems++
}

// Emit ingests one streamed item. Unexpected ingestion errors either isolate
// the single item (for ItemFailureIsolatingConnector, up to the consecutive
// failure cap) or abort the stream before the connector can checkpoint past it.
func (h *streamSyncHandler) Emit(ctx context.Context, item types.FetchedItem) error {
	if h.ingestErr != nil {
		return h.ingestErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if h.ds.Type == types.ConnectorTypeNextcloud {
		if h.nextcloudPublicationEpoch != nil {
			// The connector seals the complete fetched item. Keep source observation
			// outside that immutable artifact until constructing knowledge metadata.
			ctx = context.WithValue(
				ctx,
				nextcloudObservedEpochKey{},
				nextcloudObservedEpoch{h.ds.ID, *h.nextcloudPublicationEpoch},
			)
		}
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
	createdBefore := h.result.Created
	updatedBefore := h.result.Updated
	ingestCtx := bindFetchedConnectorItem(withKBActivitySuppressed(ctx), h.connector, h.ds, &item)
	if h.ds.Type == types.ConnectorTypeNextcloud && h.payload.Trigger == "nextcloud_candidate_retry" {
		ingestCtx = apprepo.WithNextcloudCandidateRetryClaim(ingestCtx, nextcloudCandidateRetryClaim(h.payload))
	}
	nonBlocking, err := h.svc.applyFetchedItem(ingestCtx, h.ds, &item, h.tagIDs, h.result)
	if h.ds.Type == types.ConnectorTypeNextcloud && h.result.Failed > beforeFailed {
		h.ingestErr = apperrors.NewProtocolError(fmt.Errorf(
			"nextcloud item %s could not be processed",
			item.ExternalID,
		), fmt.Sprintf("Nextcloud item %s could not be processed", item.ExternalID))
		return h.ingestErr
	}
	if item.RetryOnFailure && h.result.Failed > beforeFailed {
		h.ingestErr = fmt.Errorf("streamed item %q was not accepted; retry synchronization", item.ExternalID)
		return h.ingestErr
	}
	if nonBlocking || errors.Is(err, ErrInvalidFileType) {
		h.nonBlockingFailures++
		return nil
	}
	if err == nil {
		if h.result.Created > createdBefore || h.result.Updated > updatedBefore {
			h.consecutiveIngestFailures = 0
		}
		return nil
	}
	if h.isolateItemFailures && !isGlobalIngestAbortError(err) {
		h.consecutiveIngestFailures++
		if h.consecutiveIngestFailures < maxConsecutiveIsolatedIngestFailures {
			return fmt.Errorf("%w: %w", datasource.ErrItemIngestFailed, err)
		}
	}
	h.ingestErr = err
	return h.ingestErr
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
	if h.ingestErr != nil {
		return h.ingestErr
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
	if resyncRequired(config) {
		// One-shot legacy upgrade (design §7). The marker is cleared only
		// after the run succeeds below; a failure keeps it so the retry
		// resumes the upgraded pass from its checkpoint.
		forceFull = true
	}
	attempt, _ := asynq.GetRetryCount(ctx)
	startCursor, err := streamStartCursor(ds, forceFull, attempt)
	if err != nil {
		logger.Errorf(ctx, "failed to parse sync cursor: %v", err)
		s.updateSyncRunResult(ctx, ds, syncLog, &types.SyncResult{}, nil,
			types.SyncLogStatusFailed, fmt.Sprintf("Invalid cursor: %v", err), wasPaused)
		return err
	}

	isolateItemFailures := false
	if iso, ok := sc.(datasource.ItemFailureIsolatingConnector); ok && iso.IsolatesItemIngestFailures() {
		isolateItemFailures = true
	}
	result := &types.SyncResult{}
	handler := &streamSyncHandler{
		svc: s, ds: ds, connector: sc, tagIDs: autoTagIDs, result: result,
		syncLog: syncLog, payload: payload,
		isolateItemFailures: isolateItemFailures,
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
	abortErrMsg := ""
	if handler.ingestErr != nil {
		// Do not publish a final cursor even if the connector ignored an Emit error.
		fetchErr = handler.ingestErr
		abortErrMsg = apperrors.PublicMessage(handler.ingestErr)
	} else if fetchErr != nil {
		abortErrMsg = fmt.Sprintf("Fetch failed: %v", apperrors.PublicMessage(fetchErr))
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
			types.SyncLogStatusFailed, abortErrMsg, wasPaused)
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
	// When the stream completed without aborting and at least one in-scope item
	// was already verified up to date via the cursor fast-path, a lingering
	// per-item failure is a partial sync rather than a total outage. Keeping
	// ds.Status active preserves scheduled incremental syncs so new/edited
	// documents keep syncing and the failed item retries on future ticks.
	if handler.unchangedItems == 0 {
		if err := allFetchedItemsFailedError(result, handler.nonBlockingFailures); err != nil {
			logger.Errorf(ctx, "streaming sync failed while processing fetched items: %v", err)
			_ = s.updateSyncRunResult(
				ctx, ds, syncLog, result, resultJSON, types.SyncLogStatusFailed,
				apperrors.PublicMessage(err), wasPaused,
			)
			return err
		}
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
			errMsg += fmt.Sprintf(
				"; %d deletion failure(s) will only retry on the next full sync",
				result.DeletionFailed,
			)
		}
	}
	if err := s.updateSyncRunResult(ctx, ds, syncLog, result, resultJSON, status, errMsg, wasPaused); err != nil {
		return err
	}
	// The upgraded full pass succeeded (success or partial — per-document
	// failures are a normal retryable condition): retire the marker so it
	// never fires again. Failed runs returned earlier and keep it.
	if resyncRequired(config) {
		s.clearResyncMarker(ctx, ds)
	}
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

// cancelSyncLog persists the canceled terminal state for a sync whose data
// source or knowledge base was deleted. The caller must return its error:
// an Asynq retry is the only path that can write the state after a transient
// failure, and acknowledging with the log still "running" leaves the round
// stuck until the next sync.
func (s *DataSourceService) cancelSyncLog(
	ctx context.Context, syncLog *types.SyncLog, message string,
) error {
	syncLog.Status = types.SyncLogStatusCanceled
	syncLog.FinishedAt = timePtr(time.Now().UTC())
	syncLog.ErrorMessage = message
	if err := s.syncLogRepo.Update(ctx, syncLog); err != nil {
		return fmt.Errorf("record canceled sync log: %w", err)
	}
	return nil
}

func mergeDeletedItems(cur, prev []types.SyncDeletedItem) []types.SyncDeletedItem {
	if len(prev) == 0 {
		return cur
	}
	out := make([]types.SyncDeletedItem, 0, min(len(cur)+len(prev), maxSyncResultErrors))
	for _, d := range cur {
		if len(out) >= maxSyncResultErrors {
			return out
		}
		out = append(out, d)
	}
	for _, p := range prev {
		if len(out) >= maxSyncResultErrors {
			break
		}
		seen := false
		for _, existing := range out {
			if existing.ExternalID == p.ExternalID && existing.SourceResourceID == p.SourceResourceID {
				seen = true
				break
			}
		}
		if !seen {
			out = append(out, p)
		}
	}
	return out
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
) error {
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
		return s.syncLogRepo.UpdateResult(ctx, syncLog)
	}
	// For Nextcloud, the running log owns the source admission slot. Keep it
	// running until its cursor/state is durable; otherwise another producer
	// could start from the old cursor and race this final write.
	var logErr error
	if ds.Type != types.ConnectorTypeNextcloud {
		logErr = s.syncLogRepo.UpdateResult(ctx, syncLog)
		if logErr != nil {
			logger.Errorf(ctx, "failed to update sync log: %v", logErr)
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
	dsResultJSON := resultJSON
	if status == types.SyncLogStatusFailed && result != nil {
		if prev, err := ds.ParseSyncResult(); err == nil && prev != nil && len(prev.DeletedItems) > 0 {
			mergedItems := mergeDeletedItems(result.DeletedItems, prev.DeletedItems)
			if len(mergedItems) > len(result.DeletedItems) {
				merged := *result
				merged.DeletedItems = mergedItems
				if b, merr := merged.ToJSON(); merr == nil {
					dsResultJSON = b
				}
			}
		}
	}
	ds.LastSyncResult = dsResultJSON
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
			return stateErr
		}
	}
	if ds.Type == types.ConnectorTypeNextcloud {
		logErr = s.syncLogRepo.UpdateResult(ctx, syncLog)
	} else if stateErr == nil && s.scheduler != nil && expectedStatus == types.DataSourceStatusError &&
		ds.Status == types.DataSourceStatusActive {
		// Re-read persisted status so a concurrent PauseDataSource is never undone.
		if fresh, ferr := s.dsRepo.FindByID(ctx, ds.ID); ferr == nil && fresh != nil {
			if serr := s.scheduler.AddOrUpdate(fresh); serr != nil {
				logger.Warnf(ctx, "failed to restore cron schedule after sync recovery for ds=%s: %v", ds.ID, serr)
			}
		}
	}
	if err := errors.Join(logErr, stateErr); err != nil {
		return fmt.Errorf("persist sync result: %w", err)
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
	return nil
}

func allFetchedItemsFailedError(result *types.SyncResult, nonBlockingFailures int) error {
	if result == nil || result.Total == 0 {
		return nil
	}
	if result.Failed != result.Total || result.Created != 0 || result.Updated != 0 ||
		result.Deleted != 0 || result.Skipped != 0 {
		return nil
	}
	// An oversized or unsupported file will not recover by replaying the same
	// task. Keep the failure visible as a partial run. Oversized downloads retain
	// their connector revision until the source file is replaced. Other failures
	// still follows the existing failure/retry path, even beyond the sample cap.
	if nonBlockingFailures == result.Failed {
		return nil
	}

	detail := ""
	if len(result.Errors) > 0 {
		detail = result.Errors[0].Display()
		const maxDetailLen = 500
		if len(detail) > maxDetailLen {
			// Back off to a rune boundary: a partial multi-byte rune is invalid
			// UTF-8, which PostgreSQL rejects when the error is persisted.
			cut := maxDetailLen
			for cut > 0 && !utf8.RuneStart(detail[cut]) {
				cut--
			}
			detail = detail[:cut] + "..."
		}
	}
	if detail == "" {
		return fmt.Errorf("all fetched items failed during sync (%d/%d)", result.Failed, result.Total)
	}
	return fmt.Errorf("all fetched items failed during sync (%d/%d): %s", result.Failed, result.Total, detail)
}

// ValidateCredentials tests connectivity using raw credentials without persisting anything.
func (s *DataSourceService) ValidateCredentials(
	ctx context.Context,
	connectorType string,
	credentials map[string]interface{},
) error {
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
// Existing datasource items are replaced atomically at the knowledge-row boundary.
//
// Routing logic:
//   - Has Content bytes → CreateKnowledgeFromFile (走完整的文档解析 pipeline)
//   - Has URL only      → CreateKnowledgeFromURL  (让 WeKnora 下载并解析)
//
// Returns (isUpdate, error) — isUpdate is true when an existing item was replaced.
// Internal source observation, scoped to the stream's data source. It does not
// replace the connector's original-byte proof or current-source admission.
type (
	nextcloudObservedEpochKey struct{}
	nextcloudObservedEpoch    struct {
		dataSourceID string
		epoch        int64
	}
)

// A capability scoped to one synchronous item handoff. Only the real worker
// fetch loop and its private streaming adapter issue it; an uploaded file or
// caller-supplied channel/metadata cannot construct it.
type (
	fetchedConnectorKey  struct{}
	fetchedConnectorItem struct {
		ds               *types.DataSource
		item             *types.FetchedItem
		tenant           uint64
		source, kb, kind string
		artifact         [sha256.Size]byte
	}
)

// Hash the body separately so binding a large fetched file never allocates its base64 representation.
func fetchedConnectorDigest(item *types.FetchedItem) ([sha256.Size]byte, error) {
	descriptor := *item
	descriptor.Content = nil
	h := sha256.New()
	body := sha256.Sum256(item.Content)
	h.Write(body[:])
	if err := json.NewEncoder(h).Encode(descriptor); err != nil {
		return [sha256.Size]byte{}, err
	}
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

func (p fetchedConnectorItem) matches(ds *types.DataSource, item *types.FetchedItem) bool {
	if p.ds != ds ||
		p.item != item ||
		p.kind != ds.Type ||
		p.tenant != ds.TenantID ||
		p.kb != ds.KnowledgeBaseID ||
		p.source != ds.ID {
		return false
	}
	got, err := fetchedConnectorDigest(item)
	return err == nil && got == p.artifact
}

func bindFetchedConnectorItem(
	ctx context.Context, connector datasource.Connector,
	ds *types.DataSource, item *types.FetchedItem,
) context.Context {
	if connector == nil ||
		ds == nil ||
		item == nil ||
		ds.Type == "" ||
		connector.Type() != ds.Type ||
		ds.Type == types.ConnectorTypeNextcloud ||
		ds.Type == types.ConnectorTypeDingTalk {
		return ctx
	}
	digest, err := fetchedConnectorDigest(item)
	if err != nil {
		return ctx
	}
	return context.WithValue(ctx, fetchedConnectorKey{}, fetchedConnectorItem{
		ds, item, ds.TenantID,
		ds.ID, ds.KnowledgeBaseID, ds.Type, digest,
	})
}

func (s *DataSourceService) ingestItem(
	ctx context.Context,
	ds *types.DataSource,
	item *types.FetchedItem,
	tagIDs []string,
) (bool, error) {
	if len(item.Content) == 0 && item.URL != "" && isFileURL(item.URL, item.FileName, "") {
		fileName := item.FileName
		if fileName == "" {
			fileName = extractFileNameFromURL(item.URL)
		}
		if IsVideoType(getFileType(fileName)) || !isValidFileType(fileName) {
			return false, ErrInvalidFileType
		}
	}
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
		if observed, ok := ctx.Value(nextcloudObservedEpochKey{}).(nextcloudObservedEpoch); ok {
			if observed.dataSourceID != ds.ID || observed.epoch < 0 {
				return false, errors.New("nextcloud observed publication scope changed")
			}
			metadata["nextcloud_publication_epoch"] = strconv.FormatInt(observed.epoch, 10)
		}
		return s.ingestNextcloudItem(ctx, ds, item, metadata, tagIDs)
	}

	// Protected connectors have their own source admission and generation handoff.
	if ds.Type == types.ConnectorTypeDingTalk {
		return false, types.ErrSourceLineageUnknown
	}
	metadata["datasource_id"] = ds.ID
	metadata["external_id"] = item.ExternalID
	metadata["source_resource_id"] = item.SourceResourceID
	if channel == types.ConnectorTypeDingTalk || channel == types.ConnectorTypeNextcloud {
		return false, types.ErrSourceLineageUnknown
	}
	if proof, ok := ctx.Value(fetchedConnectorKey{}).(fetchedConnectorItem); ok && proof.matches(ds, item) {
		// The registered connector returned this exact item at the authorized
		// worker boundary. No protected source contributed to its external bytes.
		lineage, err := types.NewCompleteSourceLineage()
		if err != nil {
			return false, err
		}
		ctx, err = apprepo.WithDerivedKnowledgeInput(ctx, lineage)
		if err != nil {
			return false, err
		}
	} else if _, production := s.knowledgeService.(*knowledgeService); production || s.sourceInputs != nil {
		return false, types.ErrSourceLineageUnknown
	}
	// Reject unsupported source files before replacing an existing document.
	// Do not classify generic BadRequest errors: missing storage configuration
	// also uses that type and must remain retryable.
	if len(item.Content) > 0 && (IsVideoType(getFileType(item.FileName)) || !isValidFileType(item.FileName)) {
		return false, ErrInvalidFileType
	}
	// Existing items are replaced through a short DB transaction. Validation and
	// file storage must succeed before the previous row or resources are retired.
	isUpdate := false
	if item.ExternalID != "" {
		repo := s.knowledgeService.GetRepository()
		existing, err := repo.FindByDataSourceExternalID(ctx, ds.TenantID, ds.KnowledgeBaseID, ds.ID, item.ExternalID)
		if err != nil {
			return false, fmt.Errorf("check existing datasource knowledge: %w", err)
		}
		if existing != nil {
			// Conflict strategy "skip" (跳过已存在): the item already exists in
			// the KB, so leave it untouched and report the conflict as a skip
			// via the shared DuplicateKnowledgeError path (Tencent/WeKnora#4640 —
			// this strategy was silently ignored and the item was overwritten).
			if ds.ConflictStrategy == types.ConflictStrategySkip {
				logger.Infof(
					ctx,
					"item %q (external_id=%s) already exists as knowledge %s, conflict strategy skip → skipping",
					item.Title, item.ExternalID, existing.ID,
				)
				return false, &types.DuplicateKnowledgeError{
					Message: fmt.Sprintf(
						"knowledge already exists for external_id %s (conflict strategy: skip)",
						item.ExternalID,
					),
					Knowledge: existing,
				}
			}
			_, err := s.knowledgeService.ReplaceKnowledgeFromDataSource(
				ctx, existing.ID, item, metadata, tagIDs, channel)
			var dupErr *types.DuplicateKnowledgeError
			if err == nil || (errors.As(err, &dupErr) && dupIsSameNode(dupErr, item)) {
				s.sweepStaleSubtree(ctx, ds, item)
			}
			return err == nil, err
		}
	}

	// Case 1: content already fetched → build a FileHeader from bytes and call CreateKnowledgeFromFile
	if len(item.Content) > 0 {
		// Images ride inline as base64 data URIs; ProcessDocument's image
		// resolver stores them on the file service and swaps in persistent
		// provider:// URLs before chunking.
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
			// Only the metadata column: the processing task is already
			// queued, and a full-row save of this snapshot could put
			// parse_status back to "pending" under a worker that has moved
			// it on, which then skips post-process and strands the row.
			if uErr := s.knowledgeService.GetRepository().UpdateKnowledgeColumn(
				ctx, created.ID, "metadata", created.Metadata,
			); uErr != nil {
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
	var originalErr error
	ctx, originalErr = apprepo.WithNextcloudOriginalFetchedArtifact(ctx, ds.ID, item, tagIDs)
	if originalErr != nil {
		return false, originalErr
	}
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
	if s.capacityAdmission == nil {
		return false, apprepo.ErrNextcloudCapacityUnavailable
	}
	capacityKB, capacityErr := s.kbService.GetKnowledgeBaseByID(ctx, ds.KnowledgeBaseID)
	if capacityErr != nil || capacityKB == nil || capacityKB.TenantID != ds.TenantID {
		return false, apprepo.ErrNextcloudCapacityUnavailable
	}
	reservation, capacityErr := s.capacityAdmission.Begin(
		ctx,
		capacityKB,
		ds.ID,
		item.ExternalID,
		etag,
		int64(len(item.Content)),
	)
	if capacityErr != nil {
		return false, capacityErr
	}
	ctx = s.capacityAdmission.WriteContext(ctx, reservation)
	defer s.capacityAdmission.End(ctx, reservation, capacityKB)
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
	if err := s.capacityAdmission.Finish(ctx, reservation, candidate.ID, true); err != nil {
		return isUpdate, err
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
	children, err := repo.FindByMetadataKeyPrefix(
		ctx,
		ds.TenantID,
		ds.KnowledgeBaseID,
		"external_id",
		types.SubtreeChildPrefix(item.ExternalID),
	)
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

	part, err := writer.CreateFormFile("file", filename)
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

// PublishTargetMigrationCandidates publishes candidates under the current target migration controls.
func (s *DataSourceService) PublishTargetMigrationCandidates(
	ctx context.Context,
	row apprepo.NextcloudTargetMigration,
) error {
	if row.State != "active" || s.knowledgeService == nil {
		return apprepo.ErrNextcloudOnboardingConflict
	}
	repository := s.knowledgeService.GetRepository()
	candidates, err := repository.ListKnowledgeByKnowledgeBaseID(ctx, row.NewTenantID, row.NewKnowledgeBaseID)
	if err != nil {
		return err
	}
	publisher, ok := repository.(interface {
		PublishNextcloudVersion(context.Context, string) (bool, error)
	})
	if !ok {
		return apprepo.ErrNextcloudOnboardingConflict
	}
	for _, k := range candidates {
		if k.Channel == types.ConnectorTypeNextcloud {
			if _, err := publisher.PublishNextcloudVersion(ctx, k.ID); err != nil {
				return err
			}
		}
	}
	return nil
}
