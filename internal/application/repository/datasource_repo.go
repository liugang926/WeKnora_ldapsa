package repository

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// DataSourceRepository provides data access for data sources
type DataSourceRepository struct {
	db *gorm.DB
}

type nextcloudCredentialWriteKey struct{}

// WithNextcloudCredentialWrite marks the dedicated credential subresource's
// repository update. Ordinary data-source edits and sync state writes retain
// the database's currently committed Nextcloud machine credentials.
func WithNextcloudCredentialWrite(ctx context.Context) context.Context {
	return context.WithValue(ctx, nextcloudCredentialWriteKey{}, true)
}

// NewDataSourceRepository creates a new data source repository
func NewDataSourceRepository(db *gorm.DB) interfaces.DataSourceRepository {
	return &DataSourceRepository{db: db}
}

// Create inserts a new data source record
func (r *DataSourceRepository) Create(ctx context.Context, ds *types.DataSource) error {
	if ds == nil {
		return errors.New("data source is nil")
	}
	// GORM treats false as the zero value of bool. For a field tagged
	// default:true it replaces both the INSERT value and the in-memory field
	// with true, so a caller-selected false would be lost. Capture it, force
	// the column write, then restore the struct so Create's return value (and
	// the HTTP 201 body) match the database.
	syncDeletions := ds.SyncDeletions
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := admitDataSourceKnowledgeBase(tx, ds, false); err != nil {
			return err
		}
		if err := tx.Create(ds).Error; err != nil {
			return err
		}
		return tx.Model(&types.DataSource{}).
			Where("id = ?", ds.ID).
			UpdateColumn("sync_deletions", syncDeletions).Error
	})
	ds.SyncDeletions = syncDeletions
	return err
}

// Every data-source create and update writes the owning KB row before checking
// its sources. PostgreSQL serializes concurrent writers on that row; SQLite
// serializes the write transaction. This prevents a concurrent ordinary source
// from racing a Nextcloud source into the same dedicated KB.
func admitDataSourceKnowledgeBase(tx *gorm.DB, ds *types.DataSource, update bool) error {
	if ds.TenantID == 0 || ds.KnowledgeBaseID == "" {
		return errors.New("data source has no knowledge-base owner")
	}
	result := tx.Exec(`UPDATE knowledge_bases
		SET ever_had_nextcloud_source = ever_had_nextcloud_source
		WHERE id = ? AND tenant_id = ?`, ds.KnowledgeBaseID, ds.TenantID)
	if result.Error != nil {
		return fmt.Errorf("lock data-source knowledge base: %w", result.Error)
	}
	if result.RowsAffected != 1 {
		return errors.New("data-source knowledge-base owner not found")
	}
	var marked bool
	if err := tx.Raw(`SELECT ever_had_nextcloud_source FROM knowledge_bases
		WHERE id = ? AND tenant_id = ?`, ds.KnowledgeBaseID, ds.TenantID).
		Scan(&marked).Error; err != nil {
		return fmt.Errorf("read knowledge-base provenance: %w", err)
	}
	if ds.Type != types.ConnectorTypeNextcloud {
		if marked {
			return errors.New("knowledge base has Nextcloud source provenance")
		}
		return nil
	}
	var otherSources int64
	if err := tx.Unscoped().Model(&types.DataSource{}).
		Where("knowledge_base_id = ? AND tenant_id = ?",
			ds.KnowledgeBaseID, ds.TenantID).
		Where("id <> ?", ds.ID).Count(&otherSources).Error; err != nil {
		return fmt.Errorf("check dedicated Nextcloud knowledge base: %w", err)
	}
	if otherSources != 0 {
		return apperrors.NewProtocolError(errors.New(("nextcloud requires a dedicated knowledge base without o" +
			"ther data sources")), "Nextcloud requires a dedicated knowledge base without other data sources")
	}
	if !update {
		var existingDocuments int64
		if err := tx.Table("knowledges").
			Where("knowledge_base_id = ? AND tenant_id = ?",
				ds.KnowledgeBaseID, ds.TenantID).
			Count(&existingDocuments).Error; err != nil {
			return fmt.Errorf("check existing knowledge before Nextcloud pairing: %w", err)
		}
		if existingDocuments != 0 {
			return apperrors.NewProtocolError(errors.New(("nextcloud requires a knowledge base without existing do" +
				"cuments")), "Nextcloud requires a knowledge base without existing documents")
		}
	}
	if err := tx.Exec(`UPDATE knowledge_bases SET ever_had_nextcloud_source = TRUE
		WHERE id = ? AND tenant_id = ?`, ds.KnowledgeBaseID, ds.TenantID).Error; err != nil {
		return fmt.Errorf("mark Nextcloud knowledge-base provenance: %w", err)
	}
	return nil
}

// FindByID retrieves a data source by ID
func (r *DataSourceRepository) FindByID(ctx context.Context, id string) (*types.DataSource, error) {
	if id == "" {
		return nil, errors.New("id is empty")
	}
	var ds types.DataSource
	if err := r.db.WithContext(ctx).
		Where("id = ?", id).
		Where("deleted_at IS NULL").
		First(&ds).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("data source not found")
		}
		return nil, err
	}
	return &ds, nil
}

// FindByKnowledgeBase lists all data sources for a knowledge base
func (r *DataSourceRepository) FindByKnowledgeBase(ctx context.Context, kbID string) ([]*types.DataSource, error) {
	if kbID == "" {
		return nil, errors.New("knowledge base id is empty")
	}
	var dataSources []*types.DataSource
	if err := r.db.WithContext(ctx).
		Where("knowledge_base_id = ?", kbID).
		Where("deleted_at IS NULL").
		Order("created_at DESC").
		Find(&dataSources).Error; err != nil {
		return nil, err
	}
	return dataSources, nil
}

// FindByKnowledgeBaseIncludingDeleted is used only for historical answer
// authorization. A removed source can still have supplied text to a saved
// answer, so the normal active-source listing cannot classify old messages.
func (
	r *DataSourceRepository,
) FindByKnowledgeBaseIncludingDeleted(ctx context.Context, kbID string) ([]*types.DataSource, error) {
	if kbID == "" {
		return nil, errors.New("knowledge base id is empty")
	}
	var dataSources []*types.DataSource
	if err := r.db.WithContext(ctx).Unscoped().
		Where("knowledge_base_id = ?", kbID).
		Find(&dataSources).Error; err != nil {
		return nil, err
	}
	return dataSources, nil
}

// HasEverNextcloudSourceForTenant classifies legacy Agent messages that did
// not persist a per-turn KB scope. Include soft-deleted sources: withdrawing a
// connector must never make its old answers appear source-independent.
func (r *DataSourceRepository) HasEverNextcloudSourceForTenant(ctx context.Context, tenantID uint64) (bool, error) {
	if tenantID == 0 {
		return false, errors.New("tenant id is empty")
	}
	return r.hasEverNextcloudSourceInScope(ctx, "tenant_id = ?", tenantID)
}

// HasEverNextcloudSourceForKnowledgeBase reads the durable source-history flag.
func (r *DataSourceRepository) HasEverNextcloudSourceForKnowledgeBase(ctx context.Context, kbID string) (bool, error) {
	if kbID == "" {
		return false, errors.New("knowledge base id is empty")
	}
	// Data-source and pairing scopes use knowledge_base_id; KB scope uses id.
	return r.hasEverNextcloudSourceInScope(ctx, "knowledge_base_id = ?", kbID)
}

func (r *DataSourceRepository) hasEverNextcloudSourceInScope(ctx context.Context, scope string, id interface{}) (
	bool,
	error,
) {
	if r == nil || r.db == nil {
		return false, errors.New("source history database is unavailable")
	}
	var seen bool
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var sources, markers, pairings, aborts, tombstones int64
		if err := tx.Unscoped().Model(
			&types.DataSource{},
		).Where(scope, id).Where("type = ?", types.ConnectorTypeNextcloud).Count(&sources).Error; err !=
			nil {
			return err
		}
		kbScope := scope
		if scope == "knowledge_base_id = ?" {
			kbScope = "id = ?"
		}
		if err := tx.Table(
			"knowledge_bases",
		).Where(kbScope, id).Where(("ever_had_nextcloud_source = ? OR ever_had_nextcloud_sou" +
			"rce IS NULL OR ever_had_nextcloud_source NOT IN (?, ?)"), true, false, true).Count(&markers).Error; err !=
			nil {
			return err
		}
		if err := tx.Table("nextcloud_source_pairings").Where(scope, id).Count(&pairings).Error; err != nil {
			return err
		}
		if err := tx.Table("nextcloud_source_pairing_aborts").Where(scope, id).Count(&aborts).Error; err != nil {
			return err
		}
		tombstoneQuery := tx.Table("nextcloud_source_tombstones")
		if scope == "knowledge_base_id = ?" {
			tombstoneQuery = tombstoneQuery.Where("scope_type = ? AND scope_id = ?", "knowledge_base", id)
		} else {
			tombstoneQuery = tombstoneQuery.Where(scope, id)
		}
		if err := tombstoneQuery.Count(&tombstones).Error; err != nil {
			return err
		}
		seen = sources > 0 || markers > 0 || pairings > 0 || aborts > 0 || tombstones > 0
		return nil
	})
	return seen, err
}

// HasEverNextcloudSourceGlobally is the interim admission barrier for Agent
// histories and compaction summaries, whose transitive provenance is not yet
// persisted. Scope is the whole instance: shared KBs, tools and previous Agent
// turns can cross tenant boundaries. Every evidence table is required, and
// independent ever-source tombstones survive business-row removal. Absence
// of retained evidence does not reconstruct history erased before backfill.
func (r *DataSourceRepository) HasEverNextcloudSourceGlobally(ctx context.Context) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("source history database is unavailable")
	}
	var seen bool
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var malformed int64
		if err := tx.Table("knowledge_bases").Where(("ever_had_nextcloud_source IS NULL OR ever_had_nextcloud" +
			"_source NOT IN (?, ?)"), false, true).Count(&malformed).Error; err != nil {
			return err
		}
		if malformed != 0 {
			return errors.New("knowledge base provenance is malformed")
		}
		var sources, markers, pairings, aborts, tombstones int64
		if err := tx.Unscoped().Model(
			&types.DataSource{},
		).Where("type = ?", types.ConnectorTypeNextcloud).Count(&sources).Error; err !=
			nil {
			return err
		}
		if err := tx.Table("knowledge_bases").Where("ever_had_nextcloud_source = ?", true).Count(&markers).Error; err !=
			nil {
			return err
		}
		if err := tx.Table("nextcloud_source_pairings").Count(&pairings).Error; err != nil {
			return err
		}
		if err := tx.Table("nextcloud_source_pairing_aborts").Count(&aborts).Error; err != nil {
			return err
		}
		if err := tx.Table("nextcloud_source_tombstones").Count(&tombstones).Error; err != nil {
			return err
		}
		seen = sources > 0 || markers > 0 || pairings > 0 || aborts > 0 || tombstones > 0
		return nil
	})
	return seen, err
}

// Update updates an existing data source
func (r *DataSourceRepository) Update(ctx context.Context, ds *types.DataSource) error {
	if ds == nil {
		return errors.New("data source is nil")
	}
	if ds.ID == "" {
		return errors.New("data source id is empty")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Lock the owning KB before reading the source. Credential replacement
		// and endpoint edits must observe the same committed predecessor, even
		// when two Web workers update a source concurrently.
		if err := admitDataSourceKnowledgeBase(tx, ds, true); err != nil {
			return err
		}
		var previous types.DataSource
		if err := tx.Where("id = ? AND deleted_at IS NULL", ds.ID).First(&previous).Error; err != nil {
			return fmt.Errorf("load data source before update: %w", err)
		}
		if previous.TenantID != ds.TenantID || previous.KnowledgeBaseID != ds.KnowledgeBaseID {
			return errors.New("data source owner cannot change")
		}
		if previous.Type != ds.Type &&
			(previous.Type == types.ConnectorTypeNextcloud || ds.Type == types.ConnectorTypeNextcloud) {
			return apperrors.NewProtocolError(errors.New(
				"nextcloud data source type cannot change",
			), "Nextcloud data source type cannot change")
		}
		if previous.Type == types.ConnectorTypeNextcloud {
			if err := pairedSourceConfigImmutable(tx, &previous, ds); err != nil {
				return err
			}
			if err := requireStableNextcloudCredentialEndpoint(&previous, ds); err != nil {
				return err
			}
			if err := preserveCommittedNextcloudCredentials(ctx, &previous, ds); err != nil {
				return err
			}
		}
		if err := tx.Model(ds).Updates(ds).Error; err != nil {
			return err
		}
		// GORM Updates(struct) deliberately skips zero values, which would make
		// a user-selected sync_deletions=false impossible to persist.
		return tx.Model(&types.DataSource{}).
			Where("id = ?", ds.ID).
			UpdateColumn("sync_deletions", ds.SyncDeletions).Error
	})
}

func preserveCommittedNextcloudCredentials(ctx context.Context, previous, next *types.DataSource) error {
	if len(next.Config) == 0 {
		return nil // The Config column is not modified by this update.
	}
	oldConfig, oldErr := previous.ParseConfig()
	newConfig, newErr := next.ParseConfig()
	if oldErr != nil || newErr != nil {
		return errors.New("invalid Nextcloud data source configuration")
	}
	if oldConfig == nil {
		oldConfig = &types.DataSourceConfig{}
	}
	if newConfig == nil {
		newConfig = &types.DataSourceConfig{}
	}
	if !reflect.DeepEqual(oldConfig.Credentials, newConfig.Credentials) &&
		ctx.Value(nextcloudCredentialWriteKey{}) != true {
		// A normal PUT or old sync worker may carry a credential snapshot from
		// before a rotation or revocation. Merge the current value while the
		// KB row is locked, then write the requested non-secret changes.
		newConfig.Credentials = oldConfig.Credentials
		encoded, err := newConfig.ToJSON()
		if err != nil {
			return errors.New("cannot preserve current Nextcloud credentials")
		}
		next.Config = encoded
	}
	return nil
}

// This check must run under the KB write lock used by every data-source
// update. The service checks before network validation to prevent an immediate
// leak; this second check prevents a concurrent endpoint/credential update
// from committing a token paired with a different endpoint.
func requireStableNextcloudCredentialEndpoint(previous, next *types.DataSource) error {
	if len(next.Config) == 0 {
		return nil // GORM leaves a zero-value Config column unchanged.
	}
	oldConfig, oldErr := previous.ParseConfig()
	newConfig, newErr := next.ParseConfig()
	if oldErr != nil || newErr != nil {
		return errors.New("invalid Nextcloud data source configuration")
	}
	if oldConfig == nil {
		oldConfig = &types.DataSourceConfig{}
	}
	if newConfig == nil {
		newConfig = &types.DataSourceConfig{}
	}
	if oldConfig.HasConfiguredCredentials(types.ConnectorTypeNextcloud) ||
		newConfig.HasConfiguredCredentials(types.ConnectorTypeNextcloud) {
		oldBase, _ := oldConfig.Settings["base_url"].(string)
		newBase, _ := newConfig.Settings["base_url"].(string)
		if oldBase != newBase {
			return apperrors.NewProtocolError(errors.New(("nextcloud base URL cannot change while credentials are " +
				"configured")), "Nextcloud base URL cannot change while credentials are configured")
		}
	}
	return nil
}

// UpdateSyncState updates only fields managed by sync execution. GORM's
// Updates(struct) skips zero values, so use a map here to persist cleared error
// messages without broadening the generic Update method.
func (r *DataSourceRepository) UpdateSyncState(ctx context.Context, ds *types.DataSource) error {
	if ds == nil {
		return errors.New("data source is nil")
	}
	if ds.ID == "" {
		return errors.New("data source id is empty")
	}
	if err := r.db.WithContext(ctx).
		Model(&types.DataSource{}).
		Where("id = ?", ds.ID).
		Updates(map[string]interface{}{
			"status":           ds.Status,
			"last_sync_at":     ds.LastSyncAt,
			"last_sync_cursor": ds.LastSyncCursor,
			"last_sync_result": ds.LastSyncResult,
			"error_message":    ds.ErrorMessage,
			"updated_at":       time.Now().UTC(),
		}).Error; err != nil {
		return err
	}
	return nil
}

// UpdateNextcloudSyncStateCAS prevents a worker's stale source snapshot from
// undoing a pause, resume, or deletion committed while its sync was running.
func (r *DataSourceRepository) UpdateNextcloudSyncStateCAS(
	ctx context.Context, ds *types.DataSource, expectedStatus string,
) (bool, error) {
	if ds == nil || ds.ID == "" || ds.TenantID == 0 || ds.Type != types.ConnectorTypeNextcloud ||
		(expectedStatus != types.DataSourceStatusActive && expectedStatus != types.DataSourceStatusPaused &&
			expectedStatus != types.DataSourceStatusError) {
		return false, errors.New("invalid Nextcloud sync state comparison")
	}
	result := r.db.WithContext(ctx).Model(&types.DataSource{}).
		Where("id = ? AND tenant_id = ? AND type = ? AND status = ? AND deleted_at IS NULL",
			ds.ID, ds.TenantID, types.ConnectorTypeNextcloud, expectedStatus).
		Updates(map[string]interface{}{
			"status":           ds.Status,
			"last_sync_at":     ds.LastSyncAt,
			"last_sync_cursor": ds.LastSyncCursor,
			"last_sync_result": ds.LastSyncResult,
			"error_message":    ds.ErrorMessage,
			"updated_at":       time.Now().UTC(),
		})
	return result.RowsAffected == 1, result.Error
}

// UpdateNextcloudRetryableEventFailure never writes status or cursor. Its
// conditional update cannot reactivate a source paused after the event worker
// read its original data-source snapshot.
func (r *DataSourceRepository) UpdateNextcloudRetryableEventFailure(
	ctx context.Context, ds *types.DataSource,
) (bool, error) {
	if ds == nil || ds.ID == "" || ds.TenantID == 0 || ds.Type != types.ConnectorTypeNextcloud {
		return false, errors.New("invalid Nextcloud event failure source")
	}
	result := r.db.WithContext(ctx).Model(&types.DataSource{}).
		Where("id = ? AND tenant_id = ? AND type = ? AND status = ? AND deleted_at IS NULL",
			ds.ID, ds.TenantID, types.ConnectorTypeNextcloud, types.DataSourceStatusActive).
		Updates(map[string]interface{}{
			"last_sync_result": ds.LastSyncResult,
			"error_message":    ds.ErrorMessage,
			"updated_at":       time.Now().UTC(),
		})
	return result.RowsAffected == 1, result.Error
}

// Delete performs a soft delete
func (r *DataSourceRepository) Delete(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("id is empty")
	}
	var source types.DataSource
	if err := r.db.WithContext(ctx).Where("id = ? AND deleted_at IS NULL", id).Take(&source).Error; err != nil {
		return err
	}
	if source.Type == types.ConnectorTypeNextcloud {
		var paired int64
		if err := r.db.WithContext(
			ctx,
		).Table("nextcloud_source_pairings").Where("datasource_id = ?", id).Count(&paired).Error; err !=
			nil {
			return err
		}
		if paired != 0 {
			return ErrNextcloudSourcePairingConflict
		}
	}
	if err := r.db.WithContext(ctx).
		Where("id = ?", id).
		Delete(&types.DataSource{}).Error; err != nil {
		return err
	}
	return nil
}

// FindActive retrieves all active data sources (used for scheduling)
func (r *DataSourceRepository) FindActive(ctx context.Context) ([]*types.DataSource, error) {
	var dataSources []*types.DataSource
	if err := r.db.WithContext(ctx).
		Where("status = ?", types.DataSourceStatusActive).
		Where("deleted_at IS NULL").
		Where("sync_schedule != ''").
		Order("created_at DESC").
		Find(&dataSources).Error; err != nil {
		return nil, err
	}
	return dataSources, nil
}

// SyncLogRepository provides data access for sync logs
type SyncLogRepository struct {
	db *gorm.DB
}

// NewSyncLogRepository creates a new sync log repository
func NewSyncLogRepository(db *gorm.DB) interfaces.SyncLogRepository {
	return &SyncLogRepository{db: db}
}

// Create inserts a new sync log entry
func (r *SyncLogRepository) Create(ctx context.Context, log *types.SyncLog) error {
	if log == nil {
		return errors.New("sync log is nil")
	}
	if err := r.db.WithContext(ctx).Create(log).Error; err != nil {
		if strings.Contains(err.Error(), "nextcloud_source_sync_busy") {
			return datasource.ErrSyncAlreadyRunning
		}
		return err
	}
	return nil
}

// FindByID retrieves a sync log by ID
func (r *SyncLogRepository) FindByID(ctx context.Context, id string) (*types.SyncLog, error) {
	if id == "" {
		return nil, errors.New("id is empty")
	}
	var log types.SyncLog
	if err := r.db.WithContext(ctx).
		Where("id = ?", id).
		First(&log).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("sync log not found")
		}
		return nil, err
	}
	return &log, nil
}

// FindByDataSource lists sync logs for a data source with pagination
func (r *SyncLogRepository) FindByDataSource(ctx context.Context, dsID string, limit int, offset int) ([]*types.SyncLog, error) {
	if dsID == "" {
		return nil, errors.New("data source id is empty")
	}
	if limit <= 0 {
		limit = 10
	}
	if offset < 0 {
		offset = 0
	}
	var logs []*types.SyncLog
	if err := r.db.WithContext(ctx).
		Where("data_source_id = ?", dsID).
		Order("started_at DESC").
		Limit(limit).
		Offset(offset).
		Find(&logs).Error; err != nil {
		return nil, err
	}
	return logs, nil
}

// FindLatest retrieves the most recent sync log for a data source
func (r *SyncLogRepository) FindLatest(ctx context.Context, dsID string) (*types.SyncLog, error) {
	if dsID == "" {
		return nil, errors.New("data source id is empty")
	}
	var log types.SyncLog
	if err := r.db.WithContext(ctx).
		Where("data_source_id = ?", dsID).
		Order("started_at DESC").
		Limit(1).
		First(&log).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &log, nil
}

// HasRunningSync checks if a data source has any sync currently in "running" status.
func (r *SyncLogRepository) HasRunningSync(ctx context.Context, dsID string) (bool, error) {
	if dsID == "" {
		return false, errors.New("data source id is empty")
	}
	var count int64
	if err := r.db.WithContext(ctx).
		Model(&types.SyncLog{}).
		Where("data_source_id = ?", dsID).
		Where("status = ?", types.SyncLogStatusRunning).
		Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

// ClaimNextcloudSyncStart fences every versioned manual/cron attempt to one
// exact task. A retry may acquire only after its predecessor returned and
// relinquished the token; a crashed or still-active worker remains fail-closed.
func (r *SyncLogRepository) ClaimNextcloudSyncStart(ctx context.Context, logID, dsID string,
	tenantID uint64, trigger, taskID string, retryCount int,
) (string, error) {
	if logID == "" || dsID == "" || tenantID == 0 || retryCount < 0 ||
		(trigger != "manual" && trigger != "schedule") ||
		taskID != datasource.NextcloudSyncTaskID(logID) {
		return "", nil
	}
	token := uuid.NewString()
	query := r.db.WithContext(ctx).Model(&types.SyncLog{}).
		Where("id = ? AND data_source_id = ? AND tenant_id = ?", logID, dsID, tenantID).
		Where("status = ? AND recovery_version = 1 AND recovery_trigger = ?", types.SyncLogStatusRunning, trigger).
		Where("queue_task_id = ? AND worker_active = ?", taskID, false)
	updates := map[string]any{"worker_active": true, "worker_attempt_token": token, "error_message": ""}
	if retryCount == 0 {
		query = query.Where("worker_started_at IS NULL")
		updates["worker_started_at"] = time.Now().UTC()
	} else {
		query = query.Where("worker_started_at IS NOT NULL")
	}
	result := query.Updates(updates)
	if result.Error != nil || result.RowsAffected != 1 {
		return "", result.Error
	}
	return token, nil
}

// FinishNextcloudSyncAttempt never touches another worker's token or a row
// already finished by the sync path. Releasing after a transient error allows
// only an actual Asynq retry with the same task ID to claim again.
func (r *SyncLogRepository) FinishNextcloudSyncAttempt(ctx context.Context, logID, dsID string,
	tenantID uint64, trigger, taskID, token string, terminal bool, message string,
) (bool, error) {
	if logID == "" || dsID == "" || tenantID == 0 || token == "" ||
		(trigger != "manual" && trigger != "schedule") ||
		taskID != datasource.NextcloudSyncTaskID(logID) {
		return false, nil
	}
	updates := map[string]any{"worker_active": false, "worker_attempt_token": ""}
	if terminal {
		updates["status"] = types.SyncLogStatusFailed
		updates["finished_at"] = time.Now().UTC()
	}
	if message != "" {
		updates["error_message"] = message
	}
	result := r.db.WithContext(ctx).Model(&types.SyncLog{}).
		Where("id = ? AND data_source_id = ? AND tenant_id = ?", logID, dsID, tenantID).
		Where("status = ? AND recovery_version = 1 AND recovery_trigger = ?", types.SyncLogStatusRunning, trigger).
		Where("queue_task_id = ? AND worker_active = ? AND worker_attempt_token = ?", taskID, true, token).
		Updates(updates)
	return result.RowsAffected == 1, result.Error
}

// MarkNextcloudEnqueueUncertain never writes status from the stale producer
// snapshot. A worker may finish between Enqueue and a lost queue reply.
func (r *SyncLogRepository) MarkNextcloudEnqueueUncertain(ctx context.Context, logID, dsID string,
	tenantID uint64, trigger, taskID string,
) (bool, error) {
	if logID == "" || dsID == "" || tenantID == 0 ||
		(trigger != "manual" && trigger != "schedule") ||
		taskID != datasource.NextcloudSyncTaskID(logID) {
		return false, nil
	}
	result := r.db.WithContext(ctx).Model(&types.SyncLog{}).
		Where("id = ? AND data_source_id = ? AND tenant_id = ?", logID, dsID, tenantID).
		Where("status = ? AND recovery_version = 1 AND recovery_trigger = ?", types.SyncLogStatusRunning, trigger).
		Where("queue_task_id = ? AND worker_started_at IS NULL AND worker_active = ?", taskID, false).
		Update("error_message", datasource.NextcloudSyncEnqueueUncertain)
	return result.RowsAffected == 1, result.Error
}

// Update updates an existing sync log entry
func (r *SyncLogRepository) Update(ctx context.Context, log *types.SyncLog) error {
	if log == nil {
		return errors.New("sync log is nil")
	}
	if log.ID == "" {
		return errors.New("sync log id is empty")
	}
	if err := r.db.WithContext(ctx).
		Model(log).
		Updates(log).Error; err != nil {
		return err
	}
	return nil
}

// UpdateResult updates only fields produced by sync execution. Use an explicit
// map so empty error messages are written when a later sync succeeds.
func (r *SyncLogRepository) UpdateResult(ctx context.Context, log *types.SyncLog) error {
	if log == nil {
		return errors.New("sync log is nil")
	}
	if log.ID == "" {
		return errors.New("sync log id is empty")
	}
	if err := r.db.WithContext(ctx).
		Model(&types.SyncLog{}).
		Where("id = ?", log.ID).
		Updates(map[string]interface{}{
			"status":        log.Status,
			"finished_at":   log.FinishedAt,
			"items_total":   log.ItemsTotal,
			"items_created": log.ItemsCreated,
			"items_updated": log.ItemsUpdated,
			"items_deleted": log.ItemsDeleted,
			"items_skipped": log.ItemsSkipped,
			"items_failed":  log.ItemsFailed,
			"error_message": log.ErrorMessage,
			"result":        log.Result,
			"updated_at":    time.Now().UTC(),
		}).Error; err != nil {
		return err
	}
	return nil
}

// CancelPendingByDataSource marks all non-terminal sync logs for a data source as canceled.
func (r *SyncLogRepository) CancelPendingByDataSource(ctx context.Context, dsID string) error {
	if dsID == "" {
		return errors.New("data source id is empty")
	}
	now := time.Now().UTC()
	return r.db.WithContext(ctx).
		Model(&types.SyncLog{}).
		Where("data_source_id = ?", dsID).
		Where("status IN ?", []string{types.SyncLogStatusRunning, "pending"}).
		Updates(map[string]interface{}{
			"status":        types.SyncLogStatusCanceled,
			"finished_at":   &now,
			"error_message": "data source deleted",
		}).Error
}

// CleanupOldLogs deletes sync logs older than the retention period
func (r *SyncLogRepository) CleanupOldLogs(ctx context.Context, retentionDays int) error {
	if retentionDays <= 0 {
		retentionDays = 30
	}
	// Delete logs older than the retention period
	if err := r.db.WithContext(ctx).
		Where("started_at < NOW() - INTERVAL ? DAY", retentionDays).
		Delete(&types.SyncLog{}).Error; err != nil {
		return err
	}
	return nil
}
