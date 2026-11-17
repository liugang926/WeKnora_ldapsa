package repository

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// InstallConnectorScheduleSchema initializes separate extension tables without reserving a
// version in either upstream migration sequence.
func InstallConnectorScheduleSchema(db *gorm.DB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		// PostgreSQL's IF NOT EXISTS does not serialize simultaneous catalog inserts on first boot.
		if tx.Name() == "postgres" {
			const lock = "SELECT pg_advisory_xact_lock(hashtext('ext_connector_schedule_schema'))"
			if err := tx.Exec(lock).Error; err != nil {
				return err
			}
		}
		if err := tx.Exec(`CREATE TABLE IF NOT EXISTS ext_connector_schedule_grants (
			source_id VARCHAR(36) PRIMARY KEY REFERENCES data_sources(id) ON DELETE CASCADE,
			grant_id VARCHAR(36) NOT NULL, binding TEXT NOT NULL, user_id VARCHAR(36) NOT NULL)`).Error; err != nil {
			return err
		}
		if err := tx.Exec(`CREATE TABLE IF NOT EXISTS ext_connector_schedule_runs (
			log_id VARCHAR(36) PRIMARY KEY REFERENCES sync_logs(id) ON DELETE CASCADE,
			grant_id VARCHAR(36) NOT NULL)`).Error; err != nil {
			return err
		}
		return tx.Exec(`CREATE TABLE IF NOT EXISTS ext_connector_schedule_attempts (
            log_id VARCHAR(36) PRIMARY KEY REFERENCES ext_connector_schedule_runs(log_id) ON DELETE CASCADE,
            attempt INTEGER NOT NULL, retryable BOOLEAN NOT NULL DEFAULT FALSE)`).Error
	})
}

var (
	// ErrConnectorScheduleGrantRequired requires an authorized save of the schedule.
	ErrConnectorScheduleGrantRequired = errors.New("connector schedule requires an editor to save its schedule")
	// ErrConnectorScheduleGrantRevoked indicates the saved editor has lost access.
	ErrConnectorScheduleGrantRevoked = errors.New("connector schedule editor no longer has write access")
	// ErrConnectorScheduleRunBinding rejects a mismatched or already finished delivery.
	ErrConnectorScheduleRunBinding = errors.New("connector schedule delivery does not match an active scheduled run")
)

func ordinaryConnector(ds *types.DataSource) bool {
	return ds.Type != "" && ds.Type != types.ConnectorTypeNextcloud && ds.Type != types.ConnectorTypeDingTalk
}

func scheduleEnabled(ds *types.DataSource) bool {
	return ds.SyncSchedule != "" && (ds.SyncEnabled == nil || *ds.SyncEnabled) &&
		(ds.Status == types.DataSourceStatusActive || ds.Status == types.DataSourceStatusError)
}

func scheduleBinding(ds *types.DataSource) string {
	// Compare semantic configuration. ToJSON re-encrypts credentials with a fresh
	// nonce and the Feishu upgrade removes a one-shot resync flag after success.
	cfg, err := ds.ParseConfig()
	if err != nil || cfg == nil {
		return ""
	}
	delete(cfg.Settings, "resync_required")
	return types.GraphDigest(struct {
		Tenant                                          uint64
		KB, Source, Connector, Schedule, Mode, Conflict string
		Deletions                                       bool
		Config                                          *types.DataSourceConfig
	}{
		ds.TenantID, ds.KnowledgeBaseID, ds.ID, ds.Type, ds.SyncSchedule, ds.SyncMode,
		ds.ConflictStrategy, ds.SyncDeletions, cfg,
	})
}

type scheduleEditorSaveKey struct{}

// WithConnectorScheduleSave marks the ordinary source configuration/resume operation.
// Worker state writes and one-off manual fetches never confer future authority.
func WithConnectorScheduleSave(ctx context.Context) context.Context {
	return context.WithValue(ctx, scheduleEditorSaveKey{}, true)
}

type connectorScheduleSourceRepository struct{ *DataSourceRepository }

// WithConnectorScheduleGrants attaches transactional authorization capture to source edits.
func WithConnectorScheduleGrants(repo interfaces.DataSourceRepository) interfaces.DataSourceRepository {
	return &connectorScheduleSourceRepository{repo.(*DataSourceRepository)}
}

func (r *connectorScheduleSourceRepository) write(ctx context.Context, ds *types.DataSource, save bool,
	write func(*DataSourceRepository) error,
) error {
	if ds == nil || ds.Type == types.ConnectorTypeNextcloud || ds.Type == types.ConnectorTypeDingTalk {
		return write(r.DataSourceRepository)
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		scoped := *r.DataSourceRepository
		scoped.db = tx
		if err := write(&scoped); err != nil {
			return err
		}
		var current types.DataSource
		if err := tx.Where("id = ?", ds.ID).Take(&current).Error; err != nil {
			return err
		}
		if !ordinaryConnector(&current) {
			return nil
		}
		return scoped.saveConnectorSchedule(ctx, &current, save)
	})
}

func (r *connectorScheduleSourceRepository) Create(ctx context.Context, ds *types.DataSource) error {
	return r.write(ctx, ds, true, func(repo *DataSourceRepository) error { return repo.Create(ctx, ds) })
}

func (r *connectorScheduleSourceRepository) Update(ctx context.Context, ds *types.DataSource) error {
	return r.write(ctx, ds, ctx.Value(scheduleEditorSaveKey{}) == true,
		func(repo *DataSourceRepository) error { return repo.Update(ctx, ds) })
}

func (r *connectorScheduleSourceRepository) PatchSyncSettings(ctx context.Context, ds *types.DataSource,
	expected types.JSON, fields []string,
) error {
	return r.write(ctx, ds, slices.Contains(fields, "sync_schedule"),
		func(repo *DataSourceRepository) error { return repo.PatchSyncSettings(ctx, ds, expected, fields) })
}

func (r *DataSourceRepository) saveConnectorSchedule(ctx context.Context, ds *types.DataSource, explicit bool) error {
	binding := scheduleBinding(ds)
	principal, _ := types.PrincipalFromContext(ctx)
	human := explicit && principal.Type == types.PrincipalWebUser && !types.IsBackgroundTask(ctx)
	if !scheduleEnabled(ds) || explicit {
		if err := r.db.Exec("DELETE FROM ext_connector_schedule_grants WHERE source_id = ?", ds.ID).Error; err != nil {
			return err
		}
	} else {
		return r.db.Exec("DELETE FROM ext_connector_schedule_grants WHERE source_id = ? AND binding <> ?",
			ds.ID, binding).Error
	}
	if !human || !scheduleEnabled(ds) || binding == "" {
		return nil
	}
	actor, err := captureKnowledgeActorSelector(ctx, "")
	if err != nil {
		return err
	}
	if r.nameAuthorityFactory == nil || actor.CallerTenantID != ds.TenantID {
		return access.ErrForbidden
	}
	authority := r.nameAuthorityFactory(r.db)
	if authority == nil || authority.db != r.db {
		return access.ErrForbidden
	}
	if _, err := authority.AuthorizeCurrentKnowledgeInputWrite(ctx,
		&types.KnowledgeBase{ID: ds.KnowledgeBaseID, TenantID: ds.TenantID}); err != nil {
		return err
	}
	return r.db.Exec(`INSERT INTO ext_connector_schedule_grants (source_id, grant_id, binding, user_id)
		VALUES (?, ?, ?, ?)`, ds.ID, uuid.NewString(), binding, actor.UserID).Error
}

// ConnectorScheduleLogs is installed solely on the cron scheduler's repository.
// The manual sync service continues to write ordinary logs without a schedule receipt.
type ConnectorScheduleLogs struct {
	interfaces.SyncLogRepository
	DB *gorm.DB
}

// Create snapshots the current delegated grant before enqueue, in the log transaction.
func (r ConnectorScheduleLogs) Create(ctx context.Context, log *types.SyncLog) error {
	// Protected connector schedules supply their own durable admission.
	if log == nil || log.RecoveryTrigger != "" {
		return r.SyncLogRepository.Create(ctx, log)
	}
	return r.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var ds types.DataSource
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND tenant_id = ?", log.DataSourceID, log.TenantID).Take(&ds).Error; err != nil {
			return err
		}
		if !ordinaryConnector(&ds) || !scheduleEnabled(&ds) {
			return ErrConnectorScheduleRunBinding
		}
		// Recheck under the same source lock used to reopen a failed run. The cron
		// producer's earlier HasRunningSync can race a retry taking ownership.
		var running int64
		if err := tx.Model(&types.SyncLog{}).Where("data_source_id = ? AND status = ?", ds.ID,
			types.SyncLogStatusRunning).Count(&running).Error; err != nil {
			return err
		}
		if running != 0 {
			return datasource.ErrSyncAlreadyRunning
		}
		log.RecoveryTrigger = "schedule"
		log.QueueTaskID = log.ScheduledMinuteTaskID()
		if err := tx.Create(log).Error; err != nil {
			return err
		}
		return tx.Exec(`INSERT INTO ext_connector_schedule_runs (log_id, grant_id)
			SELECT ?, grant_id FROM ext_connector_schedule_grants WHERE source_id = ? AND binding = ?`,
			log.ID, ds.ID, scheduleBinding(&ds)).Error
	})
}

// ConnectorScheduleExecution carries the exact authorized scope for one delivery.
type ConnectorScheduleExecution struct {
	Source         *types.DataSource
	Initiator      types.TaskInitiator
	logID, binding string
}

type connectorScheduleExecutionKey struct{}

// Context carries the private run receipt through the existing write path.
func (e *ConnectorScheduleExecution) Context(ctx context.Context) context.Context {
	return context.WithValue(ctx, connectorScheduleExecutionKey{}, e)
}

func checkConnectorScheduleWrite(ctx context.Context, db *gorm.DB, kb *types.KnowledgeBase) error {
	run, ok := ctx.Value(connectorScheduleExecutionKey{}).(*ConnectorScheduleExecution)
	if !ok {
		return nil
	}
	if kb == nil || kb.ID != run.Source.KnowledgeBaseID || kb.TenantID != run.Source.TenantID {
		return ErrConnectorScheduleRunBinding
	}
	var current types.DataSource
	if err := db.WithContext(ctx).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?",
		run.Source.ID, kb.TenantID, kb.ID).Take(&current).Error; err != nil {
		return err
	}
	if !scheduleEnabled(&current) || scheduleBinding(&current) != run.binding {
		return ErrConnectorScheduleGrantRevoked
	}
	var editor string
	const activeEditor = connectorRunEditor + " AND l.status = 'running'"
	if err := db.WithContext(ctx).Raw(activeEditor, run.logID, current.ID, run.binding).
		Scan(&editor).Error; err != nil {
		return err
	}
	if editor != run.Initiator.UserID {
		return ErrConnectorScheduleGrantRevoked
	}
	return nil
}

const connectorRunEditor = `SELECT g.user_id FROM ext_connector_schedule_runs r
	JOIN ext_connector_schedule_grants g ON g.grant_id = r.grant_id
	JOIN sync_logs l ON l.id = r.log_id AND l.data_source_id = g.source_id
	WHERE r.log_id = ? AND g.source_id = ? AND g.binding = ?`

// IsOrdinaryScheduledRun detects a timed run even if a delivery claims a different trigger.
func (s *KnowledgeInputProvenanceStore) IsOrdinaryScheduledRun(ctx context.Context, logID string) (bool, error) {
	var ordinary bool
	err := s.db.WithContext(ctx).Raw(`SELECT EXISTS (SELECT 1 FROM sync_logs l
		JOIN data_sources d ON d.id = l.data_source_id AND d.tenant_id = l.tenant_id
		WHERE l.id = ? AND l.recovery_trigger = 'schedule' AND d.type NOT IN (?, ?))`,
		logID, types.ConnectorTypeNextcloud, types.ConnectorTypeDingTalk).Scan(&ordinary).Error
	return ordinary, err
}

// RestoreConnectorSchedule validates an executor-supplied ID, never an actor from the payload.
// Return the verified source snapshot used for this run, so a concurrent source edit cannot
// redirect the authorized execution to another connector or knowledge base.
func (s *KnowledgeInputProvenanceStore) RestoreConnectorSchedule(ctx context.Context,
	p types.DataSourceSyncPayload, taskID string, attempt int,
) (*ConnectorScheduleExecution, error) {
	var ds types.DataSource
	var initiator types.TaskInitiator
	db := s.db.WithContext(ctx)
	if err := db.Where("id = ? AND tenant_id = ?", p.DataSourceID, p.TenantID).Take(&ds).Error; err != nil {
		return nil, err
	}
	if !ordinaryConnector(&ds) {
		return nil, nil
	}
	var run types.SyncLog
	if err := db.Where("id = ? AND data_source_id = ? AND tenant_id = ?", p.SyncLogID, ds.ID, ds.TenantID).
		Take(&run).Error; err != nil {
		return nil, err
	}
	if p.Trigger != "schedule" || p.ForceFull || p.Initiator != (types.TaskInitiator{}) ||
		(run.Status != types.SyncLogStatusRunning && (run.Status != types.SyncLogStatusFailed || attempt == 0)) ||
		taskID != run.ScheduledMinuteTaskID() ||
		(run.QueueTaskID != "" && run.QueueTaskID != taskID) {
		return nil, ErrConnectorScheduleRunBinding
	}
	var userID string
	binding := scheduleBinding(&ds)
	err := db.Raw(connectorRunEditor, run.ID, ds.ID, binding).Scan(&userID).Error
	if err != nil {
		return nil, err
	}
	if !scheduleEnabled(&ds) || userID == "" || run.RecoveryTrigger != "schedule" || run.QueueTaskID != taskID {
		err = ErrConnectorScheduleGrantRequired
	} else {
		initiator = types.TaskInitiator{UserID: userID, CallerTenantID: ds.TenantID}
		actor := types.WithTaskAuthorization(ctx, ds.TenantID, initiator)
		fresh, check := s.AuthorizeCurrentKnowledgeInputWrite(actor,
			&types.KnowledgeBase{ID: ds.KnowledgeBaseID, TenantID: ds.TenantID})
		if check == nil {
			if err := claimConnectorScheduleAttempt(db, &run, &ds, binding, attempt); err != nil {
				return nil, err
			}
			initiator.Role = types.CallerFromContext(fresh).Role
			return &ConnectorScheduleExecution{Source: &ds, Initiator: initiator, logID: run.ID, binding: binding}, nil
		}
		if !errors.Is(check, gorm.ErrRecordNotFound) && !errors.Is(check, access.ErrForbidden) &&
			!errors.Is(check, access.ErrNotFound) && !errors.Is(check, ErrKnowledgeInputChanged) {
			return nil, check
		}
		err = ErrConnectorScheduleGrantRevoked
	}
	if update := db.Model(&types.SyncLog{}).
		Where("id = ? AND data_source_id = ? AND tenant_id = ? AND status = ?", run.ID, ds.ID, ds.TenantID,
			run.Status).
		Updates(map[string]any{
			"status": types.SyncLogStatusFailed, "finished_at": time.Now().UTC(),
			"error_message": err.Error(),
		}).Error; update != nil {
		return nil, update
	}
	return nil, err
}

// claimConnectorScheduleAttempt serializes retry admission with new cron windows.
// A retry needs its predecessor's returned-error receipt. A successful, active,
// or superseded execution cannot be taken over by replaying a queue message.
func claimConnectorScheduleAttempt(db *gorm.DB, run *types.SyncLog, ds *types.DataSource,
	binding string, attempt int,
) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var current types.DataSource
		locked := tx.Clauses(clause.Locking{Strength: "UPDATE"})
		if err := locked.Where("id = ?", ds.ID).Take(&current).Error; err != nil {
			return err
		}
		var editor string
		if err := tx.Raw(connectorRunEditor, run.ID, current.ID, binding).Scan(&editor).Error; err != nil {
			return err
		}
		if editor == "" || !scheduleEnabled(&current) || scheduleBinding(&current) != binding {
			return ErrConnectorScheduleGrantRequired
		}
		var status string
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Model(&types.SyncLog{}).
			Where("id = ?", run.ID).Select("status").Take(&status).Error; err != nil {
			return err
		}
		if status == types.SyncLogStatusRunning {
			first := tx.Exec(`INSERT INTO ext_connector_schedule_attempts (log_id, attempt)
				VALUES (?, ?) ON CONFLICT (log_id) DO NOTHING`, run.ID, attempt)
			if first.Error != nil || first.RowsAffected == 1 {
				return first.Error
			}
		} else if status != types.SyncLogStatusFailed {
			return ErrConnectorScheduleRunBinding
		}
		var newer int64
		if err := tx.Model(&types.SyncLog{}).Where("data_source_id = ? AND id <> ? AND status <> ?",
			ds.ID, run.ID, types.SyncLogStatusCanceled).
			Where("status = ? OR started_at >= ?", types.SyncLogStatusRunning, run.StartedAt).
			Count(&newer).Error; err != nil {
			return err
		}
		if newer != 0 {
			return ErrConnectorScheduleRunBinding
		}
		claim := tx.Exec(`UPDATE ext_connector_schedule_attempts SET attempt = ?, retryable = FALSE
			WHERE log_id = ? AND attempt < ? AND retryable = TRUE`, attempt, run.ID, attempt)
		if claim.Error != nil {
			return claim.Error
		}
		if claim.RowsAffected != 1 {
			return ErrConnectorScheduleRunBinding
		}
		return tx.Model(&types.SyncLog{}).Where("id = ? AND status = ?", run.ID, status).
			Updates(map[string]any{
				"status": types.SyncLogStatusRunning, "finished_at": nil, "error_message": "",
				"result": nil, "items_total": 0, "items_created": 0, "items_updated": 0,
				"items_deleted": 0, "items_skipped": 0, "items_failed": 0,
			}).Error
	})
}

// FinishConnectorScheduleError preserves the connector's recorded failure and
// publishes a retry permit only after the admitted execution has returned.
func (s *KnowledgeInputProvenanceStore) FinishConnectorScheduleError(ctx context.Context,
	run *ConnectorScheduleExecution, attempt int, retryable bool,
) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&types.SyncLog{}).Where("id = ? AND status = ?", run.logID, types.SyncLogStatusRunning).
			Updates(map[string]any{
				"status": types.SyncLogStatusFailed, "finished_at": time.Now().UTC(),
				"error_message": "connector schedule execution failed",
			}).Error; err != nil {
			return err
		}
		if !retryable {
			return nil
		}
		return tx.Exec(`UPDATE ext_connector_schedule_attempts SET retryable = TRUE WHERE log_id = ? AND attempt = ?`,
			run.logID, attempt).Error
	})
}
