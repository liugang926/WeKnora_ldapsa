package repository

import (
	"context"
	"errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

var ErrDingTalkRunBusy = errors.New("DingTalk source has an active sync attempt")
var ErrDingTalkRunExpired = errors.New("DingTalk sync attempt is no longer current")

type dingTalkExecutionKey struct{}
type dingTalkRunClaim struct {
	DataSourceID                       string `gorm:"primaryKey"`
	TenantID                           uint64
	KnowledgeBaseID, SyncLogID, TaskID string
	RetryAttempt                       int
	Token                              string
	SourceRevision                     uint64
	ExpiresAt                          time.Time
	Active                             bool
}

func (dingTalkRunClaim) TableName() string { return "dingtalk_run_claims" }
func dingTalkCAS(r *gorm.DB) error {
	if r.Error != nil {
		return r.Error
	}
	if r.RowsAffected != 1 {
		return ErrDingTalkRunExpired
	}
	return nil
}

// ClaimDingTalkRun records execution ownership, never an actor or grant. The
// worker supplies its actual queue task ID/retry count after normal authorization.
func (s *KnowledgeInputProvenanceStore) ClaimDingTalkRun(ctx context.Context, ds *types.DataSource, run, task string, retry int, ttl time.Duration) (context.Context, error) {
	if ds == nil || task == "" || run == "" || retry < 0 || ttl < time.Minute || ttl > time.Hour {
		return ctx, ErrDingTalkRunExpired
	}
	var claim dingTalkRunClaim
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var actual types.DataSource
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", ds.ID, ds.TenantID, ds.KnowledgeBaseID).Take(&actual).Error; e != nil {
			return e
		}
		if actual.Type != types.ConnectorTypeDingTalk || actual.Status == types.DataSourceStatusPaused || actual.Status == types.DataSourceStatusDeleted {
			return ErrDingTalkRunExpired
		}
		var log types.SyncLog
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND data_source_id = ? AND tenant_id = ?", run, ds.ID, ds.TenantID).Take(&log).Error; e != nil {
			return e
		}
		if log.QueueTaskID != "" && log.QueueTaskID != task {
			return ErrDingTalkRunExpired
		}
		var old dingTalkRunClaim
		e := tx.Where("data_source_id = ?", ds.ID).Take(&old).Error
		if e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
			return e
		}
		now := time.Now().UTC()
		if e == nil && old.Active && old.ExpiresAt.After(now) {
			return ErrDingTalkRunBusy
		}
		if log.Status == types.SyncLogStatusFailed {
			if old.SyncLogID != run || old.TaskID != task || retry <= old.RetryAttempt {
				return ErrDingTalkRunExpired
			}
		} else if log.Status != types.SyncLogStatusRunning {
			return ErrDingTalkRunExpired
		}
		if old.SyncLogID == run && (old.TaskID != task || retry < old.RetryAttempt) {
			return ErrDingTalkRunExpired
		}
		var revision uint64
		if e := tx.Table("dingtalk_source_input_revisions").Select("revision").Where("datasource_id = ?", ds.ID).Scan(&revision).Error; e != nil {
			return e
		}
		if revision == 0 {
			return ErrDingTalkRunExpired
		}
		claim = dingTalkRunClaim{DataSourceID: ds.ID, TenantID: ds.TenantID, KnowledgeBaseID: ds.KnowledgeBaseID, SyncLogID: run, TaskID: task, RetryAttempt: retry, Token: uuid.NewString(), SourceRevision: revision, ExpiresAt: now.Add(ttl), Active: true}
		if old.Active && old.SyncLogID != run {
			var previous types.SyncLog
			if e := tx.Where("id = ? AND data_source_id = ?", old.SyncLogID, ds.ID).Take(&previous).Error; e != nil {
				return e
			}
			if previous.Status == types.SyncLogStatusRunning {
				if e := dingTalkCAS(tx.Model(&types.SyncLog{}).Where("id = ? AND data_source_id = ? AND status = ?", old.SyncLogID, ds.ID, types.SyncLogStatusRunning).Updates(map[string]any{"status": types.SyncLogStatusFailed, "finished_at": now, "error_message": "sync attempt expired"})); e != nil {
					return e
				}
			}
		}
		if e := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "data_source_id"}}, UpdateAll: true}).Create(&claim).Error; e != nil {
			return e
		}
		return dingTalkCAS(tx.Model(&types.SyncLog{}).Where("id = ? AND status = ?", run, log.Status).Updates(map[string]any{"status": types.SyncLogStatusRunning, "queue_task_id": task, "finished_at": nil, "error_message": ""}))
	})
	if err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, dingTalkExecutionKey{}, claim), nil
}

func checkDingTalkExecution(tx *gorm.DB, f dingTalkFetch) error {
	var c dingTalkRunClaim
	e := tx.Where("data_source_id = ?", f.Source).Take(&c).Error
	if errors.Is(e, gorm.ErrRecordNotFound) {
		return nil
	} // pre-integration repository callers
	if e != nil {
		return e
	}
	if !c.Active || c.Token != f.ExecutionToken || c.SyncLogID != f.Run || c.TenantID != f.Tenant || c.KnowledgeBaseID != f.KB || c.SourceRevision != f.Revision || !c.ExpiresAt.After(time.Now().UTC()) {
		return ErrDingTalkRunExpired
	}
	return nil
}

func (s *KnowledgeInputProvenanceStore) RenewDingTalkRun(ctx context.Context, ttl time.Duration) error {
	c, ok := ctx.Value(dingTalkExecutionKey{}).(dingTalkRunClaim)
	if !ok || ttl < time.Minute || ttl > time.Hour {
		return ErrDingTalkRunExpired
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var ds types.DataSource
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND tenant_id = ?", c.DataSourceID, c.TenantID).Take(&ds).Error; e != nil {
			return e
		}
		f := dingTalkFetch{Tenant: c.TenantID, KB: c.KnowledgeBaseID, Source: c.DataSourceID, Run: c.SyncLogID, Revision: c.SourceRevision, ExecutionToken: c.Token, Config: ds.Config}
		if e := checkDingTalkRun(tx, f); e != nil {
			return e
		}
		return dingTalkCAS(tx.Model(&dingTalkRunClaim{}).Where("data_source_id = ? AND token = ? AND active = ?", c.DataSourceID, c.Token, true).Update("expires_at", time.Now().UTC().Add(ttl)))
	})
}

// FinalizeDingTalkRun atomically records the result and cursor. Failed/partial
// attempts never acknowledge a cursor. Config changes only allow failed/canceled
// log cleanup; they cannot overwrite the newly configured source's state.
// message, when non-empty, is persisted as the log's error_message (used to
// surface why a run was canceled, e.g. a concurrent configuration update).
func (s *KnowledgeInputProvenanceStore) FinalizeDingTalkRun(ctx context.Context, status string, result *types.SyncResult, cursor types.JSON, message string) error {
	c, ok := ctx.Value(dingTalkExecutionKey{}).(dingTalkRunClaim)
	if !ok || result == nil {
		return ErrDingTalkRunExpired
	}
	if status != types.SyncLogStatusSuccess && status != types.SyncLogStatusPartial && status != types.SyncLogStatusFailed && status != types.SyncLogStatusCanceled {
		return ErrDingTalkRunExpired
	}
	if (result.Failed > 0 || len(result.Errors) > 0) && status == types.SyncLogStatusSuccess {
		return ErrDingTalkRunExpired
	}
	encoded, e := result.ToJSON()
	if e != nil {
		return e
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// A deleted source may only finish its existing attempt; token checks
		// remain mandatory, and deleted rows never receive source-state writes.
		var ds types.DataSource
		if e := tx.Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND tenant_id = ?", c.DataSourceID, c.TenantID).Take(&ds).Error; e != nil {
			return e
		}
		f := dingTalkFetch{Tenant: c.TenantID, KB: c.KnowledgeBaseID, Source: c.DataSourceID, Run: c.SyncLogID, Revision: c.SourceRevision, ExecutionToken: c.Token}
		if e := checkDingTalkExecution(tx, f); e != nil {
			return e
		}
		var revision uint64
		if e := tx.Table("dingtalk_source_input_revisions").Select("revision").Where("datasource_id = ?", c.DataSourceID).Scan(&revision).Error; e != nil {
			return e
		}
		current := !ds.DeletedAt.Valid && revision == c.SourceRevision && ds.Status != types.DataSourceStatusPaused && ds.Status != types.DataSourceStatusDeleted
		if !current && status != types.SyncLogStatusFailed && status != types.SyncLogStatusCanceled {
			return ErrDingTalkRunExpired
		}
		var existing types.SyncLog
		if e := tx.Where("id = ? AND data_source_id = ?", c.SyncLogID, c.DataSourceID).Take(&existing).Error; e != nil {
			return e
		}
		if existing.Status == types.SyncLogStatusCanceled && status == types.SyncLogStatusCanceled {
			return dingTalkCAS(tx.Model(&dingTalkRunClaim{}).Where("data_source_id = ? AND token = ? AND active = ?", c.DataSourceID, c.Token, true).Update("active", false))
		}
		now := time.Now().UTC()
		values := map[string]any{"status": status, "finished_at": now, "items_total": result.Total, "items_created": result.Created, "items_updated": result.Updated, "items_deleted": result.Deleted, "items_skipped": result.Skipped, "items_failed": result.Failed, "result": encoded, "error_message": message}
		if e := dingTalkCAS(tx.Model(&types.SyncLog{}).Where("id = ? AND data_source_id = ? AND status = ?", c.SyncLogID, c.DataSourceID, types.SyncLogStatusRunning).Updates(values)); e != nil {
			return e
		}
		if current {
			sourceStatus := types.DataSourceStatusActive
			if status == types.SyncLogStatusFailed {
				sourceStatus = types.DataSourceStatusError
			}
			values = map[string]any{"last_sync_at": now, "last_sync_result": encoded, "status": sourceStatus}
			if status == types.SyncLogStatusSuccess {
				values["error_message"] = ""
			}
			if status == types.SyncLogStatusSuccess && cursor != nil {
				values["last_sync_cursor"] = cursor
			}
			if e := dingTalkCAS(tx.Model(&types.DataSource{}).Where("id = ? AND tenant_id = ?", c.DataSourceID, c.TenantID).Updates(values)); e != nil {
				return e
			}
		}
		return dingTalkCAS(tx.Model(&dingTalkRunClaim{}).Where("data_source_id = ? AND token = ? AND active = ?", c.DataSourceID, c.Token, true).Update("active", false))
	})
}
