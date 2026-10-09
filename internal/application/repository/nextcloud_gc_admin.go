package repository

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

var ErrNextcloudGCAdmin = errors.New("current GC admin session required")
var ErrNextcloudGCJobUnavailable = errors.New("GC job unavailable")
var ErrNextcloudGCRetention = errors.New("GC retention window open")

type nextcloudGCManualKey struct{}
type nextcloudGCManual struct {
	Tenant        uint64
	Job, KB, User string
	Authority     *KnowledgeInputProvenanceStore
}

type NextcloudGCPreviewItem struct {
	Kind                   string `json:"kind"`
	State                  string `json:"state"`
	Count                  int64  `json:"count"`
	EstimatedBytes         int64  `json:"estimated_bytes"`
	ConfirmedReleasedBytes int64  `json:"confirmed_released_bytes"`
}
type NextcloudGCPreview struct {
	Status                   NextcloudGCStatus        `json:"status"`
	Items                    []NextcloudGCPreviewItem `json:"items"`
	EarliestCollectAt        time.Time                `json:"earliest_collect_at"`
	CanRequestCollection     bool                     `json:"can_request_collection"`
	CollectionChecksRepeated bool                     `json:"collection_checks_repeated"`
}

func (s *NextcloudGCStore) PreviewJob(ctx context.Context, tenant uint64, id string) (*NextcloudGCPreview, error) {
	if tenant == 0 || id == "" {
		return nil, ErrNextcloudGCJobUnavailable
	}
	var job nextcloudGCJob
	if e := s.db.WithContext(ctx).Where("id=? AND tenant_id=?", id, tenant).Take(&job).Error; e != nil {
		return nil, ErrNextcloudGCJobUnavailable
	}
	out := &NextcloudGCPreview{Status: NextcloudGCStatus{ID: job.ID, TenantID: job.TenantID, KnowledgeBaseID: job.KnowledgeBaseID, KnowledgeID: job.KnowledgeID, Reason: job.Reason, State: job.State, NotBefore: job.NotBefore, OriginalNotBefore: job.OriginalNotBefore, NextAttemptAt: job.NextAttemptAt, Attempts: job.Attempts, LastErrorCode: job.LastErrorCode, CompletedAt: job.CompletedAt}, Items: []NextcloudGCPreviewItem{}, EarliestCollectAt: job.NotBefore, CollectionChecksRepeated: true}
	if e := s.db.WithContext(ctx).Model(&nextcloudGCItem{}).Select("kind,state,COUNT(*) AS count,COALESCE(SUM(estimated_bytes),0) AS estimated_bytes,COALESCE(SUM(confirmed_released_bytes),0) AS confirmed_released_bytes").Where("job_id=?", id).Group("kind,state").Order("kind,state").Scan(&out.Items).Error; e != nil {
		return nil, e
	}
	for _, item := range out.Items {
		out.Status.EstimatedBytes += item.EstimatedBytes
		out.Status.ConfirmedReleasedBytes += item.ConfirmedReleasedBytes
		if item.State != "collected" {
			out.Status.PendingItems += item.Count
		}
	}
	out.CanRequestCollection = job.State != "collected" && !time.Now().UTC().Before(job.NotBefore)
	return out, nil
}

// A request targets one persisted job; no inventory locator or safety deadline
// is accepted from the browser. Every destructive transaction repeats authority.
func (s *NextcloudGCStore) CollectJob(ctx context.Context, authority *KnowledgeInputProvenanceStore, tenant uint64, id string) (preview *NextcloudGCPreview, resultErr error) {
	if authority == nil || id == "" || tenant == 0 {
		return nil, ErrNextcloudGCAdmin
	}
	var job nextcloudGCJob
	if e := s.db.WithContext(ctx).Where("id=? AND tenant_id=?", id, tenant).Take(&job).Error; e != nil {
		return nil, ErrNextcloudGCJobUnavailable
	}
	caller := types.CallerFromContext(ctx)
	ctx = context.WithValue(ctx, nextcloudGCManualKey{}, nextcloudGCManual{tenant, id, job.KnowledgeBaseID, caller.UserID, authority})
	now := time.Now().UTC()
	if e := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := gcValidateManualAdmin(ctx, tx, job); e != nil {
			return e
		}
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=?", id, tenant).Take(&job).Error; e != nil {
			return e
		}
		nowMS, e := nextcloudLeaseNowMS(tx)
		if e != nil {
			return e
		}
		now = time.UnixMilli(nowMS).UTC()
		if now.Before(job.NotBefore) {
			return ErrNextcloudGCRetention
		}
		return gcManualAudit(tx, ctx, job, types.AuditActionNextcloudGCRequested, types.AuditOutcomeAccepted)
	}); e != nil {
		return nil, e
	}
	defer func() {
		outcome := types.AuditOutcomePartial
		if resultErr != nil {
			outcome = types.AuditOutcomeFailed
		} else if preview != nil && preview.Status.State == "collected" {
			outcome = types.AuditOutcomeSuccess
		}
		resultErr = errors.Join(resultErr, gcManualAudit(s.db.WithContext(ctx), ctx, job, types.AuditActionNextcloudGCFinished, outcome))
	}()
	if e := s.runOne(ctx, id, now); e != nil {
		return nil, e
	}
	if s.deleteObject != nil || s.prepareDeleteObject != nil {
		if e := s.runPhysical(ctx, id, now); e != nil {
			return nil, e
		}
	}
	if e := s.runLocalDerived(ctx, id, now); e != nil {
		return nil, e
	}
	if _, e := NewOriginalBodyRetentionStore(s.db).PurgeSelectedJob(ctx, tenant, id, 100); e != nil {
		return nil, e
	}
	return s.PreviewJob(ctx, tenant, id)
}

func (s *NextcloudGCStore) RetryJobAsAdmin(ctx context.Context, authority *KnowledgeInputProvenanceStore, tenant uint64, id string) (bool, error) {
	var job nextcloudGCJob
	if e := s.db.WithContext(ctx).Where("id=? AND tenant_id=?", id, tenant).Take(&job).Error; e != nil {
		return false, ErrNextcloudGCJobUnavailable
	}
	caller := types.CallerFromContext(ctx)
	ctx = context.WithValue(ctx, nextcloudGCManualKey{}, nextcloudGCManual{tenant, id, job.KnowledgeBaseID, caller.UserID, authority})
	changed := false
	e := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := gcValidateManualAdmin(ctx, tx, job); e != nil {
			return e
		}
		now, e := nextcloudLeaseNowMS(tx)
		if e != nil {
			return e
		}
		r := tx.Model(&nextcloudGCJob{}).Where("id=? AND tenant_id=? AND state IN ?", id, tenant, []string{"blocked", "retry"}).Updates(map[string]any{"state": "retry", "next_attempt_at": time.UnixMilli(now).UTC(), "updated_at": time.UnixMilli(now).UTC()})
		changed = r.RowsAffected > 0
		if r.Error != nil {
			return r.Error
		}
		if changed {
			return gcManualAudit(tx, ctx, job, types.AuditActionNextcloudGCRetry, types.AuditOutcomeAccepted)
		}
		return nil
	})
	return changed, e
}

// Called inside the actual SQL/provider transaction. Ordinary hourly sweeps
// have no manual marker and retain their existing source/lease safety gates.
func gcValidateManualAdmin(ctx context.Context, tx *gorm.DB, job nextcloudGCJob) error {
	request, ok := ctx.Value(nextcloudGCManualKey{}).(nextcloudGCManual)
	if !ok {
		return nil
	}
	if request.Authority == nil || request.Authority.displayAuthorityFactory == nil || request.User == "" || request.Tenant != job.TenantID || request.Job != job.ID || request.KB != job.KnowledgeBaseID {
		return ErrNextcloudGCAdmin
	}
	if _, machine := types.TenantAPIKeyScopeFromContext(ctx); machine {
		return ErrNextcloudGCAdmin
	}
	actual := request.Authority.displayAuthorityFactory(tx)
	if actual == nil {
		return ErrNextcloudGCAdmin
	}
	fresh, e := actual.RefreshCurrentReadActor(ctx)
	if e != nil {
		return ErrNextcloudGCAdmin
	}
	caller := types.CallerFromContext(fresh)
	principal, _ := types.PrincipalFromContext(fresh)
	if caller.UserID != request.User || caller.TenantID != job.TenantID || principal.Type != types.PrincipalWebUser || principal.ID != caller.UserID || !caller.Role.HasPermission(types.TenantRoleAdmin) {
		return ErrNextcloudGCAdmin
	}
	var kb types.KnowledgeBase
	if e := tx.Select("id", "tenant_id", "creator_id").Where("id=? AND tenant_id=?", job.KnowledgeBaseID, job.TenantID).Take(&kb).Error; e != nil {
		return ErrNextcloudGCAdmin
	}
	if _, e := actual.authorizeKnowledgeActorWrite(fresh, &kb); e != nil {
		return ErrNextcloudGCAdmin
	}
	return nil
}

func gcManualAudit(tx *gorm.DB, ctx context.Context, job nextcloudGCJob, action types.AuditAction, outcome types.AuditOutcome) error {
	caller := types.CallerFromContext(ctx)
	details, _ := json.Marshal(map[string]string{"job_id": job.ID, "reason": job.Reason})
	return tx.Create(&types.AuditLog{TenantID: job.TenantID, ActorUserID: caller.UserID, ActorRole: string(caller.Role), Action: action, ScopeType: "knowledge_base", ScopeID: job.KnowledgeBaseID, TargetType: "gc_job", TargetID: job.ID, Outcome: outcome, Details: types.JSON(details), CreatedAt: time.Now().UTC()}).Error
}
