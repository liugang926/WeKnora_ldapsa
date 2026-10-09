package repository

import (
	"context"
	"errors"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"math"
	"time"
)

func gcContentScope(job nextcloudGCJob) NextcloudContentScope {
	return NextcloudContentScope{TenantID: job.TenantID, KnowledgeBaseID: job.KnowledgeBaseID, KnowledgeID: job.KnowledgeID, DataSourceID: job.DataSourceID, ExternalID: job.ExternalID}
}

// Source/version writers take this KB row before the content sentinel. Every
// physical GC phase uses the identical order on PostgreSQL and SQLite.
func gcLockKBContent(tx *gorm.DB, job nextcloudGCJob) (int64, error) {
	result := tx.Exec("UPDATE knowledge_bases SET ever_had_nextcloud_source=ever_had_nextcloud_source WHERE id=? AND tenant_id=?", job.KnowledgeBaseID, job.TenantID)
	if result.Error != nil {
		return 0, result.Error
	}
	if result.RowsAffected != 1 {
		return 0, ErrNextcloudContentLeaseDenied
	}
	now, e := nextcloudLeaseNowMS(tx)
	if e != nil {
		return 0, e
	}
	_, e = nextcloudLockKBFence(tx, job.TenantID, job.KnowledgeBaseID, now)
	return now, e
}
func gcLeaseErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrNextcloudContentGCUncovered):
		return "content_lease_coverage_unverified"
	case errors.Is(err, ErrNextcloudContentGCBusy):
		return "content_read_or_build_busy"
	case errors.Is(err, ErrNextcloudContentLeaseDenied), errors.Is(err, ErrNextcloudContentLeaseInvalid):
		return "content_generation_unverified"
	default:
		return "content_fence_unavailable"
	}
}
func gcCheckRetiredContent(tx *gorm.DB, job nextcloudGCJob, nowMS int64) (nextcloudContentFence, error) {
	scope := gcContentScope(job)
	if e := nextcloudRequireContentLeaseCoverage(tx, scope, nowMS); e != nil {
		return nextcloudContentFence{}, e
	}
	fence, e := nextcloudLockExactFence(tx, scope, nowMS, false)
	if e != nil {
		return fence, e
	}
	if (fence.State != "retired" && fence.State != "deleting") || fence.RetiredAtMS == nil || *fence.RetiredAtMS > nowMS || fence.Epoch == math.MaxInt64 || job.NotBefore.UnixMilli() < *fence.RetiredAtMS {
		return fence, ErrNextcloudContentLeaseDenied
	}
	if e = nextcloudRequireNoActiveContentLeases(tx, scope, nowMS); e != nil {
		return fence, e
	}
	return fence, nil
}
func gcMarkContentClaim(tx *gorm.DB, job nextcloudGCJob, fence nextcloudContentFence, token string, nowMS int64) (NextcloudContentGCClaim, error) {
	if fence.State == "deleting" && fence.ClaimUntilMS != nil && *fence.ClaimUntilMS > nowMS {
		return NextcloudContentGCClaim{}, ErrNextcloudContentGCBusy
	}
	until := nowMS + nextcloudGCObjectLease.Milliseconds()
	result := tx.Exec("UPDATE nextcloud_content_fences SET state='deleting',epoch=epoch+1,claim_token=?,claim_until_ms=?,updated_at_ms=? WHERE tenant_id=? AND knowledge_base_id=? AND knowledge_id=? AND epoch=? AND state IN ('retired','deleting')", token, until, nowMS, job.TenantID, job.KnowledgeBaseID, job.KnowledgeID, fence.Epoch)
	if result.Error != nil {
		return NextcloudContentGCClaim{}, result.Error
	}
	if result.RowsAffected != 1 {
		return NextcloudContentGCClaim{}, ErrNextcloudContentLeaseDenied
	}
	return NextcloudContentGCClaim{Token: token, Scope: gcContentScope(job), Epoch: fence.Epoch + 1, ExpiresAt: time.UnixMilli(until).UTC()}, nil
}
func gcReturnContentRetired(tx *gorm.DB, claim NextcloudContentGCClaim, nowMS int64) error {
	result := tx.Exec("UPDATE nextcloud_content_fences SET state='retired',claim_token='',claim_until_ms=NULL,updated_at_ms=? WHERE tenant_id=? AND knowledge_base_id=? AND knowledge_id=? AND state='deleting' AND epoch=? AND claim_token=?", nowMS, claim.Scope.TenantID, claim.Scope.KnowledgeBaseID, claim.Scope.KnowledgeID, claim.Epoch, claim.Token)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrNextcloudContentLeaseDenied
	}
	return nil
}

// The provider call runs under the same KB/content/job/source/item/resource
// transaction locks as its final admission. A restored current version or new
// read cannot enter after the final check and before the actual local unlink.
func (s *NextcloudGCStore) deleteClaimedObject(ctx context.Context, jobID, kind, ref string, claim nextcloudGCObjectClaim, now time.Time) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		tx = bodyInternal(tx)
		identity := nextcloudGCJob{TenantID: claim.content.Scope.TenantID, KnowledgeBaseID: claim.content.Scope.KnowledgeBaseID}
		nowMS, e := gcLockKBContent(tx, identity)
		if e != nil {
			return e
		}
		if e = NewNextcloudContentLeaseStore(s.db).ValidateGCClaimInTx(ctx, tx, claim.content); e != nil {
			return e
		}
		var job nextcloudGCJob
		if e = tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", jobID).Take(&job).Error; e != nil {
			return e
		}
		if e := gcValidateManualAdmin(ctx, tx, job); e != nil {
			return e
		}

		if gcContentScope(job) != claim.content.Scope || job.State == "collected" || job.NotBefore.UnixMilli() > nowMS || (kind == "source_file" && job.OriginalNotBefore.UnixMilli() > nowMS) {
			return ErrNextcloudContentLeaseDenied
		}
		var version nextcloudSourceVersion
		if e = nextcloudVersionQuery(tx.Clauses(clause.Locking{Strength: "UPDATE"}), job.TenantID, job.KnowledgeBaseID, job.DataSourceID, job.ExternalID).Take(&version).Error; e != nil {
			return e
		}
		if tx.Migrator().HasTable(&NextcloudIndexedWithdrawal{}) {
			var withdrawn int64
			if e = tx.Model(&NextcloudIndexedWithdrawal{}).Where("tenant_id=? AND knowledge_base_id=? AND datasource_id=?", job.TenantID, job.KnowledgeBaseID, job.DataSourceID).Count(&withdrawn).Error; e != nil {
				return e
			}
			if withdrawn != 0 {
				return ErrNextcloudContentLeaseDenied
			}
		}
		if version.State != "tombstone" && version.CandidateKnowledgeID == job.KnowledgeID {
			return ErrNextcloudContentLeaseDenied
		}
		var knowledge types.Knowledge
		if e = tx.Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=? AND knowledge_base_id=? AND channel=?", job.KnowledgeID, job.TenantID, job.KnowledgeBaseID, types.ConnectorTypeNextcloud).Take(&knowledge).Error; e != nil {
			return e
		}
		if knowledge.FilePath == ref && job.OriginalNotBefore.UnixMilli() > nowMS {
			return ErrNextcloudContentLeaseDenied
		}
		meta, e := nextcloudMetadata(&knowledge)
		if e != nil || nextcloudMetadataString(meta, "datasource_id") != job.DataSourceID || nextcloudMetadataString(meta, "external_id") != job.ExternalID || nextcloudMetadataString(meta, "nextcloud_etag") != "" || knowledge.PendingSubtasksCount > 0 || (knowledge.ParseStatus != types.ParseStatusCompleted && knowledge.ParseStatus != types.ParseStatusFailed && knowledge.ParseStatus != types.ParseStatusCancelled) || (kind == "source_file" && knowledge.FilePath != ref) {
			return ErrNextcloudContentLeaseDenied
		}
		var item nextcloudGCItem
		if e = tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("job_id=? AND kind=? AND object_ref=?", jobID, kind, ref).Take(&item).Error; e != nil {
			return e
		}
		if item.State != "deleting" || item.LeaseToken != claim.token || item.LeaseUntil == nil || item.LeaseUntil.UnixMilli() <= nowMS {
			return ErrNextcloudContentLeaseDenied
		}
		var resource types.StoredResource
		if e = tx.Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", claim.resource.ID).Take(&resource).Error; e != nil {
			return e
		}
		if resource.TenantID != job.TenantID || resource.PhysicalPath != claim.resource.PhysicalPath || resource.StorageBackendID != claim.resource.StorageBackendID || resource.State != types.ResourceStateDeleting || !scopedLocalGCPath(&resource) {
			return ErrNextcloudContentLeaseDenied
		}
		var owners int64
		if e = tx.Model(&types.ResourceBinding{}).Where("resource_id=?", resource.ID).Count(&owners).Error; e != nil {
			return e
		}
		if owners != 0 {
			return ErrNextcloudContentLeaseDenied
		}
		providerCtx, cancel := context.WithDeadline(ctx, claim.content.ExpiresAt)
		defer cancel()
		var unlinked bool
		if claim.delete != nil {
			unlinked, e = claim.delete(providerCtx)
		} else {
			unlinked, e = s.deleteObject(providerCtx, &resource)
		}
		if e != nil {
			return e
		}
		return s.finishObjectInTx(tx, jobID, kind, ref, resource.ID, claim, unlinked, now)
	})
}
func (s *NextcloudGCStore) failContentClaim(ctx context.Context, jobID, kind, ref string, claim nextcloudGCObjectClaim, now time.Time) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		tx = bodyInternal(tx)
		nowMS, e := gcLockKBContent(tx, nextcloudGCJob{TenantID: claim.content.Scope.TenantID, KnowledgeBaseID: claim.content.Scope.KnowledgeBaseID})
		if e != nil {
			return e
		}
		fence, e := nextcloudLockExactFence(tx, claim.content.Scope, nowMS, false)
		if e != nil {
			return e
		}
		if fence.State != "deleting" || fence.ClaimToken != claim.content.Token || fence.Epoch != claim.content.Epoch {
			return ErrNextcloudContentLeaseDenied
		}
		var job nextcloudGCJob
		if e = tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", jobID).Take(&job).Error; e != nil {
			return e
		}
		if gcContentScope(job) != claim.content.Scope {
			return ErrNextcloudContentLeaseDenied
		}
		u := tx.Model(&nextcloudGCItem{}).Where("job_id=? AND kind=? AND object_ref=? AND state='deleting' AND lease_token=?", jobID, kind, ref, claim.token).Updates(map[string]any{"lease_token": "", "lease_until": nil, "updated_at": now})
		if u.Error != nil {
			return u.Error
		}
		if u.RowsAffected != 1 {
			return ErrNextcloudContentLeaseDenied
		}
		return gcReturnContentRetired(tx, claim.content, nowMS)
	})
}
