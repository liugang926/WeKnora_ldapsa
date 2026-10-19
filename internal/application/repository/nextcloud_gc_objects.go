package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type nextcloudGCObjectClaim struct {
	resource *types.StoredResource
	token    string
	code     string
	content  NextcloudContentGCClaim
	delete   func(context.Context) (bool, error)
}

const nextcloudGCObjectLease = 10 * time.Minute

// scopedLocalGCPath accepts only immutable backend-scoped local objects made
// for the resource's own tenant. Legacy raw paths and cloud object stores need
// backend-specific deletion/acknowledgement and remain blocked.
func scopedLocalGCPath(resource *types.StoredResource) bool {
	if resource == nil || resource.TenantID == 0 || resource.Provider != "local" ||
		resource.StorageBackendID == "" || resource.SourceProvenance != types.ResourceProvenanceNextcloud {
		return false
	}
	backendID, inner, ok := types.ParseStorageBackendPath(resource.PhysicalPath)
	if !ok || backendID != resource.StorageBackendID {
		return false
	}
	relative := strings.TrimPrefix(inner, fmt.Sprintf("local://%d/", resource.TenantID))
	if relative == inner || relative == "" {
		return false
	}
	for _, part := range strings.Split(relative, "/") {
		if part == "" || part == "." || part == ".." || strings.Contains(part, "\\") {
			return false
		}
	}
	return true
}

func (s *NextcloudGCStore) runPhysical(ctx context.Context, jobID string, now time.Time) error {
	var job nextcloudGCJob
	if err := s.db.WithContext(ctx).Where("id = ?", jobID).Take(&job).Error; err != nil {
		return err
	}
	if job.State == "collected" {
		return nil
	}
	var items []nextcloudGCItem
	if err := s.db.WithContext(ctx).Where("job_id = ? AND kind IN ? AND state <> ?",
		jobID, []string{"source_file", "extracted_image"}, "collected").
		Order("kind ASC, object_ref ASC").Find(&items).Error; err != nil {
		return err
	}
	for _, item := range items {
		if item.Kind == "source_file" && now.Before(job.OriginalNotBefore) {
			continue
		}
		claim, err := s.claimObject(ctx, jobID, item.Kind, item.ObjectRef, now)
		if err != nil {
			return errors.Join(err, s.blockObject(ctx, jobID, item.Kind, item.ObjectRef, "gc_step_failed", now))
		}
		if claim.code != "" {
			if err := s.blockObject(ctx, jobID, item.Kind, item.ObjectRef, claim.code, now); err != nil {
				return err
			}
			continue
		}
		if claim.resource == nil {
			continue // another owner retained it, or the item was already done
		}
		if s.prepareDeleteObject != nil {
			operation, err := s.prepareDeleteObject(ctx, claim.resource)
			if err != nil || operation == nil {
				if releaseErr := s.failContentClaim(ctx, jobID, item.Kind, item.ObjectRef, claim, now); releaseErr != nil {
					return errors.Join(err, releaseErr)
				}
				if recordErr := s.blockObject(ctx, jobID, item.Kind, item.ObjectRef, "provider_delete_failed", now); recordErr != nil {
					return recordErr
				}
				continue
			}
			claim.delete = operation
		}
		if err := s.deleteClaimedObject(ctx, jobID, item.Kind, item.ObjectRef, claim, now); err != nil {
			// Release only this generation/item claim. An old worker cannot
			// clear the bounded token of a successor after a crash/retry.
			if releaseErr := s.failContentClaim(ctx, jobID, item.Kind, item.ObjectRef, claim, now); releaseErr != nil {
				return errors.Join(err, releaseErr)
			}
			code := "provider_delete_failed"
			if errors.Is(err, ErrNextcloudContentLeaseDenied) || errors.Is(err, ErrNextcloudContentGCUncovered) || errors.Is(err, ErrNextcloudContentGCBusy) {
				code = gcLeaseErrorCode(err)
			}
			if recordErr := s.blockObject(ctx, jobID, item.Kind, item.ObjectRef, code, now); recordErr != nil {
				return errors.Join(err, recordErr)
			}
			continue
		}
	}
	return nil
}

// claimObject locks job, source, knowledge, item and resource in a fixed order. All new
// binds lock the resource row before insertion. Once state=deleting commits,
// no new owner can appear between the reference check and provider deletion.
func (s *NextcloudGCStore) claimObject(ctx context.Context, jobID, kind, ref string, now time.Time) (nextcloudGCObjectClaim, error) {
	claim := nextcloudGCObjectClaim{}
	var identity nextcloudGCJob
	if err := s.db.WithContext(ctx).Select("id", "tenant_id", "knowledge_base_id", "datasource_id", "knowledge_id", "external_id").Where("id=?", jobID).Take(&identity).Error; err != nil {
		return claim, err
	}

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		tx = bodyInternal(tx)
		nowMS, lockErr := gcLockKBContent(tx, identity)
		if lockErr != nil {
			claim.code = gcLeaseErrorCode(lockErr)
			return nil
		}

		var job nextcloudGCJob
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", jobID).Take(&job).Error; err != nil {
			return err
		}
		if e := gcValidateManualAdmin(ctx, tx, job); e != nil {
			return e
		}

		if gcContentScope(job) != gcContentScope(identity) {
			claim.code = "source_identity_changed"
			return nil
		}
		if tx.Dialector.Name() == "postgres" {
			// A claim committed first leaves a deleting item that withdrawal
			// must wait out; a withdrawal committed first prevents new claims.
			if job.TenantID != identity.TenantID || job.KnowledgeBaseID != identity.KnowledgeBaseID ||
				job.DataSourceID != identity.DataSourceID {
				claim.code = "source_identity_changed"
				return nil
			}
			var withdrawn int64
			if err := tx.Model(&NextcloudIndexedWithdrawal{}).
				Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ?",
					job.TenantID, job.KnowledgeBaseID, job.DataSourceID).Count(&withdrawn).Error; err != nil {
				return err
			}
			if withdrawn != 0 {
				claim.code = "source_withdrawal_gc_paused"
				return nil
			}
		}
		if job.State == "collected" || now.Before(job.NotBefore) ||
			(kind == "source_file" && now.Before(job.OriginalNotBefore)) {
			claim.code = "retention_window_open"
			return nil
		}
		// A malformed image inventory cannot prove that a source resource is
		// not also referenced as an image. Defer every physical release until
		// the complete inventory has been rebuilt.
		if job.LastErrorCode == "invalid_image_inventory" {
			claim.code = "invalid_image_inventory"
			return nil
		}
		var version nextcloudSourceVersion
		if err := nextcloudVersionQuery(tx.Clauses(clause.Locking{Strength: "UPDATE"}),
			job.TenantID, job.KnowledgeBaseID, job.DataSourceID, job.ExternalID).
			Take(&version).Error; err != nil {
			return err
		}
		if version.State != "tombstone" && version.CandidateKnowledgeID == job.KnowledgeID {
			claim.code = "candidate_is_current"
			return nil
		}
		var knowledge types.Knowledge
		if err := tx.Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND channel = ?",
				job.KnowledgeID, job.TenantID, job.KnowledgeBaseID, types.ConnectorTypeNextcloud).
			Take(&knowledge).Error; err != nil {
			return err
		}
		metadata, err := nextcloudMetadata(&knowledge)
		if err != nil {
			return err
		}
		if nextcloudMetadataString(metadata, "datasource_id") != job.DataSourceID ||
			nextcloudMetadataString(metadata, "external_id") != job.ExternalID ||
			nextcloudMetadataString(metadata, "nextcloud_etag") != "" {
			claim.code = "source_identity_changed"
			return nil
		}
		if knowledge.ParseStatus != types.ParseStatusCompleted && knowledge.ParseStatus != types.ParseStatusFailed &&
			knowledge.ParseStatus != types.ParseStatusCancelled || knowledge.PendingSubtasksCount > 0 {
			claim.code = "build_still_active"
			return nil
		}
		if kind == "source_file" && knowledge.FilePath != ref {
			claim.code = "inventory_mismatch"
			return nil
		}
		if kind == "extracted_image" && knowledge.FilePath == ref && job.OriginalNotBefore.UnixMilli() > nowMS {
			// An image item can name the same object as the original source.
			// Releasing its knowledge binding before the original's deadline
			// would silently shorten the seven-day recovery window.
			claim.code = "retention_window_open"
			return nil
		}
		var item nextcloudGCItem
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("job_id = ? AND kind = ? AND object_ref = ?", jobID, kind, ref).Take(&item).Error; err != nil {
			return err
		}
		if item.State == "collected" {
			return nil
		}
		handle, ok := types.ParseResourcePath(ref)
		if !ok {
			claim.code = "legacy_resource_reference"
			return nil
		}
		var resource types.StoredResource
		if err := tx.Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("handle = ? AND tenant_id = ?", handle, job.TenantID).
			Take(&resource).Error; err != nil {
			return err
		}
		if !scopedLocalGCPath(&resource) {
			claim.code = "unsupported_storage_backend"
			return nil
		}
		if resource.State == types.ResourceStateDeleted {
			return tx.Model(&nextcloudGCItem{}).
				Where("job_id = ? AND kind = ? AND object_ref = ?", jobID, kind, ref).
				Updates(map[string]any{"state": "collected", "updated_at": now}).Error
		}
		fence, contentErr := gcCheckRetiredContent(tx, job, nowMS)
		if contentErr != nil {
			claim.code = gcLeaseErrorCode(contentErr)
			return nil
		}
		if job.NotBefore.UnixMilli() > nowMS || (kind == "source_file" && job.OriginalNotBefore.UnixMilli() > nowMS) {
			claim.code = "retention_window_open"
			return nil
		}
		var remaining int64
		if err := tx.Model(&types.ResourceBinding{}).Where("resource_id = ?", resource.ID).Count(&remaining).Error; err != nil {
			return err
		}
		if resource.State == types.ResourceStateDeleting {
			if item.State != "deleting" || remaining != 0 {
				claim.code = "resource_delete_in_progress"
				return nil
			}
			if item.LeaseUntil != nil && nowMS < item.LeaseUntil.UnixMilli() {
				claim.code = "resource_delete_in_progress"
				return nil
			}
			claim.token = uuid.NewString()
			contentClaim, err := gcMarkContentClaim(tx, job, fence, claim.token, nowMS)
			if err != nil {
				claim.code = gcLeaseErrorCode(err)
				return nil
			}
			claim.content = contentClaim
			leaseUntil := time.UnixMilli(nowMS + nextcloudGCObjectLease.Milliseconds()).UTC()
			if err := tx.Model(&nextcloudGCItem{}).
				Where("job_id = ? AND kind = ? AND object_ref = ?", jobID, kind, ref).
				Updates(map[string]any{"lease_token": claim.token, "lease_until": &leaseUntil, "updated_at": now}).Error; err != nil {
				return err
			}
			copyOfResource := resource
			claim.resource = &copyOfResource
			return nil
		}
		if resource.State != types.ResourceStateActive || resource.DeletedAt.Valid {
			claim.code = "resource_state_unavailable"
			return nil
		}
		var owned int64
		if err := tx.Model(&types.ResourceBinding{}).Where(
			"resource_id = ? AND tenant_id = ? AND owner_type = ? AND owner_id = ?",
			resource.ID, job.TenantID, types.ResourceOwnerKnowledge, job.KnowledgeID,
		).Count(&owned).Error; err != nil {
			return err
		}
		if owned == 0 {
			claim.code = "resource_binding_missing"
			return nil
		}
		if err := tx.Where("resource_id = ? AND tenant_id = ? AND owner_type = ? AND owner_id = ?",
			resource.ID, job.TenantID, types.ResourceOwnerKnowledge, job.KnowledgeID).
			Delete(&types.ResourceBinding{}).Error; err != nil {
			return err
		}
		if err := tx.Model(&types.ResourceBinding{}).Where("resource_id = ?", resource.ID).Count(&remaining).Error; err != nil {
			return err
		}
		if remaining > 0 {
			// This version released its claim; the shared object belongs to
			// another owner and contributes zero released bytes to this job.
			return tx.Model(&nextcloudGCItem{}).
				Where("job_id = ? AND kind = ? AND object_ref = ?", jobID, kind, ref).
				Updates(map[string]any{"state": "collected", "updated_at": now}).Error
		}
		if err := tx.Model(&types.StoredResource{}).Where("id = ? AND state = ?", resource.ID, types.ResourceStateActive).
			Updates(map[string]any{"state": types.ResourceStateDeleting, "updated_at": now}).Error; err != nil {
			return err
		}
		claim.token = uuid.NewString()
		contentClaim, err := gcMarkContentClaim(tx, job, fence, claim.token, nowMS)
		if err != nil {
			return err
		}
		claim.content = contentClaim
		if err := tx.Model(&nextcloudGCItem{}).
			Where("job_id = ? AND kind = ? AND object_ref = ?", jobID, kind, ref).
			Updates(map[string]any{"state": "deleting", "lease_token": claim.token,
				"lease_until": claim.content.ExpiresAt, "updated_at": now}).Error; err != nil {
			return err
		}
		resource.State = types.ResourceStateDeleting
		copyOfResource := resource
		claim.resource = &copyOfResource
		return nil
	})
	return claim, err
}

func (s *NextcloudGCStore) finishObject(ctx context.Context, jobID, kind, ref, resourceID string, claim nextcloudGCObjectClaim, unlinked bool, now time.Time) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		tx = bodyInternal(tx)
		if _, err := gcLockKBContent(tx, nextcloudGCJob{TenantID: claim.content.Scope.TenantID, KnowledgeBaseID: claim.content.Scope.KnowledgeBaseID}); err != nil {
			return err
		}
		return s.finishObjectInTx(tx, jobID, kind, ref, resourceID, claim, unlinked, now)
	})
}
func (s *NextcloudGCStore) finishObjectInTx(tx *gorm.DB, jobID, kind, ref, resourceID string, claim nextcloudGCObjectClaim, unlinked bool, now time.Time) error {
	if err := NewNextcloudContentLeaseStore(s.db).ValidateGCClaimInTx(tx.Statement.Context, tx, claim.content); err != nil {
		return err
	}
	nowMS, err := nextcloudLeaseNowMS(tx)
	if err != nil {
		return err
	}
	var job nextcloudGCJob
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", jobID).Take(&job).Error; err != nil {
		return err
	}
	if gcContentScope(job) != claim.content.Scope {
		return ErrNextcloudContentLeaseDenied
	}

	var item nextcloudGCItem
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("job_id = ? AND kind = ? AND object_ref = ?",
		jobID, kind, ref).Take(&item).Error; err != nil {
		return err
	}
	if item.State != "deleting" || claim.token == "" || item.LeaseToken != claim.token || item.LeaseUntil == nil || item.LeaseUntil.UnixMilli() <= nowMS {
		return errors.New("Nextcloud GC item deletion claim lost")
	}
	var resource types.StoredResource
	if err := tx.Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", resourceID).
		Take(&resource).Error; err != nil {
		return err
	}
	if resource.State != types.ResourceStateDeleting {
		return errors.New("resource deletion claim lost")
	}
	var remaining int64
	if err := tx.Model(&types.ResourceBinding{}).Where("resource_id = ?", resourceID).Count(&remaining).Error; err != nil {
		return err
	}
	if remaining != 0 {
		return errors.New("resource rebound during deletion")
	}
	if err := tx.Model(&types.StoredResource{}).Where("id = ? AND state = ?", resourceID, types.ResourceStateDeleting).
		Updates(map[string]any{"state": types.ResourceStateDeleted, "deleted_at": now, "updated_at": now}).Error; err != nil {
		return err
	}
	credited := int64(0)
	if unlinked {
		credited = resource.Size
	}
	if credited < 0 {
		credited = 0
	}
	if err := tx.Model(&nextcloudGCItem{}).Where("job_id = ? AND kind = ? AND object_ref = ?", jobID, kind, ref).
		Updates(map[string]any{"state": "collected", "confirmed_released_bytes": credited,
			"lease_token": "", "lease_until": nil, "updated_at": now}).Error; err != nil {
		return err
	}
	return gcReturnContentRetired(tx, claim.content, nowMS)
}

func (s *NextcloudGCStore) blockObject(ctx context.Context, jobID, kind, ref, code string, now time.Time) error {
	var identity nextcloudGCJob
	if e := s.db.WithContext(ctx).Select("tenant_id", "knowledge_base_id").Where("id=?", jobID).Take(&identity).Error; e != nil {
		return e
	}

	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		tx = bodyInternal(tx)
		if _, e := gcLockKBContent(tx, identity); e != nil {
			return e
		}

		var job nextcloudGCJob
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", jobID).Take(&job).Error; err != nil {
			return err
		}
		if job.State == "collected" {
			return nil
		}

		if err := tx.Model(&nextcloudGCItem{}).
			Where("job_id = ? AND kind = ? AND object_ref = ? AND state NOT IN ?", jobID, kind, ref,
				[]string{"deleting", "collected"}).
			Updates(map[string]any{"state": "blocked", "updated_at": now}).Error; err != nil {
			return err
		}
		return tx.Model(&nextcloudGCJob{}).Where("id = ? AND state <> ?", jobID, "collected").
			Updates(map[string]any{"state": "blocked", "last_error_code": code,
				"next_attempt_at": now.Add(nextcloudGCRetryWindow), "updated_at": now}).Error
	})
}
