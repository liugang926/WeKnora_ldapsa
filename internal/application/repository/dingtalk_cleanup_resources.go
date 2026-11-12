package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ClaimDingTalkCleanupResource consumes the resource registry's existing
// active/deleting/deleted fence. It adds no claim identity: the already-held
// source run owns execution, and a deleting resource cannot acquire new owners.
// A nil resource means either a shared object retained by another owner or an
// already deleted exact object. Physical I/O always happens after this commits.
func (s *KnowledgeInputProvenanceStore) ClaimDingTalkCleanupResource(
	ctx context.Context,
	intent DingTalkWorkIntent,
	ref string) (*types.StoredResource,
	error,
) {
	if _, _, err := s.LoadDingTalkWork(ctx, intent); err != nil {
		return nil, err
	}
	var claimed *types.StoredResource
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		resource, err := loadDingTalkCleanupResource(ctx, tx, intent, ref)
		if err != nil {
			return err
		}
		if resource.State == types.ResourceStateDeleted && resource.DeletedAt.Valid {
			return nil
		}
		if resource.DeletedAt.Valid ||
			(resource.State != types.ResourceStateActive &&
				resource.State != types.ResourceStateDeleting) {
			return ErrDingTalkPublicationConflict
		}
		if resource.State == types.ResourceStateActive {
			if err := tx.Where("resource_id = ? AND tenant_id = ? AND owner_type = ? AND owner_id = ?",
				resource.ID,
				intent.TenantID,
				types.ResourceOwnerKnowledge,
				intent.KnowledgeID).Delete(&types.ResourceBinding{}).Error; err != nil {
				return err
			}
		}
		var remaining int64
		if err := tx.Model(&types.ResourceBinding{}).Where(
			"resource_id = ?",
			resource.ID).Count(&remaining).Error; err != nil {
			return err
		}
		if remaining > 0 {
			if resource.State == types.ResourceStateDeleting {
				return ErrDingTalkPublicationConflict
			}
			return nil
		}
		if resource.State == types.ResourceStateActive {
			if err := dingTalkCAS(tx.Model(&types.StoredResource{}).Where(
				"id = ? AND tenant_id = ? AND state = ?",
				resource.ID,
				intent.TenantID,
				types.ResourceStateActive).Updates(map[string]any{
				"state":      types.ResourceStateDeleting,
				"updated_at": time.Now().UTC(),
			})); err != nil {
				return err
			}
			resource.State = types.ResourceStateDeleting
		}
		claimed = resource
		return nil
	})
	return claimed, err
}

// FinishDingTalkCleanupResource accepts only the immutable physical location
// returned above, under a still-current source token and the same work version.
func (s *KnowledgeInputProvenanceStore) FinishDingTalkCleanupResource(
	ctx context.Context,
	intent DingTalkWorkIntent,
	ref string,
	claimed *types.StoredResource,
) error {
	if claimed == nil {
		return ErrDingTalkPublicationConflict
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		actual, err := loadDingTalkCleanupResource(ctx, tx, intent, ref)
		if err != nil {
			return err
		}
		if actual.ID != claimed.ID ||
			actual.Handle != claimed.Handle ||
			actual.TenantID != claimed.TenantID ||
			actual.PhysicalPath != claimed.PhysicalPath ||
			actual.StorageBackendID != claimed.StorageBackendID ||
			actual.Provider != claimed.Provider ||
			actual.SourceProvenance != claimed.SourceProvenance {
			return ErrDingTalkPublicationConflict
		}
		if actual.State == types.ResourceStateDeleted && actual.DeletedAt.Valid {
			return nil
		}
		if actual.State != types.ResourceStateDeleting || actual.DeletedAt.Valid {
			return ErrDingTalkPublicationConflict
		}
		var remaining int64
		if err := tx.Model(&types.ResourceBinding{}).Where(
			"resource_id = ?",
			actual.ID).Count(&remaining).Error; err != nil {
			return err
		}
		if remaining != 0 {
			return ErrDingTalkPublicationConflict
		}
		now := time.Now().UTC()
		return dingTalkCAS(tx.Model(&types.StoredResource{}).Where(
			"id = ? AND tenant_id = ? AND state = ?",
			actual.ID,
			intent.TenantID,
			types.ResourceStateDeleting).Updates(map[string]any{
			"state":      types.ResourceStateDeleted,
			"deleted_at": now,
			"updated_at": now,
		}))
	})
}

func loadDingTalkCleanupResource(ctx context.Context,
	tx *gorm.DB,
	intent DingTalkWorkIntent,
	ref string) (*types.StoredResource,
	error,
) {
	f, ok := ctx.Value(dingTalkFetchKey{}).(dingTalkFetch)
	if !ok ||
		intent.Kind != "cleanup" ||
		intent.State == "done" ||
		intent.TenantID != f.Tenant ||
		intent.KnowledgeBaseID != f.KB ||
		intent.DataSourceID != f.Source {
		return nil, ErrDingTalkPublicationConflict
	}
	if err := lockDingTalkPublicationSource(tx, f); err != nil {
		return nil, err
	}
	var stored DingTalkWorkIntent
	if err := tx.Where("id = ? AND tenant_id = ? AND data_source_id = ? AND knowledge_base_id = ?",
		intent.ID,
		f.Tenant,
		f.Source,
		f.KB).Take(&stored).Error; err != nil {
		return nil, err
	}
	if stored != intent {
		return nil, ErrDingTalkPublicationConflict
	}
	var publication dingTalkPublication
	if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND data_source_id = ? AND external_id = ?",
		f.Tenant,
		f.KB,
		f.Source,
		intent.ExternalID).Take(&publication).Error; err != nil {
		return nil, err
	}
	if publication.Generation <= intent.Generation ||
		(!publication.Tombstone &&
			publication.KnowledgeID == intent.KnowledgeID) {
		return nil, ErrDingTalkPublicationConflict
	}
	var k types.Knowledge
	if err := tx.Unscoped().Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?",
		intent.KnowledgeID,
		f.Tenant,
		f.KB).Take(&k).Error; err != nil {
		return nil, err
	}
	if !k.DeletedAt.Valid ||
		k.Channel != types.ConnectorTypeDingTalk ||
		k.GetMetadata()["datasource_id"] != f.Source ||
		k.GetMetadata()["external_id"] != intent.ExternalID {
		return nil, ErrDingTalkPublicationConflict
	}
	matched := ref == k.FilePath
	if !matched {
		var chunks []types.Chunk
		if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
			f.Tenant,
			f.KB,
			k.ID).Find(&chunks).Error; err != nil {
			return nil, err
		}
		for _, chunk := range chunks {
			if chunk.ImageInfo == "" {
				continue
			}
			var images []*types.ImageInfo
			if err := json.Unmarshal([]byte(chunk.ImageInfo), &images); err != nil {
				return nil, err
			}
			for _, image := range images {
				if image != nil && image.URL == ref {
					matched = true
				}
			}
		}
	}
	handle, ok := types.ParseResourcePath(ref)
	if !matched || !ok {
		return nil, ErrDingTalkPublicationConflict
	}
	var resource types.StoredResource
	if err := tx.Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).Where(
		"handle = ? AND tenant_id = ?",
		handle,
		f.Tenant).Take(&resource).Error; err != nil {
		return nil, err
	}
	if resource.SourceProvenance != types.ResourceProvenanceOrdinary || resource.PhysicalPath == "" {
		return nil, ErrDingTalkPublicationConflict
	}
	backend, physical, scoped := types.ParseStorageBackendPath(resource.PhysicalPath)
	if !scoped {
		physical = resource.PhysicalPath
	}
	if (scoped &&
		backend != resource.StorageBackendID) ||
		(!scoped &&
			resource.StorageBackendID != "") ||
		types.ParseProviderScheme(physical) != resource.Provider {
		return nil, ErrDingTalkPublicationConflict
	}
	return &resource, nil
}
