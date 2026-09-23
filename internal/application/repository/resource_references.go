package repository

import (
	"context"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
)

// ListKnowledgeBaseIDsByResource returns authoritative live KB owners for a
// registered resource. A textual mention of a path/handle never counts.
func (r *resourceRepository) ListKnowledgeBaseIDsByResource(
	ctx context.Context, tenantID uint64, resourceID string,
) ([]string, error) {
	if tenantID == 0 || resourceID == "" {
		return nil, nil
	}
	var ids []string
	err := r.db.WithContext(ctx).Table("resource_bindings AS b").
		Joins("JOIN knowledges AS k ON k.id = b.owner_id AND k.tenant_id = b.tenant_id").
		Joins("JOIN knowledge_bases AS kb ON kb.id = k.knowledge_base_id "+
			"AND kb.tenant_id = k.tenant_id AND kb.deleted_at IS NULL").
		Where("b.resource_id = ? AND b.owner_type = ? AND b.tenant_id = ? AND k.deleted_at IS NULL",
			resourceID, types.ResourceOwnerKnowledge, tenantID).
		Distinct("k.knowledge_base_id").
		Order("k.knowledge_base_id ASC").
		Pluck("k.knowledge_base_id", &ids).Error
	return ids, err
}

// RevokeValidGrantsByKnowledgeBase revokes capabilities for every registered
// resource explicitly bound to a document in the KB. Soft-deleted documents
// remain in scope: a deletion or restriction must not leave its old public
// capability usable until expiry.
func (r *resourceRepository) RevokeValidGrantsByKnowledgeBase(
	ctx context.Context, tenantID uint64, kbID string, now time.Time,
) (int64, error) {
	if tenantID == 0 || kbID == "" {
		return 0, nil
	}
	resourceIDs := r.db.WithContext(ctx).Table("resource_bindings AS b").
		Select("DISTINCT b.resource_id").
		Joins("JOIN knowledges AS k ON k.id = b.owner_id AND k.tenant_id = b.tenant_id").
		Where("b.owner_type = ? AND b.tenant_id = ? AND k.knowledge_base_id = ?",
			types.ResourceOwnerKnowledge, tenantID, kbID)
	result := r.db.WithContext(ctx).Model(&types.ResourceAccessGrant{}).
		Where("revoked_at IS NULL AND expires_at > ? AND resource_id IN (?)", now, resourceIDs).
		Update("revoked_at", now)
	return result.RowsAffected, result.Error
}

// IsReferencedByKnowledgeBase accepts only explicit bindings to live documents.
// Text mentioning a handle or physical path is never ownership evidence.
func (r *resourceRepository) IsReferencedByKnowledgeBase(
	ctx context.Context, tenantID uint64, kbID, resourceID string,
) (bool, error) {
	if tenantID == 0 || kbID == "" || resourceID == "" {
		return false, nil
	}
	var count int64
	err := r.db.WithContext(ctx).Table("resource_bindings AS b").
		Joins("JOIN knowledges AS k ON k.id = b.owner_id AND k.tenant_id = b.tenant_id").
		Joins("JOIN knowledge_bases AS kb ON kb.id = k.knowledge_base_id "+
			"AND kb.tenant_id = k.tenant_id AND kb.deleted_at IS NULL").
		Where("b.resource_id = ? AND b.owner_type = ? AND b.tenant_id = ? "+
			"AND k.knowledge_base_id = ? AND k.deleted_at IS NULL",
			resourceID, types.ResourceOwnerKnowledge, tenantID, kbID).Count(&count).Error
	return count > 0, err
}

func (r *resourceRepository) GetMessageFileBindings(
	ctx context.Context, tenantID uint64, resourceID, messageID string,
) (*types.MessageFileBindings, error) {
	result := &types.MessageFileBindings{}
	if tenantID == 0 || resourceID == "" {
		return result, nil
	}
	err := r.db.WithContext(ctx).Table("resource_bindings AS b").
		Joins("JOIN knowledges AS k ON k.id = b.owner_id AND k.tenant_id = b.tenant_id").
		Joins("JOIN knowledge_bases AS kb ON kb.id = k.knowledge_base_id "+
			"AND kb.tenant_id = k.tenant_id AND kb.deleted_at IS NULL").
		Where("b.resource_id = ? AND b.owner_type = ? AND b.tenant_id = ? AND k.deleted_at IS NULL",
			resourceID, types.ResourceOwnerKnowledge, tenantID).
		Distinct("k.knowledge_base_id").Pluck("k.knowledge_base_id", &result.KnowledgeBaseIDs).Error
	if err != nil {
		return nil, err
	}
	if messageID != "" {
		var count int64
		err = r.db.WithContext(ctx).Model(&types.ResourceBinding{}).
			Where("resource_id = ? AND tenant_id = ? AND owner_type = ? AND owner_id = ? AND relation = ?",
				resourceID, tenantID, types.ResourceOwnerMessage, messageID, types.ResourceRelationArtifact).
			Count(&count).Error
		if err != nil {
			return nil, err
		}
		result.MessageArtifact = count > 0
	}
	return result, nil
}
