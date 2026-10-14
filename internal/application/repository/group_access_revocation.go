package repository

import (
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// A capability issued before a directory/role change must not become live
// again if an administrator later returns its KB to inherit mode. Revoke
// anonymous resource capabilities in the same transaction as the durable
// permission version. Tenant-wide restricted scope is deliberately conservative
// because anonymous grants carry no user whose lost membership we can match.
func revokeRestrictedResourceCapabilities(tx *gorm.DB, tenantID uint64, now time.Time) error {
	// Narrow migration tests and upgrades may have no resource registry yet.
	// No capability can exist until these tables have been installed.
	for _, model := range []any{&types.ResourceAccessGrant{}, &types.ResourceBinding{}, &types.Knowledge{}} {
		if !tx.Migrator().HasTable(model) {
			return nil
		}
	}
	resourceIDs := tx.Table("resource_bindings AS b").
		Select("DISTINCT b.resource_id").
		Joins("JOIN knowledges AS k ON k.id = b.owner_id AND k.tenant_id = b.tenant_id").
		Joins("JOIN resource_access_policies AS p ON p.tenant_id = k.tenant_id "+
			"AND p.resource_id = k.knowledge_base_id").
		Where("b.owner_type = ? AND b.tenant_id = ? AND p.resource_type = ? AND p.mode = ?",
			types.ResourceOwnerKnowledge, tenantID,
			types.GroupResourceTypeKnowledgeBase, types.ResourceAccessRestricted)
	return tx.Model(&types.ResourceAccessGrant{}).
		Where("revoked_at IS NULL AND expires_at > ? AND resource_id IN (?)", now, resourceIDs).
		Update("revoked_at", now).Error
}
