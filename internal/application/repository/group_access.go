package repository

import (
	"context"
	"errors"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type groupAccessRepository struct {
	db *gorm.DB
}

// NewGroupAccessRepository persists group grants and resource policies.
func NewGroupAccessRepository(db *gorm.DB) interfaces.GroupAccessRepository {
	return &groupAccessRepository{db: db}
}

func (r *groupAccessRepository) GetDirectTenantRole(
	ctx context.Context,
	userID string,
	tenantID uint64,
) (*types.TenantRole, error) {
	var member types.TenantMember
	err := r.db.WithContext(ctx).
		Where("user_id = ? AND tenant_id = ? AND status = ?", userID, tenantID, types.TenantMemberStatusActive).
		First(&member).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	role := member.Role
	return &role, nil
}

func (r *groupAccessRepository) ListGroupRoleMatches(
	ctx context.Context,
	userID string,
	tenantID uint64,
) ([]types.GroupRoleMatch, error) {
	var rows []types.GroupRoleMatch
	err := r.db.WithContext(ctx).Raw(`
		SELECT d.id AS directory_id,
		       g.id AS directory_group_id,
		       g.display_name AS group_display_name,
		       tgr.role AS role,
		       gm.source AS membership_source,
		       gm.depth AS membership_depth,
		       d.enabled AS directory_enabled,
		       d.last_successful_sync_at AS last_successful_sync_at,
		       d.stale_after_seconds AS stale_after_seconds
		  FROM directory_identities i
		  JOIN directory_group_memberships gm ON gm.identity_id = i.id
		  JOIN directory_groups g ON g.id = gm.group_id AND g.status = ?
		  JOIN directories d ON d.id = i.directory_id
		  JOIN tenant_group_role_grants tgr ON tgr.directory_group_id = g.id
		 WHERE i.user_id = ? AND i.status = ? AND tgr.tenant_id = ?
		 ORDER BY tgr.role DESC, g.display_name ASC, g.id ASC
	`, types.DirectoryObjectActive, userID, types.DirectoryObjectActive, tenantID).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// ListEffectiveTenantRoles returns direct and group-only memberships. Tenant
// IDs are selected first and sorted in SQL, then each role is merged with the
// same freshness/maximum-role rules used by request authorization.
func (r *groupAccessRepository) ListEffectiveTenantRoles(
	ctx context.Context,
	userID string,
	now time.Time,
) ([]types.EffectiveTenantRole, error) {
	var candidates []struct{ TenantID uint64 }
	err := r.db.WithContext(ctx).Raw(`
		SELECT DISTINCT tenant_id
		  FROM tenant_members
		 WHERE user_id = ? AND status = ? AND deleted_at IS NULL
		UNION
		SELECT DISTINCT tgr.tenant_id
		  FROM directory_identities i
		  JOIN directory_group_memberships gm ON gm.identity_id = i.id
		  JOIN directory_groups g ON g.id = gm.group_id AND g.status = ?
		  JOIN tenant_group_role_grants tgr ON tgr.directory_group_id = g.id
		 WHERE i.user_id = ? AND i.status = ?
		ORDER BY tenant_id ASC
	`, userID, types.TenantMemberStatusActive, types.DirectoryObjectActive, userID, types.DirectoryObjectActive).
		Scan(&candidates).Error
	if err != nil {
		return nil, err
	}
	result := make([]types.EffectiveTenantRole, 0, len(candidates))
	for _, candidate := range candidates {
		direct, err := r.GetDirectTenantRole(ctx, userID, candidate.TenantID)
		if err != nil {
			return nil, err
		}
		matches, err := r.ListGroupRoleMatches(ctx, userID, candidate.TenantID)
		if err != nil {
			return nil, err
		}
		effective := types.EffectiveTenantRole{TenantID: candidate.TenantID, DirectRole: direct}
		if direct != nil && direct.IsValid() {
			effective.Member = true
			effective.Role = *direct
		}
		for _, match := range matches {
			if !match.Fresh(now) || !match.Role.IsValid() || match.Role == types.TenantRoleOwner {
				continue
			}
			effective.GroupMatches = append(effective.GroupMatches, match)
			if !effective.Member || match.Role.Level() > effective.Role.Level() {
				effective.Member = true
				effective.Role = match.Role
			}
		}
		if effective.Member {
			result = append(result, effective)
		}
	}
	return result, nil
}

func (r *groupAccessRepository) ListResourceGroupMatches(
	ctx context.Context,
	userID string,
	tenantID uint64,
	resourceType types.ResourceType,
	resourceID string,
) ([]types.ResourceGroupMatch, error) {
	var rows []types.ResourceGroupMatch
	err := r.db.WithContext(ctx).Raw(`
		SELECT d.id AS directory_id,
		       g.id AS directory_group_id,
		       g.display_name AS group_display_name,
		       rg.permission AS permission,
		       gm.source AS membership_source,
		       gm.depth AS membership_depth,
		       d.enabled AS directory_enabled,
		       d.last_successful_sync_at AS last_successful_sync_at,
		       d.stale_after_seconds AS stale_after_seconds
		  FROM directory_identities i
		  JOIN directory_group_memberships gm ON gm.identity_id = i.id
		  JOIN directory_groups g ON g.id = gm.group_id AND g.status = ?
		  JOIN directories d ON d.id = i.directory_id
		  JOIN resource_group_grants rg ON rg.directory_group_id = g.id
		 WHERE i.user_id = ? AND i.status = ?
		   AND rg.tenant_id = ? AND rg.resource_type = ? AND rg.resource_id = ?
		 ORDER BY g.display_name ASC, g.id ASC, rg.permission ASC
	`, types.DirectoryObjectActive, userID, types.DirectoryObjectActive, tenantID, resourceType, resourceID).
		Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *groupAccessRepository) ListTenantUserIDs(
	ctx context.Context,
	tenantID uint64,
) ([]string, error) {
	var rows []struct{ UserID string }
	err := r.db.WithContext(ctx).Raw(`
		SELECT DISTINCT user_id
		  FROM tenant_members
		 WHERE tenant_id = ? AND status = ? AND deleted_at IS NULL
		UNION
		SELECT DISTINCT i.user_id
		  FROM directory_identities i
		  JOIN directory_group_memberships gm ON gm.identity_id = i.id
		  JOIN directory_groups g ON g.id = gm.group_id AND g.status = ?
		  JOIN tenant_group_role_grants tgr ON tgr.directory_group_id = g.id
		 WHERE tgr.tenant_id = ? AND i.status = ? AND i.user_id IS NOT NULL
	`, tenantID, types.TenantMemberStatusActive, types.DirectoryObjectActive, tenantID, types.DirectoryObjectActive).
		Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		if row.UserID != "" {
			ids = append(ids, row.UserID)
		}
	}
	return ids, nil
}

func (r *groupAccessRepository) UpsertTenantGroupRoleGrant(
	ctx context.Context,
	grant *types.TenantGroupRoleGrant,
) (uint64, error) {
	var version uint64
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if grant.ID == "" {
			grant.ID = uuid.NewString()
		}
		if grant.Origin == "" {
			grant.Origin = types.GrantOriginManual
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tenant_id"}, {Name: "directory_group_id"}, {Name: "origin"}},
			DoUpdates: clause.AssignmentColumns([]string{"role", "created_by", "updated_at"}),
		}).Create(grant).Error; err != nil {
			return err
		}
		var err error
		version, err = bumpPermissionVersion(tx, grant.TenantID)
		return err
	})
	return version, err
}

func (r *groupAccessRepository) DeleteTenantGroupRoleGrant(
	ctx context.Context,
	tenantID uint64,
	directoryGroupID string,
	origin types.GrantOrigin,
) (uint64, error) {
	return r.deleteAndBump(ctx, tenantID, func(tx *gorm.DB) (*gorm.DB, error) {
		res := tx.Where("tenant_id = ? AND directory_group_id = ? AND origin = ?", tenantID, directoryGroupID, origin).
			Delete(&types.TenantGroupRoleGrant{})
		return res, res.Error
	})
}

func (r *groupAccessRepository) ListTenantGroupRoleGrants(
	ctx context.Context,
	tenantID uint64,
) ([]*types.TenantGroupRoleGrant, error) {
	var rows []*types.TenantGroupRoleGrant
	if err := r.db.WithContext(ctx).Where("tenant_id = ?", tenantID).
		Order("directory_group_id ASC, origin ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *groupAccessRepository) GetResourceAccessPolicy(
	ctx context.Context,
	tenantID uint64,
	resourceType types.ResourceType,
	resourceID string,
) (*types.ResourceAccessPolicy, error) {
	var policy types.ResourceAccessPolicy
	err := r.db.WithContext(ctx).
		Where("tenant_id = ? AND resource_type = ? AND resource_id = ?", tenantID, resourceType, resourceID).
		First(&policy).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &policy, nil
}

func (r *groupAccessRepository) UpsertResourceAccessPolicy(
	ctx context.Context,
	policy *types.ResourceAccessPolicy,
) (uint64, error) {
	var version uint64
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if policy.ID == "" {
			policy.ID = uuid.NewString()
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tenant_id"}, {Name: "resource_type"}, {Name: "resource_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"mode", "updated_by", "updated_at"}),
		}).Create(policy).Error; err != nil {
			return err
		}
		var err error
		version, err = bumpPermissionVersion(tx, policy.TenantID)
		return err
	})
	return version, err
}

func (r *groupAccessRepository) UpsertResourceGroupGrant(
	ctx context.Context,
	grant *types.ResourceGroupGrant,
) (uint64, error) {
	var version uint64
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if grant.ID == "" {
			grant.ID = uuid.NewString()
		}
		if grant.Origin == "" {
			grant.Origin = types.GrantOriginManual
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "tenant_id"},
				{Name: "resource_type"},
				{Name: "resource_id"},
				{Name: "directory_group_id"},
				{Name: "permission"},
				{Name: "origin"},
			},
			DoUpdates: clause.AssignmentColumns([]string{"created_by", "updated_at"}),
		}).Create(grant).Error; err != nil {
			return err
		}
		var err error
		version, err = bumpPermissionVersion(tx, grant.TenantID)
		return err
	})
	return version, err
}

func (r *groupAccessRepository) DeleteResourceGroupGrant(
	ctx context.Context,
	tenantID uint64,
	resourceType types.ResourceType,
	resourceID, directoryGroupID string,
	permission types.ResourcePermission,
	origin types.GrantOrigin,
) (uint64, error) {
	return r.deleteAndBump(ctx, tenantID, func(tx *gorm.DB) (*gorm.DB, error) {
		res := tx.Where(
			"tenant_id = ? AND resource_type = ? AND resource_id = ?"+
				" AND directory_group_id = ? AND permission = ? AND origin = ?",
			tenantID,
			resourceType,
			resourceID,
			directoryGroupID,
			permission,
			origin,
		).Delete(&types.ResourceGroupGrant{})
		return res, res.Error
	})
}

func (r *groupAccessRepository) deleteAndBump(
	ctx context.Context,
	tenantID uint64,
	deleteFn func(*gorm.DB) (*gorm.DB, error),
) (uint64, error) {
	var version uint64
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res, err := deleteFn(tx)
		if err != nil {
			return err
		}
		if res.RowsAffected == 0 {
			current, err := permissionVersion(tx, tenantID)
			version = current
			return err
		}
		version, err = bumpPermissionVersion(tx, tenantID)
		return err
	})
	return version, err
}

func (r *groupAccessRepository) ListResourceGroupGrants(
	ctx context.Context,
	tenantID uint64,
	resourceType types.ResourceType,
	resourceID string,
) ([]*types.ResourceGroupGrant, error) {
	var rows []*types.ResourceGroupGrant
	if err := r.db.WithContext(ctx).
		Where("tenant_id = ? AND resource_type = ? AND resource_id = ?", tenantID, resourceType, resourceID).
		Order("directory_group_id ASC, permission ASC, origin ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *groupAccessRepository) ListMissingTenantGroupLinks(
	ctx context.Context,
	tenantID uint64,
	resourceType types.ResourceType,
	resourceID string,
) ([]string, error) {
	var rows []struct{ DirectoryGroupID string }
	err := r.db.WithContext(ctx).Raw(`
		SELECT DISTINCT rg.directory_group_id
		  FROM resource_group_grants rg
		 WHERE rg.tenant_id = ? AND rg.resource_type = ? AND rg.resource_id = ?
		   AND NOT EXISTS (
		       SELECT 1 FROM tenant_group_role_grants tgr
		        WHERE tgr.tenant_id = rg.tenant_id
		          AND tgr.directory_group_id = rg.directory_group_id
		   )
		 ORDER BY rg.directory_group_id ASC
	`, tenantID, resourceType, resourceID).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.DirectoryGroupID)
	}
	return ids, nil
}

func permissionVersion(tx *gorm.DB, tenantID uint64) (uint64, error) {
	var row types.PermissionVersion
	err := tx.Where("tenant_id = ?", tenantID).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return row.Version, nil
}

func (r *groupAccessRepository) GetPermissionVersion(
	ctx context.Context,
	tenantID uint64,
) (uint64, error) {
	return permissionVersion(r.db.WithContext(ctx), tenantID)
}

// Compile-time guard: mutations intentionally use the shared transactional
// bump helper declared in directory.go. Keep this reference so accidental
// removal is caught even when only this file is selected by a narrow build.
var _ = time.Time{}
