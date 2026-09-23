package interfaces

import (
	"context"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
)

// GroupAccessRepository persists workspace and resource group grants.
type GroupAccessRepository interface {
	GetDirectTenantRole(
		ctx context.Context,
		userID string,
		tenantID uint64,
	) (*types.TenantRole, error)
	ListGroupRoleMatches(
		ctx context.Context,
		userID string,
		tenantID uint64,
	) ([]types.GroupRoleMatch, error)
	ListEffectiveTenantRoles(
		ctx context.Context,
		userID string,
		now time.Time,
	) ([]types.EffectiveTenantRole, error)
	ListResourceGroupMatches(
		ctx context.Context,
		userID string,
		tenantID uint64,
		resourceType types.ResourceType,
		resourceID string,
	) ([]types.ResourceGroupMatch, error)
	ListTenantUserIDs(ctx context.Context, tenantID uint64) ([]string, error)

	UpsertTenantGroupRoleGrant(
		ctx context.Context,
		grant *types.TenantGroupRoleGrant,
	) (uint64, error)
	DeleteTenantGroupRoleGrant(
		ctx context.Context,
		tenantID uint64,
		directoryGroupID string,
		origin types.GrantOrigin,
	) (uint64, error)
	ListTenantGroupRoleGrants(
		ctx context.Context,
		tenantID uint64,
	) ([]*types.TenantGroupRoleGrant, error)

	GetResourceAccessPolicy(
		ctx context.Context,
		tenantID uint64,
		resourceType types.ResourceType,
		resourceID string,
	) (*types.ResourceAccessPolicy, error)
	UpsertResourceAccessPolicy(
		ctx context.Context,
		policy *types.ResourceAccessPolicy,
	) (uint64, error)
	UpsertResourceGroupGrant(ctx context.Context, grant *types.ResourceGroupGrant) (uint64, error)
	DeleteResourceGroupGrant(
		ctx context.Context,
		tenantID uint64,
		resourceType types.ResourceType,
		resourceID, directoryGroupID string,
		permission types.ResourcePermission,
		origin types.GrantOrigin,
	) (uint64, error)
	ListResourceGroupGrants(
		ctx context.Context,
		tenantID uint64,
		resourceType types.ResourceType,
		resourceID string,
	) ([]*types.ResourceGroupGrant, error)
	ListMissingTenantGroupLinks(
		ctx context.Context,
		tenantID uint64,
		resourceType types.ResourceType,
		resourceID string,
	) ([]string, error)

	GetPermissionVersion(ctx context.Context, tenantID uint64) (uint64, error)
}

// PermissionInvalidator is optional. A future local/Redis cache can implement
// it; the database version remains authoritative when the hook is absent or a
// notification is lost.
type PermissionInvalidator interface {
	InvalidateTenantPermissions(ctx context.Context, tenantID, version uint64) error
}

// GroupAccessService resolves effective workspace and resource permissions.
type GroupAccessService interface {
	EffectiveTenantRole(
		ctx context.Context,
		userID string,
		tenantID uint64,
		now time.Time,
	) (types.EffectiveTenantRole, error)
	ListEffectiveTenantRoles(
		ctx context.Context,
		userID string,
		now time.Time,
	) ([]types.EffectiveTenantRole, error)
	UpsertTenantGroupRoleGrant(ctx context.Context, grant *types.TenantGroupRoleGrant) error
	DeleteTenantGroupRoleGrant(
		ctx context.Context,
		tenantID uint64,
		groupID string,
		origin types.GrantOrigin,
	) error
	SetResourceAccessPolicy(ctx context.Context, policy *types.ResourceAccessPolicy) error
	UpsertResourceGroupGrant(ctx context.Context, grant *types.ResourceGroupGrant) error
	DeleteResourceGroupGrant(
		ctx context.Context,
		tenantID uint64,
		resourceType types.ResourceType,
		resourceID, groupID string,
		permission types.ResourcePermission,
		origin types.GrantOrigin,
	) error
	EffectivePermission(
		ctx context.Context,
		tenantID uint64,
		resourceType types.ResourceType,
		resourceID string,
		action types.ResourceAction,
		now time.Time,
	) (types.EffectiveResourcePermission, error)
	Authorize(
		ctx context.Context,
		tenantID uint64,
		resourceType types.ResourceType,
		resourceID string,
		action types.ResourceAction,
	) error
	PreviewModeChange(
		ctx context.Context,
		tenantID uint64,
		resourceType types.ResourceType,
		resourceID string,
		proposed types.ResourceAccessMode,
		now time.Time,
	) (*types.ResourceAccessImpactPreview, error)
}
