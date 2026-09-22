package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

var (
	ErrInvalidGroupGrant     = errors.New("invalid directory group grant")
	ErrInvalidResourcePolicy = errors.New("invalid resource access policy")
	ErrResourceAccessDenied  = errors.New("resource access denied")
)

type groupAccessService struct {
	repo             interfaces.GroupAccessRepository
	invalidator      interfaces.PermissionInvalidator
	directoryRuntime interfaces.DirectoryRuntimeService
	now              func() time.Time
}

func NewGroupAccessService(repo interfaces.GroupAccessRepository) interfaces.GroupAccessService {
	return NewGroupAccessServiceWithInvalidator(repo, nil)
}

func NewGroupAccessServiceWithInvalidator(repo interfaces.GroupAccessRepository, invalidator interfaces.PermissionInvalidator) interfaces.GroupAccessService {
	return &groupAccessService{repo: repo, invalidator: invalidator, now: time.Now}
}

// ConfigureGroupAccessDirectoryRuntime makes the directory feature flag the
// top-level switch for every group-derived permission. Existing policies stay
// persisted while the module is off, but authorization reverts to the legacy
// workspace/share behaviour until the directory is enabled again.
func ConfigureGroupAccessDirectoryRuntime(groupAccess interfaces.GroupAccessService, runtime interfaces.DirectoryRuntimeService) {
	if impl, ok := groupAccess.(*groupAccessService); ok {
		impl.directoryRuntime = runtime
	}
}

func (s *groupAccessService) directoryModuleDisabled(ctx context.Context) bool {
	if s.directoryRuntime == nil {
		return false
	}
	health, err := s.directoryRuntime.GetStatus(ctx)
	return err == nil && health != nil && !health.Enabled
}

func (s *groupAccessService) EffectiveTenantRole(ctx context.Context, userID string, tenantID uint64, now time.Time) (types.EffectiveTenantRole, error) {
	result := types.EffectiveTenantRole{TenantID: tenantID}
	if strings.TrimSpace(userID) == "" || tenantID == 0 {
		return result, nil
	}
	direct, err := s.repo.GetDirectTenantRole(ctx, userID, tenantID)
	if err != nil {
		return result, err
	}
	result.DirectRole = direct
	if direct != nil && direct.IsValid() {
		result.Member = true
		result.Role = *direct
	}
	if s.directoryModuleDisabled(ctx) {
		return result, nil
	}
	matches, err := s.repo.ListGroupRoleMatches(ctx, userID, tenantID)
	if err != nil {
		return result, err
	}
	for _, match := range matches {
		// Directory groups may never grant owner, even if a corrupt legacy row
		// reached the database before validation existed.
		if !match.Fresh(now) || !match.Role.IsValid() || match.Role == types.TenantRoleOwner {
			continue
		}
		result.GroupMatches = append(result.GroupMatches, match)
		if !result.Member || match.Role.Level() > result.Role.Level() {
			result.Member = true
			result.Role = match.Role
		}
	}
	return result, nil
}

func (s *groupAccessService) ListEffectiveTenantRoles(ctx context.Context, userID string, now time.Time) ([]types.EffectiveTenantRole, error) {
	roles, err := s.repo.ListEffectiveTenantRoles(ctx, strings.TrimSpace(userID), now)
	if err != nil {
		return nil, err
	}
	if s.directoryModuleDisabled(ctx) {
		directOnly := make([]types.EffectiveTenantRole, 0, len(roles))
		for _, role := range roles {
			if role.DirectRole == nil || !role.DirectRole.IsValid() {
				continue
			}
			role.Member = true
			role.Role = *role.DirectRole
			role.GroupMatches = nil
			directOnly = append(directOnly, role)
		}
		roles = directOnly
	}
	// The repository contract is already sorted, but sort again at the public
	// service boundary so alternate implementations cannot make login tenant
	// selection nondeterministic.
	sort.SliceStable(roles, func(i, j int) bool { return roles[i].TenantID < roles[j].TenantID })
	return roles, nil
}

func (s *groupAccessService) UpsertTenantGroupRoleGrant(ctx context.Context, grant *types.TenantGroupRoleGrant) error {
	if grant == nil || grant.TenantID == 0 || strings.TrimSpace(grant.DirectoryGroupID) == "" ||
		!grant.Role.IsValid() || grant.Role == types.TenantRoleOwner {
		return fmt.Errorf("%w: tenant, group and viewer/contributor/admin role are required", ErrInvalidGroupGrant)
	}
	if grant.Origin == "" {
		grant.Origin = types.GrantOriginManual
	}
	if grant.Origin != types.GrantOriginManual && grant.Origin != types.GrantOriginDirectory {
		return fmt.Errorf("%w: invalid origin %q", ErrInvalidGroupGrant, grant.Origin)
	}
	version, err := s.repo.UpsertTenantGroupRoleGrant(ctx, grant)
	if err == nil {
		s.invalidate(ctx, grant.TenantID, version)
	}
	return err
}

func (s *groupAccessService) DeleteTenantGroupRoleGrant(ctx context.Context, tenantID uint64, groupID string, origin types.GrantOrigin) error {
	if tenantID == 0 || strings.TrimSpace(groupID) == "" {
		return ErrInvalidGroupGrant
	}
	version, err := s.repo.DeleteTenantGroupRoleGrant(ctx, tenantID, groupID, origin)
	if err == nil {
		s.invalidate(ctx, tenantID, version)
	}
	return err
}

func (s *groupAccessService) SetResourceAccessPolicy(ctx context.Context, policy *types.ResourceAccessPolicy) error {
	if policy == nil || policy.TenantID == 0 || strings.TrimSpace(policy.ResourceID) == "" ||
		!policy.ResourceType.IsValid() || !policy.Mode.IsValid() {
		return ErrInvalidResourcePolicy
	}
	version, err := s.repo.UpsertResourceAccessPolicy(ctx, policy)
	if err == nil {
		s.invalidate(ctx, policy.TenantID, version)
	}
	return err
}

func (s *groupAccessService) UpsertResourceGroupGrant(ctx context.Context, grant *types.ResourceGroupGrant) error {
	if grant == nil || grant.TenantID == 0 || strings.TrimSpace(grant.ResourceID) == "" ||
		strings.TrimSpace(grant.DirectoryGroupID) == "" || !grant.ResourceType.IsValid() ||
		!grant.Permission.ValidFor(grant.ResourceType) {
		return ErrInvalidGroupGrant
	}
	if grant.Origin == "" {
		grant.Origin = types.GrantOriginManual
	}
	if grant.Origin != types.GrantOriginManual && grant.Origin != types.GrantOriginDirectory {
		return fmt.Errorf("%w: invalid origin %q", ErrInvalidGroupGrant, grant.Origin)
	}
	version, err := s.repo.UpsertResourceGroupGrant(ctx, grant)
	if err == nil {
		s.invalidate(ctx, grant.TenantID, version)
	}
	return err
}

func (s *groupAccessService) DeleteResourceGroupGrant(
	ctx context.Context,
	tenantID uint64,
	resourceType types.ResourceType,
	resourceID, groupID string,
	permission types.ResourcePermission,
	origin types.GrantOrigin,
) error {
	if tenantID == 0 || strings.TrimSpace(resourceID) == "" || strings.TrimSpace(groupID) == "" ||
		!resourceType.IsValid() || !permission.ValidFor(resourceType) {
		return ErrInvalidGroupGrant
	}
	version, err := s.repo.DeleteResourceGroupGrant(ctx, tenantID, resourceType, resourceID, groupID, permission, origin)
	if err == nil {
		s.invalidate(ctx, tenantID, version)
	}
	return err
}

func (s *groupAccessService) invalidate(ctx context.Context, tenantID, version uint64) {
	if s.invalidator != nil && version > 0 {
		// The durable version changed inside the mutation transaction. A failed
		// best-effort notification cannot make stale cache entries authoritative.
		_ = s.invalidator.InvalidateTenantPermissions(ctx, tenantID, version)
	}
}

func (s *groupAccessService) EffectivePermission(
	ctx context.Context,
	tenantID uint64,
	resourceType types.ResourceType,
	resourceID string,
	action types.ResourceAction,
	now time.Time,
) (types.EffectiveResourcePermission, error) {
	if s.directoryModuleDisabled(ctx) {
		return types.EffectiveResourcePermission{
			Allowed: true,
			Mode:    types.ResourceAccessInherit,
			Action:  action,
			Reason:  "directory_module_disabled",
		}, nil
	}
	policy, err := s.repo.GetResourceAccessPolicy(ctx, tenantID, resourceType, resourceID)
	if err != nil {
		return types.EffectiveResourcePermission{}, err
	}
	mode := types.ResourceAccessInherit
	if policy != nil {
		mode = policy.Mode
	}
	return s.effectivePermissionWithMode(ctx, tenantID, resourceType, resourceID, action, mode, now)
}

func (s *groupAccessService) effectivePermissionWithMode(
	ctx context.Context,
	tenantID uint64,
	resourceType types.ResourceType,
	resourceID string,
	action types.ResourceAction,
	mode types.ResourceAccessMode,
	now time.Time,
) (types.EffectiveResourcePermission, error) {
	result := types.EffectiveResourcePermission{Mode: mode, Action: action}
	if tenantID == 0 || strings.TrimSpace(resourceID) == "" || !resourceType.IsValid() || !action.ValidFor(resourceType) || !mode.IsValid() {
		return result, ErrInvalidResourcePolicy
	}
	if mode == types.ResourceAccessInherit {
		// Inherit is an overlay no-op: the existing workspace/share/API-key
		// authorizers remain authoritative, preserving pre-migration behaviour.
		result.Allowed = true
		result.Reason = "inherit_workspace_authorization"
		return result, nil
	}
	if _, apiKey := types.TenantAPIKeyScopeFromContext(ctx); apiKey {
		result.Reason = "machine_principal_denied"
		return result, nil
	}
	principal, ok := types.PrincipalFromContext(ctx)
	if !ok || principal.Type != types.PrincipalWebUser || strings.TrimSpace(principal.ID) == "" || types.IsSyntheticUserID(principal.ID) {
		result.Reason = "verifiable_user_required"
		return result, nil
	}
	caller := types.CallerFromContext(ctx)
	if caller.UserID == "" {
		caller.UserID = principal.ID
	}
	if caller.UserID != principal.ID || caller.TenantID != tenantID {
		result.Reason = "workspace_membership_required"
		return result, nil
	}
	effectiveRole, err := s.EffectiveTenantRole(ctx, caller.UserID, tenantID, now)
	if err != nil {
		return result, err
	}
	result.EffectiveRole = effectiveRole
	if !effectiveRole.Member {
		result.Reason = "workspace_membership_required"
		return result, nil
	}
	if effectiveRole.Role == types.TenantRoleOwner || effectiveRole.Role == types.TenantRoleAdmin {
		result.Allowed = true
		result.Reason = "workspace_manager"
		return result, nil
	}
	if action == types.ResourceActionManage {
		result.Reason = "management_requires_owner_or_admin"
		return result, nil
	}
	matches, err := s.repo.ListResourceGroupMatches(ctx, caller.UserID, tenantID, resourceType, resourceID)
	if err != nil {
		return result, err
	}
	for _, match := range matches {
		if !match.Fresh(now) {
			continue
		}
		result.GroupMatches = append(result.GroupMatches, match)
		if permissionSatisfies(resourceType, match.Permission, action) {
			result.Allowed = true
		}
	}
	if result.Allowed {
		result.Reason = "directory_group_grant"
	} else {
		result.Reason = "matching_group_grant_required"
	}
	return result, nil
}

func permissionSatisfies(resourceType types.ResourceType, permission types.ResourcePermission, action types.ResourceAction) bool {
	if permission == types.ResourcePermissionEdit {
		return action == types.ResourceActionEdit ||
			(resourceType == types.GroupResourceTypeKnowledgeBase && action == types.ResourceActionRead) ||
			(resourceType == types.GroupResourceTypeAgent && action == types.ResourceActionUse)
	}
	return (resourceType == types.GroupResourceTypeKnowledgeBase && permission == types.ResourcePermissionRead && action == types.ResourceActionRead) ||
		(resourceType == types.GroupResourceTypeAgent && permission == types.ResourcePermissionUse && action == types.ResourceActionUse)
}

func (s *groupAccessService) Authorize(ctx context.Context, tenantID uint64, resourceType types.ResourceType, resourceID string, action types.ResourceAction) error {
	permission, err := s.EffectivePermission(ctx, tenantID, resourceType, resourceID, action, s.now().UTC())
	if err != nil {
		return err
	}
	if !permission.Allowed {
		return fmt.Errorf("%w: %s", ErrResourceAccessDenied, permission.Reason)
	}
	return nil
}

func (s *groupAccessService) PreviewModeChange(
	ctx context.Context,
	tenantID uint64,
	resourceType types.ResourceType,
	resourceID string,
	proposed types.ResourceAccessMode,
	now time.Time,
) (*types.ResourceAccessImpactPreview, error) {
	if !proposed.IsValid() || !resourceType.IsValid() || tenantID == 0 || strings.TrimSpace(resourceID) == "" {
		return nil, ErrInvalidResourcePolicy
	}
	current := types.ResourceAccessInherit
	policy, err := s.repo.GetResourceAccessPolicy(ctx, tenantID, resourceType, resourceID)
	if err != nil {
		return nil, err
	}
	if policy != nil {
		current = policy.Mode
	}
	missing, err := s.repo.ListMissingTenantGroupLinks(ctx, tenantID, resourceType, resourceID)
	if err != nil {
		return nil, err
	}
	ids, err := s.repo.ListTenantUserIDs(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	preview := &types.ResourceAccessImpactPreview{
		TenantID: tenantID, ResourceType: resourceType, ResourceID: resourceID,
		CurrentMode: current, ProposedMode: proposed, MissingTenantGroupIDs: missing,
	}
	action := types.ResourceActionRead
	if resourceType == types.GroupResourceTypeAgent {
		action = types.ResourceActionUse
	}
	for _, userID := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		role, err := s.EffectiveTenantRole(ctx, userID, tenantID, now)
		if err != nil {
			return nil, err
		}
		if !role.Member {
			continue
		}
		preview.WorkspaceUserCount++
		// Do not inherit the administrator/API-key principal that requested the
		// preview; evaluate each affected human as that human.
		userCtx := types.WithCaller(context.Background(), types.Caller{TenantID: tenantID, UserID: userID, Role: role.Role})
		userCtx = types.WithPrincipal(userCtx, types.Principal{Type: types.PrincipalWebUser, ID: userID})
		currentPermission, err := s.effectivePermissionWithMode(userCtx, tenantID, resourceType, resourceID, action, current, now)
		if err != nil {
			return nil, err
		}
		proposedPermission, err := s.effectivePermissionWithMode(userCtx, tenantID, resourceType, resourceID, action, proposed, now)
		if err != nil {
			return nil, err
		}
		if proposedPermission.Allowed {
			preview.AllowedUserCount++
		}
		if currentPermission.Allowed != proposedPermission.Allowed {
			preview.AffectedUserCount++
			if len(preview.AffectedUserIDs) < 100 {
				preview.AffectedUserIDs = append(preview.AffectedUserIDs, userID)
			}
		}
	}
	sort.Strings(preview.AffectedUserIDs)
	return preview, nil
}
