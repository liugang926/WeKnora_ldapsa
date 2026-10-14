package service

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type backgroundRoleRepo struct {
	interfaces.GroupAccessRepository
	mode       types.ResourceAccessMode
	direct     *types.TenantRole
	matches    []types.GroupRoleMatch
	roleChecks int
}

func (r *backgroundRoleRepo) GetResourceAccessPolicy(
	context.Context, uint64, types.ResourceType, string,
) (*types.ResourceAccessPolicy, error) {
	return &types.ResourceAccessPolicy{Mode: r.mode}, nil
}

func (r *backgroundRoleRepo) GetDirectTenantRole(context.Context, string, uint64) (*types.TenantRole, error) {
	r.roleChecks++
	return r.direct, nil
}

func (r *backgroundRoleRepo) ListGroupRoleMatches(context.Context, string, uint64) ([]types.GroupRoleMatch, error) {
	return r.matches, nil
}

func TestInheritedBackgroundAccessRechecksGroupRoleDowngrades(t *testing.T) {
	now := time.Now().UTC()
	repo := &backgroundRoleRepo{
		mode: types.ResourceAccessInherit,
		matches: []types.GroupRoleMatch{{
			Role: types.TenantRoleAdmin, DirectoryEnabled: true, LastSuccessfulSyncAt: &now,
		}},
	}
	access := NewGroupAccessService(repo)
	ctx := types.WithTaskAuthorization(context.Background(), 7, types.TaskInitiator{
		UserID: "user-1", Role: types.TenantRoleAdmin, CallerTenantID: 7,
	})
	check := func(action types.ResourceAction, allowed bool, reason string) {
		t.Helper()
		permission, err := access.EffectivePermission(ctx, 7,
			types.GroupResourceTypeKnowledgeBase, "kb", action, now)
		require.NoError(t, err)
		require.Equal(t, allowed, permission.Allowed)
		if reason != "" {
			require.Equal(t, reason, permission.Reason)
		}
	}
	check(types.ResourceActionManage, true, "")
	check(types.ResourceActionEdit, true, "")
	repo.matches[0].Role = types.TenantRoleContributor
	check(types.ResourceActionEdit, true, "")
	check(types.ResourceActionManage, false, "workspace_role_required")
	repo.matches[0].Role = types.TenantRoleViewer
	check(types.ResourceActionRead, true, "")
	check(types.ResourceActionEdit, false, "workspace_role_required")
	repo.matches = nil
	check(types.ResourceActionEdit, false, "workspace_membership_required")
	check(types.ResourceActionRead, false, "workspace_membership_required")
}

func TestInheritedBackgroundRoleChecksPreserveDisabledAndSharedBehavior(t *testing.T) {
	now := time.Now().UTC()
	repo := &backgroundRoleRepo{mode: types.ResourceAccessInherit}
	access := NewGroupAccessService(repo)
	sharedCtx := types.WithTaskAuthorization(context.Background(), 8, types.TaskInitiator{
		UserID: "user-1", Role: types.TenantRoleContributor, CallerTenantID: 7,
	})
	permission, err := access.EffectivePermission(sharedCtx, 8,
		types.GroupResourceTypeKnowledgeBase, "shared-kb", types.ResourceActionEdit, now)
	require.NoError(t, err)
	require.True(t, permission.Allowed)
	require.Zero(t, repo.roleChecks)
	// A restricted source never gains the legacy share exemption.
	repo.mode = types.ResourceAccessRestricted
	permission, err = access.EffectivePermission(sharedCtx, 8,
		types.GroupResourceTypeKnowledgeBase, "shared-kb", types.ResourceActionEdit, now)
	require.NoError(t, err)
	require.False(t, permission.Allowed)
	require.Equal(t, "workspace_membership_required", permission.Reason)
	// Legacy human payloads lacking caller workspace use the execution scope
	// for live membership checks rather than fabricating a shared admission.
	repo.mode = types.ResourceAccessInherit
	legacyCtx := types.WithTaskAuthorization(context.Background(), 8, types.TaskInitiator{
		UserID: "user-1", Role: types.TenantRoleAdmin,
	})
	permission, err = access.EffectivePermission(legacyCtx, 8,
		types.GroupResourceTypeKnowledgeBase, "shared-kb", types.ResourceActionEdit, now)
	require.NoError(t, err)
	require.False(t, permission.Allowed)
	require.Equal(t, "workspace_membership_required", permission.Reason)
	ConfigureGroupAccessDirectoryRuntime(access, disabledDirectoryRuntime{})
	permission, err = access.EffectivePermission(legacyCtx, 8,
		types.GroupResourceTypeKnowledgeBase, "shared-kb", types.ResourceActionEdit, now)
	require.NoError(t, err)
	require.True(t, permission.Allowed)
	require.Equal(t, "directory_module_disabled", permission.Reason)
}
