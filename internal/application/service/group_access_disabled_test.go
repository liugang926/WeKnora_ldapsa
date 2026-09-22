package service

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type disabledDirectoryRuntime struct {
	interfaces.DirectoryRuntimeService
}

func (disabledDirectoryRuntime) GetStatus(context.Context) (*types.DirectoryHealth, error) {
	return &types.DirectoryHealth{Enabled: false}, nil
}

type disabledDirectoryGroupRepo struct {
	interfaces.GroupAccessRepository
}

func (disabledDirectoryGroupRepo) GetDirectTenantRole(context.Context, string, uint64) (*types.TenantRole, error) {
	role := types.TenantRoleViewer
	return &role, nil
}

func (disabledDirectoryGroupRepo) ListGroupRoleMatches(context.Context, string, uint64) ([]types.GroupRoleMatch, error) {
	panic("group memberships must not be read while the directory module is disabled")
}

func (disabledDirectoryGroupRepo) GetResourceAccessPolicy(context.Context, uint64, types.ResourceType, string) (*types.ResourceAccessPolicy, error) {
	panic("resource policies must not be read while the directory module is disabled")
}

func TestGroupAccessDirectoryDisabledRestoresLegacyAuthorization(t *testing.T) {
	access := NewGroupAccessService(disabledDirectoryGroupRepo{})
	ConfigureGroupAccessDirectoryRuntime(access, disabledDirectoryRuntime{})

	permission, err := access.EffectivePermission(
		context.Background(), 7, types.GroupResourceTypeKnowledgeBase, "kb-1", types.ResourceActionRead, time.Now(),
	)
	require.NoError(t, err)
	require.True(t, permission.Allowed)
	require.Equal(t, types.ResourceAccessInherit, permission.Mode)
	require.Equal(t, "directory_module_disabled", permission.Reason)

	role, err := access.EffectiveTenantRole(context.Background(), "user-1", 7, time.Now())
	require.NoError(t, err)
	require.True(t, role.Member)
	require.Equal(t, types.TenantRoleViewer, role.Role)
	require.Empty(t, role.GroupMatches)
}
