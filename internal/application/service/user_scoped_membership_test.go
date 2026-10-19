package service

import (
	"context"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type sourceOnlyMembershipRoles struct {
	interfaces.GroupAccessService
	roles []types.EffectiveTenantRole
}

func (s sourceOnlyMembershipRoles) ListEffectiveTenantRoles(context.Context, string, time.Time) ([]types.EffectiveTenantRole, error) {
	return s.roles, nil
}

func TestBuildLoginMembershipsSourceOnlyScopeAndManualMembership(t *testing.T) {
	active := &types.Tenant{ID: 7, Name: "Existing workspace"}
	role := types.EffectiveTenantRole{TenantID: 7, Member: true, ScopedOnly: true, Role: types.TenantRoleViewer}
	user := &types.User{ID: "source-reader", TenantID: 0}
	direct := &provisioningMemberService{}
	service := &userService{memberService: direct, groupAccess: sourceOnlyMembershipRoles{roles: []types.EffectiveTenantRole{role}}}
	result := service.BuildLoginMemberships(context.Background(), user, active)
	require.Len(t, result, 1)
	require.True(t, result[0].ScopedOnly)
	require.Equal(t, types.TenantRoleViewer, result[0].Role)
	require.Zero(t, user.TenantID, "membership projection must not assign a tenant or create a membership")
	require.Empty(t, direct.members)
	direct.members = []*types.TenantMember{{TenantID: 7, UserID: user.ID, Status: types.TenantMemberStatusActive, Role: types.TenantRoleAdmin}}
	result = service.BuildLoginMemberships(context.Background(), user, active)
	require.Len(t, result, 1)
	require.False(t, result[0].ScopedOnly, "source grants must preserve ordinary membership")
	require.Equal(t, types.TenantRoleAdmin, result[0].Role)
	direct.members = nil
	role.ScopedOnly = false
	service.groupAccess = sourceOnlyMembershipRoles{roles: []types.EffectiveTenantRole{role}}
	result = service.BuildLoginMemberships(context.Background(), user, active)
	require.False(t, result[0].ScopedOnly, "ordinary directory membership opens the normal workspace")
}
