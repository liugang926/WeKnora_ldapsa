package service

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type restrictedSystemAdminRepo struct {
	interfaces.GroupAccessRepository
	directRoles map[string]types.TenantRole
}

func (r *restrictedSystemAdminRepo) GetResourceAccessPolicy(
	context.Context, uint64, types.ResourceType, string,
) (*types.ResourceAccessPolicy, error) {
	return &types.ResourceAccessPolicy{Mode: types.ResourceAccessRestricted}, nil
}

func (r *restrictedSystemAdminRepo) GetDirectTenantRole(
	_ context.Context,
	userID string,
	_ uint64,
) (*types.TenantRole, error) {
	role, ok := r.directRoles[userID]
	if !ok {
		return nil, nil
	}
	return &role, nil
}

func (r *restrictedSystemAdminRepo) ListGroupRoleMatches(
	context.Context,
	string,
	uint64,
) ([]types.GroupRoleMatch, error) {
	return nil, nil
}

func (r *restrictedSystemAdminRepo) ListResourceGroupMatches(
	context.Context, string, uint64, types.ResourceType, string,
) ([]types.ResourceGroupMatch, error) {
	return nil, nil
}

func systemAdminResourceContext(userID string, callerTenant uint64) context.Context {
	ctx := types.WithPrincipal(context.Background(), types.Principal{Type: types.PrincipalWebUser, ID: userID})
	ctx = types.WithCaller(ctx, types.Caller{TenantID: callerTenant, UserID: userID, Role: types.TenantRoleViewer})
	return context.WithValue(ctx, types.SystemAdminContextKey, true)
}

func TestRestrictedResourceDoesNotLetSystemAdminBypassWorkspaceMembership(t *testing.T) {
	tests := []struct {
		name         string
		userID       string
		callerTenant uint64
		roles        map[string]types.TenantRole
		wantAllowed  bool
		wantReason   string
	}{
		{
			name: "no workspace membership", userID: "sys-no-member", callerTenant: 7,
			roles: map[string]types.TenantRole{}, wantReason: "workspace_membership_required",
		},
		{
			name: "cross workspace caller", userID: "sys-cross", callerTenant: 8,
			roles: map[string]types.TenantRole{
				"sys-cross": types.TenantRoleAdmin,
			}, wantReason: "workspace_membership_required",
		},
		{
			name: "viewer without resource grant", userID: "sys-viewer", callerTenant: 7,
			roles: map[string]types.TenantRole{
				"sys-viewer": types.TenantRoleViewer,
			}, wantReason: "matching_group_grant_required",
		},
		{
			name: "workspace admin", userID: "sys-workspace-admin", callerTenant: 7,
			roles:       map[string]types.TenantRole{"sys-workspace-admin": types.TenantRoleAdmin},
			wantAllowed: true, wantReason: "workspace_manager",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := NewGroupAccessService(&restrictedSystemAdminRepo{directRoles: tt.roles})
			permission, err := service.EffectivePermission(
				systemAdminResourceContext(tt.userID, tt.callerTenant),
				7, types.GroupResourceTypeKnowledgeBase, "kb-restricted", types.ResourceActionRead, time.Now().UTC(),
			)
			if err != nil {
				t.Fatal(err)
			}
			if permission.Allowed != tt.wantAllowed || permission.Reason != tt.wantReason {
				t.Fatalf("permission = %+v, want allowed=%v reason=%q", permission, tt.wantAllowed, tt.wantReason)
			}
		})
	}
}
