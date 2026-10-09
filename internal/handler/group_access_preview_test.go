package handler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type previewGroupRepo struct {
	groupAccessRepoStub
	userIDs []string
}

func (r *previewGroupRepo) ListTenantUserIDs(context.Context, uint64) ([]string, error) {
	return r.userIDs, nil
}

type previewAccessService struct {
	interfaces.GroupAccessService
	roles map[string]types.EffectiveTenantRole
}

func (s *previewAccessService) EffectiveTenantRole(
	_ context.Context, userID string, _ uint64, _ time.Time,
) (types.EffectiveTenantRole, error) {
	return s.roles[userID], nil
}

func (s *previewAccessService) EffectivePermission(
	ctx context.Context, tenantID uint64, _ types.ResourceType, _ string,
	_ types.ResourceAction, _ time.Time,
) (types.EffectiveResourcePermission, error) {
	caller := types.CallerFromContext(ctx)
	principal, _ := types.PrincipalFromContext(ctx)
	if caller.UserID != principal.ID || caller.TenantID != tenantID || principal.Type != types.PrincipalWebUser {
		return types.EffectiveResourcePermission{}, fmt.Errorf("preview did not replace the manager principal")
	}
	return types.EffectiveResourcePermission{Allowed: true}, nil
}

func TestResourceEffectivePermissionPreview(t *testing.T) {
	for _, resourceType := range []types.ResourceType{types.GroupResourceTypeKnowledgeBase, types.GroupResourceTypeAgent} {
		t.Run(string(resourceType), func(t *testing.T) {
			now := time.Now().UTC()
			stale := now.Add(-time.Hour)
			readPermission := types.ResourcePermissionRead
			if resourceType == types.GroupResourceTypeAgent {
				readPermission = types.ResourcePermissionUse
			}
			directory := &types.Directory{ID: "d", Enabled: true, LastSuccessfulSyncAt: &now}
			staleDirectory := &types.Directory{ID: "stale", Enabled: true, LastSuccessfulSyncAt: &stale}
			user1, user2, denied := "user-1", "user-2", "user-3"
			repo := &groupAccessDirectoryStub{
				memberships: map[string][]*types.DirectoryGroupMembership{
					"edit":    {{IdentityID: "i1", Source: types.DirectoryMembershipNested, Depth: 2}},
					"read":    {{IdentityID: "i2", Source: types.DirectoryMembershipPrimary}},
					"stale":   {{IdentityID: "i3", Source: types.DirectoryMembershipDirect}},
					"foreign": {{IdentityID: "foreign", Source: types.DirectoryMembershipDirect}},
				},
				identities: map[string]*types.DirectoryIdentity{
					"i1":      {DirectoryID: "d", UserID: &user1, Status: types.DirectoryObjectActive},
					"i2":      {DirectoryID: "d", UserID: &user2, Status: types.DirectoryObjectActive},
					"i3":      {DirectoryID: "stale", UserID: &denied, Status: types.DirectoryObjectActive},
					"foreign": {DirectoryID: "another", UserID: &denied, Status: types.DirectoryObjectActive},
				},
			}
			roles := map[string]types.EffectiveTenantRole{}
			for _, userID := range []string{"admin", user1, user2, denied} {
				roles[userID] = types.EffectiveTenantRole{TenantID: 7, Member: true, Role: types.TenantRoleViewer}
			}
			roles["admin"] = types.EffectiveTenantRole{TenantID: 7, Member: true, Role: types.TenantRoleAdmin}
			h := &GroupAccessHandler{
				directories: repo,
				groups:      &previewGroupRepo{userIDs: []string{denied, "outside", user2, "suspended", "admin", user1}},
				access:      &previewAccessService{roles: roles},
			}
			var grants []validatedResourceGrant
			for _, item := range []struct {
				id         string
				permission types.ResourcePermission
				directory  *types.Directory
			}{{"edit", types.ResourcePermissionEdit, directory}, {"read", readPermission, directory},
				{"stale", readPermission, staleDirectory}, {"foreign", readPermission, directory}} {
				grants = append(grants, validatedResourceGrant{
					directory: item.directory,
					group:     &types.DirectoryGroup{ID: item.id, DisplayName: item.id},
					request:   resourceGrantRequest{Permission: item.permission},
				})
			}
			ctx := types.WithCaller(context.Background(), types.Caller{UserID: "requesting-admin", TenantID: 7})
			impact, err := h.previewResourceAccess(ctx, 7, resourceType, "resource", types.ResourceAccessRestricted, grants, 0, 2)
			require.NoError(t, err)
			require.Equal(t, 4, impact.EffectiveUserTotal)
			require.Equal(t, 4, impact.CurrentlyAllowed)
			require.Equal(t, 3, impact.AllowedAfter)
			require.Equal(t, 1, impact.LosingAccess)
			require.True(t, impact.Truncated)
			require.Len(t, impact.EffectiveUsers, 2)
			require.Equal(t, "admin", impact.EffectiveUsers[0].UserID)
			require.Equal(t, "edit", impact.EffectiveUsers[0].PermissionAfter)
			require.Equal(t, user1, impact.EffectiveUsers[1].UserID)
			require.Equal(t, "edit", impact.EffectiveUsers[1].PermissionAfter)
			require.Equal(t, types.DirectoryMembershipNested, impact.EffectiveUsers[1].GroupMatches[0].MembershipSource)
			require.Equal(t, 2, impact.EffectiveUsers[1].GroupMatches[0].MembershipDepth)

			page, err := h.previewResourceAccess(ctx, 7, resourceType, "resource", types.ResourceAccessRestricted, grants, 2, 2)
			require.NoError(t, err)
			require.False(t, page.Truncated)
			require.Equal(t, string(readPermission), page.EffectiveUsers[0].PermissionAfter)
			require.Equal(t, types.DirectoryMembershipPrimary, page.EffectiveUsers[0].GroupMatches[0].MembershipSource)
			require.False(t, page.EffectiveUsers[1].AllowedAfter)
			require.Equal(t, "none", page.EffectiveUsers[1].PermissionAfter)
			require.Empty(t, page.EffectiveUsers[1].GroupMatches)
			inherit, err := h.previewResourceAccess(ctx, 7, resourceType, "resource", types.ResourceAccessInherit, grants, 100, 2)
			require.NoError(t, err)
			require.Empty(t, inherit.EffectiveUsers)
			require.Equal(t, 4, inherit.AllowedAfter)
		})
	}
}

func TestResourcePermissionPreviewPagination(t *testing.T) {
	for _, query := range []string{"offset=-1", "offset=x", "limit=0", "limit=101", "limit=x"} {
		ctx, _ := groupAccessTestContext("POST", "/preview?"+query, "", 7)
		_, _, err := resourcePreviewPagination(ctx)
		require.Error(t, err)
	}
	ctx, _ := groupAccessTestContext("POST", "/preview?offset=100&limit=20", "", 7)
	offset, limit, err := resourcePreviewPagination(ctx)
	require.NoError(t, err)
	require.Equal(t, 100, offset)
	require.Equal(t, 20, limit)
}
