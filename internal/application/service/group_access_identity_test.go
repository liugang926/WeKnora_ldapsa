package service

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestGroupAccessRechecksDirectoryIdentityForDirectWorkspaceAdmin(t *testing.T) {
	now := time.Now().UTC()
	userID := "user-1"
	directory := &types.Directory{
		ID: "directory-1", Enabled: true, LastSuccessfulSyncAt: &now, StaleAfterSeconds: 900,
	}
	identity := &types.DirectoryIdentity{
		ID: "identity-1", DirectoryID: directory.ID, UserID: &userID, Status: types.DirectoryObjectActive,
	}
	directoryRepo := &stubDirectoryRepoForAuth{identities: []*types.DirectoryIdentity{identity}, directory: directory}
	users := &userService{
		userRepo: &stubUserRepoForAuth{users: map[string]*types.User{
			"user-1": {ID: "user-1", IsActive: true},
		}},
		directoryRepo: directoryRepo,
	}
	access := NewGroupAccessService(&restrictedSystemAdminRepo{
		directRoles: map[string]types.TenantRole{"user-1": types.TenantRoleAdmin},
	})
	ConfigureGroupAccessUserEligibility(access, users)
	ctx := backgroundTaskAuthorizationContext(context.Background(), 7, types.TaskInitiator{UserID: "user-1"})
	check := func(allowed bool) {
		t.Helper()
		permission, err := access.EffectivePermission(
			ctx, 7, types.GroupResourceTypeKnowledgeBase, "kb", types.ResourceActionEdit, now,
		)
		require.NoError(t, err)
		require.Equal(t, allowed, permission.Allowed)
		role, err := access.EffectiveTenantRole(ctx, "user-1", 7, now)
		require.NoError(t, err)
		require.Equal(t, allowed, role.Member)
	}
	check(true)
	identity.Status = types.DirectoryObjectDisabled
	check(false)
	identity.Status = types.DirectoryObjectActive
	stale := now.Add(-16 * time.Minute)
	directory.LastSuccessfulSyncAt = &stale
	check(false)
	directory.LastSuccessfulSyncAt = &now
	check(true)
	// Local/OIDC accounts with no directory link retain their direct role.
	directoryRepo.identities = nil
	check(true)
}
