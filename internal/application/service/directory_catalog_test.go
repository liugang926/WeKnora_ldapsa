package service

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type catalogRepo struct {
	*runtimeDirectoryRepo
	groups      []*types.DirectoryGroup
	edges       []*types.DirectoryGroupEdge
	memberships []*types.DirectoryGroupMembership
}

func (r *catalogRepo) ListGroups(context.Context, string, string, int, int) ([]*types.DirectoryGroup, error) {
	return r.groups, nil
}

func (r *catalogRepo) ListGroupEdges(context.Context, string) ([]*types.DirectoryGroupEdge, error) {
	return r.edges, nil
}

func (r *catalogRepo) ListDirectoryMemberships(context.Context, string) ([]*types.DirectoryGroupMembership, error) {
	return r.memberships, nil
}

func TestDirectoryCatalogGroupMembersShowsNestedAndPrimaryFromSavedSnapshot(t *testing.T) {
	runtime, users, repo := liveLoginRuntimeFixture(t, nil, nil)
	repo.identities = []*types.DirectoryIdentity{
		{
			ID:              "u1-id",
			ObjectGUID:      "u1",
			DisplayName:     "张三",
			SAMAccountName:  "zhangsan",
			Status:          types.DirectoryObjectActive,
			SnapshotVersion: 7,
		},
		{
			ID:              "u2-id",
			ObjectGUID:      "u2",
			DisplayName:     "李四",
			SAMAccountName:  "lisi",
			Status:          types.DirectoryObjectDisabled,
			SnapshotVersion: 7,
		},
	}
	runtime.repo = &catalogRepo{
		runtimeDirectoryRepo: repo,
		groups: []*types.DirectoryGroup{
			{
				ID:              "root-id",
				ObjectGUID:      "root",
				DisplayName:     "研发中心",
				Status:          types.DirectoryObjectActive,
				SnapshotVersion: 7,
			},
			{
				ID:              "leaf-id",
				ObjectGUID:      "leaf",
				DisplayName:     "应用组",
				Status:          types.DirectoryObjectActive,
				SnapshotVersion: 7,
			},
		},
		edges: []*types.DirectoryGroupEdge{{ParentGroupID: "root-id", ChildGroupID: "leaf-id", SnapshotVersion: 7}},
		memberships: []*types.DirectoryGroupMembership{
			{
				GroupID:         "root-id",
				IdentityID:      "u1-id",
				Direct:          true,
				Source:          types.DirectoryMembershipDirect,
				SnapshotVersion: 7,
			},
			{
				GroupID:         "leaf-id",
				IdentityID:      "u2-id",
				Direct:          true,
				Primary:         true,
				Source:          types.DirectoryMembershipPrimary,
				SnapshotVersion: 7,
			},
			{
				GroupID:         "root-id",
				IdentityID:      "u2-id",
				Depth:           1,
				Source:          types.DirectoryMembershipNested,
				SnapshotVersion: 7,
			},
		},
	}
	result, err := runtime.CatalogGroupMembers(context.Background(), "root", "", 1, 1)
	require.NoError(t, err)
	require.Equal(t, 2, result.Total)
	require.Len(t, result.Items, 1)
	require.Equal(t, "李四", result.Items[0].DisplayName)
	require.True(t, result.Items[0].Disabled)
	require.Equal(t, "nested", result.Items[0].Origins[0].Source)
	require.Equal(t, "primary", result.Items[0].Origins[0].OriginSource)
	require.Equal(
		t,
		[]string{"leaf", "root"},
		[]string{result.Items[0].Origins[0].Path[0].ObjectGUID, result.Items[0].Origins[0].Path[1].ObjectGUID},
	)
	result, err = runtime.CatalogGroupMembers(context.Background(), "root", "张三", 20, 0)
	require.NoError(t, err)
	require.Equal(t, 1, result.Total)
	require.Equal(t, "direct", result.Items[0].Origins[0].Source)
	old := runtime.now().Add(-time.Hour)
	repo.directory.LastSuccessfulSyncAt = &old
	_, err = runtime.CatalogGroupMembers(context.Background(), "root", "", 20, 0)
	require.NoError(t, err, "the saved snapshot must remain inspectable while access is paused")
	require.Zero(t, users.registerCalls)
	require.Zero(t, users.generateCalls)
	broken := runtime.repo.(*catalogRepo)
	broken.memberships = broken.memberships[:2]
	_, err = runtime.CatalogGroupMembers(context.Background(), "root", "", 20, 0)
	require.ErrorIs(
		t,
		err,
		ErrDirectoryUnavailable,
		"a mismatched stored closure must not produce a misleading preview",
	)
}

type catalogMembers struct {
	interfaces.TenantMemberService
	calls  int
	member *types.TenantMember
}

func (m *catalogMembers) AddMember(
	_ context.Context,
	id string,
	tenant uint64,
	role types.TenantRole,
	_ *string,
) (*types.TenantMember, error) {
	m.calls++
	m.member = &types.TenantMember{UserID: id, TenantID: tenant, Role: role}
	return m.member, nil
}

func TestDirectoryCatalogListsUnprovisionedUsersAndGroups(t *testing.T) {
	runtime, users, repo := liveLoginRuntimeFixture(t, nil, nil)
	repo.identities = []*types.DirectoryIdentity{
		{
			ID:              "i1",
			DirectoryID:     "corp-ad",
			ObjectGUID:      "u1",
			DisplayName:     "张三",
			SAMAccountName:  "zhangsan",
			Status:          types.DirectoryObjectActive,
			SnapshotVersion: 7,
		},
		{
			ID:              "i2",
			DirectoryID:     "corp-ad",
			ObjectGUID:      "u2",
			DisplayName:     "李四",
			SAMAccountName:  "lisi",
			Status:          types.DirectoryObjectDisabled,
			SnapshotVersion: 7,
		},
		{ID: "old", ObjectGUID: "old", SnapshotVersion: 6},
	}
	runtime.repo = &catalogRepo{runtimeDirectoryRepo: repo, groups: []*types.DirectoryGroup{
		{ID: "group-id", ObjectGUID: "g1", DisplayName: "研发组", Status: types.DirectoryObjectActive, SnapshotVersion: 7},
	}}
	page, err := runtime.Catalog(context.Background(), "users", "", 1, 0)
	require.NoError(t, err)
	require.Equal(t, 2, page.Total)
	require.Len(t, page.Items, 1)
	require.Empty(t, page.Items[0].LinkedUserID)
	require.Equal(t, "张三", page.Items[0].DisplayName)
	page, err = runtime.Catalog(context.Background(), "users", "LISI", 20, 0)
	require.NoError(t, err)
	require.True(t, page.Items[0].Disabled)
	page, err = runtime.Catalog(context.Background(), "groups", "研发", 20, 0)
	require.NoError(t, err)
	require.Equal(t, "group-id", page.Items[0].DirectoryGroupID)
	require.Zero(t, users.registerCalls)
	require.Zero(t, users.generateCalls)
	old := runtime.now().Add(-time.Hour)
	repo.directory.LastSuccessfulSyncAt = &old
	page, err = runtime.Catalog(context.Background(), "users", "", 20, 0)
	require.NoError(t, err)
	require.False(t, page.Fresh)
	require.Equal(t, 2, page.Total)
	runtime.config.Directory.Enabled = false
	page, err = runtime.Catalog(context.Background(), "groups", "", 20, 0)
	require.NoError(t, err)
	require.False(t, page.Enabled)
	require.Empty(t, page.Items)
}

func TestDirectoryAddMemberRejectsUnsafeStateAndRoles(t *testing.T) {
	for _, test := range []string{"owner", "disabled", "stale", "conflict", "valid"} {
		t.Run(test, func(t *testing.T) {
			runtime, users, repo := liveLoginRuntimeFixture(t, nil, nil)
			members := &catalogMembers{}
			runtime.members = members
			role := types.TenantRoleViewer
			switch test {
			case "owner":
				role = types.TenantRoleOwner
			case "disabled":
				repo.loginSnapshot.Identity.Status = types.DirectoryObjectDisabled
			case "stale":
				old := runtime.now().Add(-time.Hour)
				repo.directory.LastSuccessfulSyncAt = &old
			case "conflict":
				repo.loginSnapshot.Identity.UserID = nil
				repo.identity = repo.loginSnapshot.Identity
				users.emailCollision = &types.User{ID: "local-user"}
			}
			member, err := runtime.AddTenantDirectoryMember(context.Background(), 10000, "user-guid", role)
			if test == "valid" {
				require.NoError(t, err)
				require.Equal(t, "user-1", member.UserID)
				require.Equal(t, 1, members.calls)
			} else {
				require.Error(t, err)
				require.Zero(t, members.calls)
			}
			require.Zero(t, users.generateCalls)
			require.Zero(t, users.registerCalls)
		})
	}
}

type catalogLinkService struct {
	interfaces.DirectoryService
	identity *types.DirectoryIdentity
}

func (s *catalogLinkService) LinkIdentity(_ context.Context, _, userID string) error {
	s.identity.UserID = &userID
	return nil
}

func TestDirectoryAddMemberProvisionsBeforeFirstLogin(t *testing.T) {
	runtime, users, repo := liveLoginRuntimeFixture(t, nil, nil)
	repo.loginSnapshot.Identity.UserID = nil
	repo.identity = repo.loginSnapshot.Identity
	repo.identity.SAMAccountName = "new-ad-user"
	runtime.directories = &catalogLinkService{identity: repo.identity}
	members := &catalogMembers{}
	runtime.members = members
	member, err := runtime.AddTenantDirectoryMember(
		context.Background(),
		10000,
		"user-guid",
		types.TenantRoleContributor,
	)
	require.NoError(t, err)
	require.Equal(t, 1, users.registerCalls)
	require.Equal(t, 1, members.calls)
	require.Equal(t, "unexpected", member.UserID)
	require.Equal(t, types.TenantRoleContributor, member.Role)
	require.Equal(t, member.UserID, *repo.identity.UserID)
	require.Zero(t, users.generateCalls)
}
