package service

import (
	"context"
	"errors"
	"math"
	"testing"

	ldapdirectory "github.com/Tencent/WeKnora/internal/infrastructure/directory"
	"github.com/stretchr/testify/require"
)

func browseFixture() *ldapdirectory.Snapshot {
	return &ldapdirectory.Snapshot{
		DirectoryID: "corp-ad",
		Users: []ldapdirectory.User{
			{
				ObjectGUID:     "u2",
				DisplayName:    "李四",
				SAMAccountName: "lisi",
				Email:          "li@example.test",
				Enabled:        true,
			},
			{
				ObjectGUID:        "u1",
				DisplayName:       "张三",
				SAMAccountName:    "zhangsan",
				UserPrincipalName: "zs@example.test",
				Enabled:           true,
			},
			{ObjectGUID: "u3", DisplayName: "王五", SAMAccountName: "wangwu", Enabled: false},
		},
		Groups: []ldapdirectory.Group{
			{ObjectGUID: "root", DisplayName: "研发中心", SAMAccountName: "Engineering"},
			{ObjectGUID: "child", DisplayName: "平台组", SAMAccountName: "Platform"},
			{ObjectGUID: "leaf", DisplayName: "应用组", SAMAccountName: "Apps"},
		},
		GroupMemberships: []ldapdirectory.GroupMembership{
			{MemberGroupGUID: "child", ParentGroupGUID: "root"},
			{MemberGroupGUID: "leaf", ParentGroupGUID: "child"},
		},
		DirectMemberships: []ldapdirectory.UserGroupMembership{
			{UserGUID: "u1", GroupGUID: "root", Source: ldapdirectory.MembershipDirect},
			{UserGUID: "u3", GroupGUID: "root", Source: ldapdirectory.MembershipPrimary},
		},
		EffectiveMemberships: []ldapdirectory.EffectiveMembership{
			{
				UserGUID:        "u1",
				GroupGUID:       "root",
				OriginGroupGUID: "root",
				Source:          ldapdirectory.MembershipDirect,
				OriginSource:    ldapdirectory.MembershipDirect,
			},
			{
				UserGUID:        "u1",
				GroupGUID:       "root",
				OriginGroupGUID: "leaf",
				Source:          ldapdirectory.MembershipNested,
				OriginSource:    ldapdirectory.MembershipPrimary,
				Depth:           2,
			},
			{
				UserGUID:        "u2",
				GroupGUID:       "root",
				OriginGroupGUID: "child",
				Source:          ldapdirectory.MembershipNested,
				OriginSource:    ldapdirectory.MembershipDirect,
				Depth:           1,
			},
			{
				UserGUID:        "u3",
				GroupGUID:       "root",
				OriginGroupGUID: "root",
				Source:          ldapdirectory.MembershipPrimary,
				OriginSource:    ldapdirectory.MembershipPrimary,
			},
		},
		UnresolvedMembers: []ldapdirectory.UnresolvedMember{
			{ParentGroupGUID: "root", MemberDN: "CN=computer"},
		},
	}
}

func TestDirectoryBrowseSearchAndStablePagination(t *testing.T) {
	runtime, _, _ := liveLoginRuntimeFixture(t, nil, nil)
	snapshot := browseFixture()
	adapter := &runtimeLDAPAdapter{syncResult: snapshot}
	runtime.newAdapter = func(ldapdirectory.Config) (liveDirectoryAdapter, error) { return adapter, nil }
	ctx := context.Background()
	for _, query := range []string{" 张三 ", "ZHANGSAN", "zs@example.test"} {
		page, err := runtime.QueryUsers(ctx, query, 20, 0)
		require.NoError(t, err)
		require.Equal(t, 1, page.Total)
		require.Equal(t, "u1", page.Items[0].ObjectGUID)
	}
	first, err := runtime.QueryUsers(ctx, "", 1, 0)
	require.NoError(t, err)
	require.True(t, first.Truncated)
	require.Equal(t, 3, first.Total)
	// LDAP result order must not change which objects land on a page.
	snapshot.Users[0], snapshot.Users[2] = snapshot.Users[2], snapshot.Users[0]
	again, err := runtime.QueryUsers(ctx, "", 1, 0)
	require.NoError(t, err)
	require.Equal(t, first.Items, again.Items)
	second, err := runtime.QueryUsers(ctx, "", 1, 1)
	require.NoError(t, err)
	require.NotEqual(t, first.Items[0].ObjectGUID, second.Items[0].ObjectGUID)
	empty, err := runtime.QueryUsers(ctx, "", 1, math.MaxInt)
	require.NoError(t, err)
	require.Empty(t, empty.Items)
	require.False(t, empty.Truncated)
	for _, query := range []string{"研发", "ENGINEERING"} {
		groups, err := runtime.QueryGroups(ctx, query, 20, 0)
		require.NoError(t, err)
		require.Equal(t, 1, groups.Total)
		require.Equal(t, 1, groups.Items[0].DirectMemberCount)
		require.Equal(t, 3, groups.Items[0].EffectiveMemberCount)
	}
	groups, err := runtime.QueryGroups(ctx, "", 1, 2)
	require.NoError(t, err)
	require.Len(t, groups.Items, 1)
	require.Equal(t, 3, groups.Total)
	require.False(t, groups.Truncated)
	adapter.syncErr = errors.New("offline")
	_, err = runtime.QueryGroupMembers(ctx, "root", "", 20, 0)
	require.Error(t, err)
	adapter.syncErr = nil
	runtime.config.Directory.Enabled = false
	_, err = runtime.QueryUsers(ctx, "", 20, 0)
	require.ErrorIs(t, err, ErrDirectoryDisabled)
}

func TestDirectoryGroupMembersOriginsAndPagination(t *testing.T) {
	snapshot := browseFixture()
	result, err := directoryGroupMembers(snapshot, "root", "", 20, 0)
	require.NoError(t, err)
	require.Equal(t, 3, result.Total)
	require.Len(t, result.ChildGroups, 1)
	require.Empty(t, result.ParentGroups)
	require.Equal(t, 1, result.UnresolvedMemberCount)
	require.Equal(t, "u1", result.Items[0].ObjectGUID)
	require.Len(t, result.Items[0].Origins, 2)
	origin := result.Items[0].Origins[1]
	require.Equal(t, "nested", origin.Source)
	require.Equal(t, "primary", origin.OriginSource)
	require.Equal(t, 2, origin.Depth)
	require.Equal(t, "leaf", origin.Path[0].ObjectGUID)
	require.Equal(t, "child", origin.Path[1].ObjectGUID)
	require.Equal(t, "root", origin.Path[2].ObjectGUID)
	require.True(t, result.Items[2].Disabled)
	require.Equal(t, "primary", result.Items[2].Origins[0].Source)
	page, err := directoryGroupMembers(snapshot, "root", "", 1, 1)
	require.NoError(t, err)
	require.Equal(t, result.Items[1], page.Items[0])
	for _, query := range []string{"李", "LISI", "li@example.test"} {
		filtered, err := directoryGroupMembers(snapshot, "root", query, 20, 0)
		require.NoError(t, err)
		require.Equal(t, 1, filtered.Total)
		require.Equal(t, "u2", filtered.Items[0].ObjectGUID)
	}
	child, err := directoryGroupMembers(snapshot, "child", "", 20, 0)
	require.NoError(t, err)
	require.Equal(t, "root", child.ParentGroups[0].ObjectGUID)
	require.Equal(t, "leaf", child.ChildGroups[0].ObjectGUID)
	_, err = directoryGroupMembers(snapshot, "other-directory-group", "", 20, 0)
	require.ErrorIs(t, err, ErrDirectoryIdentityUnavailable)
}

func TestDirectoryPageBounds(t *testing.T) {
	items := make([]int, 150)
	page, more := directoryPage(items, 1000, -1)
	require.Len(t, page, 100)
	require.True(t, more)
	page, more = directoryPage(items, 0, 150)
	require.Empty(t, page)
	require.False(t, more)
}

func TestDirectoryGroupMembersShortestPathAndMissingOrigin(t *testing.T) {
	snapshot := browseFixture()
	snapshot.GroupMemberships = append(
		snapshot.GroupMemberships,
		ldapdirectory.GroupMembership{MemberGroupGUID: "leaf", ParentGroupGUID: "root"},
	)
	result, err := directoryGroupMembers(snapshot, "root", "张三", 20, 0)
	require.NoError(t, err)
	require.Len(t, result.Items[0].Origins, 2)
	require.Len(t, result.Items[0].Origins[1].Path, 2)
	require.Equal(t, 1, result.Items[0].Origins[1].Depth)
	snapshot.EffectiveMemberships[1].OriginGroupGUID = "missing"
	_, err = directoryGroupMembers(snapshot, "root", "", 20, 0)
	require.ErrorIs(t, err, ldapdirectory.ErrIncompleteResults)
}
