package service

import (
	"context"
	"sort"
	"strings"

	ldapdirectory "github.com/Tencent/WeKnora/internal/infrastructure/directory"
	"github.com/Tencent/WeKnora/internal/types"
)

// Catalog only reads the last committed synchronization snapshot. Browsing
// does not bind to AD, provision users, or grant access to the workspace.
func (s *directoryRuntimeService) Catalog(
	ctx context.Context,
	kind, query string,
	limit, offset int,
) (*types.DirectoryCatalogResult, error) {
	if kind != "users" && kind != "groups" {
		return nil, ErrInvalidDirectoryConfig
	}
	directory, err := s.persistedDirectory(ctx)
	if err != nil {
		return nil, err
	}
	result := &types.DirectoryCatalogResult{
		Items: []types.DirectoryCatalogItem{}, Enabled: directory.Enabled,
		Fresh: directory.Enabled && directory.IsFresh(s.now().UTC()) &&
			directory.LastSyncError == "",
		LastSuccessAt: directory.LastSuccessfulSyncAt, SyncIntervalSeconds: directory.SyncIntervalSeconds,
	}
	if !directory.Enabled || directory.SnapshotVersion == 0 {
		return result, nil
	}
	if kind == "users" {
		identities, err := s.listAllIdentities(ctx, directory.ID)
		if err != nil {
			return nil, err
		}
		for _, user := range identities {
			if user.SnapshotVersion != directory.SnapshotVersion ||
				!containsFold(
					query,
					user.DisplayName,
					user.SAMAccountName,
					user.UPN,
					user.Email,
					user.DN,
				) {
				continue
			}
			item := types.DirectoryCatalogItem{DirectoryObjectSummary: types.DirectoryObjectSummary{
				DirectoryID: directory.ID, IdentityID: user.ID, ObjectGUID: user.ObjectGUID, DN: user.DN,
				DisplayName: user.DisplayName, AccountName: user.SAMAccountName,
				Email: user.Email, UserPrincipalName: user.UPN,
				Disabled: user.Status != types.DirectoryObjectActive, Status: string(user.Status),
			}}
			if user.UserID != nil {
				item.LinkedUserID = *user.UserID
			}
			result.Items = append(result.Items, item)
		}
	} else {
		groups, err := s.listAllGroups(ctx, directory.ID)
		if err != nil {
			return nil, err
		}
		for _, group := range groups {
			if group.SnapshotVersion != directory.SnapshotVersion ||
				!containsFold(query, group.DisplayName, group.SAMAccountName, group.Email, group.DN) {
				continue
			}
			result.Items = append(result.Items, types.DirectoryCatalogItem{
				DirectoryGroupID: group.ID,
				DirectoryObjectSummary: types.DirectoryObjectSummary{
					DirectoryID: directory.ID, ObjectGUID: group.ObjectGUID,
					DN: group.DN, DisplayName: group.DisplayName, AccountName: group.SAMAccountName, Email: group.Email,
					Disabled: group.Status != types.DirectoryObjectActive, Status: string(group.Status),
				},
			})
		}
	}
	// Detect a concurrent snapshot replacement instead of returning mixed pages.
	current, err := s.repo.Get(ctx, directory.ID)
	if err != nil {
		return nil, err
	}
	if current.SnapshotVersion != directory.SnapshotVersion ||
		current.ConfigVersion != directory.ConfigVersion {
		return nil, ErrDirectoryUnavailable
	}
	sort.Slice(result.Items, func(i, j int) bool {
		return directoryObjectLess(
			result.Items[i].DirectoryObjectSummary,
			result.Items[j].DirectoryObjectSummary,
		)
	})
	result.Total = len(result.Items)
	result.Items, _ = directoryPage(result.Items, limit, offset)
	return result, nil
}

// CatalogGroupMembers explains the effective access of a synchronized group
// using the committed snapshot, so workspace managers can preview it without
// a live AD connection or system-administrator privileges.
func (s *directoryRuntimeService) CatalogGroupMembers(
	ctx context.Context,
	groupGUID, query string,
	limit, offset int,
) (*types.DirectoryGroupMembersResult, error) {
	if strings.TrimSpace(groupGUID) == "" {
		return nil, ErrInvalidDirectoryConfig
	}
	directory, err := s.persistedDirectory(ctx)
	if err != nil {
		return nil, err
	}
	if !directory.Enabled || directory.SnapshotVersion == 0 {
		return nil, ErrDirectoryDisabled
	}
	identities, err := s.listAllIdentities(ctx, directory.ID)
	if err != nil {
		return nil, err
	}
	groups, err := s.listAllGroups(ctx, directory.ID)
	if err != nil {
		return nil, err
	}
	edges, err := s.repo.ListGroupEdges(ctx, directory.ID)
	if err != nil {
		return nil, err
	}
	memberships, err := s.repo.ListDirectoryMemberships(ctx, directory.ID)
	if err != nil {
		return nil, err
	}
	snapshot := &ldapdirectory.Snapshot{DirectoryID: directory.ID}
	userGUIDs, groupGUIDs := []string{}, []string{}
	userByID, groupByID := map[string]string{}, map[string]string{}
	for _, identity := range identities {
		if identity.SnapshotVersion != directory.SnapshotVersion {
			continue
		}
		userByID[identity.ID] = identity.ObjectGUID
		userGUIDs = append(userGUIDs, identity.ObjectGUID)
		snapshot.Users = append(
			snapshot.Users,
			ldapdirectory.User{
				ObjectGUID: identity.ObjectGUID, SID: identity.ObjectSID,
				DN: identity.DN, DisplayName: identity.DisplayName, SAMAccountName: identity.SAMAccountName,
				UserPrincipalName: identity.UPN, Email: identity.Email,
				Enabled: identity.Status == types.DirectoryObjectActive,
			},
		)
	}
	for _, group := range groups {
		if group.SnapshotVersion != directory.SnapshotVersion {
			continue
		}
		groupByID[group.ID] = group.ObjectGUID
		groupGUIDs = append(groupGUIDs, group.ObjectGUID)
		snapshot.Groups = append(
			snapshot.Groups,
			ldapdirectory.Group{
				ObjectGUID: group.ObjectGUID, SID: group.ObjectSID,
				DN: group.DN, DisplayName: group.DisplayName, SAMAccountName: group.SAMAccountName, Email: group.Email,
			},
		)
	}
	if !containsString(groupGUIDs, groupGUID) {
		return nil, ErrDirectoryIdentityUnavailable
	}
	for _, edge := range edges {
		if edge.SnapshotVersion != directory.SnapshotVersion {
			continue
		}
		parent, parentOK := groupByID[edge.ParentGroupID]
		child, childOK := groupByID[edge.ChildGroupID]
		if !parentOK || !childOK {
			return nil, ErrDirectoryUnavailable
		}
		snapshot.GroupMemberships = append(
			snapshot.GroupMemberships,
			ldapdirectory.GroupMembership{ParentGroupGUID: parent, MemberGroupGUID: child},
		)
	}
	stored := map[string]bool{}
	for _, member := range memberships {
		if member.SnapshotVersion != directory.SnapshotVersion {
			continue
		}
		group, groupOK := groupByID[member.GroupID]
		user, userOK := userByID[member.IdentityID]
		if !groupOK || !userOK {
			return nil, ErrDirectoryUnavailable
		}
		if group == groupGUID {
			stored[user] = true
		}
		if member.Direct {
			source := ldapdirectory.MembershipDirect
			if member.Primary {
				source = ldapdirectory.MembershipPrimary
			}
			snapshot.DirectMemberships = append(
				snapshot.DirectMemberships,
				ldapdirectory.UserGroupMembership{
					UserGUID: user, GroupGUID: group, Source: source,
				},
			)
		}
	}
	snapshot.EffectiveMemberships, err = ldapdirectory.ComputeEffectiveMemberships(
		userGUIDs,
		groupGUIDs,
		snapshot.DirectMemberships,
		snapshot.GroupMemberships,
	)
	if err != nil {
		return nil, ErrDirectoryUnavailable
	}
	computed := map[string]bool{}
	for _, member := range snapshot.EffectiveMemberships {
		if member.GroupGUID == groupGUID {
			computed[member.UserGUID] = true
		}
	}
	if len(stored) != len(computed) {
		return nil, ErrDirectoryUnavailable
	}
	for user := range stored {
		if !computed[user] {
			return nil, ErrDirectoryUnavailable
		}
	}
	current, err := s.repo.Get(ctx, directory.ID)
	if err != nil {
		return nil, err
	}
	if current.SnapshotVersion != directory.SnapshotVersion ||
		current.ConfigVersion != directory.ConfigVersion {
		return nil, ErrDirectoryUnavailable
	}
	return directoryGroupMembers(snapshot, groupGUID, query, limit, offset)
}

// The route requires Owner (same as ordinary direct member additions). It
// creates an AD-only linked identity without requiring a prior user login.
func (s *directoryRuntimeService) AddTenantDirectoryMember(
	ctx context.Context,
	tenantID uint64,
	objectGUID string,
	role types.TenantRole,
) (*types.TenantMember, error) {
	if tenantID == 0 || strings.TrimSpace(objectGUID) == "" ||
		(role != types.TenantRoleViewer && role != types.TenantRoleContributor && role != types.TenantRoleAdmin) {
		return nil, ErrInvalidDirectoryConfig
	}
	if s.members == nil {
		return nil, ErrDirectoryUnavailable
	}
	directory, err := s.persistedDirectory(ctx)
	if err != nil {
		return nil, err
	}
	snapshot, err := s.repo.GetLoginSnapshot(ctx, directory.ID, objectGUID)
	if err != nil {
		return nil, err
	}
	identity, err := s.validateLoginSnapshot(snapshot, directory.ConfigVersion)
	if err != nil {
		return nil, err
	}
	user, err := s.resolveDirectoryUser(ctx, identity, ldapdirectory.User{
		ObjectGUID:     identity.ObjectGUID,
		SAMAccountName: identity.SAMAccountName, DisplayName: identity.DisplayName, Email: identity.Email,
	})
	if err != nil {
		return nil, err
	}
	// Revalidate after provisioning; no session is issued by this operation.
	current, err := s.repo.GetLoginSnapshot(ctx, directory.ID, objectGUID)
	if err != nil {
		return nil, err
	}
	identity, err = s.validateLoginSnapshot(current, directory.ConfigVersion)
	if err != nil {
		return nil, err
	}
	if identity.UserID == nil || *identity.UserID != user.ID {
		return nil, ErrDirectoryIdentityUnavailable
	}
	actor, _ := types.UserIDFromContext(ctx)
	return s.members.AddMember(ctx, user.ID, tenantID, role, &actor)
}
