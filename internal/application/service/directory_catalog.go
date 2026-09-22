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
func (s *directoryRuntimeService) Catalog(ctx context.Context, kind, query string, limit, offset int) (*types.DirectoryCatalogResult, error) {
	if kind != "users" && kind != "groups" {
		return nil, ErrInvalidDirectoryConfig
	}
	directory, err := s.persistedDirectory(ctx)
	if err != nil {
		return nil, err
	}
	result := &types.DirectoryCatalogResult{Items: []types.DirectoryCatalogItem{}, Enabled: directory.Enabled,
		Fresh:         directory.Enabled && directory.IsFresh(s.now().UTC()) && directory.LastSyncError == "",
		LastSuccessAt: directory.LastSuccessfulSyncAt, SyncIntervalSeconds: directory.SyncIntervalSeconds}
	if !directory.Enabled || directory.SnapshotVersion == 0 {
		return result, nil
	}
	if kind == "users" {
		identities, err := s.listAllIdentities(ctx, directory.ID)
		if err != nil {
			return nil, err
		}
		for _, user := range identities {
			if user.SnapshotVersion != directory.SnapshotVersion || !containsFold(query, user.DisplayName, user.SAMAccountName, user.UPN, user.Email, user.DN) {
				continue
			}
			item := types.DirectoryCatalogItem{DirectoryObjectSummary: types.DirectoryObjectSummary{
				DirectoryID: directory.ID, IdentityID: user.ID, ObjectGUID: user.ObjectGUID, DN: user.DN,
				DisplayName: user.DisplayName, AccountName: user.SAMAccountName, Email: user.Email, UserPrincipalName: user.UPN,
				Disabled: user.Status != types.DirectoryObjectActive, Status: string(user.Status)}}
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
			if group.SnapshotVersion != directory.SnapshotVersion || !containsFold(query, group.DisplayName, group.SAMAccountName, group.Email, group.DN) {
				continue
			}
			result.Items = append(result.Items, types.DirectoryCatalogItem{DirectoryGroupID: group.ID,
				DirectoryObjectSummary: types.DirectoryObjectSummary{DirectoryID: directory.ID, ObjectGUID: group.ObjectGUID,
					DN: group.DN, DisplayName: group.DisplayName, AccountName: group.SAMAccountName, Email: group.Email,
					Disabled: group.Status != types.DirectoryObjectActive, Status: string(group.Status)}})
		}
	}
	// Detect a concurrent snapshot replacement instead of returning mixed pages.
	current, err := s.repo.Get(ctx, directory.ID)
	if err != nil {
		return nil, err
	}
	if current.SnapshotVersion != directory.SnapshotVersion || current.ConfigVersion != directory.ConfigVersion {
		return nil, ErrDirectoryUnavailable
	}
	sort.Slice(result.Items, func(i, j int) bool {
		return directoryObjectLess(result.Items[i].DirectoryObjectSummary, result.Items[j].DirectoryObjectSummary)
	})
	result.Total = len(result.Items)
	result.Items, _ = directoryPage(result.Items, limit, offset)
	return result, nil
}

// The route requires Owner (same as ordinary direct member additions). It
// creates an AD-only linked identity without requiring a prior user login.
func (s *directoryRuntimeService) AddTenantDirectoryMember(ctx context.Context, tenantID uint64, objectGUID string, role types.TenantRole) (*types.TenantMember, error) {
	if tenantID == 0 || strings.TrimSpace(objectGUID) == "" || (role != types.TenantRoleViewer && role != types.TenantRoleContributor && role != types.TenantRoleAdmin) {
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
	user, err := s.resolveDirectoryUser(ctx, identity, ldapdirectory.User{ObjectGUID: identity.ObjectGUID,
		SAMAccountName: identity.SAMAccountName, DisplayName: identity.DisplayName, Email: identity.Email})
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
