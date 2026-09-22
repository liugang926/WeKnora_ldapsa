package service

import (
	"context"
	"fmt"
	"sort"
	"strings"

	ldapdirectory "github.com/Tencent/WeKnora/internal/infrastructure/directory"
	"github.com/Tencent/WeKnora/internal/types"
)

func directoryObjectLess(a, b types.DirectoryObjectSummary) bool {
	x, y := strings.ToLower(a.DisplayName), strings.ToLower(b.DisplayName)
	if x != y {
		return x < y
	}
	x, y = strings.ToLower(a.AccountName), strings.ToLower(b.AccountName)
	if x != y {
		return x < y
	}
	return a.ObjectGUID < b.ObjectGUID
}

func directoryPage[T any](items []T, limit, offset int) ([]T, bool) {
	limit = normalizeSearchLimit(limit)
	if offset < 0 {
		offset = 0
	}
	if offset >= len(items) {
		return []T{}, false
	}
	// Clamp before adding, so an untrusted offset cannot overflow.
	end := offset + min(limit, len(items)-offset)
	return items[offset:end], end < len(items)
}

func (s *directoryRuntimeService) QueryGroupMembers(ctx context.Context, groupGUID, query string, limit, offset int) (*types.DirectoryGroupMembersResult, error) {
	snapshot, _, err := s.liveSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return directoryGroupMembers(snapshot, groupGUID, query, limit, offset)
}

func directoryGroupMembers(snapshot *ldapdirectory.Snapshot, groupGUID, query string, limit, offset int) (*types.DirectoryGroupMembersResult, error) {
	groups := make(map[string]types.DirectoryObjectSummary, len(snapshot.Groups))
	for _, group := range snapshot.Groups {
		groups[group.ObjectGUID] = types.DirectoryObjectSummary{DirectoryID: snapshot.DirectoryID,
			ObjectGUID: group.ObjectGUID, SID: group.SID, DN: group.DN, DisplayName: group.DisplayName,
			AccountName: group.SAMAccountName, Email: group.Email}
	}
	group, ok := groups[groupGUID]
	if !ok {
		return nil, ErrDirectoryIdentityUnavailable
	}
	result := &types.DirectoryGroupMembersResult{Group: group, Items: []types.DirectoryGroupMember{},
		ParentGroups: []types.DirectoryObjectSummary{}, ChildGroups: []types.DirectoryObjectSummary{}}
	children := map[string][]string{}
	for _, edge := range snapshot.GroupMemberships {
		children[edge.ParentGroupGUID] = append(children[edge.ParentGroupGUID], edge.MemberGroupGUID)
		if edge.MemberGroupGUID == groupGUID {
			result.ParentGroups = append(result.ParentGroups, groups[edge.ParentGroupGUID])
		}
		if edge.ParentGroupGUID == groupGUID {
			result.ChildGroups = append(result.ChildGroups, groups[edge.MemberGroupGUID])
		}
	}
	for _, edges := range children {
		sort.Strings(edges)
	}
	// Reverse BFS records one deterministic shortest path, avoiding exponential
	// enumeration in diamond-shaped hierarchies. Every origin remains visible.
	next := map[string]string{groupGUID: ""}
	queue := []string{groupGUID}
	for i := 0; i < len(queue); i++ {
		for _, child := range children[queue[i]] {
			if _, seen := next[child]; seen {
				continue
			}
			next[child] = queue[i]
			queue = append(queue, child)
		}
	}
	origins := map[string][]types.DirectoryMembershipOrigin{}
	for _, membership := range snapshot.EffectiveMemberships {
		if membership.GroupGUID != groupGUID {
			continue
		}
		origin := membership.OriginGroupGUID
		if origin == "" {
			origin = membership.GroupGUID
		}
		if _, found := next[origin]; !found {
			return nil, fmt.Errorf("%w: missing membership path", ldapdirectory.ErrIncompleteResults)
		}
		path := []types.DirectoryObjectSummary{}
		for current := origin; current != ""; current = next[current] {
			path = append(path, groups[current])
		}
		originSource := membership.OriginSource
		if originSource == "" {
			originSource = membership.Source
		}
		origins[membership.UserGUID] = append(origins[membership.UserGUID], types.DirectoryMembershipOrigin{
			Source: string(membership.Source), OriginSource: string(originSource), Depth: len(path) - 1, Path: path})
	}
	for _, user := range snapshot.Users {
		memberOrigins := origins[user.ObjectGUID]
		if len(memberOrigins) == 0 || !containsFold(query, user.DisplayName, user.SAMAccountName, user.UserPrincipalName, user.Email, user.DN) {
			continue
		}
		sort.Slice(memberOrigins, func(i, j int) bool {
			if memberOrigins[i].Depth != memberOrigins[j].Depth {
				return memberOrigins[i].Depth < memberOrigins[j].Depth
			}
			if memberOrigins[i].Source != memberOrigins[j].Source {
				return memberOrigins[i].Source < memberOrigins[j].Source
			}
			return memberOrigins[i].Path[0].ObjectGUID < memberOrigins[j].Path[0].ObjectGUID
		})
		result.Items = append(result.Items, types.DirectoryGroupMember{DirectoryObjectSummary: types.DirectoryObjectSummary{
			DirectoryID: snapshot.DirectoryID, ObjectGUID: user.ObjectGUID, SID: user.SID, DN: user.DN,
			DisplayName: user.DisplayName, AccountName: user.SAMAccountName, UserPrincipalName: user.UserPrincipalName,
			Email: user.Email, Disabled: !user.Enabled}, Origins: memberOrigins})
	}
	for _, member := range snapshot.UnresolvedMembers {
		if member.ParentGroupGUID == groupGUID {
			result.UnresolvedMemberCount++
		}
	}
	sort.Slice(result.Items, func(i, j int) bool {
		return directoryObjectLess(result.Items[i].DirectoryObjectSummary, result.Items[j].DirectoryObjectSummary)
	})
	sort.Slice(result.ParentGroups, func(i, j int) bool { return directoryObjectLess(result.ParentGroups[i], result.ParentGroups[j]) })
	sort.Slice(result.ChildGroups, func(i, j int) bool { return directoryObjectLess(result.ChildGroups[i], result.ChildGroups[j]) })
	result.Total = len(result.Items)
	result.Items, _ = directoryPage(result.Items, limit, offset)
	return result, nil
}
