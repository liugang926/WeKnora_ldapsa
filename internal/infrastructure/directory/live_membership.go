package directory

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/go-ldap/ldap/v3"
)

var liveGroupIdentityAttributes = []string{
	"objectGUID",
	"objectSid",
	"distinguishedName",
	"sAMAccountName",
	"name",
	"cn",
	"displayName",
	"mail",
}

// collectEffectiveGroupObjectGUIDs resolves only the authenticated user's
// selected-scope group closure. It runs on the same controller on which the
// password was accepted and never mutates the durable, atomically-applied
// directory snapshot.
func (a *Adapter) collectEffectiveGroupObjectGUIDs(
	ctx context.Context,
	conn ldapConnection,
	user User,
) ([]string, error) {
	collector := liveMembershipCollector{
		adapter:      a,
		conn:         conn,
		groupsByGUID: make(map[string]Group),
		guidBySID:    make(map[string]string),
		guidByDN:     make(map[string]string),
		edges:        make(map[string]GroupMembership),
		queued:       make(map[string]struct{}),
	}

	directGroups, err := collector.groupsContaining(ctx, user.DN)
	if err != nil {
		return nil, fmt.Errorf("query direct groups: %w", err)
	}
	seedGroups := make(map[string]string, len(directGroups)+1)
	for _, group := range directGroups {
		stored, _, err := collector.addGroup(group)
		if err != nil {
			return nil, err
		}
		seedGroups[strings.ToLower(stored.ObjectGUID)] = stored.ObjectGUID
		collector.enqueue(stored)
	}

	primarySID, err := PrimaryGroupSID(user.SID, user.PrimaryGroupRID)
	if err != nil {
		return nil, fmt.Errorf("derive primary group for %q: %w", user.DN, err)
	}
	parsedPrimarySID, err := ParseSID(primarySID)
	if err != nil {
		return nil, err
	}
	primarySIDBytes, err := parsedPrimarySID.Bytes()
	if err != nil {
		return nil, err
	}
	primaryFilter := fmt.Sprintf("(&%s(objectSid=%s))", a.config.GroupFilter, escapeBinaryFilterValue(primarySIDBytes))
	primaryEntries, err := a.searchPaged(ctx, conn, a.config.GroupBaseDN, primaryFilter, liveGroupIdentityAttributes)
	if err != nil {
		return nil, fmt.Errorf("query primary group: %w", err)
	}
	collector.responseEntries += len(primaryEntries)
	if collector.responseEntries > a.config.ResultLimit {
		return nil, fmt.Errorf("%w: aggregate live group result limit %d exceeded", ErrIncompleteResults, a.config.ResultLimit)
	}
	if len(primaryEntries) != 1 {
		return nil, fmt.Errorf("%w: primary group %q matched %d selected-scope groups", ErrIncompleteResults, primarySID, len(primaryEntries))
	}
	primaryGroup, err := parseGroupIdentityEntry(primaryEntries[0])
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(primaryGroup.SID, primarySID) {
		return nil, fmt.Errorf("%w: primary group SID response did not match %q", ErrIncompleteResults, primarySID)
	}
	primaryGroup, _, err = collector.addGroup(primaryGroup)
	if err != nil {
		return nil, err
	}
	seedGroups[strings.ToLower(primaryGroup.ObjectGUID)] = primaryGroup.ObjectGUID
	collector.enqueue(primaryGroup)

	for len(collector.queue) > 0 {
		child := collector.queue[0]
		collector.queue = collector.queue[1:]
		parents, err := collector.groupsContaining(ctx, child.DN)
		if err != nil {
			return nil, fmt.Errorf("query parents of group %q: %w", child.DN, err)
		}
		for _, parent := range parents {
			stored, added, err := collector.addGroup(parent)
			if err != nil {
				return nil, err
			}
			edge := GroupMembership{MemberGroupGUID: child.ObjectGUID, ParentGroupGUID: stored.ObjectGUID}
			edgeKey := strings.ToLower(edge.MemberGroupGUID) + "\x00" + strings.ToLower(edge.ParentGroupGUID)
			if _, duplicate := collector.edges[edgeKey]; duplicate {
				return nil, fmt.Errorf("%w: duplicate live group edge %q -> %q", ErrInvalidMembershipGraph, child.ObjectGUID, stored.ObjectGUID)
			}
			collector.edges[edgeKey] = edge
			if added {
				collector.enqueue(stored)
			}
		}
	}

	groupGUIDs := make([]string, 0, len(collector.groupsByGUID))
	for _, group := range collector.groupsByGUID {
		groupGUIDs = append(groupGUIDs, group.ObjectGUID)
	}
	edges := make([]GroupMembership, 0, len(collector.edges))
	for _, edge := range collector.edges {
		edges = append(edges, edge)
	}
	// A synthetic root collapses all direct/primary origins into one traversal.
	// Login only compares the effective GUID set, so expanding once avoids the
	// O(seed*graph) provenance rows required by the full synchronization path.
	const syntheticRoot = "__weknora_live_login_root__"
	groupGUIDs = append(groupGUIDs, syntheticRoot)
	for _, seedGUID := range seedGroups {
		edges = append(edges, GroupMembership{MemberGroupGUID: syntheticRoot, ParentGroupGUID: seedGUID})
	}
	effective, err := ComputeEffectiveMemberships(
		[]string{user.ObjectGUID},
		groupGUIDs,
		[]UserGroupMembership{{UserGUID: user.ObjectGUID, GroupGUID: syntheticRoot, Source: MembershipDirect}},
		edges,
	)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(effective))
	result := make([]string, 0, len(effective))
	for _, membership := range effective {
		if membership.GroupGUID == syntheticRoot {
			continue
		}
		key := strings.ToLower(membership.GroupGUID)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, membership.GroupGUID)
	}
	sort.Strings(result)
	return result, nil
}

func escapeBinaryFilterValue(value []byte) string {
	const hex = "0123456789abcdef"
	var escaped strings.Builder
	escaped.Grow(len(value) * 3)
	for _, b := range value {
		escaped.WriteByte('\\')
		escaped.WriteByte(hex[b>>4])
		escaped.WriteByte(hex[b&0x0f])
	}
	return escaped.String()
}

type liveMembershipCollector struct {
	adapter         *Adapter
	conn            ldapConnection
	groupsByGUID    map[string]Group
	guidBySID       map[string]string
	guidByDN        map[string]string
	edges           map[string]GroupMembership
	queue           []Group
	queued          map[string]struct{}
	responseEntries int
}

func (c *liveMembershipCollector) groupsContaining(ctx context.Context, memberDN string) ([]Group, error) {
	filter := fmt.Sprintf("(&%s(member=%s))", c.adapter.config.GroupFilter, ldap.EscapeFilter(memberDN))
	entries, err := c.adapter.searchPaged(ctx, c.conn, c.adapter.config.GroupBaseDN, filter, liveGroupIdentityAttributes)
	if err != nil {
		return nil, err
	}
	c.responseEntries += len(entries)
	if c.responseEntries > c.adapter.config.ResultLimit {
		return nil, fmt.Errorf("%w: aggregate live group result limit %d exceeded", ErrIncompleteResults, c.adapter.config.ResultLimit)
	}
	result := make([]Group, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		group, err := parseGroupIdentityEntry(entry)
		if err != nil {
			return nil, err
		}
		key := strings.ToLower(group.ObjectGUID)
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("%w: membership query repeated group %q", ErrDuplicateDirectoryObject, group.ObjectGUID)
		}
		seen[key] = struct{}{}
		result = append(result, group)
	}
	return result, nil
}

func (c *liveMembershipCollector) addGroup(group Group) (Group, bool, error) {
	guidKey := strings.ToLower(strings.TrimSpace(group.ObjectGUID))
	sidKey := strings.ToUpper(strings.TrimSpace(group.SID))
	dnKey := normalizeDN(group.DN)
	if existing, ok := c.groupsByGUID[guidKey]; ok {
		if !strings.EqualFold(existing.SID, group.SID) || normalizeDN(existing.DN) != dnKey {
			return Group{}, false, fmt.Errorf("%w: live group %q changed identity attributes", ErrDuplicateDirectoryObject, group.ObjectGUID)
		}
		return existing, false, nil
	}
	if owner, ok := c.guidBySID[sidKey]; ok {
		return Group{}, false, fmt.Errorf("%w: objectSid %q is shared by groups %q and %q", ErrDuplicateDirectoryObject, group.SID, owner, group.ObjectGUID)
	}
	if owner, ok := c.guidByDN[dnKey]; ok {
		return Group{}, false, fmt.Errorf("%w: DN %q is shared by groups %q and %q", ErrDuplicateDirectoryObject, group.DN, owner, group.ObjectGUID)
	}
	if len(c.groupsByGUID) >= c.adapter.config.ResultLimit {
		return Group{}, false, fmt.Errorf("%w: live group result limit %d exceeded", ErrIncompleteResults, c.adapter.config.ResultLimit)
	}
	c.groupsByGUID[guidKey] = group
	c.guidBySID[sidKey] = group.ObjectGUID
	c.guidByDN[dnKey] = group.ObjectGUID
	return group, true, nil
}

func (c *liveMembershipCollector) enqueue(group Group) {
	key := strings.ToLower(group.ObjectGUID)
	if _, exists := c.queued[key]; exists {
		return
	}
	c.queued[key] = struct{}{}
	c.queue = append(c.queue, group)
}
