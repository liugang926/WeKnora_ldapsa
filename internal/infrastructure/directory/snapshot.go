package directory

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-ldap/ldap/v3"
)

var userAttributes = []string{
	"objectGUID",
	"objectSid",
	"distinguishedName",
	"sAMAccountName",
	"userPrincipalName",
	"displayName",
	"name",
	"cn",
	"mail",
	"userAccountControl",
	"primaryGroupID",
}

var groupAttributes = []string{
	"objectGUID",
	"objectSid",
	"distinguishedName",
	"sAMAccountName",
	"name",
	"displayName",
	"cn",
	"mail",
	"member",
}

type parsedGroup struct {
	group   Group
	members []string
}

// Sync returns a complete, validated snapshot from one controller. Results
// from different controllers are never combined.
func (a *Adapter) Sync(ctx context.Context) (*Snapshot, error) {
	attempts := make([]ControllerAttempt, 0, len(a.config.Controllers))
	for _, controller := range a.config.Controllers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		conn, err := a.open(ctx, controller)
		if err != nil {
			attempts = append(attempts, ControllerAttempt{ControllerURL: controller.URL, Err: err})
			continue
		}
		snapshot, err := a.syncOnConnection(ctx, conn, controller.URL)
		conn.Close()
		if err == nil {
			return snapshot, nil
		}
		if isTerminalSyncError(err) {
			return nil, err
		}
		attempts = append(attempts, ControllerAttempt{ControllerURL: controller.URL, Err: contextOrError(ctx, err)})
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, &FailoverError{Operation: "synchronization", Attempts: attempts}
}

func (a *Adapter) syncOnConnection(
	ctx context.Context,
	conn ldapConnection,
	controllerURL string,
) (*Snapshot, error) {
	if err := conn.Bind(a.config.BindDN, a.config.BindPassword); err != nil {
		if isInvalidCredentials(err) {
			return nil, fmt.Errorf("%w: service account bind rejected", ErrInvalidServiceCredentials)
		}
		return nil, fmt.Errorf("service account bind: %w", err)
	}

	userEntries, err := a.searchPaged(ctx, conn, a.config.UserBaseDN, a.config.UserFilter, userAttributes)
	if err != nil {
		return nil, fmt.Errorf("search users: %w", err)
	}
	groupEntries, err := a.searchPaged(ctx, conn, a.config.GroupBaseDN, a.config.GroupFilter, groupAttributes)
	if err != nil {
		return nil, fmt.Errorf("search groups: %w", err)
	}

	users := make([]User, 0, len(userEntries))
	groups := make([]parsedGroup, 0, len(groupEntries))
	guids := make(map[string]string, len(userEntries)+len(groupEntries))
	sids := make(map[string]string, len(userEntries)+len(groupEntries))
	dns := make(map[string]string, len(userEntries)+len(groupEntries))
	for _, entry := range userEntries {
		user, err := parseUserEntry(entry)
		if err != nil {
			return nil, err
		}
		if err := recordIdentity(guids, sids, dns, user.ObjectGUID, user.SID, user.DN, "user"); err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	for _, entry := range groupEntries {
		group, err := a.parseGroupEntry(ctx, conn, entry)
		if err != nil {
			return nil, err
		}
		if err := recordIdentity(guids, sids, dns, group.group.ObjectGUID, group.group.SID, group.group.DN, "group"); err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}

	snapshot, err := buildSnapshot(a.config.DirectoryID, controllerURL, a.now(), users, groups)
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (a *Adapter) searchPaged(
	ctx context.Context,
	conn ldapConnection,
	baseDN string,
	filter string,
	attributes []string,
) ([]*ldap.Entry, error) {
	entries := make([]*ldap.Entry, 0)
	cookie := []byte(nil)
	seenCookies := make(map[string]struct{})
	for page := 0; page < a.config.MaxPages; page++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		paging := ldap.NewControlPaging(a.config.PageSize)
		paging.SetCookie(cookie)
		request := ldap.NewSearchRequest(
			baseDN,
			ldap.ScopeWholeSubtree,
			ldap.NeverDerefAliases,
			0,
			queryTimeLimit(a.config.QueryTimeout),
			false,
			filter,
			attributes,
			domainSearchControls(paging),
		)
		result, err := conn.Search(request)
		if err != nil {
			return nil, contextOrError(ctx, err)
		}
		if result == nil {
			return nil, fmt.Errorf("%w: server returned no search result object", ErrIncompleteResults)
		}
		if len(result.Referrals) != 0 {
			return nil, fmt.Errorf("%w: LDAP referrals were not followed", ErrIncompleteResults)
		}
		if len(result.Entries) > int(a.config.PageSize) {
			return nil, fmt.Errorf("%w: server exceeded requested page size", ErrIncompleteResults)
		}
		if len(entries)+len(result.Entries) > a.config.ResultLimit {
			return nil, fmt.Errorf("%w: result limit %d exceeded", ErrIncompleteResults, a.config.ResultLimit)
		}
		entries = append(entries, result.Entries...)

		control := ldap.FindControl(result.Controls, ldap.ControlTypePaging)
		responsePaging, ok := control.(*ldap.ControlPaging)
		if !ok || responsePaging == nil {
			return nil, fmt.Errorf("%w: server omitted paging response control", ErrIncompleteResults)
		}
		nextCookie := responsePaging.Cookie
		if len(nextCookie) == 0 {
			return entries, nil
		}
		if len(result.Entries) == 0 {
			return nil, fmt.Errorf("%w: paging made no progress", ErrIncompleteResults)
		}
		cookieKey := string(nextCookie)
		if _, duplicate := seenCookies[cookieKey]; duplicate {
			return nil, fmt.Errorf("%w: server repeated a paging cookie", ErrIncompleteResults)
		}
		seenCookies[cookieKey] = struct{}{}
		cookie = append(cookie[:0], nextCookie...)
	}
	return nil, fmt.Errorf("%w: page limit %d exceeded", ErrIncompleteResults, a.config.MaxPages)
}

func parseUserEntry(entry *ldap.Entry) (User, error) {
	guid, err := entryObjectGUID(entry)
	if err != nil {
		return User{}, err
	}
	sid, err := entryObjectSID(entry)
	if err != nil {
		return User{}, err
	}
	dn, err := entryDN(entry)
	if err != nil {
		return User{}, err
	}
	sam := strings.TrimSpace(entry.GetAttributeValue("sAMAccountName"))
	upn := strings.TrimSpace(entry.GetAttributeValue("userPrincipalName"))
	if sam == "" && upn == "" {
		return User{}, fmt.Errorf("%w: user %q has neither sAMAccountName nor UPN", ErrInvalidDirectoryObject, dn)
	}
	uac, err := parseUintAttribute(entry, "userAccountControl", 32, true)
	if err != nil {
		return User{}, err
	}
	primaryGroupID, err := parseUintAttribute(entry, "primaryGroupID", 32, true)
	if err != nil || primaryGroupID == 0 {
		if err == nil {
			err = fmt.Errorf("attribute is zero")
		}
		return User{}, fmt.Errorf("%w: user %q primaryGroupID: %v", ErrInvalidDirectoryObject, dn, err)
	}
	return User{
		ObjectGUID:        guid,
		SID:               sid,
		DN:                dn,
		SAMAccountName:    sam,
		UserPrincipalName: upn,
		DisplayName:       directoryDisplayName(entry),
		Email:             strings.TrimSpace(entry.GetAttributeValue("mail")),
		Enabled:           uac&2 == 0,
		PrimaryGroupRID:   uint32(primaryGroupID),
	}, nil
}

func (a *Adapter) parseGroupEntry(
	ctx context.Context,
	conn ldapConnection,
	entry *ldap.Entry,
) (parsedGroup, error) {
	group, err := parseGroupIdentityEntry(entry)
	if err != nil {
		return parsedGroup{}, err
	}
	members, err := a.readAllMembers(ctx, conn, entry)
	if err != nil {
		return parsedGroup{}, fmt.Errorf("group %q members: %w", group.DN, err)
	}
	return parsedGroup{group: group, members: members}, nil
}

func parseGroupIdentityEntry(entry *ldap.Entry) (Group, error) {
	guid, err := entryObjectGUID(entry)
	if err != nil {
		return Group{}, err
	}
	sid, err := entryObjectSID(entry)
	if err != nil {
		return Group{}, err
	}
	dn, err := entryDN(entry)
	if err != nil {
		return Group{}, err
	}
	displayName := directoryDisplayName(entry)
	return Group{
		ObjectGUID:     guid,
		SID:            sid,
		DN:             dn,
		SAMAccountName: strings.TrimSpace(entry.GetAttributeValue("sAMAccountName")),
		DisplayName:    displayName,
		Email:          strings.TrimSpace(entry.GetAttributeValue("mail")),
	}, nil
}

func entryObjectGUID(entry *ldap.Entry) (string, error) {
	raw := rawAttribute(entry, "objectGUID")
	if len(raw) != 16 || bytes.Equal(raw, make([]byte, 16)) {
		return "", fmt.Errorf("%w: entry %q has missing or invalid objectGUID", ErrInvalidDirectoryObject, entry.DN)
	}
	return ParseObjectGUID(raw)
}

func entryObjectSID(entry *ldap.Entry) (string, error) {
	raw := rawAttribute(entry, "objectSid")
	sid, err := ParseObjectSID(raw)
	if err != nil {
		return "", fmt.Errorf("entry %q: %w", entry.DN, err)
	}
	return sid.String(), nil
}

func rawAttribute(entry *ldap.Entry, name string) []byte {
	for _, attribute := range entry.Attributes {
		if !strings.EqualFold(attribute.Name, name) {
			continue
		}
		if len(attribute.ByteValues) > 0 {
			return attribute.ByteValues[0]
		}
		if len(attribute.Values) > 0 {
			return []byte(attribute.Values[0])
		}
	}
	return nil
}

func entryDN(entry *ldap.Entry) (string, error) {
	dn := strings.TrimSpace(entry.DN)
	if dn == "" {
		dn = strings.TrimSpace(entry.GetAttributeValue("distinguishedName"))
	}
	if dn == "" {
		return "", fmt.Errorf("%w: entry has no distinguished name", ErrInvalidDirectoryObject)
	}
	if _, err := ldap.ParseDN(dn); err != nil {
		return "", fmt.Errorf("%w: malformed DN %q", ErrInvalidDirectoryObject, dn)
	}
	return dn, nil
}

func parseUintAttribute(entry *ldap.Entry, name string, bitSize int, required bool) (uint64, error) {
	value := strings.TrimSpace(entry.GetAttributeValue(name))
	if value == "" {
		if required {
			return 0, fmt.Errorf("%w: entry %q has no %s", ErrInvalidDirectoryObject, entry.DN, name)
		}
		return 0, nil
	}
	parsed, err := strconv.ParseUint(value, 10, bitSize)
	if err != nil {
		return 0, fmt.Errorf("%w: entry %q has invalid %s", ErrInvalidDirectoryObject, entry.DN, name)
	}
	return parsed, nil
}

func recordIdentity(
	guids map[string]string,
	sids map[string]string,
	dns map[string]string,
	guid string,
	sid string,
	dn string,
	kind string,
) error {
	checks := []struct {
		index map[string]string
		key   string
		name  string
	}{
		{guids, strings.ToLower(guid), "objectGUID"},
		{sids, strings.ToUpper(sid), "objectSid"},
		{dns, normalizeDN(dn), "DN"},
	}
	for _, check := range checks {
		if prior, exists := check.index[check.key]; exists {
			return fmt.Errorf("%w: %s %q is shared by %s and %s", ErrDuplicateDirectoryObject, check.name, check.key, prior, kind)
		}
		check.index[check.key] = kind
	}
	return nil
}

func normalizeDN(dn string) string {
	return strings.ToLower(strings.TrimSpace(dn))
}

func buildSnapshot(
	directoryID string,
	controllerURL string,
	completedAt time.Time,
	users []User,
	parsedGroups []parsedGroup,
) (*Snapshot, error) {
	userByDN := make(map[string]User, len(users))
	groupByDN := make(map[string]Group, len(parsedGroups))
	groupBySID := make(map[string]Group, len(parsedGroups))
	groups := make([]Group, 0, len(parsedGroups))
	userGUIDs := make([]string, 0, len(users))
	groupGUIDs := make([]string, 0, len(parsedGroups))
	for _, user := range users {
		userByDN[normalizeDN(user.DN)] = user
		userGUIDs = append(userGUIDs, user.ObjectGUID)
	}
	for _, parsed := range parsedGroups {
		group := parsed.group
		groups = append(groups, group)
		groupByDN[normalizeDN(group.DN)] = group
		groupBySID[strings.ToUpper(group.SID)] = group
		groupGUIDs = append(groupGUIDs, group.ObjectGUID)
	}

	seeds := make([]UserGroupMembership, 0)
	nesting := make([]GroupMembership, 0)
	unresolved := make([]UnresolvedMember, 0)
	for _, parsed := range parsedGroups {
		seenMembers := make(map[string]struct{}, len(parsed.members))
		for _, memberDN := range parsed.members {
			normalized := normalizeDN(memberDN)
			if normalized == "" {
				return nil, fmt.Errorf("%w: group %q has an empty member DN", ErrInvalidDirectoryObject, parsed.group.DN)
			}
			if _, duplicate := seenMembers[normalized]; duplicate {
				return nil, fmt.Errorf("%w: group %q repeats member %q", ErrDuplicateDirectoryObject, parsed.group.DN, memberDN)
			}
			seenMembers[normalized] = struct{}{}
			if user, ok := userByDN[normalized]; ok {
				seeds = append(seeds, UserGroupMembership{
					UserGUID: user.ObjectGUID, GroupGUID: parsed.group.ObjectGUID, Source: MembershipDirect,
				})
				continue
			}
			if group, ok := groupByDN[normalized]; ok {
				nesting = append(nesting, GroupMembership{
					MemberGroupGUID: group.ObjectGUID, ParentGroupGUID: parsed.group.ObjectGUID,
				})
				continue
			}
			unresolved = append(unresolved, UnresolvedMember{
				ParentGroupGUID: parsed.group.ObjectGUID,
				MemberDN:        memberDN,
			})
		}
	}
	for _, user := range users {
		primarySID, err := PrimaryGroupSID(user.SID, user.PrimaryGroupRID)
		if err != nil {
			return nil, fmt.Errorf("user %q primary group: %w", user.DN, err)
		}
		group, ok := groupBySID[strings.ToUpper(primarySID)]
		if !ok {
			return nil, fmt.Errorf("%w: primary group %q for user %q is outside the group snapshot", ErrIncompleteResults, primarySID, user.DN)
		}
		seeds = append(seeds, UserGroupMembership{
			UserGUID: user.ObjectGUID, GroupGUID: group.ObjectGUID, Source: MembershipPrimary,
		})
	}

	effective, err := ComputeEffectiveMemberships(userGUIDs, groupGUIDs, seeds, nesting)
	if err != nil {
		return nil, err
	}
	sort.Slice(users, func(i, j int) bool { return users[i].ObjectGUID < users[j].ObjectGUID })
	sort.Slice(groups, func(i, j int) bool { return groups[i].ObjectGUID < groups[j].ObjectGUID })
	sort.Slice(seeds, func(i, j int) bool {
		if seeds[i].UserGUID != seeds[j].UserGUID {
			return seeds[i].UserGUID < seeds[j].UserGUID
		}
		if seeds[i].GroupGUID != seeds[j].GroupGUID {
			return seeds[i].GroupGUID < seeds[j].GroupGUID
		}
		return seeds[i].Source < seeds[j].Source
	})
	sort.Slice(nesting, func(i, j int) bool {
		if nesting[i].MemberGroupGUID != nesting[j].MemberGroupGUID {
			return nesting[i].MemberGroupGUID < nesting[j].MemberGroupGUID
		}
		return nesting[i].ParentGroupGUID < nesting[j].ParentGroupGUID
	})
	sort.Slice(unresolved, func(i, j int) bool {
		if unresolved[i].ParentGroupGUID != unresolved[j].ParentGroupGUID {
			return unresolved[i].ParentGroupGUID < unresolved[j].ParentGroupGUID
		}
		return unresolved[i].MemberDN < unresolved[j].MemberDN
	})
	return &Snapshot{
		DirectoryID:          directoryID,
		ControllerURL:        controllerURL,
		CompletedAt:          completedAt.UTC(),
		Users:                users,
		Groups:               groups,
		DirectMemberships:    seeds,
		GroupMemberships:     nesting,
		EffectiveMemberships: effective,
		UnresolvedMembers:    unresolved,
	}, nil
}

type memberRange struct {
	start    int
	end      int
	terminal bool
	values   []string
}

func (a *Adapter) readAllMembers(
	ctx context.Context,
	conn ldapConnection,
	entry *ldap.Entry,
) ([]string, error) {
	exact, ranged, err := memberAttributes(entry)
	if err != nil {
		return nil, err
	}
	if exact != nil {
		return validateMemberValues(exact, a.config.ResultLimit)
	}
	if ranged == nil {
		return []string{}, nil
	}
	if ranged.start != 0 {
		return nil, fmt.Errorf("%w: first member range starts at %d", ErrIncompleteResults, ranged.start)
	}
	values := append([]string(nil), ranged.values...)
	if len(values) > a.config.ResultLimit {
		return nil, fmt.Errorf("%w: group member result limit exceeded", ErrIncompleteResults)
	}
	current := ranged
	for page := 1; !current.terminal; page++ {
		if page >= a.config.MaxPages {
			return nil, fmt.Errorf("%w: group member range page limit exceeded", ErrIncompleteResults)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		next := current.end + 1
		attribute := fmt.Sprintf("member;range=%d-*", next)
		request := ldap.NewSearchRequest(
			entry.DN,
			ldap.ScopeBaseObject,
			ldap.NeverDerefAliases,
			2,
			queryTimeLimit(a.config.QueryTimeout),
			false,
			"(objectClass=*)",
			[]string{attribute},
			domainSearchControls(),
		)
		result, err := conn.Search(request)
		if err != nil {
			return nil, contextOrError(ctx, err)
		}
		if result == nil {
			return nil, fmt.Errorf("%w: missing member range response", ErrIncompleteResults)
		}
		if len(result.Referrals) != 0 || len(result.Entries) != 1 {
			return nil, fmt.Errorf("%w: malformed member range response", ErrIncompleteResults)
		}
		if normalizeDN(result.Entries[0].DN) != normalizeDN(entry.DN) {
			return nil, fmt.Errorf("%w: member range response changed the group DN", ErrIncompleteResults)
		}
		exact, following, err := memberAttributes(result.Entries[0])
		if err != nil {
			return nil, err
		}
		if exact != nil || following == nil || following.start != next {
			return nil, fmt.Errorf("%w: member range did not continue at %d", ErrIncompleteResults, next)
		}
		current = following
		if len(values)+len(current.values) > a.config.ResultLimit {
			return nil, fmt.Errorf("%w: group member result limit exceeded", ErrIncompleteResults)
		}
		values = append(values, current.values...)
	}
	return validateMemberValues(values, a.config.ResultLimit)
}

func memberAttributes(entry *ldap.Entry) ([]string, *memberRange, error) {
	var exact []string
	var ranged *memberRange
	for _, attribute := range entry.Attributes {
		name := strings.ToLower(strings.TrimSpace(attribute.Name))
		switch {
		case name == "member":
			if exact != nil || ranged != nil {
				return nil, nil, fmt.Errorf("%w: conflicting member attributes", ErrIncompleteResults)
			}
			exact = append([]string{}, attribute.Values...)
		case strings.HasPrefix(name, "member;range="):
			if exact != nil || ranged != nil {
				return nil, nil, fmt.Errorf("%w: multiple member ranges in one entry", ErrIncompleteResults)
			}
			parsed, err := parseMemberRange(name, attribute.Values)
			if err != nil {
				return nil, nil, err
			}
			ranged = &parsed
		}
	}
	return exact, ranged, nil
}

func parseMemberRange(name string, values []string) (memberRange, error) {
	value := strings.TrimPrefix(name, "member;range=")
	parts := strings.Split(value, "-")
	if len(parts) != 2 {
		return memberRange{}, fmt.Errorf("%w: malformed member range %q", ErrIncompleteResults, name)
	}
	start, err := strconv.Atoi(parts[0])
	if err != nil || start < 0 {
		return memberRange{}, fmt.Errorf("%w: malformed member range start %q", ErrIncompleteResults, name)
	}
	rangeValue := memberRange{start: start, values: append([]string(nil), values...)}
	if parts[1] == "*" {
		rangeValue.end = start + len(values) - 1
		rangeValue.terminal = true
		return rangeValue, nil
	}
	end, err := strconv.Atoi(parts[1])
	if err != nil || end < start || len(values) != end-start+1 {
		return memberRange{}, fmt.Errorf("%w: inconsistent member range %q", ErrIncompleteResults, name)
	}
	rangeValue.end = end
	return rangeValue, nil
}

func validateMemberValues(values []string, limit int) ([]string, error) {
	if len(values) > limit {
		return nil, fmt.Errorf("%w: group member result limit exceeded", ErrIncompleteResults)
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		normalized := normalizeDN(value)
		if normalized == "" {
			return nil, fmt.Errorf("%w: group has an empty member DN", ErrInvalidDirectoryObject)
		}
		if _, duplicate := seen[normalized]; duplicate {
			return nil, fmt.Errorf("%w: repeated group member %q", ErrDuplicateDirectoryObject, value)
		}
		seen[normalized] = struct{}{}
	}
	return append([]string(nil), values...), nil
}
