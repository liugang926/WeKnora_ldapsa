package directory

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/go-ldap/ldap/v3"
	"github.com/stretchr/testify/require"
)

type bindCall struct {
	username string
	password string
}

type fakeSearchResponse struct {
	result *ldap.SearchResult
	err    error
}

type fakeLDAPConnection struct {
	bindCalls       []bindCall
	bindErrors      []error
	searchRequests  []*ldap.SearchRequest
	searchResponses []fakeSearchResponse
	closed          bool
}

func (f *fakeLDAPConnection) Bind(username, password string) error {
	f.bindCalls = append(f.bindCalls, bindCall{username: username, password: password})
	index := len(f.bindCalls) - 1
	if index < len(f.bindErrors) {
		return f.bindErrors[index]
	}
	return nil
}

func (f *fakeLDAPConnection) Search(request *ldap.SearchRequest) (*ldap.SearchResult, error) {
	f.searchRequests = append(f.searchRequests, request)
	index := len(f.searchRequests) - 1
	if index >= len(f.searchResponses) {
		return nil, fmt.Errorf("unexpected search call %d", index+1)
	}
	response := f.searchResponses[index]
	return response.result, response.err
}

func (f *fakeLDAPConnection) Close() {
	f.closed = true
}

func TestAuthenticateRejectsEmptyPasswordBeforeConnecting(t *testing.T) {
	adapter := testAdapter(t, "ldaps://dc1.example.test:636")
	dials := 0
	adapter.dial = func(context.Context, Controller, *tls.Config, Config) (ldapConnection, error) {
		dials++
		return nil, errors.New("must not dial")
	}
	_, err := adapter.Authenticate(context.Background(), "alice", "")
	require.ErrorIs(t, err, ErrEmptyPassword)
	require.Zero(t, dials)
}

func TestAuthenticateEscapesFilterAndBindsResolvedDN(t *testing.T) {
	connection := &fakeLDAPConnection{searchResponses: successfulAuthenticationResponses(testUserEntry("alice", 1107, 513), 513)}
	adapter := testAdapter(t, "ldaps://dc1.example.test:636")
	adapter.dial = singleConnectionDialer(connection)

	identifier := `*)(sAMAccountName=*)`
	result, err := adapter.Authenticate(context.Background(), identifier, "secret")
	require.NoError(t, err)
	require.Equal(t, "ldaps://dc1.example.test:636", result.ControllerURL)
	require.Equal(t, []bindCall{
		{username: "CN=svc,DC=example,DC=test", password: "service-secret"},
		{username: "CN=alice,OU=Users,DC=example,DC=test", password: "secret"},
		{username: "CN=svc,DC=example,DC=test", password: "service-secret"},
	}, connection.bindCalls)
	require.Len(t, connection.searchRequests, 4)
	for _, request := range connection.searchRequests {
		control := ldap.FindControl(request.Controls, domainScopeControlOID)
		require.NotNil(t, control, "all login and membership searches must stay in one domain")
		stringControl, ok := control.(*ldap.ControlString)
		require.True(t, ok)
		require.False(t, stringControl.Criticality, "non-AD test servers may ignore domain scope")
		require.Empty(t, stringControl.ControlValue)
	}
	filter := connection.searchRequests[0].Filter
	require.NotContains(t, filter, identifier)
	require.Contains(t, filter, ldap.EscapeFilter(identifier))
	require.Contains(t, filter, "sAMAccountName=")
	require.Contains(t, filter, "userPrincipalName=")
	require.True(t, connection.closed)
}

func TestAuthenticateCustomLoginFilterIsEscapedAndScoped(t *testing.T) {
	connection := &fakeLDAPConnection{searchResponses: successfulAuthenticationResponses(testUserEntry("alice", 1107, 513), 513)}
	adapter := testAdapter(t, "ldaps://dc1.example.test:636")
	adapter.config.UserFilter = "(&(objectClass=user)(department=engineering))"
	adapter.config.LoginFilter = "(|(employeeID={login})(userPrincipalName={login}))"
	adapter.dial = singleConnectionDialer(connection)

	identifier := `*)(department=*)`
	_, err := adapter.Authenticate(context.Background(), identifier, "secret")
	require.NoError(t, err)
	require.Len(t, connection.searchRequests, 4)
	filter := connection.searchRequests[0].Filter
	require.Contains(t, filter, adapter.config.UserFilter)
	require.NotContains(t, filter, identifier)
	require.Equal(t,
		"(&(&(objectClass=user)(department=engineering))(|(employeeID="+ldap.EscapeFilter(identifier)+")(userPrincipalName="+ldap.EscapeFilter(identifier)+")))",
		filter,
	)
}

func TestNewAdapterRejectsUnsafeLoginFilterTemplates(t *testing.T) {
	base := Config{
		DirectoryID: "directory",
		Controllers: []Controller{{URL: "ldaps://dc.example.test", TLSMode: TLSModeLDAPS}},
		BindDN:      "CN=svc,DC=example,DC=test", BindPassword: "service-secret",
		UserBaseDN: "DC=example,DC=test", GroupBaseDN: "DC=example,DC=test",
	}

	missing := base
	missing.LoginFilter = "(sAMAccountName=alice)"
	_, err := NewAdapter(missing)
	require.ErrorContains(t, err, "must contain {login}")

	invalid := base
	invalid.LoginFilter = "(&(sAMAccountName={login})"
	_, err = NewAdapter(invalid)
	require.ErrorContains(t, err, "invalid directory login filter")
}

func TestAuthenticateInvalidCredentialsDoesNotTryAnotherController(t *testing.T) {
	first := &fakeLDAPConnection{
		bindErrors: []error{nil, ldap.NewError(ldap.LDAPResultInvalidCredentials, errors.New("bad password"))},
		searchResponses: []fakeSearchResponse{{result: &ldap.SearchResult{
			Entries: []*ldap.Entry{testUserEntry("alice", 1107, 513)},
		}}},
	}
	second := &fakeLDAPConnection{}
	adapter := testAdapter(t, "ldaps://dc1.example.test:636", "ldaps://dc2.example.test:636")
	dials := 0
	adapter.dial = func(_ context.Context, controller Controller, _ *tls.Config, _ Config) (ldapConnection, error) {
		dials++
		if controller.URL == "ldaps://dc1.example.test:636" {
			return first, nil
		}
		return second, nil
	}

	_, err := adapter.Authenticate(context.Background(), "alice", "wrong")
	require.ErrorIs(t, err, ErrInvalidCredentials)
	require.Equal(t, 1, dials)
	require.Empty(t, second.bindCalls)
}

func TestAuthenticateFailsOverOperationalFailureInOrder(t *testing.T) {
	connection := &fakeLDAPConnection{searchResponses: successfulAuthenticationResponses(testUserEntry("alice", 1107, 513), 513)}
	adapter := testAdapter(t, "ldaps://dc1.example.test:636", "ldaps://dc2.example.test:636")
	var order []string
	adapter.dial = func(_ context.Context, controller Controller, _ *tls.Config, _ Config) (ldapConnection, error) {
		order = append(order, controller.URL)
		if len(order) == 1 {
			return nil, errors.New("connection refused")
		}
		return connection, nil
	}

	result, err := adapter.Authenticate(context.Background(), "alice@example.test", "secret")
	require.NoError(t, err)
	require.Equal(t, "ldaps://dc2.example.test:636", result.ControllerURL)
	require.Equal(t, []string{"ldaps://dc1.example.test:636", "ldaps://dc2.example.test:636"}, order)
}

func TestAuthenticateVerifiesDirectPrimaryAndNestedGroupsOnSameController(t *testing.T) {
	user := testUserEntry("alice", 1107, 513)
	direct := testGroupEntry("Engineering", 41, 2000, user.DN)
	primary := testGroupEntry("Domain Users", 42, 513)
	parent := testGroupEntry("Staff", 43, 2001, direct.DN)
	connection := &fakeLDAPConnection{searchResponses: []fakeSearchResponse{
		{result: &ldap.SearchResult{Entries: []*ldap.Entry{user}}},
		{result: pagedResult([]*ldap.Entry{direct}, nil)},
		{result: pagedResult([]*ldap.Entry{primary}, nil)},
		{result: pagedResult([]*ldap.Entry{parent}, nil)},
		{result: pagedResult(nil, nil)},
		{result: pagedResult(nil, nil)},
	}}
	adapter := testAdapter(t, "ldaps://dc1.example.test:636")
	adapter.dial = singleConnectionDialer(connection)

	result, err := adapter.Authenticate(context.Background(), "alice", "secret")
	require.NoError(t, err)
	directGUID, err := ParseObjectGUID(testGUIDBytes(41))
	require.NoError(t, err)
	primaryGUID, err := ParseObjectGUID(testGUIDBytes(42))
	require.NoError(t, err)
	parentGUID, err := ParseObjectGUID(testGUIDBytes(43))
	require.NoError(t, err)
	want := []string{directGUID, primaryGUID, parentGUID}
	sort.Strings(want)
	require.Equal(t, want, result.EffectiveGroupObjectGUIDs)
	require.Equal(t, []bindCall{
		{username: "CN=svc,DC=example,DC=test", password: "service-secret"},
		{username: user.DN, password: "secret"},
		{username: "CN=svc,DC=example,DC=test", password: "service-secret"},
	}, connection.bindCalls)
	require.Contains(t, connection.searchRequests[1].Filter, "(objectClass=group)")
	require.Contains(t, connection.searchRequests[1].Filter, "(member="+ldap.EscapeFilter(user.DN)+")")
	require.Contains(t, connection.searchRequests[2].Filter, "(objectSid="+escapeBinaryFilterValue(testSIDBytes(513))+")")
}

func TestAuthenticateLiveMembershipCycleFailsClosed(t *testing.T) {
	user := testUserEntry("alice", 1107, 513)
	groupA := testGroupEntry("A", 51, 2000, user.DN)
	primary := testGroupEntry("Domain Users", 52, 513)
	groupB := testGroupEntry("B", 53, 2001, groupA.DN)
	groupAAsParent := testGroupEntry("A", 51, 2000, groupB.DN)
	connection := &fakeLDAPConnection{searchResponses: []fakeSearchResponse{
		{result: &ldap.SearchResult{Entries: []*ldap.Entry{user}}},
		{result: pagedResult([]*ldap.Entry{groupA}, nil)},
		{result: pagedResult([]*ldap.Entry{primary}, nil)},
		{result: pagedResult([]*ldap.Entry{groupB}, nil)},
		{result: pagedResult(nil, nil)},
		{result: pagedResult([]*ldap.Entry{groupAAsParent}, nil)},
	}}
	adapter := testAdapter(t, "ldaps://dc1.example.test:636")
	adapter.dial = singleConnectionDialer(connection)

	_, err := adapter.Authenticate(context.Background(), "alice", "secret")
	require.ErrorIs(t, err, ErrMembershipCycle)
}

func TestAuthenticateRetriesCompleteFlowAfterLiveMembershipOperationalFailure(t *testing.T) {
	user := testUserEntry("alice", 1107, 513)
	first := &fakeLDAPConnection{searchResponses: []fakeSearchResponse{
		{result: &ldap.SearchResult{Entries: []*ldap.Entry{user}}},
		{result: &ldap.SearchResult{}}, // Missing mandatory paging response control.
	}}
	second := &fakeLDAPConnection{searchResponses: successfulAuthenticationResponses(user, 513)}
	adapter := testAdapter(t, "ldaps://dc1.example.test:636", "ldaps://dc2.example.test:636")
	dials := 0
	adapter.dial = func(_ context.Context, _ Controller, _ *tls.Config, _ Config) (ldapConnection, error) {
		dials++
		if dials == 1 {
			return first, nil
		}
		return second, nil
	}

	result, err := adapter.Authenticate(context.Background(), "alice", "secret")
	require.NoError(t, err)
	require.Equal(t, "ldaps://dc2.example.test:636", result.ControllerURL)
	require.Len(t, first.bindCalls, 3)
	require.Len(t, second.bindCalls, 3)
}

func TestAuthenticateInvalidServiceRebindIsTerminal(t *testing.T) {
	connection := &fakeLDAPConnection{
		bindErrors:      []error{nil, nil, ldap.NewError(ldap.LDAPResultInvalidCredentials, errors.New("service password changed"))},
		searchResponses: []fakeSearchResponse{{result: &ldap.SearchResult{Entries: []*ldap.Entry{testUserEntry("alice", 1107, 513)}}}},
	}
	adapter := testAdapter(t, "ldaps://dc1.example.test:636", "ldaps://dc2.example.test:636")
	dials := 0
	adapter.dial = func(context.Context, Controller, *tls.Config, Config) (ldapConnection, error) {
		dials++
		return connection, nil
	}

	_, err := adapter.Authenticate(context.Background(), "alice", "secret")
	require.ErrorIs(t, err, ErrInvalidServiceCredentials)
	require.Equal(t, 1, dials)
}

func TestAuthenticateUserNotFoundIsTerminal(t *testing.T) {
	first := &fakeLDAPConnection{searchResponses: []fakeSearchResponse{{result: &ldap.SearchResult{}}}}
	adapter := testAdapter(t, "ldaps://dc1.example.test:636", "ldaps://dc2.example.test:636")
	dials := 0
	adapter.dial = func(context.Context, Controller, *tls.Config, Config) (ldapConnection, error) {
		dials++
		return first, nil
	}
	_, err := adapter.Authenticate(context.Background(), "missing", "secret")
	require.ErrorIs(t, err, ErrUserNotFound)
	require.Equal(t, 1, dials)
}

func TestSearchPagedRequiresCompleteProgressingPages(t *testing.T) {
	t.Run("two complete pages", func(t *testing.T) {
		connection := &fakeLDAPConnection{searchResponses: []fakeSearchResponse{
			{result: pagedResult([]*ldap.Entry{{DN: "CN=one"}}, []byte("next"))},
			{result: pagedResult([]*ldap.Entry{{DN: "CN=two"}}, nil)},
		}}
		adapter := testAdapter(t, "ldaps://dc.example.test")
		adapter.config.PageSize = 1
		entries, err := adapter.searchPaged(context.Background(), connection, "DC=example,DC=test", "(objectClass=*)", []string{"dn"})
		require.NoError(t, err)
		require.Len(t, entries, 2)
		require.Len(t, connection.searchRequests, 2)
		for _, request := range connection.searchRequests {
			require.NotNil(t, ldap.FindControl(request.Controls, domainScopeControlOID))
			require.NotNil(t, ldap.FindControl(request.Controls, ldap.ControlTypePaging))
		}
		secondControl := ldap.FindControl(connection.searchRequests[1].Controls, ldap.ControlTypePaging)
		require.Equal(t, []byte("next"), secondControl.(*ldap.ControlPaging).Cookie)
	})

	tests := []struct {
		name      string
		responses []fakeSearchResponse
		limit     int
	}{
		{"missing paging control", []fakeSearchResponse{{result: &ldap.SearchResult{}}}, 100},
		{"no progress", []fakeSearchResponse{{result: pagedResult(nil, []byte("next"))}}, 100},
		{"repeated cookie", []fakeSearchResponse{
			{result: pagedResult([]*ldap.Entry{{DN: "CN=one"}}, []byte("same"))},
			{result: pagedResult([]*ldap.Entry{{DN: "CN=two"}}, []byte("same"))},
		}, 100},
		{"result limit", []fakeSearchResponse{{result: pagedResult([]*ldap.Entry{{DN: "one"}, {DN: "two"}}, nil)}}, 1},
		{"referral", []fakeSearchResponse{{result: &ldap.SearchResult{
			Referrals: []string{"ldap://other.example.test"},
			Controls:  []ldap.Control{ldap.NewControlPaging(1)},
		}}}, 100},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connection := &fakeLDAPConnection{searchResponses: test.responses}
			adapter := testAdapter(t, "ldaps://dc.example.test")
			adapter.config.PageSize = 10
			adapter.config.ResultLimit = test.limit
			_, err := adapter.searchPaged(context.Background(), connection, "DC=x", "(objectClass=*)", nil)
			require.ErrorIs(t, err, ErrIncompleteResults)
		})
	}
}

func TestStartTLSNegotiationHonorsConnectionTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()

	controller := Controller{URL: "ldap://" + listener.Addr().String(), TLSMode: TLSModeStartTLS}
	started := time.Now()
	_, err = defaultDialConnection(context.Background(), controller, &tls.Config{
		ServerName: "localhost", MinVersion: tls.VersionTLS12,
	}, Config{ConnectTimeout: 75 * time.Millisecond, QueryTimeout: 5 * time.Second})
	require.ErrorContains(t, err, "StartTLS negotiation exceeded connection timeout")
	require.Less(t, time.Since(started), time.Second)
	select {
	case conn := <-accepted:
		_ = conn.Close()
	default:
	}
}

func TestBuildSnapshotIncludesDirectNestedAndPrimaryMembership(t *testing.T) {
	user := User{
		ObjectGUID: "user", SID: "S-1-5-21-1-2-3-1107", DN: "CN=Alice,OU=Users,DC=example,DC=test", PrimaryGroupRID: 513,
	}
	groups := []parsedGroup{
		{group: Group{ObjectGUID: "domain-users", SID: "S-1-5-21-1-2-3-513", DN: "CN=Domain Users,OU=Groups,DC=example,DC=test"}},
		{group: Group{ObjectGUID: "engineering", SID: "S-1-5-21-1-2-3-2001", DN: "CN=Engineering,OU=Groups,DC=example,DC=test"}, members: []string{user.DN}},
		{group: Group{ObjectGUID: "staff", SID: "S-1-5-21-1-2-3-2002", DN: "CN=Staff,OU=Groups,DC=example,DC=test"}, members: []string{"CN=Engineering,OU=Groups,DC=example,DC=test"}},
	}
	snapshot, err := buildSnapshot("directory", "ldaps://dc", time.Unix(100, 0), []User{user}, groups)
	require.NoError(t, err)
	require.Len(t, snapshot.DirectMemberships, 2)
	require.Equal(t, []GroupMembership{{MemberGroupGUID: "engineering", ParentGroupGUID: "staff"}}, snapshot.GroupMemberships)
	require.Len(t, snapshot.EffectiveMemberships, 3)
	require.Equal(t, time.Unix(100, 0).UTC(), snapshot.CompletedAt)
}

func TestBuildSnapshotRejectsMissingPrimaryGroupAndCycle(t *testing.T) {
	user := User{ObjectGUID: "u", SID: "S-1-5-21-1-2-3-1000", DN: "CN=U,DC=x", PrimaryGroupRID: 513}
	_, err := buildSnapshot("d", "dc", time.Now(), []User{user}, nil)
	require.ErrorIs(t, err, ErrIncompleteResults)

	groups := []parsedGroup{
		{group: Group{ObjectGUID: "primary", SID: "S-1-5-21-1-2-3-513", DN: "CN=Primary,DC=x"}},
		{group: Group{ObjectGUID: "a", SID: "S-1-5-21-1-2-3-100", DN: "CN=A,DC=x"}, members: []string{"CN=B,DC=x"}},
		{group: Group{ObjectGUID: "b", SID: "S-1-5-21-1-2-3-101", DN: "CN=B,DC=x"}, members: []string{"CN=A,DC=x"}},
	}
	_, err = buildSnapshot("d", "dc", time.Now(), []User{user}, groups)
	require.ErrorIs(t, err, ErrMembershipCycle)
}

func TestReadAllMembersContinuesADRangesAndRejectsGap(t *testing.T) {
	entry := &ldap.Entry{DN: "CN=Large,DC=x", Attributes: []*ldap.EntryAttribute{
		{Name: "member;range=0-1", Values: []string{"CN=A,DC=x", "CN=B,DC=x"}},
	}}
	connection := &fakeLDAPConnection{searchResponses: []fakeSearchResponse{{result: &ldap.SearchResult{Entries: []*ldap.Entry{{
		DN:         "CN=Large,DC=x",
		Attributes: []*ldap.EntryAttribute{{Name: "member;range=2-*", Values: []string{"CN=C,DC=x"}}},
	}}}}}}
	adapter := testAdapter(t, "ldaps://dc.example.test")
	members, err := adapter.readAllMembers(context.Background(), connection, entry)
	require.NoError(t, err)
	require.Equal(t, []string{"CN=A,DC=x", "CN=B,DC=x", "CN=C,DC=x"}, members)
	require.Equal(t, []string{"member;range=2-*"}, connection.searchRequests[0].Attributes)

	gapConnection := &fakeLDAPConnection{searchResponses: []fakeSearchResponse{{result: &ldap.SearchResult{Entries: []*ldap.Entry{{
		DN:         "CN=Large,DC=x",
		Attributes: []*ldap.EntryAttribute{{Name: "member;range=3-*", Values: []string{"CN=C,DC=x"}}},
	}}}}}}
	_, err = adapter.readAllMembers(context.Background(), gapConnection, entry)
	require.ErrorIs(t, err, ErrIncompleteResults)
}

func testAdapter(t *testing.T, controllerURLs ...string) *Adapter {
	t.Helper()
	controllers := make([]Controller, 0, len(controllerURLs))
	for _, controllerURL := range controllerURLs {
		controllers = append(controllers, Controller{URL: controllerURL, TLSMode: TLSModeLDAPS})
	}
	adapter, err := NewAdapter(Config{
		DirectoryID: "directory",
		Controllers: controllers,
		BindDN:      "CN=svc,DC=example,DC=test", BindPassword: "service-secret",
		UserBaseDN: "OU=Users,DC=example,DC=test", GroupBaseDN: "OU=Groups,DC=example,DC=test",
	})
	require.NoError(t, err)
	return adapter
}

func singleConnectionDialer(connection ldapConnection) dialConnection {
	return func(context.Context, Controller, *tls.Config, Config) (ldapConnection, error) {
		return connection, nil
	}
}

func testUserEntry(account string, rid uint32, primaryGroupRID uint32) *ldap.Entry {
	dn := fmt.Sprintf("CN=%s,OU=Users,DC=example,DC=test", account)
	return &ldap.Entry{DN: dn, Attributes: []*ldap.EntryAttribute{
		rawTestAttribute("objectGUID", testGUIDBytes(byte(rid))),
		rawTestAttribute("objectSid", testSIDBytes(rid)),
		{Name: "distinguishedName", Values: []string{dn}},
		{Name: "sAMAccountName", Values: []string{account}},
		{Name: "userPrincipalName", Values: []string{account + "@example.test"}},
		{Name: "displayName", Values: []string{account}},
		{Name: "mail", Values: []string{account + "@example.test"}},
		{Name: "userAccountControl", Values: []string{"512"}},
		{Name: "primaryGroupID", Values: []string{fmt.Sprint(primaryGroupRID)}},
	}}
}

func rawTestAttribute(name string, value []byte) *ldap.EntryAttribute {
	return &ldap.EntryAttribute{Name: name, Values: []string{string(value)}, ByteValues: [][]byte{value}}
}

func testGUIDBytes(seed byte) []byte {
	return []byte{seed, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
}

func pagedResult(entries []*ldap.Entry, cookie []byte) *ldap.SearchResult {
	paging := ldap.NewControlPaging(10)
	paging.SetCookie(cookie)
	return &ldap.SearchResult{Entries: entries, Controls: []ldap.Control{paging}}
}

func successfulAuthenticationResponses(user *ldap.Entry, primaryGroupRID uint32) []fakeSearchResponse {
	return []fakeSearchResponse{
		{result: &ldap.SearchResult{Entries: []*ldap.Entry{user}}},
		{result: pagedResult(nil, nil)},
		{result: pagedResult([]*ldap.Entry{testGroupEntry("Domain Users", 40, primaryGroupRID)}, nil)},
		{result: pagedResult(nil, nil)},
	}
}

func testGroupEntry(name string, guidSeed byte, rid uint32, members ...string) *ldap.Entry {
	dn := fmt.Sprintf("CN=%s,OU=Groups,DC=example,DC=test", name)
	attributes := []*ldap.EntryAttribute{
		rawTestAttribute("objectGUID", testGUIDBytes(guidSeed)),
		rawTestAttribute("objectSid", testSIDBytes(rid)),
		{Name: "distinguishedName", Values: []string{dn}},
		{Name: "sAMAccountName", Values: []string{name}},
		{Name: "displayName", Values: []string{name}},
	}
	if members != nil {
		attributes = append(attributes, &ldap.EntryAttribute{Name: "member", Values: members})
	}
	return &ldap.Entry{DN: dn, Attributes: attributes}
}
