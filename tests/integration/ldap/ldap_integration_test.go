//go:build integration

package ldapintegration_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/infrastructure/directory"
	"github.com/go-ldap/ldap/v3"
)

const (
	userBaseDN  = "ou=people,dc=example,dc=test"
	groupBaseDN = "ou=groups,dc=example,dc=test"
	aliceDN     = "cn=Alice Example," + userBaseDN

	aliceGUID   = "11111111-2222-3333-4444-555555555555"
	bobGUID     = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	charlieGUID = "cccccccc-dddd-eeee-ffff-000000000001"
)

type fixtureConfig struct {
	LDAPSURL     string
	StartTLSURL  string
	ServerName   string
	CAFile       string
	BindDN       string
	BindPassword string
}

func loadFixture(t *testing.T) fixtureConfig {
	t.Helper()

	fixture := fixtureConfig{
		LDAPSURL:     os.Getenv("LDAP_IT_LDAPS_URL"),
		StartTLSURL:  os.Getenv("LDAP_IT_STARTTLS_URL"),
		ServerName:   os.Getenv("LDAP_IT_SERVER_NAME"),
		CAFile:       os.Getenv("LDAP_IT_CA_FILE"),
		BindDN:       os.Getenv("LDAP_IT_BIND_DN"),
		BindPassword: os.Getenv("LDAP_IT_BIND_PASSWORD"),
	}
	if fixture.LDAPSURL == "" || fixture.StartTLSURL == "" || fixture.ServerName == "" ||
		fixture.CAFile == "" || fixture.BindDN == "" || fixture.BindPassword == "" {
		t.Skip("set LDAP_IT_* variables or run tests/integration/ldap/run.sh")
	}
	return fixture
}

func adapterConfig(fixture fixtureConfig, controllers ...directory.Controller) directory.Config {
	return directory.Config{
		DirectoryID:    "integration-ad",
		Controllers:    controllers,
		BindDN:         fixture.BindDN,
		BindPassword:   fixture.BindPassword,
		UserBaseDN:     userBaseDN,
		GroupBaseDN:    groupBaseDN,
		UserFilter:     "(objectClass=adTestUser)",
		GroupFilter:    "(objectClass=adTestGroup)",
		ConnectTimeout: 2 * time.Second,
		QueryTimeout:   5 * time.Second,
		PageSize:       1,
		MaxPages:       20,
		ResultLimit:    100,
		TLS: directory.TLSOptions{
			CAFile: fixture.CAFile,
		},
	}
}

func newAdapter(t *testing.T, config directory.Config) *directory.Adapter {
	t.Helper()

	adapter, err := directory.NewAdapter(config)
	if err != nil {
		t.Fatalf("create directory adapter: %v", err)
	}
	return adapter
}

func ldapsController(fixture fixtureConfig) directory.Controller {
	return directory.Controller{
		URL:        fixture.LDAPSURL,
		TLSMode:    directory.TLSModeLDAPS,
		ServerName: fixture.ServerName,
	}
}

func startTLSController(fixture fixtureConfig) directory.Controller {
	return directory.Controller{
		URL:        fixture.StartTLSURL,
		TLSMode:    directory.TLSModeStartTLS,
		ServerName: fixture.ServerName,
	}
}

func TestNetworkBindUserSearchAndPaging(t *testing.T) {
	fixture := loadFixture(t)

	caPEM, err := os.ReadFile(fixture.CAFile)
	if err != nil {
		t.Fatalf("read fixture CA: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("fixture CA file contained no certificates")
	}
	conn, err := ldap.DialURL(
		fixture.LDAPSURL,
		ldap.DialWithTLSConfig(&tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
			ServerName: fixture.ServerName,
		}),
	)
	if err != nil {
		t.Fatalf("dial fixture over LDAPS: %v", err)
	}
	defer conn.Close()

	if err := conn.Bind(fixture.BindDN, fixture.BindPassword); err != nil {
		t.Fatalf("service account bind: %v", err)
	}
	request := ldap.NewSearchRequest(
		userBaseDN,
		ldap.ScopeWholeSubtree,
		ldap.NeverDerefAliases,
		0,
		5,
		false,
		"(objectClass=adTestUser)",
		[]string{"sAMAccountName"},
		nil,
	)
	result, err := conn.SearchWithPaging(request, 1)
	if err != nil {
		t.Fatalf("paged user search: %v", err)
	}
	if got, want := len(result.Entries), 3; got != want {
		t.Fatalf("paged user search returned %d entries, want %d", got, want)
	}
	accounts := make([]string, 0, len(result.Entries))
	for _, entry := range result.Entries {
		accounts = append(accounts, entry.GetAttributeValue("sAMAccountName"))
	}
	sort.Strings(accounts)
	if got, want := accounts, []string{"alice", "bob", "charlie"}; !equalStrings(got, want) {
		t.Fatalf("paged user accounts = %v, want %v", got, want)
	}

	if err := conn.Bind(aliceDN, "AlicePass!123"); err != nil {
		t.Fatalf("user bind after service-account search: %v", err)
	}
}

func TestAdapterLDAPSAuthenticateAndSync(t *testing.T) {
	fixture := loadFixture(t)
	adapter := newAdapter(t, adapterConfig(fixture, ldapsController(fixture)))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, identifier := range []string{"alice", "alice@example.test"} {
		result, err := adapter.Authenticate(ctx, identifier, "AlicePass!123")
		if err != nil {
			t.Fatalf("authenticate %q: %v", identifier, err)
		}
		if result.User.ObjectGUID != aliceGUID {
			t.Fatalf("authenticate %q GUID = %q, want %q", identifier, result.User.ObjectGUID, aliceGUID)
		}
		if result.ControllerURL != fixture.LDAPSURL {
			t.Fatalf("authenticate %q controller = %q, want %q", identifier, result.ControllerURL, fixture.LDAPSURL)
		}
	}
	if _, err := adapter.Authenticate(ctx, "alice", "wrong password"); !errors.Is(err, directory.ErrInvalidCredentials) {
		t.Fatalf("wrong password error = %v, want ErrInvalidCredentials", err)
	}
	if _, err := adapter.Authenticate(ctx, "charlie", "CharliePass!123"); !errors.Is(err, directory.ErrUserDisabled) {
		t.Fatalf("disabled user error = %v, want ErrUserDisabled", err)
	}

	snapshot, err := adapter.Sync(ctx)
	if err != nil {
		t.Fatalf("sync over LDAPS: %v", err)
	}
	assertFixtureSnapshot(t, snapshot, fixture.LDAPSURL)
}

func TestAdapterStartTLSAuthenticateAndSync(t *testing.T) {
	fixture := loadFixture(t)
	adapter := newAdapter(t, adapterConfig(fixture, startTLSController(fixture)))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := adapter.Authenticate(ctx, "bob", "BobPass!123")
	if err != nil {
		t.Fatalf("authenticate over StartTLS: %v", err)
	}
	if result.User.ObjectGUID != bobGUID || result.ControllerURL != fixture.StartTLSURL {
		t.Fatalf("StartTLS authentication result = %#v", result)
	}
	snapshot, err := adapter.Sync(ctx)
	if err != nil {
		t.Fatalf("sync over StartTLS: %v", err)
	}
	assertFixtureSnapshot(t, snapshot, fixture.StartTLSURL)
	groupByName := make(map[string]string, len(snapshot.Groups))
	for _, group := range snapshot.Groups {
		groupByName[group.DisplayName] = group.ObjectGUID
	}
	wantGroups := []string{groupByName["Domain Users"], groupByName["Platform"], groupByName["Engineering"]}
	sort.Strings(wantGroups)
	if !equalStrings(result.EffectiveGroupObjectGUIDs, wantGroups) {
		t.Fatalf("Bob live effective groups = %v, want primary/direct/nested %v", result.EffectiveGroupObjectGUIDs, wantGroups)
	}
}

func TestAdapterOrderedControllerFailover(t *testing.T) {
	fixture := loadFixture(t)
	closedEndpoint := directory.Controller{
		URL:        "ldaps://127.0.0.1:1",
		TLSMode:    directory.TLSModeLDAPS,
		ServerName: fixture.ServerName,
	}
	adapter := newAdapter(t, adapterConfig(fixture, closedEndpoint, ldapsController(fixture)))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := adapter.Authenticate(ctx, "alice", "AlicePass!123")
	if err != nil {
		t.Fatalf("authenticate after first-controller failure: %v", err)
	}
	if result.ControllerURL != fixture.LDAPSURL {
		t.Fatalf("authentication selected %q, want second controller %q", result.ControllerURL, fixture.LDAPSURL)
	}
	snapshot, err := adapter.Sync(ctx)
	if err != nil {
		t.Fatalf("sync after first-controller failure: %v", err)
	}
	if snapshot.ControllerURL != fixture.LDAPSURL {
		t.Fatalf("sync selected %q, want second controller %q", snapshot.ControllerURL, fixture.LDAPSURL)
	}
}

func TestAdapterRejectsUntrustedCertificate(t *testing.T) {
	fixture := loadFixture(t)
	config := adapterConfig(fixture, ldapsController(fixture))
	config.TLS.CAFile = ""
	adapter := newAdapter(t, config)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	snapshot, err := adapter.Sync(ctx)
	if err == nil {
		t.Fatal("sync with an untrusted certificate unexpectedly succeeded")
	}
	if snapshot != nil {
		t.Fatalf("failed sync returned partial snapshot: %#v", snapshot)
	}
}

func assertFixtureSnapshot(t *testing.T, snapshot *directory.Snapshot, controllerURL string) {
	t.Helper()

	if snapshot == nil {
		t.Fatal("sync returned a nil snapshot")
	}
	if snapshot.DirectoryID != "integration-ad" || snapshot.ControllerURL != controllerURL {
		t.Fatalf("snapshot identity = (%q, %q), want (%q, %q)", snapshot.DirectoryID, snapshot.ControllerURL, "integration-ad", controllerURL)
	}
	if got, want := len(snapshot.Users), 3; got != want {
		t.Fatalf("snapshot users = %d, want %d", got, want)
	}
	if got, want := len(snapshot.Groups), 3; got != want {
		t.Fatalf("snapshot groups = %d, want %d", got, want)
	}

	users := make(map[string]directory.User, len(snapshot.Users))
	for _, user := range snapshot.Users {
		users[user.ObjectGUID] = user
	}
	if !users[aliceGUID].Enabled || !users[bobGUID].Enabled {
		t.Fatalf("active fixture users were not enabled: %#v", users)
	}
	if users[charlieGUID].Enabled {
		t.Fatalf("disabled fixture user was marked enabled: %#v", users[charlieGUID])
	}

	groups := make(map[string]directory.Group, len(snapshot.Groups))
	groupGUIDByName := make(map[string]string, len(snapshot.Groups))
	for _, group := range snapshot.Groups {
		groups[group.ObjectGUID] = group
		groupGUIDByName[group.DisplayName] = group.ObjectGUID
	}
	domainUsersGUID := groupGUIDByName["Domain Users"]
	engineeringGUID := groupGUIDByName["Engineering"]
	platformGUID := groupGUIDByName["Platform"]
	if domainUsersGUID == "" || engineeringGUID == "" || platformGUID == "" {
		t.Fatalf("snapshot groups missing expected display names: %#v", groups)
	}

	for _, userGUID := range []string{aliceGUID, bobGUID, charlieGUID} {
		if !hasDirectMembership(snapshot.DirectMemberships, userGUID, domainUsersGUID, directory.MembershipPrimary) {
			t.Errorf("user %s has no primary Domain Users membership", userGUID)
		}
	}
	if !hasDirectMembership(snapshot.DirectMemberships, aliceGUID, engineeringGUID, directory.MembershipDirect) {
		t.Error("Alice has no direct Engineering membership")
	}
	if !hasDirectMembership(snapshot.DirectMemberships, bobGUID, platformGUID, directory.MembershipDirect) {
		t.Error("Bob has no direct Platform membership")
	}
	if !hasDirectMembership(snapshot.DirectMemberships, charlieGUID, platformGUID, directory.MembershipDirect) {
		t.Error("Charlie has no direct Platform membership")
	}
	if !hasGroupMembership(snapshot.GroupMemberships, platformGUID, engineeringGUID) {
		t.Error("Platform is not nested in Engineering")
	}
	for _, userGUID := range []string{bobGUID, charlieGUID} {
		if !hasEffectiveMembership(snapshot.EffectiveMemberships, userGUID, engineeringGUID, platformGUID, 1) {
			t.Errorf("user %s has no depth-one nested Engineering membership from Platform", userGUID)
		}
	}
	if len(snapshot.UnresolvedMembers) != 0 {
		t.Fatalf("snapshot has unresolved members: %#v", snapshot.UnresolvedMembers)
	}
}

func hasDirectMembership(memberships []directory.UserGroupMembership, userGUID, groupGUID string, source directory.MembershipSource) bool {
	for _, membership := range memberships {
		if membership.UserGUID == userGUID && membership.GroupGUID == groupGUID && membership.Source == source {
			return true
		}
	}
	return false
}

func hasGroupMembership(memberships []directory.GroupMembership, memberGUID, parentGUID string) bool {
	for _, membership := range memberships {
		if membership.MemberGroupGUID == memberGUID && membership.ParentGroupGUID == parentGUID {
			return true
		}
	}
	return false
}

func hasEffectiveMembership(memberships []directory.EffectiveMembership, userGUID, groupGUID, originGUID string, depth int) bool {
	for _, membership := range memberships {
		if membership.UserGUID == userGUID && membership.GroupGUID == groupGUID &&
			membership.Source == directory.MembershipNested && membership.OriginSource == directory.MembershipDirect &&
			membership.OriginGroupGUID == originGUID && membership.Depth == depth {
			return true
		}
	}
	return false
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
