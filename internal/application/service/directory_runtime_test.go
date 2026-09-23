package service

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/config"
	ldapdirectory "github.com/Tencent/WeKnora/internal/infrastructure/directory"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type runtimeDirectoryRepo struct {
	interfaces.DirectoryRepository
	directory     *types.Directory
	identity      *types.DirectoryIdentity
	identities    []*types.DirectoryIdentity
	loginSnapshot *types.DirectoryLoginSnapshot
	updates       int
}

func (r *runtimeDirectoryRepo) Get(context.Context, string) (*types.Directory, error) {
	return r.directory, nil
}

func (r *runtimeDirectoryRepo) Update(_ context.Context, directory *types.Directory) error {
	r.updates++
	r.directory = directory
	return nil
}

func (r *runtimeDirectoryRepo) GetIdentity(
	context.Context,
	string,
) (*types.DirectoryIdentity, error) {
	return r.identity, nil
}

func (r *runtimeDirectoryRepo) GetLoginSnapshot(
	context.Context,
	string,
	string,
) (*types.DirectoryLoginSnapshot, error) {
	return r.loginSnapshot, nil
}

func (r *runtimeDirectoryRepo) ListIdentities(
	context.Context,
	string,
	int,
	int,
) ([]*types.DirectoryIdentity, error) {
	return r.identities, nil
}

type runtimeUserService struct {
	interfaces.UserService
	emailCollision *types.User
	user           *types.User
	registerCalls  int
	generateCalls  int
	onGenerate     func()
}

func (s *runtimeUserService) GetUserByID(context.Context, string) (*types.User, error) {
	return s.user, nil
}

func (s *runtimeUserService) GetUserByEmail(context.Context, string) (*types.User, error) {
	return s.emailCollision, nil
}

func (s *runtimeUserService) FindUserByEmailOrUsernameFold(
	context.Context,
	string,
	string,
) (*types.User, error) {
	return s.emailCollision, nil
}

func (s *runtimeUserService) Register(
	context.Context,
	*types.RegisterRequest,
) (*types.User, error) {
	s.registerCalls++
	return &types.User{ID: "unexpected"}, nil
}

func (s *runtimeUserService) GenerateTokens(context.Context, *types.User) (string, string, error) {
	s.generateCalls++
	if s.onGenerate != nil {
		s.onGenerate()
	}
	return "access", "refresh", nil
}

func (s *runtimeUserService) BuildLoginMemberships(
	context.Context,
	*types.User,
	*types.Tenant,
) []types.Membership {
	return nil
}

type runtimeLDAPAdapter struct {
	authenticateResult *ldapdirectory.AuthenticationResult
	authenticateErr    error
	onAuthenticate     func()
	syncResult         *ldapdirectory.Snapshot
	syncErr            error
}

func (a *runtimeLDAPAdapter) Authenticate(
	context.Context,
	string,
	string,
) (*ldapdirectory.AuthenticationResult, error) {
	if a.onAuthenticate != nil {
		a.onAuthenticate()
	}
	return a.authenticateResult, a.authenticateErr
}

func (a *runtimeLDAPAdapter) Sync(context.Context) (*ldapdirectory.Snapshot, error) {
	if a.syncResult == nil && a.syncErr == nil {
		return nil, errors.New("unexpected sync")
	}
	return a.syncResult, a.syncErr
}

type runtimeDirectorySyncService struct {
	interfaces.DirectoryService
	repo              *runtimeDirectoryRepo
	groups            []string
	trigger           types.DirectorySyncTrigger
	runCalls          int
	failureCalls      int
	failureCode       string
	failureTrigger    types.DirectorySyncTrigger
	failureContextErr error
}

func (s *runtimeDirectorySyncService) RunSync(
	ctx context.Context,
	directoryID string,
	fetch func(context.Context) (*types.DirectorySnapshot, error),
) (*types.DirectorySnapshotResult, error) {
	s.runCalls++
	snapshot, err := fetch(ctx)
	if err != nil {
		return nil, err
	}
	s.trigger = snapshot.Trigger
	s.repo.directory.SnapshotVersion++
	now := time.Now().UTC()
	s.repo.directory.LastSuccessfulSyncAt = &now
	s.repo.directory.LastSyncError = ""
	s.repo.loginSnapshot.Directory = s.repo.directory
	s.repo.loginSnapshot.Identity.SnapshotVersion = s.repo.directory.SnapshotVersion
	s.repo.loginSnapshot.EffectiveGroupObjectGUIDs = append([]string(nil), s.groups...)
	completed := now
	return &types.DirectorySnapshotResult{SyncRun: &types.DirectorySyncRun{
		DirectoryID: directoryID, Trigger: snapshot.Trigger, Status: types.DirectorySyncSuccess,
		StartedAt: now, CompletedAt: &completed, SnapshotVersion: s.repo.directory.SnapshotVersion,
	}}, nil
}

func (s *runtimeDirectorySyncService) RecordSyncFailure(
	ctx context.Context,
	_ string,
	code string,
	_ string,
	_ time.Time,
	_, _ uint64,
	trigger types.DirectorySyncTrigger,
) (*types.DirectorySyncRun, error) {
	s.failureCalls++
	s.failureCode = code
	s.failureTrigger = trigger
	s.failureContextErr = ctx.Err()
	return &types.DirectorySyncRun{
		Status:    types.DirectorySyncFailed,
		ErrorCode: code,
		Trigger:   trigger,
	}, nil
}

type runtimeTokenRepo struct {
	interfaces.AuthTokenRepository
	revoked []string
}

func (r *runtimeTokenRepo) RevokeTokensByUserID(_ context.Context, userID string) error {
	r.revoked = append(r.revoked, userID)
	return nil
}

func databaseRuntimeConfig() *config.Config {
	return &config.Config{Directory: &config.DirectoryConfig{
		ManagementSource: config.DirectoryManagementDatabase,
		ID:               "corp-ad",
	}}
}

func liveLoginRuntimeFixture(
	t *testing.T,
	liveGroups, snapshotGroups []string,
) (*directoryRuntimeService, *runtimeUserService, *runtimeDirectoryRepo) {
	t.Helper()
	now := time.Now().UTC()
	cfg := &config.Config{Directory: &config.DirectoryConfig{
		Enabled: true, ManagementSource: config.DirectoryManagementFile,
		ID: "corp-ad", ProviderDisplayName: "Corporate AD",
		Servers: []config.DirectoryServerConfig{
			{URL: "ldaps://dc.example.test:636", TLSMode: config.DirectoryTLSLDAPS},
		},
		BindDN: "cn=svc,dc=example,dc=test", BindPassword: "service-secret",
		BaseDN: "dc=example,dc=test", UserBaseDN: "ou=users,dc=example,dc=test",
		GroupBaseDN: "ou=groups,dc=example,dc=test",
		UserFilter:  "(objectClass=user)", GroupFilter: "(objectClass=group)",
		ConnectTimeout: time.Second, QueryTimeout: time.Second, PageSize: 100, ResultLimit: 1000,
		SyncInterval: time.Minute, StaleAfter: 15 * time.Minute,
	}}
	directory, err := directoryFromDeployment(cfg.Directory)
	if err != nil {
		t.Fatal(err)
	}
	directory.ConfigVersion = 1
	directory.SnapshotVersion = 7
	directory.LastSuccessfulSyncAt = &now
	userID := "user-1"
	identity := &types.DirectoryIdentity{
		ID: "identity-1", DirectoryID: directory.ID, ObjectGUID: "user-guid", UserID: &userID,
		Status: types.DirectoryObjectActive, SnapshotVersion: directory.SnapshotVersion,
	}
	repo := &runtimeDirectoryRepo{
		directory: directory,
		loginSnapshot: &types.DirectoryLoginSnapshot{
			Directory: directory, Identity: identity,
			EffectiveGroupObjectGUIDs: append([]string(nil), snapshotGroups...),
		},
	}
	users := &runtimeUserService{user: &types.User{ID: userID, Username: "alice", IsActive: true}}
	adapter := &runtimeLDAPAdapter{authenticateResult: &ldapdirectory.AuthenticationResult{
		User: ldapdirectory.User{
			ObjectGUID: "user-guid", SID: "S-1-5-21-1-2-3-1107", DN: "cn=alice,ou=users,dc=example,dc=test",
			SAMAccountName: "alice", Enabled: true, PrimaryGroupRID: 513,
		},
		EffectiveGroupObjectGUIDs: append(
			[]string(nil),
			liveGroups...), ControllerURL: "ldaps://dc.example.test:636",
	}}
	runtime := newDirectoryRuntimeService(cfg, nil, repo, users, nil, nil, nil,
		func(ldapdirectory.Config) (liveDirectoryAdapter, error) { return adapter, nil })
	runtime.now = func() time.Time { return now }
	return runtime, users, repo
}

func TestDirectoryRuntimeGetConfigNeverReturnsSecret(t *testing.T) {
	repo := &runtimeDirectoryRepo{directory: &types.Directory{
		ID: "corp-ad", Name: "Corporate AD", Enabled: true,
		ConfigSource: types.DirectoryConfigSourceDatabase,
		TLSMode:      types.DirectoryTLSLDAPS, ServerURLs: types.StringArray{"ldaps://dc.example.test:636"},
		ServerNames: types.StringArray{"dc.example.test"}, BaseDN: "DC=example,DC=test",
		UserBaseDN: "DC=example,DC=test", GroupBaseDN: "DC=example,DC=test",
		UserFilter: "(objectClass=user)", GroupFilter: "(objectClass=group)",
		ServiceAccountDN: "CN=svc,DC=example,DC=test", PasswordCiphertext: "enc:v1:not-returned",
		EnterpriseCAPEM:       "-----BEGIN CERTIFICATE-----\nnot-returned\n-----END CERTIFICATE-----",
		ConnectTimeoutSeconds: 5, QueryTimeoutSeconds: 10, PageSize: 500, ResultLimit: 10000,
		SyncIntervalSeconds: 300, StaleAfterSeconds: 900,
	}}
	runtime := newDirectoryRuntimeService(
		databaseRuntimeConfig(),
		nil,
		repo,
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	got, err := runtime.GetConfig(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !got.HasBindPassword || got.BindPasswordSource != directorySourceDatabase {
		t.Fatalf("expected configured database secret metadata, got %+v", got)
	}
	if got.CAFile != "" {
		t.Fatalf(
			"CA PEM/path must not be returned for a database-managed directory: %q",
			got.CAFile,
		)
	}
	if len(got.Servers) != 1 || got.Servers[0].ServerName != "dc.example.test" {
		t.Fatalf("server_name must round-trip without exposing secrets: %+v", got.Servers)
	}
}

func TestDirectoryRuntimeUpdateRejectsCredentialWithoutAESKey(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "")
	runtime := newDirectoryRuntimeService(
		databaseRuntimeConfig(), nil, &runtimeDirectoryRepo{}, nil, nil, nil, nil, nil,
	)
	secret := "service-secret"
	_, err := runtime.UpdateConfig(context.Background(), &types.DirectoryAdminConfigUpdate{
		Enabled: true, DisplayName: "Corporate AD",
		Servers:   []types.DirectoryAdminServer{{Address: "ldaps://dc.example.test:636"}},
		Transport: types.DirectoryTLSLDAPS, BaseDN: "DC=example,DC=test",
		BindDN: "CN=svc,DC=example,DC=test", BindPassword: &secret,
		UserFilter: "(objectClass=user)", GroupFilter: "(objectClass=group)",
	})
	if !errors.Is(err, ErrDirectoryEncryptionKey) {
		t.Fatalf("expected ErrDirectoryEncryptionKey, got %v", err)
	}
}

func TestConvertDirectorySnapshotPreservesPrimaryAndNestedInputs(t *testing.T) {
	started := time.Now().UTC()
	converted, warnings, err := convertDirectorySnapshot(&ldapdirectory.Snapshot{
		DirectoryID: "corp-ad",
		Users: []ldapdirectory.User{{
			ObjectGUID: "user-guid", SID: "S-1-5-21-1-2-3-1100", DN: "CN=Alice,DC=example,DC=test",
			SAMAccountName: "alice", Enabled: true, PrimaryGroupRID: 513,
		}},
		Groups: []ldapdirectory.Group{
			{
				ObjectGUID:     "group-users",
				SID:            "S-1-5-21-1-2-3-513",
				DN:             "CN=Users,DC=example,DC=test",
				SAMAccountName: "Domain Users",
				DisplayName:    "Users",
				Email:          "users@example.test",
			},
			{
				ObjectGUID:  "group-parent",
				SID:         "S-1-5-21-1-2-3-2000",
				DN:          "CN=Parent,DC=example,DC=test",
				DisplayName: "Parent",
			},
		},
		DirectMemberships: []ldapdirectory.UserGroupMembership{{
			UserGUID: "user-guid", GroupGUID: "group-users", Source: ldapdirectory.MembershipPrimary,
		}},
		GroupMemberships: []ldapdirectory.GroupMembership{{
			MemberGroupGUID: "group-users", ParentGroupGUID: "group-parent",
		}},
		UnresolvedMembers: []ldapdirectory.UnresolvedMember{
			{ParentGroupGUID: "group-parent", MemberDN: "CN=Computer,DC=example,DC=test"},
		},
	}, started)
	if err != nil {
		t.Fatal(err)
	}
	if !converted.Complete || !converted.PaginationComplete || len(warnings) != 1 {
		t.Fatalf("unexpected completion/warning state: %+v warnings=%v", converted, warnings)
	}
	if got := converted.Identities[0].PrimaryGroupSID; got != "S-1-5-21-1-2-3-513" {
		t.Fatalf("unexpected primary group SID %q", got)
	}
	if len(converted.Memberships) != 1 || !converted.Memberships[0].Primary ||
		converted.Memberships[0].Source != types.DirectoryMembershipPrimary {
		t.Fatalf("primary membership provenance was lost: %+v", converted.Memberships)
	}
	if len(converted.GroupEdges) != 1 ||
		converted.GroupEdges[0].ParentGroupObjectGUID != "group-parent" {
		t.Fatalf("nested group edge was not converted: %+v", converted.GroupEdges)
	}
	if got := converted.Groups[0]; got.SAMAccountName != "Domain Users" ||
		got.Email != "users@example.test" {
		t.Fatalf("group account name/email were not preserved: %+v", got)
	}
}

func TestDirectoryRuntimeLoginRejectsBlankPasswordBeforeNetwork(t *testing.T) {
	runtime := newDirectoryRuntimeService(
		databaseRuntimeConfig(),
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	_, err := runtime.Login(context.Background(), "alice", "")
	if !errors.Is(err, ldapdirectory.ErrEmptyPassword) {
		t.Fatalf("expected empty password rejection, got %v", err)
	}
}

func TestDirectoryRuntimeLoginRejectsRecentFailedSyncBeforeNetwork(t *testing.T) {
	now := time.Now().UTC()
	repo := &runtimeDirectoryRepo{directory: &types.Directory{
		ID: "corp-ad", Enabled: true, LastSuccessfulSyncAt: &now,
		StaleAfterSeconds: 900, LastSyncError: "incomplete_results",
	}}
	dials := 0
	runtime := newDirectoryRuntimeService(databaseRuntimeConfig(), nil, repo, nil, nil, nil, nil,
		func(ldapdirectory.Config) (liveDirectoryAdapter, error) {
			dials++
			return nil, errors.New("must not create adapter")
		})
	_, err := runtime.Login(context.Background(), "alice", "secret")
	if !errors.Is(err, ErrDirectoryUnavailable) {
		t.Fatalf("expected failed sync to block login, got %v", err)
	}
	if dials != 0 {
		t.Fatalf("adapter factory called %d times, want 0", dials)
	}
}

func TestDirectoryRuntimeLoginRequiresExactLiveGroupSetBeforeIssuingToken(t *testing.T) {
	runtime, users, repo := liveLoginRuntimeFixture(t,
		[]string{"GROUP-B", "group-a"}, []string{"group-a", "group-b"})
	repo.loginSnapshot.Identity.DisplayName = "Alice Directory"
	result, err := runtime.Login(context.Background(), "alice", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if result.Token != "access" || users.generateCalls != 1 {
		t.Fatalf(
			"matching live groups did not issue exactly one token: result=%+v calls=%d",
			result,
			users.generateCalls,
		)
	}
	if result.User.DisplayName != "Alice Directory" || result.User.Username != "alice" {
		t.Fatalf(
			"LDAP login should show directory name without changing account username: %+v",
			result.User,
		)
	}
}

func TestDirectoryRuntimeLoginRejectsMembershipMismatchBeforeTokenOrProvisioning(t *testing.T) {
	runtime, users, _ := liveLoginRuntimeFixture(t,
		[]string{"group-a"}, []string{"group-a", "group-removed-live"})
	_, err := runtime.Login(context.Background(), "alice", "secret")
	if !errors.Is(err, ErrDirectoryMembershipMismatch) {
		t.Fatalf("expected membership mismatch, got %v", err)
	}
	if users.generateCalls != 0 || users.registerCalls != 0 {
		t.Fatalf(
			"mismatched memberships reached token/provisioning: tokens=%d registrations=%d",
			users.generateCalls,
			users.registerCalls,
		)
	}
}

func TestDirectoryRuntimeLoginMismatchRunsCompleteLoginSyncThenRechecks(t *testing.T) {
	runtime, users, repo := liveLoginRuntimeFixture(t,
		[]string{"group-current"}, []string{"group-old"})
	adapter := &runtimeLDAPAdapter{
		authenticateResult: &ldapdirectory.AuthenticationResult{
			User: ldapdirectory.User{
				ObjectGUID: "user-guid", SID: "S-1-5-21-1-2-3-1107", DN: "cn=alice,ou=users,dc=example,dc=test",
				SAMAccountName: "alice", Enabled: true, PrimaryGroupRID: 513,
			},
			EffectiveGroupObjectGUIDs: []string{
				"group-current",
			}, ControllerURL: "ldaps://dc.example.test:636",
		},
		syncResult: &ldapdirectory.Snapshot{
			DirectoryID: "corp-ad", ControllerURL: "ldaps://dc.example.test:636",
			Users: []ldapdirectory.User{{
				ObjectGUID: "user-guid", SID: "S-1-5-21-1-2-3-1107", DN: "cn=alice,ou=users,dc=example,dc=test",
				SAMAccountName: "alice", Enabled: true, PrimaryGroupRID: 513,
			}},
			Groups: []ldapdirectory.Group{{
				ObjectGUID: "group-current", SID: "S-1-5-21-1-2-3-513",
				DN:          "cn=Domain Users,ou=groups,dc=example,dc=test",
				DisplayName: "Domain Users",
			}},
			DirectMemberships: []ldapdirectory.UserGroupMembership{{
				UserGUID: "user-guid", GroupGUID: "group-current", Source: ldapdirectory.MembershipPrimary,
			}},
		},
	}
	runtime.newAdapter = func(ldapdirectory.Config) (liveDirectoryAdapter, error) { return adapter, nil }
	syncService := &runtimeDirectorySyncService{repo: repo, groups: []string{"group-current"}}
	runtime.directories = syncService

	result, err := runtime.Login(context.Background(), "alice", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if result.Token != "access" || users.generateCalls != 1 {
		t.Fatalf(
			"successful complete refresh did not resume login: result=%+v token calls=%d",
			result,
			users.generateCalls,
		)
	}
	if syncService.runCalls != 1 || syncService.trigger != types.DirectorySyncTriggerLogin {
		t.Fatalf(
			"login mismatch sync calls/trigger = %d/%q, want 1/%q",
			syncService.runCalls,
			syncService.trigger,
			types.DirectorySyncTriggerLogin,
		)
	}
}

func TestDirectoryRuntimeLoginMismatchHonorsSyncCooldown(t *testing.T) {
	runtime, users, repo := liveLoginRuntimeFixture(t,
		[]string{"group-current"}, []string{"group-old"})
	now := runtime.now().UTC()
	repo.directory.LastSyncAttemptAt = &now
	syncService := &runtimeDirectorySyncService{repo: repo, groups: []string{"group-current"}}
	runtime.directories = syncService

	_, err := runtime.Login(context.Background(), "alice", "secret")
	if !errors.Is(err, ErrDirectoryMembershipMismatch) {
		t.Fatalf("expected membership mismatch during cooldown, got %v", err)
	}
	if syncService.runCalls != 0 {
		t.Fatalf("mismatch triggered %d syncs during cooldown", syncService.runCalls)
	}
	if users.generateCalls != 0 {
		t.Fatalf("mismatch during cooldown issued %d tokens", users.generateCalls)
	}
}

func TestDirectoryRuntimeRunSyncRecordsAdapterConstructionFailure(t *testing.T) {
	runtime, _, repo := liveLoginRuntimeFixture(t, []string{"group-a"}, []string{"group-a"})
	syncService := &runtimeDirectorySyncService{repo: repo}
	runtime.directories = syncService
	runtime.newAdapter = func(ldapdirectory.Config) (liveDirectoryAdapter, error) {
		return nil, errors.New("invalid enterprise CA")
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := runtime.runSync(requestCtx, directoryManualTrigger)
	if err == nil || !strings.Contains(err.Error(), "invalid enterprise CA") {
		t.Fatalf("expected adapter construction failure, got %v", err)
	}
	if syncService.runCalls != 0 {
		t.Fatalf("adapter failure unexpectedly entered leased sync %d times", syncService.runCalls)
	}
	if syncService.failureCalls != 1 || syncService.failureCode != "directory_sync_failed" ||
		syncService.failureTrigger != types.DirectorySyncTriggerManual {
		t.Fatalf(
			"failure record = calls:%d code:%q trigger:%q",
			syncService.failureCalls,
			syncService.failureCode,
			syncService.failureTrigger,
		)
	}
	if syncService.failureContextErr != nil {
		t.Fatalf(
			"failure record inherited canceled request context: %v",
			syncService.failureContextErr,
		)
	}
	runtime.stateMu.RLock()
	failures := runtime.consecutiveFailures
	runtime.stateMu.RUnlock()
	if failures != 1 {
		t.Fatalf("consecutive failures = %d, want 1", failures)
	}
}

func TestDirectoryRuntimeLoginRevokesTokenWhenConfigChangesDuringIssuance(t *testing.T) {
	runtime, users, repo := liveLoginRuntimeFixture(t, []string{"group-a"}, []string{"group-a"})
	tokens := &runtimeTokenRepo{}
	runtime.tokens = tokens
	users.onGenerate = func() {
		repo.loginSnapshot.Directory.ConfigVersion++
	}

	result, err := runtime.Login(context.Background(), "alice", "secret")
	if !errors.Is(err, ErrDirectoryUnavailable) {
		t.Fatalf("expected config-version barrier failure, got result=%+v err=%v", result, err)
	}
	if users.generateCalls != 1 {
		t.Fatalf("GenerateTokens calls = %d, want 1", users.generateCalls)
	}
	if len(tokens.revoked) != 1 || tokens.revoked[0] != "user-1" {
		t.Fatalf("post-issuance barrier revoked users = %v, want [user-1]", tokens.revoked)
	}
}

func TestDirectoryRuntimeLoginRechecksDirectoryStateAfterLDAP(t *testing.T) {
	runtime, users, repo := liveLoginRuntimeFixture(t, []string{"group-a"}, []string{"group-a"})
	adapter := &runtimeLDAPAdapter{authenticateResult: &ldapdirectory.AuthenticationResult{
		User: ldapdirectory.User{
			ObjectGUID: "user-guid",
			Enabled:    true,
		}, EffectiveGroupObjectGUIDs: []string{"group-a"},
	}}
	adapter.onAuthenticate = func() { repo.loginSnapshot.Directory.LastSyncError = "configuration changed" }
	runtime.newAdapter = func(ldapdirectory.Config) (liveDirectoryAdapter, error) { return adapter, nil }

	_, err := runtime.Login(context.Background(), "alice", "secret")
	if !errors.Is(err, ErrDirectoryUnavailable) {
		t.Fatalf("expected post-bind state change to fail closed, got %v", err)
	}
	if users.generateCalls != 0 {
		t.Fatalf("post-bind failed state issued %d tokens", users.generateCalls)
	}
}

func TestDirectoryRuntimeConflictRequiresExplicitAdministratorLink(t *testing.T) {
	identity := &types.DirectoryIdentity{
		ID: "identity-1", DirectoryID: "corp-ad", ObjectGUID: "guid-1", Status: types.DirectoryObjectActive,
	}
	repo := &runtimeDirectoryRepo{identity: identity}
	users := &runtimeUserService{
		emailCollision: &types.User{ID: "local-1", Email: "alice@example.test"},
	}
	runtime := newDirectoryRuntimeService(
		databaseRuntimeConfig(),
		nil,
		repo,
		users,
		nil,
		nil,
		nil,
		nil,
	)

	_, err := runtime.resolveDirectoryUser(context.Background(), identity, ldapdirectory.User{
		ObjectGUID: "guid-1", SAMAccountName: "alice", Email: "alice@example.test",
	})
	if !errors.Is(err, ErrDirectoryIdentityLinkRequired) {
		t.Fatalf("expected explicit-link conflict, got %v", err)
	}
	if users.registerCalls != 0 {
		t.Fatalf(
			"conflicting local account was auto-merged/provisioned; register calls=%d",
			users.registerCalls,
		)
	}
}

func TestDirectoryRuntimeRevokesDisabledAndOutOfScopeIdentitySessions(t *testing.T) {
	activeUser, disabledUser, missingUser := "active-user", "disabled-user", "missing-user"
	repo := &runtimeDirectoryRepo{identities: []*types.DirectoryIdentity{
		{ID: "active", UserID: &activeUser, Status: types.DirectoryObjectActive},
		{ID: "disabled", UserID: &disabledUser, Status: types.DirectoryObjectDisabled},
		{ID: "missing", UserID: &missingUser, Status: types.DirectoryObjectOutOfScope},
	}}
	tokens := &runtimeTokenRepo{}
	runtime := newDirectoryRuntimeService(
		databaseRuntimeConfig(),
		nil,
		repo,
		nil,
		nil,
		tokens,
		nil,
		nil,
	)
	runtime.revokeSuspendedIdentitySessions(context.Background(), "corp-ad")

	if len(tokens.revoked) != 2 || tokens.revoked[0] != disabledUser ||
		tokens.revoked[1] != missingUser {
		t.Fatalf("revoked users = %v, want [%s %s]", tokens.revoked, disabledUser, missingUser)
	}
}

func TestFileManagedDisablePersistsStateAndRevokesSessions(t *testing.T) {
	lastSuccess := time.Now().UTC()
	userID := "directory-user"
	repo := &runtimeDirectoryRepo{
		directory: &types.Directory{
			ID: "corp-ad", Name: "Corporate AD", Enabled: true, ConfigSource: types.DirectoryConfigSourceFile,
			TLSMode: types.DirectoryTLSLDAPS, ServerURLs: types.StringArray{"ldaps://dc.example.test:636"},
			BaseDN: "dc=example,dc=test", UserBaseDN: "dc=example,dc=test", GroupBaseDN: "dc=example,dc=test",
			UserFilter: "(objectClass=user)", GroupFilter: "(objectClass=group)",
			ServiceAccountDN: "cn=svc,dc=example,dc=test", LastSuccessfulSyncAt: &lastSuccess,
		},
		identities: []*types.DirectoryIdentity{
			{ID: "identity-1", UserID: &userID, Status: types.DirectoryObjectActive},
		},
	}
	tokens := &runtimeTokenRepo{}
	cfg := &config.Config{Directory: &config.DirectoryConfig{
		Enabled: false, ManagementSource: config.DirectoryManagementFile,
		ID: "corp-ad", ProviderDisplayName: "Corporate AD",
	}}
	runtime := newDirectoryRuntimeService(cfg, nil, repo, nil, nil, tokens, nil, nil)

	directory, err := runtime.ensureFileDirectory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if directory.Enabled || repo.updates != 1 {
		t.Fatalf(
			"file-managed disable was not persisted: directory=%+v updates=%d",
			directory,
			repo.updates,
		)
	}
	if len(tokens.revoked) != 1 || tokens.revoked[0] != userID {
		t.Fatalf("file-managed disable revoked users=%v, want [%s]", tokens.revoked, userID)
	}
}

func TestFileManagedSecurityFingerprintDetectsSecretAndSamePathCARotation(t *testing.T) {
	caPath := t.TempDir() + "/enterprise-ca.pem"
	if err := os.WriteFile(caPath, []byte("first-ca"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Directory: &config.DirectoryConfig{
		Enabled: true, ManagementSource: config.DirectoryManagementFile,
		ID: "corp-ad", ProviderDisplayName: "Corporate AD",
		Servers: []config.DirectoryServerConfig{
			{URL: "ldaps://dc.example.test:636", TLSMode: config.DirectoryTLSLDAPS},
		},
		BindDN: "cn=svc,dc=example,dc=test", BindPassword: "first-secret", CAFile: caPath,
		BaseDN: "dc=example,dc=test", UserBaseDN: "ou=users,dc=example,dc=test",
		GroupBaseDN: "ou=groups,dc=example,dc=test",
		UserFilter:  "(objectClass=user)", GroupFilter: "(objectClass=group)", LoginFilter: "(sAMAccountName={login})",
		ConnectTimeout: time.Second, QueryTimeout: time.Second, PageSize: 100, ResultLimit: 1000,
		SyncInterval: time.Minute, StaleAfter: 15 * time.Minute,
	}}
	existing, err := directoryFromDeployment(cfg.Directory)
	if err != nil {
		t.Fatal(err)
	}
	existing.ConfigVersion = 1
	now := time.Now().UTC()
	existing.LastSuccessfulSyncAt = &now
	userID := "directory-user"
	repo := &runtimeDirectoryRepo{
		directory: existing,
		identities: []*types.DirectoryIdentity{
			{
				ID:          "identity-1",
				DirectoryID: existing.ID,
				UserID:      &userID,
				Status:      types.DirectoryObjectActive,
			},
		},
	}
	tokens := &runtimeTokenRepo{}
	runtime := newDirectoryRuntimeService(cfg, nil, repo, nil, nil, tokens, nil, nil)

	if _, err := runtime.ensureFileDirectory(context.Background()); err != nil {
		t.Fatal(err)
	}
	if repo.updates != 0 {
		t.Fatalf("unchanged restart updated directory %d times", repo.updates)
	}
	firstFingerprint := existing.SecurityConfigFingerprint
	if len(firstFingerprint) != 64 ||
		strings.Contains(firstFingerprint, cfg.Directory.BindPassword) {
		t.Fatalf("unsafe security fingerprint %q", firstFingerprint)
	}

	if err := os.WriteFile(caPath, []byte("rotated-ca"), 0o600); err != nil {
		t.Fatal(err)
	}
	rotated, err := runtime.ensureFileDirectory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if repo.updates != 1 || rotated.SecurityConfigFingerprint == firstFingerprint {
		t.Fatalf(
			"same-path CA rotation was not persisted: updates=%d fingerprints=%q/%q",
			repo.updates,
			firstFingerprint,
			rotated.SecurityConfigFingerprint,
		)
	}
	if len(tokens.revoked) != 1 || tokens.revoked[0] != userID {
		t.Fatalf("CA rotation revoked users = %v", tokens.revoked)
	}

	caFingerprint := rotated.SecurityConfigFingerprint
	cfg.Directory.BindPassword = "rotated-secret"
	rotated, err = runtime.ensureFileDirectory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if repo.updates != 2 || rotated.SecurityConfigFingerprint == caFingerprint {
		t.Fatalf("bind-secret rotation was not persisted: updates=%d", repo.updates)
	}
}

func TestFileManagedSecurityFingerprintRejectsUnreadableCA(t *testing.T) {
	cfg := &config.DirectoryConfig{
		Enabled: true, ManagementSource: config.DirectoryManagementFile, ID: "corp-ad",
		Servers: []config.DirectoryServerConfig{
			{URL: "ldaps://dc.example.test:636", TLSMode: config.DirectoryTLSLDAPS},
		},
		CAFile: t.TempDir() + "/missing-ca.pem",
	}
	_, err := directoryFromDeployment(cfg)
	if err == nil || !strings.Contains(err.Error(), "read directory CA file") {
		t.Fatalf("expected stable CA fingerprint error, got %v", err)
	}
}

func TestCombineLDAPFilters(t *testing.T) {
	got := combineLDAPFilters(
		"(objectClass=user)",
		"(!(userAccountControl:1.2.840.113556.1.4.803:=2))",
	)
	want := "(&(objectClass=user)(!(userAccountControl:1.2.840.113556.1.4.803:=2)))"
	if got != want {
		t.Fatalf("unexpected combined filter: %s", got)
	}
}

func TestDirectoryRuntimePassesFileManagedLoginTemplateInsideAllowedScope(t *testing.T) {
	cfg := &config.Config{Directory: &config.DirectoryConfig{
		Enabled: true, ManagementSource: config.DirectoryManagementFile, ID: "corp-ad",
		Servers: []config.DirectoryServerConfig{
			{URL: "ldaps://dc.example.test:636", TLSMode: config.DirectoryTLSLDAPS},
		},
		BindDN: "cn=svc,dc=example,dc=test", BindPassword: "secret", BaseDN: "dc=example,dc=test",
		UserBaseDN: "dc=example,dc=test", GroupBaseDN: "dc=example,dc=test",
		UserFilter: "(objectClass=user)", GroupFilter: "(objectClass=group)",
		AllowedLoginFilter: "(department=engineering)", LoginFilter: "(employeeID={login})",
		ConnectTimeout: time.Second, QueryTimeout: time.Second, PageSize: 100, ResultLimit: 1000,
	}}
	directory, err := directoryFromDeployment(cfg.Directory)
	if err != nil {
		t.Fatal(err)
	}
	var captured ldapdirectory.Config
	runtime := newDirectoryRuntimeService(cfg, nil, nil, nil, nil, nil, nil,
		func(input ldapdirectory.Config) (liveDirectoryAdapter, error) {
			captured = input
			return nil, nil
		})
	_, err = runtime.adapterFor(context.Background(), directory, "")
	if err != nil {
		t.Fatal(err)
	}
	if captured.LoginFilter != "(employeeID={login})" {
		t.Fatalf("login filter was not passed to adapter: %q", captured.LoginFilter)
	}
	if captured.UserFilter != "(&(objectClass=user)(department=engineering))" {
		t.Fatalf("allowed scope was not ANDed with user filter: %q", captured.UserFilter)
	}
}
