package service

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	secutils "github.com/Tencent/WeKnora/internal/utils"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const (
	dingTalkTestCorpID = "ding-corp-1"
	dingTalkTestAppKey = "ding-app-key"
)

// fakeDingTalkClient is the canned DingTalk Open Platform double. Every
// login test scripts the flows through these fields; unset optional errors
// default to the happy path.
type fakeDingTalkClient struct {
	contactCorpID  string
	contactUnionID string
	contactNick    string
	contactErr     error
	// contactByToken overrides the default contact profile per exchanged
	// code, so concurrent logins can present different DingTalk users.
	contactByToken map[string]fakeDingTalkContact

	exchangeErr error

	userIDByUnionID    map[string]string
	userIDByUnionIDErr map[string]error

	userIDByInAppCode map[string]string
	inAppErr          error

	details map[string]fakeDingTalkProfile
}

type fakeDingTalkProfile struct {
	JobNumber string
	Name      string
	UnionID   string
}

type fakeDingTalkContact struct {
	UnionID string
	CorpID  string
	Nick    string
}

func (f *fakeDingTalkClient) BuildAuthorizeURL(redirectURL, state string) string {
	query := url.Values{}
	query.Set("redirect_uri", redirectURL)
	query.Set("state", state)
	return "https://login.dingtalk.example/oauth2/auth?" + query.Encode()
}

func (f *fakeDingTalkClient) ExchangeUserAccessToken(_ context.Context, code string) (string, error) {
	if f.exchangeErr != nil {
		return "", f.exchangeErr
	}
	return "user-token:" + code, nil
}

func (f *fakeDingTalkClient) GetUserContactMe(
	_ context.Context, token string,
) (string, string, string, error) {
	if f.contactErr != nil {
		return "", "", "", f.contactErr
	}
	if contact, ok := f.contactByToken[token]; ok {
		return contact.UnionID, contact.CorpID, contact.Nick, nil
	}
	return f.contactUnionID, f.contactCorpID, f.contactNick, nil
}

func (f *fakeDingTalkClient) GetCorpAccessToken(_ context.Context) (string, error) {
	return "corp-access-token", nil
}

func (f *fakeDingTalkClient) GetUserIDByUnionID(_ context.Context, unionID string) (string, error) {
	if err := f.userIDByUnionIDErr[unionID]; err != nil {
		return "", err
	}
	userID := f.userIDByUnionID[unionID]
	if userID == "" {
		return "", ErrDingTalkNotEnterpriseMember
	}
	return userID, nil
}

func (f *fakeDingTalkClient) GetInAppUserID(_ context.Context, authCode string) (string, error) {
	if f.inAppErr != nil {
		return "", f.inAppErr
	}
	userID := f.userIDByInAppCode[authCode]
	if userID == "" {
		return "", ErrDingTalkNotEnterpriseMember
	}
	return userID, nil
}

func (f *fakeDingTalkClient) GetUserDetail(
	_ context.Context, userid string,
) (string, string, string, error) {
	profile := f.details[userid]
	return profile.JobNumber, profile.Name, profile.UnionID, nil
}

type dingTalkTestEnv struct {
	db     *gorm.DB
	svc    *userService
	client *fakeDingTalkClient
	repo   interfaces.DingTalkRepository
	cfg    *config.DingTalkAuthConfig
}

func loadDingTalkMigrationSQL(t *testing.T, db *gorm.DB, filename string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "sqlite", filename))
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(raw)).Error)
}

// setupDingTalkService wires a real sqlite-backed userService with the fake
// client. The single-connection pool mirrors the application's SQLite
// configuration so concurrent binding attempts serialize like production.
func setupDingTalkService(t *testing.T) *dingTalkTestEnv {
	t.Helper()
	db, err := gorm.Open(
		sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared&_foreign_keys=on&_busy_timeout=5000"),
		&gorm.Config{},
	)
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })

	loadDingTalkMigrationSQL(t, db, "000028_directory_group_access.up.sql")
	loadDingTalkMigrationSQL(t, db, "000087_dingtalk_sso.up.sql")
	require.NoError(t, db.AutoMigrate(&types.User{}, &types.AuthToken{}, &types.Tenant{}))
	// users.tenant_id carries a FK to tenants; seed the home workspace once.
	require.NoError(t, db.Create(&types.Tenant{ID: 7, Name: "Corp Workspace", Status: "active"}).Error)

	client := &fakeDingTalkClient{
		contactCorpID:     dingTalkTestCorpID,
		contactUnionID:    "union-1",
		contactNick:       "张三",
		userIDByUnionID:   map[string]string{"union-1": "dt-user-1"},
		userIDByInAppCode: map[string]string{"inapp-code-1": "dt-user-1"},
		details: map[string]fakeDingTalkProfile{
			"dt-user-1": {JobNumber: "00123", Name: "张三", UnionID: "union-1"},
		},
	}
	cfg := &config.DingTalkAuthConfig{
		Enable:    true,
		CorpID:    dingTalkTestCorpID,
		AppKey:    dingTalkTestAppKey,
		AppSecret: "ding-app-secret",
	}
	repo := apprepo.NewDingTalkRepository(db)
	env := &dingTalkTestEnv{
		db:     db,
		client: client,
		repo:   repo,
		cfg:    cfg,
		svc: &userService{
			userRepo:       apprepo.NewUserRepository(db),
			tokenRepo:      apprepo.NewAuthTokenRepository(db),
			config:         &config.Config{DingTalkAuth: cfg},
			directoryRepo:  apprepo.NewDirectoryRepository(db),
			dingtalkRepo:   repo,
			dingtalkClient: client,
		},
	}
	return env
}

// createFreshDirectory inserts an enabled directory whose snapshot counts
// as fresh, so validateUserSessionEligibility's IsFresh gate passes.
func (e *dingTalkTestEnv) createFreshDirectory(t *testing.T) *types.Directory {
	t.Helper()
	now := time.Now().UTC()
	directory := &types.Directory{
		ID:                 "dir-1",
		Name:               "corp-ad",
		Protocol:           types.DirectoryProtocolAD,
		Enabled:            true,
		ConfigSource:       types.DirectoryConfigSourceDatabase,
		TLSMode:            types.DirectoryTLSLDAPS,
		ServerURLs:         types.StringArray{"ldaps://ad.example.com"},
		BaseDN:             "DC=example,DC=com",
		UserBaseDN:         "DC=example,DC=com",
		GroupBaseDN:        "DC=example,DC=com",
		UserFilter:         "(objectClass=user)",
		GroupFilter:        "(objectClass=group)",
		ServiceAccountDN:   "CN=svc,DC=example,DC=com",
		PasswordCiphertext: "ciphertext",
		LastSuccessfulSyncAt: &now,
	}
	require.NoError(t, e.db.Create(directory).Error)
	return directory
}

func (e *dingTalkTestEnv) createUser(t *testing.T, username string, active bool) *types.User {
	t.Helper()
	user := &types.User{
		ID:           "user-" + username,
		Username:     username,
		Email:        username + "@example.com",
		PasswordHash: "hash",
		TenantID:     7,
		IsActive:     true,
	}
	require.NoError(t, e.db.Create(user).Error)
	if !active {
		// GORM omits zero-valued fields that carry a default tag, so the
		// disabled flag has to be persisted explicitly.
		require.NoError(t, e.db.Model(&types.User{}).
			Where("id = ?", user.ID).Update("is_active", false).Error)
		user.IsActive = false
	}
	return user
}

func (e *dingTalkTestEnv) createADIdentity(
	t *testing.T,
	samAccountName, userID string,
	status types.DirectoryObjectStatus,
) *types.DirectoryIdentity {
	t.Helper()
	identity := &types.DirectoryIdentity{
		ID:             "ad-" + samAccountName,
		DirectoryID:    "dir-1",
		ObjectGUID:     "guid-" + samAccountName,
		DN:             "CN=" + samAccountName + ",DC=example,DC=com",
		SAMAccountName: samAccountName,
		DisplayName:    "AD " + samAccountName,
		UserID:         &userID,
		Status:         status,
	}
	require.NoError(t, e.db.Create(identity).Error)
	return identity
}

func (e *dingTalkTestEnv) createBinding(
	t *testing.T,
	dingTalkUserID, jobNumber, userID string,
) *types.DingTalkIdentity {
	t.Helper()
	binding := &types.DingTalkIdentity{
		CorpID:         dingTalkTestCorpID,
		DingTalkUserID: dingTalkUserID,
		UnionID:        "union-" + dingTalkUserID,
		JobNumber:      jobNumber,
		DisplayName:    "钉钉 " + dingTalkUserID,
		UserID:         &userID,
		Status:         types.DingTalkIdentityActive,
	}
	require.NoError(t, e.repo.CreateBinding(context.Background(), binding))
	return binding
}

func (e *dingTalkTestEnv) events(eventType string) []types.DingTalkAuthEvent {
	var events []types.DingTalkAuthEvent
	e.db.Where("event_type = ?", eventType).Order("id ASC").Find(&events)
	return events
}

func (e *dingTalkTestEnv) userCount(t *testing.T) int64 {
	t.Helper()
	var count int64
	require.NoError(t, e.db.Model(&types.User{}).Count(&count).Error)
	return count
}

func (e *dingTalkTestEnv) activeBinding(t *testing.T, dingTalkUserID string) *types.DingTalkIdentity {
	t.Helper()
	binding, err := e.repo.FindActiveByDingTalkID(context.Background(), dingTalkTestCorpID, dingTalkUserID)
	require.NoError(t, err)
	return binding
}

func requireRejection(t *testing.T, err error, reason string) *DingTalkRejectionError {
	t.Helper()
	require.Error(t, err)
	var rejection *DingTalkRejectionError
	require.True(t, errors.As(err, &rejection), "expected DingTalkRejectionError, got %v", err)
	require.Equal(t, reason, rejection.Reason)
	return rejection
}

func TestDingTalkLoginNewBindingMatchesJobNumberExactly(t *testing.T) {
	env := setupDingTalkService(t)
	ctx := context.Background()
	env.createFreshDirectory(t)
	user := env.createUser(t, "zhangsan", true)
	// Leading zeros must survive: "00123" is the job number, not "123".
	env.createADIdentity(t, "00123", user.ID, types.DirectoryObjectActive)

	resp, err := env.svc.LoginWithDingTalkCode(ctx, "auth-code-1")
	require.NoError(t, err)
	require.True(t,resp.Success)
	require.Equal(t,user.ID, resp.User.ID)
	require.NotEmpty(t, resp.Token)
	require.NotEmpty(t, resp.RefreshToken)
	require.False(t, resp.IsNewUser, "dingtalk login must never report a new user")

	binding := env.activeBinding(t, "dt-user-1")
	require.NotNil(t, binding)
	require.Equal(t,"00123", binding.JobNumber)
	require.Equal(t,user.ID, *binding.UserID)

	// No account was created: the same single user row remains.
	require.EqualValues(t, 1, env.userCount(t))

	require.Len(t, env.events(types.DingTalkEventBind), 1)
	require.Len(t, env.events(types.DingTalkEventLoginSuccess), 1)
	require.Empty(t, env.events(types.DingTalkEventLoginRejected))
}

func TestDingTalkLoginExistingBindingKeepsUserOnJobNumberChange(t *testing.T) {
	env := setupDingTalkService(t)
	ctx := context.Background()
	env.createFreshDirectory(t)
	user := env.createUser(t, "lisi", true)
	env.createADIdentity(t, "00200", user.ID, types.DirectoryObjectActive)
	env.createBinding(t, "dt-user-1", "00200", user.ID)

	// The DingTalk profile now reports a different, legitimately reassigned
	// job number (AC-05): binding keeps the user, snapshot is refreshed.
	env.client.details["dt-user-1"] = fakeDingTalkProfile{
		JobNumber: "00999", Name: "李四新", UnionID: "union-1",
	}

	resp, err := env.svc.LoginWithDingTalkCode(ctx, "auth-code-1")
	require.NoError(t, err)
	require.True(t,resp.Success)
	require.Equal(t,user.ID, resp.User.ID)

	binding := env.activeBinding(t, "dt-user-1")
	require.Equal(t,user.ID, *binding.UserID, "user link must not change on job number change")
	require.Equal(t,"00999", binding.JobNumber)
	require.Len(t, env.events(types.DingTalkEventSnapshotUpdate), 1)
}

func TestDingTalkLoginRejections(t *testing.T) {
	env := setupDingTalkService(t)
	ctx := context.Background()
	env.createFreshDirectory(t)

	t.Run("empty job number", func(t *testing.T) {
		original := env.client.details["dt-user-1"]
		env.client.details["dt-user-1"] = fakeDingTalkProfile{JobNumber: "", Name: "无名", UnionID: "union-1"}
		defer func() { env.client.details["dt-user-1"] = original }()

		_, err := env.svc.LoginWithDingTalkCode(ctx, "auth-code-1")
		requireRejection(t, err, types.DingTalkRejectEmptyJobNumber)
		require.Empty(t, env.events(types.DingTalkEventBind))
	})

	t.Run("no ad match creates nothing", func(t *testing.T) {
		usersBefore := env.userCount(t)
		_, err := env.svc.LoginWithDingTalkCode(ctx, "auth-code-1")
		requireRejection(t, err, types.DingTalkRejectNoADMatch)
		require.Equal(t, usersBefore, env.userCount(t), "no account may be auto-created")
	})

	t.Run("duplicate job number", func(t *testing.T) {
		userA := env.createUser(t, "dup-a", true)
		userB := env.createUser(t, "dup-b", true)
		env.createADIdentity(t, "00300", userA.ID, types.DirectoryObjectActive)
		env.createADIdentity(t, "00300-guid2", userB.ID, types.DirectoryObjectActive)
		// Both identities share the same SAM account name.
		require.NoError(t, env.db.Model(&types.DirectoryIdentity{}).
			Where("id = ?", "ad-00300-guid2").
			Update("sam_account_name", "00300").Error)

		env.client.details["dt-user-1"] = fakeDingTalkProfile{JobNumber: "00300", Name: "三", UnionID: "union-1"}
		_, err := env.svc.LoginWithDingTalkCode(ctx, "auth-code-1")
		requireRejection(t, err, types.DingTalkRejectDuplicateJobNumber)
	})

	t.Run("ad identity disabled", func(t *testing.T) {
		user := env.createUser(t, "disabled-ad", false)
		env.createADIdentity(t, "00400", user.ID, types.DirectoryObjectDisabled)
		env.client.details["dt-user-1"] = fakeDingTalkProfile{JobNumber: "00400", Name: "四", UnionID: "union-1"}

		_, err := env.svc.LoginWithDingTalkCode(ctx, "auth-code-1")
		requireRejection(t, err, types.DingTalkRejectADOrUserDisabled)
	})

	t.Run("job number reused by another dingtalk account", func(t *testing.T) {
		user := env.createUser(t, "holder", true)
		env.createADIdentity(t, "00500", user.ID, types.DirectoryObjectActive)
		env.createBinding(t, "dt-holder", "00500", user.ID)
		env.client.details["dt-user-1"] = fakeDingTalkProfile{JobNumber: "00500", Name: "五", UnionID: "union-1"}

		_, err := env.svc.LoginWithDingTalkCode(ctx, "auth-code-1")
		requireRejection(t, err, types.DingTalkRejectBindingConflict)
		// The original binding is untouched.
		binding := env.activeBinding(t, "dt-holder")
		require.Equal(t,user.ID, *binding.UserID)
	})

	t.Run("wrong corp", func(t *testing.T) {
		env.client.contactCorpID = "some-other-corp"
		defer func() { env.client.contactCorpID = dingTalkTestCorpID }()

		_, err := env.svc.LoginWithDingTalkCode(ctx, "auth-code-1")
		requireRejection(t, err, types.DingTalkRejectWrongCorp)
	})

	t.Run("not enterprise member", func(t *testing.T) {
		env.client.userIDByUnionIDErr = map[string]error{"union-1": ErrDingTalkNotEnterpriseMember}
		defer func() { env.client.userIDByUnionIDErr = nil }()

		_, err := env.svc.LoginWithDingTalkCode(ctx, "auth-code-1")
		requireRejection(t, err, types.DingTalkRejectNotEnterpriseMember)
	})

	t.Run("existing binding linked user disabled", func(t *testing.T) {
		t.Cleanup(func() {
			e := env
			_ = e.db.Where("dingtalk_user_id = ?", "dt-user-1").Delete(&types.DingTalkIdentity{}).Error
		})
		user := env.createUser(t, "inactive-bound", false)
		env.createADIdentity(t, "00600", user.ID, types.DirectoryObjectActive)
		env.createBinding(t, "dt-user-1", "00600", user.ID)

		_, err := env.svc.LoginWithDingTalkCode(ctx, "auth-code-1")
		requireRejection(t, err, types.DingTalkRejectUserInactive)
	})

	t.Run("existing binding directory identity suspended", func(t *testing.T) {
		t.Cleanup(func() {
			e := env
			_ = e.db.Where("dingtalk_user_id = ?", "dt-user-1").Delete(&types.DingTalkIdentity{}).Error
		})
		user := env.createUser(t, "suspended-ad", true)
		env.createADIdentity(t, "00700", user.ID, types.DirectoryObjectDisabled)
		env.createBinding(t, "dt-user-1", "00700", user.ID)

		_, err := env.svc.LoginWithDingTalkCode(ctx, "auth-code-1")
		requireRejection(t, err, types.DingTalkRejectADOrUserDisabled)
	})

	t.Run("module disabled", func(t *testing.T) {
		env.cfg.Enable = false
		defer func() { env.cfg.Enable = true }()

		_, err := env.svc.LoginWithDingTalkCode(ctx, "auth-code-1")
		requireRejection(t, err, types.DingTalkRejectDisabled)
	})

	rejected := env.events(types.DingTalkEventLoginRejected)
	require.NotEmpty(t, rejected)
	for _, event := range rejected {
		require.NotEmpty(t, event.Reason, "every login_rejected row must carry a reason")
	}
}

func TestDingTalkLoginConcurrentBindingRaceAllowsExactlyOneWinner(t *testing.T) {
	env := setupDingTalkService(t)
	ctx := context.Background()
	env.createFreshDirectory(t)
	user := env.createUser(t, "raced", true)
	env.createADIdentity(t, "00800", user.ID, types.DirectoryObjectActive)

	// Two different DingTalk accounts present the same job number at the
	// same time; only one may end up bound (AC-04).
	env.client.contactByToken = map[string]fakeDingTalkContact{
		"user-token:code-a": {UnionID: "union-race-a", CorpID: dingTalkTestCorpID, Nick: "A"},
		"user-token:code-b": {UnionID: "union-race-b", CorpID: dingTalkTestCorpID, Nick: "B"},
	}
	env.client.userIDByUnionID = map[string]string{
		"union-race-a": "dt-race-a",
		"union-race-b": "dt-race-b",
	}
	env.client.details["dt-race-a"] = fakeDingTalkProfile{JobNumber: "00800", Name: "A", UnionID: "union-race-a"}
	env.client.details["dt-race-b"] = fakeDingTalkProfile{JobNumber: "00800", Name: "B", UnionID: "union-race-b"}

	var wg sync.WaitGroup
	results := make([]*types.OIDCCallbackResponse, 2)
	errs := make([]error, 2)
	codes := []string{"code-a", "code-b"}
	for i := range codes {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			results[slot], errs[slot] = env.svc.LoginWithDingTalkCode(ctx, codes[slot])
		}(i)
	}
	wg.Wait()

	successes := 0
	conflicts := 0
	for i := range codes {
		switch {
		case errs[i] == nil && results[i] != nil && results[i].Success:
			successes++
		default:
			requireRejection(t, errs[i], types.DingTalkRejectBindingConflict)
			conflicts++
		}
	}
	require.Equal(t, 1, successes, "exactly one concurrent binder must win")
	require.Equal(t, 1, conflicts)

	// The user ends up with exactly one active binding.
	active, err := env.repo.FindActiveByUserID(ctx, user.ID)
	require.NoError(t, err)
	require.NotNil(t, active)
	require.Len(t, env.events(types.DingTalkEventBind), 1)
}

func TestDingTalkInAppLogin(t *testing.T) {
	env := setupDingTalkService(t)
	ctx := context.Background()
	env.createFreshDirectory(t)
	user := env.createUser(t, "inapp", true)
	env.createADIdentity(t, "00900", user.ID, types.DirectoryObjectActive)
	env.client.details["dt-user-1"] = fakeDingTalkProfile{JobNumber: "00900", Name: "免登", UnionID: "union-1"}

	resp, err := env.svc.LoginWithDingTalkAuthCode(ctx, "inapp-code-1")
	require.NoError(t, err)
	require.True(t,resp.Success)
	require.Equal(t,user.ID, resp.User.ID)
	require.NotEmpty(t, resp.Token)
	require.Len(t, env.events(types.DingTalkEventBind), 1)
	require.Len(t, env.events(types.DingTalkEventLoginSuccess), 1)

	t.Run("not enterprise member", func(t *testing.T) {
		_, err := env.svc.LoginWithDingTalkAuthCode(ctx, "unknown-code")
		requireRejection(t, err, types.DingTalkRejectNotEnterpriseMember)
	})
}

func TestDingTalkUnlinkRevokesBindingAndSessions(t *testing.T) {
	env := setupDingTalkService(t)
	ctx := context.Background()
	env.createFreshDirectory(t)
	user := env.createUser(t, "unlinked", true)
	env.createADIdentity(t, "01000", user.ID, types.DirectoryObjectActive)
	env.client.details["dt-user-1"] = fakeDingTalkProfile{JobNumber: "01000", Name: "解绑", UnionID: "union-1"}

	login, err := env.svc.LoginWithDingTalkCode(ctx, "auth-code-1")
	require.NoError(t, err)
	require.True(t,login.Success)

	// Tokens issued by the login must be revoked by the unlink.
	var activeTokens int64
	env.db.Model(&types.AuthToken{}).Where("user_id = ? AND is_revoked = ?", user.ID, false).Count(&activeTokens)
	require.EqualValues(t, 2, activeTokens, "login should have created one access and one refresh token")

	require.NoError(t, env.svc.UnlinkDingTalkIdentity(ctx, "operator-1", user.ID))

	binding := env.activeBinding(t, "dt-user-1")
	require.Nil(t, binding, "binding must no longer be active")

	var row types.DingTalkIdentity
	require.NoError(t, env.db.Where("corp_id = ? AND dingtalk_user_id = ?", dingTalkTestCorpID, "dt-user-1").
		First(&row).Error)
	require.Equal(t,types.DingTalkIdentityUnlinked, row.Status)
	require.Nil(t, row.UserID)
	require.NotNil(t, row.UnboundAt)

	var revokedTokens int64
	env.db.Model(&types.AuthToken{}).Where("user_id = ? AND is_revoked = ?", user.ID, true).Count(&revokedTokens)
	require.EqualValues(t, 2, revokedTokens, "unlink must revoke all outstanding sessions")

	unbinds := env.events(types.DingTalkEventUnbind)
	require.Len(t, unbinds, 1)
	require.Contains(t, unbinds[0].Detail, "operator-1")

	// A later login re-binds the same DingTalk user via the historical row.
	login2, err := env.svc.LoginWithDingTalkCode(ctx, "auth-code-1")
	require.NoError(t, err)
	require.True(t,login2.Success)
	rebound := env.activeBinding(t, "dt-user-1")
	require.NotNil(t, rebound)
	require.EqualValues(t, row.ID, rebound.ID, "rebind must reactivate the historical row, not duplicate it")
}

func TestDingTalkAuthConfigSurface(t *testing.T) {
	env := setupDingTalkService(t)

	resp, err := env.svc.GetDingTalkAuthConfig(context.Background())
	require.NoError(t, err)
	require.True(t,resp.Success)
	require.True(t,resp.Enabled)
	require.Equal(t,dingTalkTestCorpID, resp.CorpID)
	require.Equal(t,"钉钉", resp.ProviderDisplayName)

	env.cfg.ProviderDisplayName = "公司钉钉"
	resp, err = env.svc.GetDingTalkAuthConfig(context.Background())
	require.NoError(t, err)
	require.Equal(t,"公司钉钉", resp.ProviderDisplayName)

	env.cfg.Enable = false
	resp, err = env.svc.GetDingTalkAuthConfig(context.Background())
	require.NoError(t, err)
	require.False(t,resp.Enabled)
	require.Empty(t, resp.CorpID)
}

func TestBuildDingTalkAuthorizeURLSignsState(t *testing.T) {
	env := setupDingTalkService(t)

	resp, err := env.svc.BuildDingTalkAuthorizeURL(context.Background(), "https://app.example.com/api/v1/auth/dingtalk/callback")
	require.NoError(t, err)
	require.True(t,resp.Success)
	require.Contains(t, resp.AuthorizationURL, "state="+url.QueryEscape(resp.State))
	require.Contains(t, resp.AuthorizationURL, "redirect_uri=")
	require.NotEmpty(t, resp.Nonce)
	require.NotEqual(t, resp.Nonce, resp.State)

	payload, err := secutils.VerifyDingTalkState(resp.State)
	require.NoError(t, err)
	require.Equal(t,resp.Nonce, payload.Nonce)
}

func TestDingTalkErrorsNeverLeakSecrets(t *testing.T) {
	env := setupDingTalkService(t)
	ctx := context.Background()

	// Every failure path must keep the app secret / codes out of error text.
	env.client.exchangeErr = fmt.Errorf("secret %s leaked", env.cfg.AppSecret)
	_, err := env.svc.LoginWithDingTalkCode(ctx, "auth-code-1")
	requireRejection(t, err, types.DingTalkRejectUnavailable)
	require.NotContains(t, err.Error(), env.cfg.AppSecret)
	require.NotContains(t, err.Error(), "auth-code-1")
}
