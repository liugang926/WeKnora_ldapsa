package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/types"
	secutils "github.com/Tencent/WeKnora/internal/utils"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// dingTalkTestAESKey is a 32-byte SYSTEM_AES_KEY stand-in so secret sealing
// works without depending on the ambient environment.
const dingTalkTestAESKey = "0123456789abcdef0123456789abcdef"

// newDingTalkAdminEnv wires the sqlite-backed userService in DATABASE config
// mode with a scripted credential-verification client.
func newDingTalkAdminEnv(t *testing.T) *dingTalkTestEnv {
	t.Helper()
	t.Setenv("SYSTEM_AES_KEY", dingTalkTestAESKey)
	env := setupDingTalkService(t)
	env.cfg.ConfigSource = config.DingTalkConfigSourceDatabase
	env.svc.dingTalkClientFactory = func(cfg *config.DingTalkAuthConfig) DingTalkClient {
		env.cfg.AppKey = cfg.AppKey
		env.cfg.AppSecret = cfg.AppSecret
		env.cfg.CorpID = cfg.CorpID
		return env.client
	}
	return env
}

// saveDingTalkSettings is the direct-row counterpart of the admin save, for
// seeding pre-existing configuration.
func (e *dingTalkTestEnv) saveDingTalkSettings(t *testing.T, settings *types.DingTalkAuthSettings) {
	t.Helper()
	settings.ID = 1
	require.NoError(t, e.repo.SaveSettings(context.Background(), settings))
	e.svc.invalidateDingTalkConfigCache()
}

func TestDingTalkMigrationUpDownUpIsRepeatable(t *testing.T) {
	t.Helper()
	db, err := gorm.Open(
		sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared&_foreign_keys=on&_busy_timeout=5000"),
		&gorm.Config{},
	)
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })

	exec := func(filename string) {
		raw, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "sqlite", filename))
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(raw)).Error)
	}
	exec("000087_dingtalk_sso.up.sql")
	exec("000088_dingtalk_admin_config.up.sql")

	// The widened CHECK accepts the two admin event types, and the rebuilt
	// table keeps its rows and indexes.
	require.NoError(t, db.Exec(
		"INSERT INTO dingtalk_auth_events (event_type, detail) VALUES (?, ?)",
		types.DingTalkEventConfigUpdate, "changed=enabled").Error)
	var detail string
	require.NoError(t, db.Raw(
		"SELECT detail FROM dingtalk_auth_events WHERE event_type = ?",
		types.DingTalkEventConfigUpdate).Scan(&detail).Error)
	require.Equal(t, "changed=enabled", detail)
	var idxCount int64
	require.NoError(t, db.Raw(
		"SELECT COUNT(*) FROM sqlite_master WHERE type='index' "+
			"AND tbl_name='dingtalk_auth_events' AND name LIKE "+
			"'idx_%'").
		Scan(&idxCount).Error)
	require.EqualValues(t, 2, idxCount, "both audit indexes must survive the rebuild")

	// Down then up again must be repeatable on the migrated database.
	exec("000088_dingtalk_admin_config.down.sql")
	var adminEvents int64
	require.NoError(t, db.Raw(
		"SELECT COUNT(*) FROM dingtalk_auth_events WHERE event_type IN ('config_update','config_test')").
		Scan(&adminEvents).Error)
	require.EqualValues(t, 0, adminEvents, "down must drop the admin-only audit rows")
	exec("000088_dingtalk_admin_config.up.sql")
	require.NoError(t, db.Exec(
		"INSERT INTO dingtalk_auth_events (event_type) VALUES ('config_test')").Error)

	// The settings table exists after the round trip and accepts the row 1.
	var tableCount int64
	require.NoError(t, db.Raw(
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='dingtalk_auth_settings'").
		Scan(&tableCount).Error)
	require.EqualValues(t, 1, tableCount)
}

func TestDingTalkEffectiveConfigLoaderMatrix(t *testing.T) {
	ctx := context.Background()

	t.Run("explicit file mode never consults the settings row", func(t *testing.T) {
		env := setupDingTalkService(t)
		env.cfg.ConfigSource = config.DingTalkConfigSourceFile // hard rollback switch
		env.saveDingTalkSettings(t, &types.DingTalkAuthSettings{
			Enabled: true, CorpID: "db-corp", AppKey: "db-key",
		})

		cfg, err := env.svc.dingTalkEffectiveConfig(ctx)
		require.NoError(t, err)
		require.Equal(t, dingTalkTestCorpID, cfg.CorpID, "env config must win in file mode")
		require.Equal(t, dingTalkTestAppKey, cfg.AppKey)

		resp, err := env.svc.GetDingTalkAuthConfig(ctx)
		require.NoError(t, err)
		require.True(t, resp.Enabled)
		require.Equal(t, dingTalkTestCorpID, resp.CorpID)
	})

	t.Run("database mode without row falls back to env", func(t *testing.T) {
		env := newDingTalkAdminEnv(t)

		cfg, err := env.svc.dingTalkEffectiveConfig(ctx)
		require.NoError(t, err)
		require.Equal(t, dingTalkTestCorpID, cfg.CorpID)
		require.Equal(t, "ding-app-secret", cfg.AppSecret)
		require.True(t, cfg.Enable, "env enable applies until a row exists")
	})

	t.Run("database mode overlays non-empty row fields only", func(t *testing.T) {
		env := newDingTalkAdminEnv(t)
		env.saveDingTalkSettings(t, &types.DingTalkAuthSettings{
			Enabled: false, // enabled always follows the row
			CorpID:  "db-corp",
			// AppKey, AgentID, DisplayName, RedirectURL left empty → env.
			AppSecretCiphertext: sealDingTalkSecret(t, "db-secret"),
		})

		cfg, err := env.svc.dingTalkEffectiveConfig(ctx)
		require.NoError(t, err)
		require.False(t, cfg.Enable, "enabled must follow the database row")
		require.Equal(t, "db-corp", cfg.CorpID)
		require.Equal(t, dingTalkTestAppKey, cfg.AppKey, "empty row field keeps env value")
		require.Equal(t, "db-secret", cfg.AppSecret, "stored ciphertext replaces env secret")

		resp, err := env.svc.GetDingTalkAuthConfig(ctx)
		require.NoError(t, err)
		require.False(t, resp.Enabled)
		require.Empty(t, resp.CorpID, "public endpoint must hide corp id when disabled")
	})

	t.Run("database mode with empty ciphertext falls back to env secret", func(t *testing.T) {
		env := newDingTalkAdminEnv(t)
		env.saveDingTalkSettings(t, &types.DingTalkAuthSettings{
			Enabled: true, CorpID: "db-corp", AppKey: "db-key",
		})

		cfg, err := env.svc.dingTalkEffectiveConfig(ctx)
		require.NoError(t, err)
		require.Equal(t, "ding-app-secret", cfg.AppSecret)
	})

	t.Run("undecryptable ciphertext fails closed", func(t *testing.T) {
		env := newDingTalkAdminEnv(t)
		env.saveDingTalkSettings(t, &types.DingTalkAuthSettings{
			Enabled: true, CorpID: "db-corp", AppKey: "db-key",
			AppSecretCiphertext: "enc:v1:not-valid-base64!!!",
		})

		_, err := env.svc.dingTalkEffectiveConfig(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "decrypt dingtalk app secret")

		// Login must fail closed (503-class plain error), not fall back.
		_, err = env.svc.LoginWithDingTalkCode(ctx, "auth-code-1")
		require.Error(t, err)
		_, isRejection := err.(*DingTalkRejectionError)
		require.False(t, isRejection, "a broken seal is an outage, not a rejection")
	})

	t.Run("cache expires after the TTL", func(t *testing.T) {
		env := newDingTalkAdminEnv(t)
		base := time.Now()
		now := base
		env.svc.dingTalkNow = func() time.Time { return now }

		cfg, err := env.svc.dingTalkEffectiveConfig(ctx)
		require.NoError(t, err)
		require.True(t, cfg.Enable)

		// Seed the row behind the loader's back (SaveSettings + NO cache
		// invalidation) so only the TTL can expose it.
		require.NoError(t, env.repo.SaveSettings(ctx, &types.DingTalkAuthSettings{
			ID: 1, Enabled: false, CorpID: "db-corp",
		}))

		// Inside the TTL the cached value still applies.
		now = base.Add(dingTalkConfigCacheTTL - time.Second)
		cfg, err = env.svc.dingTalkEffectiveConfig(ctx)
		require.NoError(t, err)
		require.True(t, cfg.Enable, "cached effective config within TTL")

		now = base.Add(dingTalkConfigCacheTTL + time.Second)
		cfg, err = env.svc.dingTalkEffectiveConfig(ctx)
		require.NoError(t, err)
		require.False(t, cfg.Enable, "expired cache must re-read the row")
	})
}

// sealDingTalkSecret encrypts with the same AES key system as production.
func sealDingTalkSecret(t *testing.T, secret string) string {
	t.Helper()
	ciphertext, err := secutils.EncryptAESGCM(secret, []byte(dingTalkTestAESKey))
	require.NoError(t, err)
	return ciphertext
}

func TestDingTalkAdminConfigGetIsRedacted(t *testing.T) {
	ctx := context.Background()

	t.Run("database view reports provenance without values", func(t *testing.T) {
		env := newDingTalkAdminEnv(t)
		saved, err := env.svc.UpdateDingTalkAdminConfig(ctx, &types.DingTalkAdminConfigUpdate{
			Enabled: true, CorpID: dingTalkTestCorpID, AppKey: "saved-key",
			DisplayName: "公司钉钉", AppSecret: "saved-secret-value",
		})
		require.NoError(t, err)
		require.True(t, saved.Success)
		require.True(t, saved.HasSecret)
		require.Equal(t, types.DingTalkSecretSourceDatabase, saved.SecretFrom)
		require.Equal(t, config.DingTalkConfigSourceDatabase, saved.ConfigSource)
		require.EqualValues(t, 1, saved.ConfigVersion)

		// The wire format must never carry the secret or its ciphertext.
		raw, err := json.Marshal(saved)
		require.NoError(t, err)
		require.NotContains(t, string(raw), "saved-secret-value")
		require.NotContains(t, string(raw), "app_secret")
		require.NotContains(t, string(raw), "ciphertext")
	})

	t.Run("env-backed secret is reported as env", func(t *testing.T) {
		env := newDingTalkAdminEnv(t)
		view, err := env.svc.GetDingTalkAdminConfig(ctx)
		require.NoError(t, err)
		require.True(t, view.HasSecret)
		require.Equal(t, types.DingTalkSecretSourceEnv, view.SecretFrom)
		require.Empty(t, view.ConfigVersion, "no row yet")
	})

	t.Run("file mode view reports the deployment config", func(t *testing.T) {
		env := setupDingTalkService(t)
		view, err := env.svc.GetDingTalkAdminConfig(ctx)
		require.NoError(t, err)
		require.Equal(t, config.DingTalkConfigSourceFile, view.ConfigSource)
		require.True(t, view.Enabled)
		require.True(t, view.HasSecret)
		require.Equal(t, types.DingTalkSecretSourceEnv, view.SecretFrom)
	})
}

func TestDingTalkAdminConfigUpdate(t *testing.T) {
	ctx := context.Background()

	t.Run("file mode is read-only", func(t *testing.T) {
		env := setupDingTalkService(t)
		_, err := env.svc.UpdateDingTalkAdminConfig(ctx, &types.DingTalkAdminConfigUpdate{Enabled: true})
		require.ErrorIs(t, err, ErrDingTalkConfigFileManaged)
	})

	t.Run("enabled requires corp id and app key", func(t *testing.T) {
		env := newDingTalkAdminEnv(t)
		_, err := env.svc.UpdateDingTalkAdminConfig(ctx, &types.DingTalkAdminConfigUpdate{
			Enabled: true, AppKey: "key-only",
		})
		require.ErrorIs(t, err, ErrInvalidDingTalkConfig)
		require.Contains(t, err.Error(), "corp_id")
	})

	t.Run("enabled requires a secret from some source", func(t *testing.T) {
		env := newDingTalkAdminEnv(t)
		// No row secret and no env secret either.
		env.cfg.AppSecret = ""
		env.svc.invalidateDingTalkConfigCache()
		_, err := env.svc.UpdateDingTalkAdminConfig(ctx, &types.DingTalkAdminConfigUpdate{
			Enabled: true, CorpID: "corp", AppKey: "key", ClearSecret: true,
		})
		require.ErrorIs(t, err, ErrInvalidDingTalkConfig)
		require.Contains(t, err.Error(), "app secret")
	})

	t.Run("empty app secret keeps the stored value", func(t *testing.T) {
		env := newDingTalkAdminEnv(t)
		_, err := env.svc.UpdateDingTalkAdminConfig(ctx, &types.DingTalkAdminConfigUpdate{
			Enabled: true, CorpID: dingTalkTestCorpID, AppKey: "saved-key",
			AppSecret: "first-secret",
		})
		require.NoError(t, err)

		second, err := env.svc.UpdateDingTalkAdminConfig(ctx, &types.DingTalkAdminConfigUpdate{
			Enabled: true, CorpID: dingTalkTestCorpID, AppKey: "saved-key",
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, second.ConfigVersion, "version bumps on every save")

		cfg, err := env.svc.dingTalkEffectiveConfig(ctx)
		require.NoError(t, err)
		require.Equal(t, "first-secret", cfg.AppSecret, "omitted secret keeps the stored one")
	})

	t.Run("clear secret falls back to env", func(t *testing.T) {
		env := newDingTalkAdminEnv(t)
		_, err := env.svc.UpdateDingTalkAdminConfig(ctx, &types.DingTalkAdminConfigUpdate{
			Enabled: true, CorpID: dingTalkTestCorpID, AppKey: "saved-key",
			AppSecret: "stored-secret",
		})
		require.NoError(t, err)

		view, err := env.svc.UpdateDingTalkAdminConfig(ctx, &types.DingTalkAdminConfigUpdate{
			Enabled: true, CorpID: dingTalkTestCorpID, AppKey: "saved-key", ClearSecret: true,
		})
		require.NoError(t, err)
		require.True(t, view.HasSecret)
		require.Equal(t, types.DingTalkSecretSourceEnv, view.SecretFrom)

		cfg, err := env.svc.dingTalkEffectiveConfig(ctx)
		require.NoError(t, err)
		require.Equal(t, "ding-app-secret", cfg.AppSecret)
	})

	t.Run("saving without SYSTEM_AES_KEY fails loudly", func(t *testing.T) {
		env := newDingTalkAdminEnv(t)
		t.Setenv("SYSTEM_AES_KEY", "")
		_, err := env.svc.UpdateDingTalkAdminConfig(ctx, &types.DingTalkAdminConfigUpdate{
			Enabled: true, CorpID: "corp", AppKey: "key", AppSecret: "no-key-set",
		})
		require.ErrorIs(t, err, ErrDirectoryEncryptionKey)
	})

	t.Run("save invalidates the cache immediately", func(t *testing.T) {
		env := newDingTalkAdminEnv(t)
		resp, err := env.svc.GetDingTalkAuthConfig(ctx)
		require.NoError(t, err)
		require.True(t, resp.Enabled)
		require.Equal(t, dingTalkTestCorpID, resp.CorpID)

		_, err = env.svc.UpdateDingTalkAdminConfig(ctx, &types.DingTalkAdminConfigUpdate{
			Enabled: true, CorpID: "new-corp", AppKey: "new-key", AppSecret: "new-secret",
		})
		require.NoError(t, err)

		resp, err = env.svc.GetDingTalkAuthConfig(ctx)
		require.NoError(t, err)
		require.Equal(t, "new-corp", resp.CorpID, "public config must observe the save at once")
	})

	t.Run("audit records changed field names and operator, never values", func(t *testing.T) {
		env := newDingTalkAdminEnv(t)
		ctxWithOperator := context.WithValue(ctx, types.UserIDContextKey, "admin-user-1")
		_, err := env.svc.UpdateDingTalkAdminConfig(ctxWithOperator, &types.DingTalkAdminConfigUpdate{
			Enabled: true, CorpID: "audit-corp", AppKey: "audit-key",
			DisplayName: "审计钉钉", AppSecret: "audit-secret-value",
		})
		require.NoError(t, err)

		events := env.events(types.DingTalkEventConfigUpdate)
		require.Len(t, events, 1)
		require.Contains(t, events[0].Detail, "operator=admin-user-1")
		// enabled stayed true (env baseline), so it must NOT be listed.
		for _, field := range []string{"corp_id", "app_key", "display_name", "app_secret"} {
			require.Contains(t, events[0].Detail, field)
		}
		require.Contains(t, events[0].Detail, "changed=")
		require.NotContains(t, events[0].Detail, "enabled,")
		require.NotContains(t, events[0].Detail, "audit-corp")
		require.NotContains(t, events[0].Detail, "audit-key")
		require.NotContains(t, events[0].Detail, "audit-secret-value")
		require.NotContains(t, events[0].Detail, "ding-app-secret")
	})
}

func TestDingTalkAdminConnectionTest(t *testing.T) {
	ctx := context.Background()

	t.Run("success confirms corp", func(t *testing.T) {
		env := newDingTalkAdminEnv(t)
		env.client.verifyErr = nil
		result, err := env.svc.TestDingTalkConnection(ctx, &types.DingTalkAdminConfigUpdate{
			CorpID: "probe-corp", AppKey: "probe-key", AppSecret: "probe-secret",
		})
		require.NoError(t, err)
		require.True(t, result.OK)
		require.Contains(t, result.Message, "probe-corp")
		require.Empty(t, result.ErrorCode)
		// The probe carried the candidate credentials, not the saved env ones.
		require.Equal(t, "probe-key", env.cfg.AppKey)
		require.Equal(t, "probe-secret", env.cfg.AppSecret)

		events := env.events(types.DingTalkEventConfigTest)
		require.Len(t, events, 1)
		require.Contains(t, events[0].Detail, "ok=true")
	})

	t.Run("credential rejection carries the platform error code", func(t *testing.T) {
		env := newDingTalkAdminEnv(t)
		env.client.verifyErr = &DingTalkCredentialError{
			Code: "invalid_client_secret", Message: "secret mismatch",
		}
		result, err := env.svc.TestDingTalkConnection(ctx, &types.DingTalkAdminConfigUpdate{
			CorpID: "corp", AppKey: "key", AppSecret: "bad-secret",
		})
		require.NoError(t, err)
		require.False(t, result.OK)
		require.Equal(t, "invalid_client_secret", result.ErrorCode)
		require.Equal(t, "secret mismatch", result.Message)
		require.Len(t, env.events(types.DingTalkEventConfigTest), 1)
	})

	t.Run("transport failure is reported without an error code", func(t *testing.T) {
		env := newDingTalkAdminEnv(t)
		env.client.verifyErr = fmt.Errorf(
			"%w: https://api.dingtalk.com/v1.0/oauth2/accessToken", ErrDingTalkAPITransient)
		result, err := env.svc.TestDingTalkConnection(ctx, &types.DingTalkAdminConfigUpdate{
			CorpID: "corp", AppKey: "key", AppSecret: "secret",
		})
		require.NoError(t, err)
		require.False(t, result.OK)
		require.Empty(t, result.ErrorCode)
		require.NotEmpty(t, result.Message)
	})

	t.Run("missing fields are a validation error", func(t *testing.T) {
		env := newDingTalkAdminEnv(t)
		// Empty the env baseline so neither the candidate nor the effective
		// config can supply the required fields.
		env.cfg.CorpID, env.cfg.AppKey, env.cfg.AppSecret = "", "", ""
		env.svc.invalidateDingTalkConfigCache()
		_, err := env.svc.TestDingTalkConnection(ctx, &types.DingTalkAdminConfigUpdate{
			CorpID: "corp-only",
		})
		require.ErrorIs(t, err, ErrInvalidDingTalkConfig)
	})

	t.Run("unspecified candidate fields fall back to effective values", func(t *testing.T) {
		env := newDingTalkAdminEnv(t)
		result, err := env.svc.TestDingTalkConnection(ctx, &types.DingTalkAdminConfigUpdate{})
		require.NoError(t, err)
		require.True(t, result.OK)
		require.Equal(t, dingTalkTestAppKey, env.cfg.AppKey)
		require.Equal(t, "ding-app-secret", env.cfg.AppSecret)
	})

	t.Run("file mode tests the deployment config itself", func(t *testing.T) {
		env := setupDingTalkService(t)
		var probed *config.DingTalkAuthConfig
		env.svc.dingTalkClientFactory = func(cfg *config.DingTalkAuthConfig) DingTalkClient {
			probed = cfg
			return env.client
		}
		result, err := env.svc.TestDingTalkConnection(ctx, &types.DingTalkAdminConfigUpdate{
			CorpID: "ignored-in-file-mode", AppKey: "ignored", AppSecret: "ignored",
		})
		require.NoError(t, err)
		require.True(t, result.OK)
		require.NotNil(t, probed)
		require.Equal(t, dingTalkTestCorpID, probed.CorpID, "file mode ignores echoed form values")
		require.Equal(t, "ding-app-secret", probed.AppSecret)
	})
}

// TestDingTalkFileModeLoginBehaviourUnchanged is the regression red line:
// with config_source=file — the hard rollback switch (DINGTALK_AUTH_CONFIG_
// SOURCE=file) — the login path resolves exactly as before even when a
// settings row exists. (With the source unset, a row now means database
// management; that path is covered by the adoption tests below.)
func TestDingTalkFileModeLoginBehaviourUnchanged(t *testing.T) {
	env := setupDingTalkService(t)
	env.cfg.ConfigSource = "file"
	ctx := context.Background()
	env.createFreshDirectory(t)
	user := env.createUser(t, "filemode", true)
	env.createADIdentity(t, "00123", user.ID, types.DirectoryObjectActive)

	// A stale database row must not influence file mode in any way.
	env.saveDingTalkSettings(t, &types.DingTalkAuthSettings{
		Enabled: false, CorpID: "db-corp", AppKey: "db-key",
		AppSecretCiphertext: sealDingTalkSecret(t, "db-secret"),
	})

	resp, err := env.svc.LoginWithDingTalkCode(ctx, "auth-code-1")
	require.NoError(t, err)
	require.True(t, resp.Success)
	require.Equal(t, user.ID, resp.User.ID)

	binding := env.activeBinding(t, "dt-user-1")
	require.NotNil(t, binding)
	require.Equal(t, dingTalkTestCorpID, binding.CorpID, "binding must use the env corp id")

	// Disabling via env still rejects, unaffected by the row's enabled=true.
	env.cfg.Enable = false
	defer func() { env.cfg.Enable = true }()
	_, err = env.svc.LoginWithDingTalkCode(ctx, "auth-code-1")
	_ = requireRejection(t, err, types.DingTalkRejectDisabled)
}

// TestDingTalkAdoptEnvConfigIntoSettings covers the one-click migration from
// env-managed to UI-managed: unset config source + no row → the env values
// (including a re-sealed secret) land in the settings row and the module is
// database-managed from that moment, without a restart.
func TestDingTalkAdoptEnvConfigIntoSettings(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", dingTalkTestAESKey)
	env := setupDingTalkService(t)
	env.cfg.ConfigSource = "" // legacy default: env-managed until a row exists
	env.cfg.Enable = true
	env.cfg.CorpID = dingTalkTestCorpID
	env.cfg.AppKey = "ding-app-key"
	env.cfg.AppSecret = "ding-app-secret"
	env.cfg.AgentID = "4620050040"
	ctx := context.Background()

	view, err := env.svc.AdoptDingTalkEnvConfig(ctx)
	require.NoError(t, err)
	require.True(t, view.Success)
	require.Equal(t, config.DingTalkConfigSourceDatabase, view.ConfigSource)
	require.True(t, view.Enabled)
	require.Equal(t, dingTalkTestCorpID, view.CorpID)
	require.Equal(t, "ding-app-key", view.AppKey)
	require.True(t, view.HasSecret)
	require.Equal(t, types.DingTalkSecretSourceDatabase, view.SecretFrom)

	// The row is authoritative immediately: cache invalidated by the adopt.
	env.cfg.CorpID = "changed-env-corp-after-adopt"
	fresh, err := env.svc.GetDingTalkAdminConfig(ctx)
	require.NoError(t, err)
	require.Equal(t, dingTalkTestCorpID, fresh.CorpID, "database row wins after adoption")

	// A second adoption is refused — the UI already owns the settings.
	_, err = env.svc.AdoptDingTalkEnvConfig(ctx)
	require.ErrorIs(t, err, ErrDingTalkConfigAlreadyManaged)
}

// Adoption must stay impossible while the hard rollback switch
// (DINGTALK_AUTH_CONFIG_SOURCE=file) is engaged, and an env baseline without
// corp/app key must not be adoptable.
func TestDingTalkAdoptEnvConfigRefusals(t *testing.T) {
	t.Run("explicit file mode refuses adoption", func(t *testing.T) {
		t.Setenv("SYSTEM_AES_KEY", dingTalkTestAESKey)
		env := setupDingTalkService(t)
		env.cfg.ConfigSource = config.DingTalkConfigSourceFile
		env.cfg.CorpID = dingTalkTestCorpID
		env.cfg.AppKey = "ding-app-key"
		_, err := env.svc.AdoptDingTalkEnvConfig(context.Background())
		require.ErrorIs(t, err, ErrDingTalkConfigFileManaged)
	})

	t.Run("env without corp/app key refuses adoption", func(t *testing.T) {
		t.Setenv("SYSTEM_AES_KEY", dingTalkTestAESKey)
		env := setupDingTalkService(t)
		env.cfg.ConfigSource = ""
		env.cfg.CorpID = ""
		env.cfg.AppKey = ""
		_, err := env.svc.AdoptDingTalkEnvConfig(context.Background())
		require.ErrorIs(t, err, ErrInvalidDingTalkConfig)
	})
}
