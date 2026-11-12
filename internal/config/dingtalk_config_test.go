package config

import (
	"strings"
	"testing"
)

// TestApplyDingTalkEnvOverrides pins down the env-vs-YAML contract for the
// DingTalk SSO module, mirroring the OIDC rules: environment wins over
// YAML, then defaults (display name, callback URL) are back-filled.
func TestApplyDingTalkEnvOverrides(t *testing.T) {
	t.Run("defaults when nothing configured", func(t *testing.T) {
		cfg := &Config{}
		applyDingTalkEnvOverrides(cfg)
		if cfg.DingTalkAuth == nil {
			t.Fatal("DingTalkAuth section must be initialized")
		}
		if cfg.DingTalkAuth.Enable {
			t.Fatal("module must stay disabled by default")
		}
		if cfg.DingTalkAuth.ProviderDisplayName != "钉钉" {
			t.Fatalf("display name = %q, want the Chinese default", cfg.DingTalkAuth.ProviderDisplayName)
		}
	})

	t.Run("environment overrides yaml", func(t *testing.T) {
		t.Setenv("DINGTALK_AUTH_ENABLE", "true")
		t.Setenv("DINGTALK_AUTH_CORP_ID", "ding-env-corp")
		t.Setenv("DINGTALK_AUTH_APP_KEY", "ding-env-key")
		t.Setenv("DINGTALK_AUTH_APP_SECRET", "ding-env-secret")
		t.Setenv("DINGTALK_AUTH_AGENT_ID", "ding-env-agent")
		t.Setenv("DINGTALK_AUTH_DISPLAY_NAME", "公司钉钉")
		t.Setenv("DINGTALK_AUTH_REDIRECT_URL", "https://app.example.com/api/v1/auth/dingtalk/callback")

		cfg := &Config{DingTalkAuth: &DingTalkAuthConfig{
			CorpID: "yaml-corp", AppKey: "yaml-key", AppSecret: "yaml-secret",
		}}
		applyDingTalkEnvOverrides(cfg)

		d := cfg.DingTalkAuth
		if !d.Enable || d.CorpID != "ding-env-corp" || d.AppKey != "ding-env-key" ||
			d.AppSecret != "ding-env-secret" || d.AgentID != "ding-env-agent" ||
			d.ProviderDisplayName != "公司钉钉" ||
			d.RedirectURL != "https://app.example.com/api/v1/auth/dingtalk/callback" {
			t.Fatalf("overrides not applied: %+v", d)
		}
	})

	t.Run("redirect url derives from frontend base", func(t *testing.T) {
		cfg := &Config{
			FrontendBaseURL: "https://weknora.example.com/",
			DingTalkAuth:    &DingTalkAuthConfig{},
		}
		applyDingTalkEnvOverrides(cfg)
		want := "https://weknora.example.com/api/v1/auth/dingtalk/callback"
		if cfg.DingTalkAuth.RedirectURL != want {
			t.Fatalf("redirect_url = %q, want %q", cfg.DingTalkAuth.RedirectURL, want)
		}
	})

	t.Run("explicit redirect url wins over derivation", func(t *testing.T) {
		cfg := &Config{
			FrontendBaseURL: "https://weknora.example.com",
			DingTalkAuth: &DingTalkAuthConfig{
				RedirectURL: "https://other.example.com/api/v1/auth/dingtalk/callback",
			},
		}
		applyDingTalkEnvOverrides(cfg)
		if cfg.DingTalkAuth.RedirectURL != "https://other.example.com/api/v1/auth/dingtalk/callback" {
			t.Fatalf("redirect_url = %q", cfg.DingTalkAuth.RedirectURL)
		}
	})

	t.Run("config source defaults to file and follows the env var", func(t *testing.T) {
		cfg := &Config{}
		applyDingTalkEnvOverrides(cfg)
		if cfg.DingTalkAuth.ConfigSource != DingTalkConfigSourceFile {
			t.Fatalf("config_source = %q, want file default", cfg.DingTalkAuth.ConfigSource)
		}

		t.Setenv("DINGTALK_AUTH_CONFIG_SOURCE", "database")
		applyDingTalkEnvOverrides(cfg)
		if cfg.DingTalkAuth.ConfigSource != DingTalkConfigSourceDatabase {
			t.Fatalf("config_source = %q, want database", cfg.DingTalkAuth.ConfigSource)
		}
		if !cfg.DingTalkAuth.IsDatabaseManaged() {
			t.Fatal("IsDatabaseManaged must be true for the database source")
		}
	})
}

func TestValidateConfigDingTalk(t *testing.T) {
	t.Run("disabled module needs nothing", func(t *testing.T) {
		cfg := &Config{DingTalkAuth: &DingTalkAuthConfig{}}
		if err := ValidateConfig(cfg); err != nil {
			t.Fatalf("disabled module must validate: %v", err)
		}
	})

	t.Run("enabled module requires corp id, app key and secret", func(t *testing.T) {
		cfg := &Config{DingTalkAuth: &DingTalkAuthConfig{Enable: true}}
		err := ValidateConfig(cfg)
		if err == nil {
			t.Fatal("expected validation error for incomplete dingtalk config")
		}
		for _, want := range []string{"corp_id", "app_key", "app_secret"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q should mention %s", err, want)
			}
		}
	})

	t.Run("complete module validates", func(t *testing.T) {
		cfg := &Config{DingTalkAuth: &DingTalkAuthConfig{
			Enable: true, CorpID: "corp", AppKey: "key", AppSecret: "secret",
		}}
		if err := ValidateConfig(cfg); err != nil {
			t.Fatalf("complete module must validate: %v", err)
		}
	})

	t.Run("illegal config source is rejected", func(t *testing.T) {
		cfg := &Config{DingTalkAuth: &DingTalkAuthConfig{ConfigSource: "both"}}
		err := ValidateConfig(cfg)
		if err == nil {
			t.Fatal("expected validation error for illegal config_source")
		}
		if !strings.Contains(err.Error(), "config_source") {
			t.Fatalf("error %q should mention config_source", err)
		}
	})

	t.Run("database source defers field requirements to save time", func(t *testing.T) {
		cfg := &Config{DingTalkAuth: &DingTalkAuthConfig{
			Enable: true, ConfigSource: DingTalkConfigSourceDatabase,
		}}
		if err := ValidateConfig(cfg); err != nil {
			t.Fatalf("database-managed module must validate without env fields: %v", err)
		}
	})
}
