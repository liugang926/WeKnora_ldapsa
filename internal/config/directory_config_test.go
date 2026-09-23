package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func clearDirectoryEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"LDAP_ENABLED", "LDAP_CONFIG_SOURCE", "LDAP_DIRECTORY_ID", "LDAP_PROVIDER_DISPLAY_NAME",
		"LDAP_URLS", "LDAP_TLS_MODE", "LDAP_SERVER_NAMES", "LDAP_BIND_DN", "LDAP_BIND_PASSWORD",
		"LDAP_BIND_PASSWORD_FILE", "LDAP_BASE_DN", "LDAP_USER_BASE_DN", "LDAP_GROUP_BASE_DN",
		"LDAP_USER_FILTER", "LDAP_GROUP_FILTER", "LDAP_LOGIN_FILTER", "LDAP_ALLOWED_LOGIN_FILTER",
		"LDAP_CA_FILE", "LDAP_CONNECT_TIMEOUT", "LDAP_QUERY_TIMEOUT", "LDAP_PAGE_SIZE",
		"LDAP_RESULT_LIMIT", "LDAP_SYNC_INTERVAL", "LDAP_STALE_AFTER",
	} {
		t.Setenv(name, "")
		_ = os.Unsetenv(name)
	}
}

func TestDirectoryDefaultsDisabled(t *testing.T) {
	clearDirectoryEnv(t)
	cfg := &Config{}
	if err := applyDirectoryEnvOverrides(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Directory == nil || cfg.Directory.Enabled {
		t.Fatal("directory must exist and remain disabled by default")
	}
	if cfg.Directory.SyncInterval != 5*time.Minute || cfg.Directory.StaleAfter != 15*time.Minute {
		t.Fatalf("unexpected sync defaults: interval=%s stale=%s",
			cfg.Directory.SyncInterval, cfg.Directory.StaleAfter)
	}
}

func TestDirectoryPasswordFileAndEnvironment(t *testing.T) {
	clearDirectoryEnv(t)
	dir := t.TempDir()
	passwordPath := filepath.Join(dir, "bind-password")
	if err := os.WriteFile(passwordPath, []byte("not-logged-or-returned\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LDAP_ENABLED", "true")
	t.Setenv("LDAP_URLS", "ldaps://dc1.example.test:636,ldaps://dc2.example.test:636")
	t.Setenv("LDAP_TLS_MODE", "ldaps")
	t.Setenv("LDAP_BIND_DN", "CN=svc,OU=Service Accounts,DC=example,DC=test")
	t.Setenv("LDAP_BIND_PASSWORD_FILE", passwordPath)
	t.Setenv("LDAP_BASE_DN", "DC=example,DC=test")

	cfg := &Config{}
	if err := applyDirectoryEnvOverrides(cfg); err != nil {
		t.Fatal(err)
	}
	if got := cfg.Directory.BindPassword; got != "not-logged-or-returned" {
		t.Fatalf("password file was not resolved, got %q", got)
	}
	if len(cfg.Directory.Servers) != 2 || !cfg.Directory.ConfiguredFromEnvironment {
		t.Fatalf("unexpected servers/source: %#v", cfg.Directory)
	}
	if err := ValidateConfig(cfg); err != nil {
		t.Fatal(err)
	}
}

func TestDirectorySecretFileRejectsNonRegularFiles(t *testing.T) {
	if _, err := readDirectorySecretFile("/dev/null"); err == nil ||
		!strings.Contains(err.Error(), "regular file") {
		t.Fatalf("expected device file rejection, got %v", err)
	}
}

func TestDirectoryRejectsAmbiguousSecretSources(t *testing.T) {
	clearDirectoryEnv(t)
	t.Setenv("LDAP_BIND_PASSWORD", "inline")
	t.Setenv("LDAP_BIND_PASSWORD_FILE", "/run/secrets/ldap")
	if err := applyDirectoryEnvOverrides(&Config{}); err == nil {
		t.Fatal("expected conflicting secret sources to be rejected")
	}
}

func TestDirectoryEnvironmentPasswordPreservesWhitespace(t *testing.T) {
	clearDirectoryEnv(t)
	t.Setenv("LDAP_BIND_PASSWORD", "  secret with spaces  ")
	cfg := &Config{}
	if err := applyDirectoryEnvOverrides(cfg); err != nil {
		t.Fatal(err)
	}
	if got := cfg.Directory.BindPassword; got != "  secret with spaces  " {
		t.Fatalf("LDAP password was normalized: %q", got)
	}
}

func TestDirectoryRejectsInsecureOrMismatchedTransport(t *testing.T) {
	clearDirectoryEnv(t)
	cfg := &Config{Directory: &DirectoryConfig{
		Enabled:          true,
		ManagementSource: DirectoryManagementFile,
		BindDN:           "cn=svc,dc=example,dc=test",
		BindPassword:     "secret",
		BaseDN:           "dc=example,dc=test",
		Servers: []DirectoryServerConfig{{
			URL: "ldap://dc.example.test:389", TLSMode: DirectoryTLSLDAPS,
		}},
		ConnectTimeout: time.Second,
		QueryTimeout:   time.Second,
		PageSize:       10,
		ResultLimit:    20,
		SyncInterval:   time.Minute,
		StaleAfter:     time.Minute,
	}}
	if err := ValidateConfig(cfg); err == nil {
		t.Fatal("expected ldap:// without StartTLS to be rejected")
	}
}

func TestDirectoryRejectsUnsafeLoginFilterTemplate(t *testing.T) {
	base := &DirectoryConfig{
		Enabled: true, ManagementSource: DirectoryManagementFile,
		BindDN: "cn=svc,dc=example,dc=test", BindPassword: "secret", BaseDN: "dc=example,dc=test",
		Servers: []DirectoryServerConfig{
			{URL: "ldaps://dc.example.test:636", TLSMode: DirectoryTLSLDAPS},
		},
		ConnectTimeout: time.Second, QueryTimeout: time.Second, PageSize: 10, ResultLimit: 20,
		SyncInterval: time.Minute, StaleAfter: time.Minute,
	}
	missing := *base
	missing.LoginFilter = "(sAMAccountName=alice)"
	if err := ValidateConfig(&Config{Directory: &missing}); err == nil ||
		!strings.Contains(err.Error(), "must contain {login}") {
		t.Fatalf("expected missing placeholder rejection, got %v", err)
	}
	invalid := *base
	invalid.LoginFilter = "(&(sAMAccountName={login})"
	if err := ValidateConfig(&Config{Directory: &invalid}); err == nil ||
		!strings.Contains(err.Error(), "login_filter is invalid") {
		t.Fatalf("expected invalid filter rejection, got %v", err)
	}
}
