package nextcloud

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// Opt-in component check against the owned Nextcloud PHP/PG and actual LDAP
// fixture. Credentials remain in the external mode-0600 fixture, never logs.
func TestNextcloudLiveOwnedRootAuthorizationAndContentRevision(t *testing.T) {
	path := os.Getenv("WEKNORA_NEXTCLOUD_ROOT_AUTH_FIXTURE")
	if path == "" {
		t.Skip("owned Nextcloud/LDAP fixture required")
	}
	var f struct {
		Binding   string `json:"binding_id"`
		Key       string `json:"key_id"`
		Token     string `json:"token"`
		Instance  string `json:"instance_id"`
		Directory string `json:"directory_id"`
		Alice     string `json:"alice_guid"`
		Bob       string `json:"bob_guid"`
		File      string `json:"file_id"`
		Revision  string `json:"source_content_revision"`
	}
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &f))
	base := os.Getenv("WEKNORA_NEXTCLOUD_ROOT_AUTH_ORIGIN")
	require.NotEmpty(t, base)
	t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
	t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", base)
	cfg := &types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		ResourceIDs: []string{f.Binding},
		Settings:    map[string]any{"base_url": base},
		Credentials: map[string]any{"key_id": f.Key, "token": f.Token},
	}
	require.NoError(t, AuthorizeCurrentRoot(context.Background(), cfg, f.Instance, f.Binding, f.Directory, f.Alice))
	require.Error(t, AuthorizeCurrentRoot(context.Background(), cfg, f.Instance, f.Binding, f.Directory, f.Bob))
	file, err := strconv.ParseInt(f.File, 10, 64)
	require.NoError(t, err)
	old, err := AuthorizeCurrentFile(context.Background(), cfg, f.Instance, f.Binding, f.Directory, f.Alice, file)
	require.NoError(t, err)
	require.False(t, old.Allow, "legacy request revision zero cannot read current nonzero bytes")
	current, err := AuthorizeCurrentFile(
		types.WithNextcloudContentRevision(context.Background(), f.Revision),
		cfg,
		f.Instance,
		f.Binding,
		f.Directory,
		f.Alice,
		file,
	)
	require.NoError(t, err)
	require.True(t, current.Allow)
	require.Equal(t, f.Revision, current.SourceContentRevision)
	t.Log(
		("actual Nextcloud signed machine HTTP with live LDAP: allowed Alice root/cu" +
			"rrent file; denied Bob root and legacy content revision"),
	)
}
