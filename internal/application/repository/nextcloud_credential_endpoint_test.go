package repository

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func nextcloudEndpointTestConfig(t *testing.T, baseURL, token string) types.JSON {
	t.Helper()
	config := map[string]interface{}{
		"type":         types.ConnectorTypeNextcloud,
		"settings":     map[string]interface{}{"base_url": baseURL},
		"resource_ids": []string{"dev-published"},
		"credentials":  map[string]interface{}{},
	}
	if token != "" {
		config["credentials"] = map[string]interface{}{"token": token}
	}
	encoded, err := json.Marshal(config)
	require.NoError(t, err)
	return types.JSON(encoded)
}

func TestNextcloudCredentialAndEndpointUpdatesCannotCombineAcrossStaleWriters(t *testing.T) {
	db := provenanceTestDB(t)
	require.NoError(t, db.Exec("INSERT INTO knowledge_bases (id, tenant_id) VALUES ('kb', 7)").Error)
	repo := NewDataSourceRepository(db)
	ctx := context.Background()
	base := "https://trusted.example"
	old := &types.DataSource{
		ID: "paired", TenantID: 7, KnowledgeBaseID: "kb",
		Type: types.ConnectorTypeNextcloud, Config: nextcloudEndpointTestConfig(t, base, "private-token"),
	}
	require.NoError(t, repo.Create(ctx, old))

	update := func(updateCtx context.Context, url, token string) error {
		return repo.Update(updateCtx, &types.DataSource{
			ID: old.ID, TenantID: old.TenantID,
			KnowledgeBaseID: old.KnowledgeBaseID, Type: old.Type,
			Config: nextcloudEndpointTestConfig(t, url, token),
		})
	}
	storedURL := func() string {
		t.Helper()
		stored, err := repo.FindByID(ctx, old.ID)
		require.NoError(t, err)
		parsed, err := stored.ParseConfig()
		require.NoError(t, err)
		value, _ := parsed.Settings["base_url"].(string)
		return value
	}

	require.ErrorContains(t, update(ctx, "https://attacker.example", "private-token"), "base URL")
	require.ErrorContains(t, update(ctx, "https://attacker.example", ""), "base URL")
	require.Equal(t, base, storedURL())

	// Clearing first permits a legitimate endpoint edit. A credential writer
	// that read the old URL before this edit must then fail under the KB lock.
	require.NoError(t, update(WithNextcloudCredentialWrite(ctx), base, ""))
	require.NoError(t, update(ctx, base, "private-token"),
		"a stale ordinary update should preserve the committed revocation")
	staleResult, err := repo.FindByID(ctx, old.ID)
	require.NoError(t, err)
	staleConfig, err := staleResult.ParseConfig()
	require.NoError(t, err)
	require.Empty(t, staleConfig.Credentials, "stale ordinary PUT or sync status must not revive a token")
	require.NoError(t, update(ctx, "https://attacker.example", ""))
	require.ErrorContains(t, update(WithNextcloudCredentialWrite(ctx), base, "new-token"), "base URL")
	require.Equal(t, "https://attacker.example", storedURL())
	stored, err := repo.FindByID(ctx, old.ID)
	require.NoError(t, err)
	parsed, err := stored.ParseConfig()
	require.NoError(t, err)
	require.Empty(t, parsed.Credentials)
	require.NotContains(t, strings.ToLower(string(stored.Config)), "new-token")
}
