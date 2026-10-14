package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// The data-source PUT handler permits a KB editor and intentionally preserves
// stored credentials. A changed endpoint must be denied before the connector
// receives those credentials for its live validation request.
func TestKBEditorCannotRepointPairedNextcloudAndLeakBearer(t *testing.T) {
	var requests atomic.Int32
	attacker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Header.Get("Authorization") != "" {
			t.Errorf("Bearer sent to untrusted host")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer attacker.Close()
	// Approve the destination here so the endpoint guard itself, rather than
	// the independent server allowlist, must stop a stored-token redirect.
	t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
	t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", attacker.URL)
	storedConfig, err := (&types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		Settings:    map[string]interface{}{"base_url": "https://trusted.example"},
		Credentials: map[string]interface{}{"token": "private-machine-token"},
		ResourceIDs: []string{"dev-published"},
	}).ToJSON()
	require.NoError(t, err)
	stored := &types.DataSource{
		ID: "paired", TenantID: 7, KnowledgeBaseID: "kb",
		Type: types.ConnectorTypeNextcloud, Config: storedConfig,
	}
	proposedConfig, err := (&types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		Settings:    map[string]interface{}{"base_url": attacker.URL},
		ResourceIDs: []string{"dev-published"},
	}).ToJSON()
	require.NoError(t, err)
	registry := datasource.NewConnectorRegistry()
	require.NoError(t, registry.Register(nextcloud.NewConnector()))
	svc := &DataSourceService{
		dsRepo:            newKBDeleteDSRepo(stored.KnowledgeBaseID, stored),
		connectorRegistry: registry,
	}
	_, err = svc.UpdateDataSource(context.Background(), &types.DataSource{
		ID: stored.ID, TenantID: stored.TenantID, KnowledgeBaseID: stored.KnowledgeBaseID,
		Type: stored.Type, Config: proposedConfig,
	})
	require.ErrorContains(t, err, "base URL cannot change")
	require.Zero(t, requests.Load(), "validation disclosed the preserved Bearer")

	_, err = svc.UpdateDataSource(context.Background(), &types.DataSource{
		ID: stored.ID, TenantID: stored.TenantID, KnowledgeBaseID: stored.KnowledgeBaseID,
		Type: types.ConnectorTypeRSS, Config: proposedConfig,
	})
	require.ErrorContains(t, err, "type cannot change")
	require.Zero(t, requests.Load(), "type transition disclosed the preserved Bearer")
}
