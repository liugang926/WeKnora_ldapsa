package nextcloud

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestRootMigrationSignsExactSourceAndRejectsChangedAck(t *testing.T) {
	proof := RootMigration{
		OperationID:     "move",
		PairOperationID: "pair",
		BindingID:       "root",
		InstanceID:      "instance",
		TenantID:        "7",
		KnowledgeBaseID: "kb",
		DataSourceID:    "source",
		OldRootHash:     "old",
		NewRootHash:     "new",
		State:           "observed",
	}
	bad := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer source-secret", r.Header.Get("Authorization"))
		require.NotEmpty(t, r.Header.Get("X-WeKnora-Signature"))
		require.Contains(t, r.URL.Path, "/bindings/root/root-migrations/move")
		p := proof
		if r.Method == http.MethodPost {
			var received RootMigration
			require.NoError(t, json.NewDecoder(r.Body).Decode(&received))
			require.Equal(t, proof, received)
			p.State = "finalized"
			if bad {
				p.NewRootHash = "another"
			}
		}
		writeJSON(w, map[string]any{"migration": p})
	}))
	defer server.Close()
	allowHTTPTestOrigin(t, server.URL)
	cfg := &types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		ResourceIDs: []string{"root"},
		Settings:    map[string]interface{}{"base_url": server.URL},
		Credentials: map[string]interface{}{
			"token":  "source-secret",
			"key_id": "pair_01234567890123456789012345678901",
		},
	}
	got, err := ReadRootMigration(context.Background(), cfg, "root", "move")
	require.NoError(t, err)
	require.Equal(t, proof, got)
	ack, err := AcknowledgeRootMigration(context.Background(), cfg, proof)
	require.NoError(t, err)
	require.Equal(t, "finalized", ack.State)
	bad = true
	_, err = AcknowledgeRootMigration(context.Background(), cfg, proof)
	require.ErrorContains(t, err, "ACK mismatch")
}
