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

func TestCommitSourcePairingSignsAndChecksExactAck(t *testing.T) {
	called := 0
	wrongAck := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called++
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, apiPath+"/bindings/dev-published/source-pairing/commit", r.URL.Path)
		require.Equal(t, "Bearer pairing-token", r.Header.Get("Authorization"))
		require.Equal(t, "pair_82d622251eb647ad92642759e33dfa03", r.Header.Get("X-WeKnora-Key-Id"))
		require.NotEmpty(t, r.Header.Get("X-WeKnora-Signature"))
		var body map[string]string
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, "7", body["tenant_id"])
		pair := map[string]any{
			"operation_id": body["operation_id"], "instance_id": body["instance_id"],
			"binding_id": "dev-published", "tenant_id": body["tenant_id"],
			"knowledge_base_id": body["knowledge_base_id"], "data_source_id": body["data_source_id"], "state": "active",
		}
		if wrongAck {
			pair["data_source_id"] = "another-source"
		}
		writeJSON(w, map[string]any{"pairing": pair, "changed": called == 1})
	}))
	defer server.Close()
	allowHTTPTestOrigin(t, server.URL)
	config := &types.DataSourceConfig{
		Type:     types.ConnectorTypeNextcloud,
		Settings: map[string]interface{}{"base_url": server.URL},
		Credentials: map[string]interface{}{
			"token":  "pairing-token",
			"key_id": "pair_82d622251eb647ad92642759e33dfa03",
		},
		ResourceIDs: []string{"dev-published"},
	}
	pair := SourcePairingCommit{
		OperationID: "82d62225-1eb6-47ad-9264-2759e33dfa03",
		InstanceID:  "instance-1", BindingID: "dev-published", TenantID: 7,
		KnowledgeBaseID: "kb-1", DataSourceID: "source-1",
	}
	require.NoError(t, CommitSourcePairing(context.Background(), config, pair))
	require.NoError(t, CommitSourcePairing(context.Background(), config, pair), "unchanged ACK is idempotent")
	wrongAck = true
	require.ErrorContains(t, CommitSourcePairing(context.Background(), config, pair), "tuple mismatch")
}

func TestAbortSourcePairingSignsExactIntentAndRejectsWrongAck(t *testing.T) {
	wrongAck := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, apiPath+"/bindings/dev-published/source-pairing/abort", r.URL.Path)
		require.Equal(t, "Bearer pairing-token", r.Header.Get("Authorization"))
		require.Equal(t, "pair_82d622251eb647ad92642759e33dfa03", r.Header.Get("X-WeKnora-Key-Id"))
		require.NotEmpty(t, r.Header.Get("X-WeKnora-Signature"))
		var body map[string]string
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, "7", body["tenant_id"])
		require.NotContains(t, body, "data_source_id", "Nextcloud has not learned the pending source ID")
		binding := "dev-published"
		if wrongAck {
			binding = "wrong-binding"
		}
		writeJSON(w, map[string]any{"pairing": map[string]any{
			"operation_id": body["operation_id"], "instance_id": body["instance_id"],
			"binding_id": binding, "tenant_id": body["tenant_id"],
			"knowledge_base_id": body["knowledge_base_id"], "data_source_id": nil,
			"key_id": "pair_82d622251eb647ad92642759e33dfa03", "state": "aborted",
		}})
	}))
	defer server.Close()
	allowHTTPTestOrigin(t, server.URL)
	config := &types.DataSourceConfig{
		Type:     types.ConnectorTypeNextcloud,
		Settings: map[string]interface{}{"base_url": server.URL},
		Credentials: map[string]interface{}{
			"token":  "pairing-token",
			"key_id": "pair_82d622251eb647ad92642759e33dfa03",
		},
		ResourceIDs: []string{"dev-published"},
	}
	pair := SourcePairingCommit{
		OperationID: "82d62225-1eb6-47ad-9264-2759e33dfa03",
		InstanceID:  "instance-1", BindingID: "dev-published", TenantID: 7,
		KnowledgeBaseID: "kb-1", DataSourceID: "source-1",
	}
	require.NoError(t, AbortSourcePairing(context.Background(), config, pair))
	wrongAck = true
	require.ErrorContains(t, AbortSourcePairing(context.Background(), config, pair), "tuple mismatch")
}
