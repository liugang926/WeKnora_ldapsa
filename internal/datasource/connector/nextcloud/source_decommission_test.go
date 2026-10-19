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

func TestEmptyDecommissionSignsAndRequiresExactAck(t *testing.T) {
	operationID := "be4c7f4d-5082-47fa-a54f-24ad09a05830"
	intent := SourceDecommission{
		OperationID:     operationID,
		PairOperationID: "1cf17b55-26a2-4c04-bc9a-9c47eaa561b7",
		InstanceID:      "instance-1", BindingID: "dev-published", TenantID: "7",
		KnowledgeBaseID: "kb-1", DataSourceID: "source-1",
		KeyID:            "pair_1cf17b5526a24c04bc9a9c47eaa561b7",
		PublicationEpoch: 1, State: "prepared",
	}
	wrongAck := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer pairing-token", r.Header.Get("Authorization"))
		require.Equal(t, intent.KeyID, r.Header.Get("X-WeKnora-Key-Id"))
		require.NotEmpty(t, r.Header.Get("X-WeKnora-Signature"))
		path := apiPath + "/bindings/dev-published/decommission/" + operationID
		switch r.Method {
		case http.MethodGet:
			require.Equal(t, path, r.URL.Path)
			writeJSON(w, map[string]any{"decommission": intent})
		case http.MethodPost:
			require.Equal(t, path+"/ack", r.URL.Path)
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, true, body["logical_withdrawn"])
			require.Equal(t, true, body["inventory_complete"])
			require.Equal(t, float64(0), body["inventory_count"])
			require.Equal(t, EmptyDecommissionInventorySHA256, body["inventory_sha256"])
			response := intent
			response.State = "acknowledged"
			response.InventorySHA256 = EmptyDecommissionInventorySHA256
			if wrongAck {
				response.DataSourceID = "other-source"
			}
			writeJSON(w, map[string]any{"decommission": response})
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()
	allowHTTPTestOrigin(t, server.URL)
	config := &types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		Settings:    map[string]interface{}{"base_url": server.URL},
		Credentials: map[string]interface{}{"token": "pairing-token", "key_id": intent.KeyID},
		ResourceIDs: []string{intent.BindingID},
	}
	read, err := ReadSourceDecommission(context.Background(), config, intent.BindingID, operationID)
	require.NoError(t, err)
	require.True(t, read.sameIdentity(intent))
	require.NoError(t, AcknowledgeEmptySourceDecommission(context.Background(), config, read))
	wrongAck = true
	require.ErrorContains(t, AcknowledgeEmptySourceDecommission(context.Background(), config, read), "tuple mismatch")
}
