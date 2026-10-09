package nextcloud

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestSourceRotationSignsNewCommitAndOldAbortWithExactAck(t *testing.T) {
	wrongAck := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		action := strings.TrimPrefix(r.URL.Path,
			apiPath+"/bindings/dev-published/source-pairing/rotation/")
		require.Contains(t, []string{"commit", "finalize", "abort"}, action)
		require.Equal(t, http.MethodPost, r.Method)
		require.NotEmpty(t, r.Header.Get("X-WeKnora-Signature"))
		key := "rot_6e35904269aa489ebd9b8754bf5beee1"
		bearer := "Bearer new-secret"
		state := "committed"
		switch action {
		case "abort":
			key, bearer, state = "pair_old", "Bearer old-secret", "aborted"
		case "finalize":
			state = "finalized"
		}
		require.Equal(t, key, r.Header.Get("X-WeKnora-Key-Id"))
		require.Equal(t, bearer, r.Header.Get("Authorization"))
		var body map[string]string
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, "7", body["tenant_id"])
		require.Equal(t, "pair-op", body["pair_operation_id"])
		if wrongAck {
			key = "wrong-key"
		}
		writeJSON(w, map[string]any{"rotation": map[string]any{
			"operation_id": body["operation_id"], "pair_operation_id": body["pair_operation_id"],
			"binding_id": "dev-published", "instance_id": body["instance_id"],
			"tenant_id": body["tenant_id"], "knowledge_base_id": body["knowledge_base_id"],
			"data_source_id": body["data_source_id"], "old_key_id": "pair_old",
			"new_key_id": key, "state": state,
		}, "changed": true})
	}))
	defer server.Close()
	allowHTTPTestOrigin(t, server.URL)
	newCfg := &types.DataSourceConfig{
		Type:     types.ConnectorTypeNextcloud,
		Settings: map[string]interface{}{"base_url": server.URL},
		Credentials: map[string]interface{}{
			"token":  "new-secret",
			"key_id": "rot_6e35904269aa489ebd9b8754bf5beee1",
		},
		ResourceIDs: []string{"dev-published"},
	}
	oldCfg := &types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		Settings:    map[string]interface{}{"base_url": server.URL},
		Credentials: map[string]interface{}{"token": "old-secret", "key_id": "pair_old"},
		ResourceIDs: []string{"dev-published"},
	}
	rotation := SourceRotation{
		OperationID:     "6e359042-69aa-489e-bd9b-8754bf5beee1",
		PairOperationID: "pair-op", InstanceID: "instance-1", BindingID: "dev-published",
		TenantID: 7, KnowledgeBaseID: "kb-1", DataSourceID: "source-1",
	}
	require.NoError(t, CommitSourceRotation(context.Background(), newCfg, rotation))
	require.NoError(t, FinalizeSourceRotation(context.Background(), newCfg, rotation))
	require.NoError(t, AbortSourceRotation(context.Background(), oldCfg, rotation))
	wrongAck = true
	require.ErrorContains(t, CommitSourceRotation(context.Background(), newCfg, rotation), "tuple mismatch")
}
