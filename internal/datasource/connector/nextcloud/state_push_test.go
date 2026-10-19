package nextcloud

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestNextcloudStatePushActualHTTPPersistenceACKAndFixedOrigin(t *testing.T) {
	for _, result := range []string{"accepted", "wrong_event", "unpersisted", "redirect", "unavailable"} {
		t.Run(result, func(t *testing.T) {
			key := []byte("purpose-derived-test-key-32bytes!")
			event := "durable-event"
			body := []byte(`{"event_id":"durable-event","schema_version":2}`)
			var calls atomic.Int32
			destination := httptest.NewServer(
				http.HandlerFunc(
					func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(http.StatusOK) },
				),
			)
			defer destination.Close()
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, http.MethodPost, r.Method)
				require.Equal(t, "/index.php/apps/integration_weknora/api/v1/status", r.URL.EscapedPath())
				require.Empty(t, r.Header.Get("Authorization"))
				raw, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Equal(t, body, raw)
				digest := sha256.Sum256(raw)
				mac := hmac.New(sha256.New, key)
				mac.Write(
					StatePushCanonical(
						r.URL.EscapedPath(),
						hex.EncodeToString(digest[:]),
						r.Header.Get("X-WeKnora-Timestamp"),
						r.Header.Get("X-WeKnora-Nonce"),
						r.Header.Get("X-WeKnora-Connection-Id"),
						r.Header.Get("X-WeKnora-Key-Id"),
					),
				)
				require.Equal(t, hex.EncodeToString(mac.Sum(nil)), r.Header.Get("X-WeKnora-Signature"))
				switch result {
				case "accepted":
					writeNextcloudTestResponse(
						t,
						w,
						`{"status":"accepted","persisted":true,"event_id":"durable-event"}`,
					)
				case "wrong_event":
					writeNextcloudTestResponse(t, w, `{"status":"accepted","persisted":true,"event_id":"other"}`)
				case "unpersisted":
					writeNextcloudTestResponse(
						t,
						w,
						`{"status":"accepted","persisted":false,"event_id":"durable-event"}`,
					)
				case "redirect":
					http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
				default:
					w.WriteHeader(http.StatusServiceUnavailable)
				}
			}))
			defer source.Close()
			t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
			t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", source.URL)
			cfg := &types.DataSourceConfig{
				Type:        types.ConnectorTypeNextcloud,
				Settings:    map[string]any{"base_url": source.URL},
				Credentials: map[string]any{"token": "content-key-stays-private", "key_id": "machine-test"},
				ResourceIDs: []string{"binding"},
			}
			err := PushProcessingState(context.Background(), cfg, "connection-000001", "status-key", event, key, body)
			if result == "accepted" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Zero(t, calls.Load(), "status credentials must never follow redirect")
		})
	}
}
