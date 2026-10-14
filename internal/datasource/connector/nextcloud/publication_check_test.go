package nextcloud

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCheckCurrentPublicationSignsExactPairedVersion(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodPost || r.URL.Path != apiPath+
			"/bindings/binding-1/files/42/publication-check" || r.URL.RawQuery != "" {
			t.Errorf("unexpected publication route: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") != "Bearer paired-secret" ||
			r.Header.Get("X-WeKnora-Key-Id") != "default" {
			t.Error("publication request lacked the paired machine credential")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		canonical, err := machineCanonicalRequest(r.Method, r.URL.EscapedPath(), r.URL.RawQuery,
			body, r.Header.Get("X-WeKnora-Timestamp"), r.Header.Get("X-WeKnora-Nonce"), "default")
		if err != nil {
			t.Error(err)
		}
		key := sha256.Sum256([]byte("paired-secret"))
		mac := hmac.New(sha256.New, key[:])
		_, _ = mac.Write([]byte(canonical))
		signature, err := hex.DecodeString(r.Header.Get("X-WeKnora-Signature"))
		if err != nil || !hmac.Equal(signature, mac.Sum(nil)) {
			t.Error("publication body was not signed")
		}
		var payload struct {
			InstanceID string `json:"instance_id"`
			ETag       string `json:"etag"`
			Path       string `json:"path"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Error(err)
		}
		if payload.InstanceID != "instance-1" || payload.ETag != "etag-42" ||
			payload.Path != "folder/document.md" {
			t.Errorf("unexpected publication identity: %+v", payload)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	config := authorizationConfig(t, server.URL)
	config.ResourceIDs = []string{"binding-1"}
	if err := CheckCurrentPublication(context.Background(), config, "instance-1", "binding-1",
		42, "etag-42", "folder/document.md"); err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("expected one exact source request, got %d", requests)
	}
	if err := CheckCurrentPublication(context.Background(), config, "instance-1", "another",
		42, "etag-42", "folder/document.md"); err == nil || requests != 1 {
		t.Fatal("cross-binding publication check was allowed")
	}
}

func TestCheckCurrentPublicationFailsClosed(t *testing.T) {
	for _, status := range []int{
		http.StatusOK, http.StatusConflict, http.StatusNotFound,
		http.StatusLocked, http.StatusServiceUnavailable, http.StatusFound,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			}))
			defer server.Close()
			config := authorizationConfig(t, server.URL)
			config.ResourceIDs = []string{"binding-1"}
			if err := CheckCurrentPublication(context.Background(), config, "instance-1",
				"binding-1", 42, "etag-42", "file.md"); err == nil {
				t.Fatalf("source status %d permitted publication", status)
			}
		})
	}
}
