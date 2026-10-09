package nextcloud

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/datasource"
	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
)

func authorizationConfig(t *testing.T, baseURL string) *types.DataSourceConfig {
	allowHTTPTestOrigin(t, baseURL)
	return &types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		Settings:    map[string]interface{}{"base_url": baseURL},
		Credentials: map[string]interface{}{"token": "paired-secret"},
	}
}

func TestAuthorizeCurrentFileUsesAuthenticatedSourceContract(t *testing.T) {
	const guid = "00112233-4455-6677-8899-aabbccddeeff"
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Header.Get("Authorization") != "Bearer paired-secret" {
			t.Errorf("missing source authorization header")
		}
		if r.Header.Get("X-WeKnora-Key-Id") != "default" ||
			r.Header.Get("X-WeKnora-Nonce") == "" || r.Header.Get("X-WeKnora-Signature") == "" {
			t.Errorf("missing machine request signature")
		}
		switch r.URL.Path {
		case apiPath + "/capabilities":
			if r.Method != http.MethodGet {
				t.Errorf("capabilities used %s", r.Method)
			}
			if _, err := fmt.Fprint(w, `{"protocol_version":"1","instance_id":"instance-1"}`); err != nil {
				t.Logf("write source authorization fixture response: %v", err)
			}
		case apiPath + "/bindings/binding-1/authorize":
			if r.Method != http.MethodPost {
				t.Errorf("authorize used %s", r.Method)
			}
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read request body: %v", err)
			}
			var payload map[string]any
			if err := json.Unmarshal(raw, &payload); err != nil {
				t.Errorf("decode request: %v", err)
			}
			canonical, err := machineCanonicalRequest(r.Method, r.URL.EscapedPath(), r.URL.RawQuery,
				raw, r.Header.Get("X-WeKnora-Timestamp"), r.Header.Get("X-WeKnora-Nonce"), "default")
			if err != nil {
				t.Errorf("canonicalize request: %v", err)
			}
			key := sha256.Sum256([]byte("paired-secret"))
			mac := hmac.New(sha256.New, key[:])
			_, _ = mac.Write([]byte(canonical))
			if !hmac.Equal(mac.Sum(nil), mustDecodeHex(t, r.Header.Get("X-WeKnora-Signature"))) {
				t.Error("authorization POST signature did not bind the raw body")
			}
			if payload["directory_id"] != "ad-1" || payload["object_guid"] != guid ||
				payload["file_id"] != float64(42) {
				t.Errorf("unexpected payload: %#v", payload)
			}
			if _, err := fmt.Fprint(w, `{"allow":true,"reason":"authorized","policy_revision":"rev-1","source_et`+
				`ag":"etag-42","checked_at":1780000000}`); err != nil {
				t.Logf("write source authorization fixture response: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	decision, err := AuthorizeCurrentFile(context.Background(), authorizationConfig(t, server.URL),
		"instance-1", "binding-1", "ad-1", guid, 42)
	if err != nil || !decision.Allow || decision.PolicyRevision != "rev-1" ||
		decision.SourceETag != "etag-42" || requests != 2 {
		t.Fatalf("decision=%+v err=%v requests=%d", decision, err, requests)
	}
}

func TestAuthorizeCurrentFileRejectsChangedInstanceAndBadResponses(t *testing.T) {
	const guid = "00112233-4455-6677-8899-aabbccddeeff"
	for _, tc := range []struct {
		name       string
		response   string
		statusCode int
		instanceID string
	}{
		{
			name: "changed instance",
			response: `{"allow":true,"reason":"authorized","policy_revision":"rev","checked_at"` +
				`:1780000000}`, instanceID: "another",
		},
		{name: "upstream forbidden", statusCode: http.StatusForbidden},
		{name: "missing allow", response: `{"reason":"authorized","policy_revision":"rev","checked_at":1780000000}`},
		{name: "malformed allow", response: `{"allow":"true","reason":"authorized","policy_revision":"rev","checked_a` +
			`t":1780000000}`},
		{name: "missing revision", response: `{"allow":true,"reason":"authorized","checked_at":1780000000}`},
		{
			name: "missing source etag",
			response: `{"allow":true,"reason":"authorized","policy_revision":"rev","checked_at"` +
				`:1780000000}`,
		},
		{
			name: "empty source etag",
			response: `{"allow":true,"reason":"authorized","policy_revision":"rev","source_etag` +
				`":"","checked_at":1780000000}`,
		},
		{name: "oversized response", response: string(make([]byte, maxAuthorizationResponseBytes+1))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == apiPath+"/capabilities" {
					instanceID := "instance-1"
					if tc.instanceID != "" {
						instanceID = tc.instanceID
					}
					if _, err := fmt.Fprintf(w, `{"protocol_version":"1","instance_id":%q}`, instanceID); err != nil {
						t.Logf("write source authorization fixture response: %v", err)
					}
					return
				}
				if tc.statusCode != 0 {
					w.WriteHeader(tc.statusCode)
				}
				if _, err := fmt.Fprint(w, tc.response); err != nil {
					t.Logf("write source authorization fixture response: %v", err)
				}
			}))
			defer server.Close()
			decision, err := AuthorizeCurrentFile(context.Background(), authorizationConfig(t, server.URL),
				"instance-1", "binding-1", "ad-1", guid, 42)
			if err == nil || decision.Allow {
				t.Fatalf("source error allowed access: decision=%+v err=%v", decision, err)
			}
		})
	}
}

func TestAuthorizeCurrentFilePreservesExplicitDeny(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == apiPath+"/capabilities" {
			if _, err := fmt.Fprint(w, `{"protocol_version":"1","instance_id":"instance-1"}`); err != nil {
				t.Logf("write source authorization fixture response: %v", err)
			}
			return
		}
		if _, err := fmt.Fprint(w, `{"allow":false,"reason":"publication_withdrawn","policy_revision":null,"`+
			`checked_at":1780000000}`); err != nil {
			t.Logf("write source authorization fixture response: %v", err)
		}
	}))
	defer server.Close()
	decision, err := AuthorizeCurrentFile(context.Background(), authorizationConfig(t, server.URL),
		"instance-1", "binding-1", "ad-1", "00112233-4455-6677-8899-aabbccddeeff", 42)
	if err != nil || decision.Allow || decision.Reason != "publication_withdrawn" {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
}

func TestAuthorizeCurrentFilePreservesWrappedPublicDiagnostic(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	decision, err := AuthorizeCurrentFile(context.Background(), authorizationConfig(t, server.URL),
		"instance-1", "binding-1", "ad-1", "00112233-4455-6677-8899-aabbccddeeff", 42)
	if err == nil || decision.Allow {
		t.Fatal("source failure must still deny authorization")
	}
	want := "verify Nextcloud instance: " + datasource.ErrRetryableSource.Error() +
		": Nextcloud /capabilities returned status 503"
	if apperrors.PublicMessage(err) != want {
		t.Fatalf("wrapped public diagnostic changed: got %q want %q", apperrors.PublicMessage(err), want)
	}
	if !stderrors.Is(err, datasource.ErrRetryableSource) {
		t.Fatal("wrapped retryable-source sentinel identity was lost")
	}
}
