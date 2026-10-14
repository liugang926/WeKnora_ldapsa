package nextcloud

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAuthorizeCurrentManagerBindsPurposeAndFreshCurrentDecision(t *testing.T) {
	scope := ManagerAuthorizationScope{InstanceID: "instance-1", OperationID: "operation-1", TenantID: "7", KnowledgeBaseID: "kb-1", DataSourceID: "ds-1", ProcessingTemplateID: "approved-template", DirectoryID: "ad-1", ObjectGUID: "00112233-4455-6677-8899-aabbccddeeff", PurposeSHA256: strings.Repeat("1", 64)}
	for _, tc := range []struct {
		name   string
		status int
		mutate func(map[string]any)
		want   error
	}{
		{name: "current allow", status: http.StatusOK},
		{name: "new binding starts at epoch zero", status: http.StatusOK, mutate: func(v map[string]any) { v["publication_epoch"] = "0" }},
		{name: "negative epoch", status: http.StatusOK, mutate: func(v map[string]any) { v["publication_epoch"] = "-1" }, want: ErrManagerAuthorizationUnavailable},
		{name: "epoch canonical alias", status: http.StatusOK, mutate: func(v map[string]any) { v["publication_epoch"] = "00" }, want: ErrManagerAuthorizationUnavailable},
		{name: "epoch uint overflow", status: http.StatusOK, mutate: func(v map[string]any) { v["publication_epoch"] = "18446744073709551616" }, want: ErrManagerAuthorizationUnavailable},
		{name: "current delegate revoked", status: http.StatusForbidden, mutate: func(v map[string]any) { v["allow"] = false; v["reason"] = "manager_denied" }, want: ErrManagerAuthorizationDenied},
		{name: "unknown authority", status: http.StatusServiceUnavailable, want: ErrManagerAuthorizationUnavailable},
		{name: "unauthenticated is unavailable", status: http.StatusUnauthorized, want: ErrManagerAuthorizationUnavailable},
		{name: "changed template", status: http.StatusOK, mutate: func(v map[string]any) { v["processing_template_id"] = "another" }, want: ErrManagerAuthorizationUnavailable},
		{name: "changed immutable purpose", status: http.StatusOK, mutate: func(v map[string]any) { v["purpose_sha256"] = strings.Repeat("2", 64) }, want: ErrManagerAuthorizationUnavailable},
		{name: "stale cached allow", status: http.StatusOK, mutate: func(v map[string]any) { v["checked_at"] = time.Now().Add(-30 * time.Second).Unix() }, want: ErrManagerAuthorizationUnavailable},
		{name: "unbound forbidden", status: http.StatusForbidden, mutate: func(v map[string]any) {
			delete(v, "data_source_id")
			v["allow"] = false
			v["reason"] = "manager_denied"
		}, want: ErrManagerAuthorizationUnavailable},
		{name: "wrong numeric wire type", status: http.StatusOK, mutate: func(v map[string]any) { v["publication_epoch"] = 1 }, want: ErrManagerAuthorizationUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nonces := map[string]bool{}
			managerRequests := 0
			var mu sync.Mutex
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				raw, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					w.WriteHeader(500)
					return
				}
				if r.Header.Get("Authorization") != "Bearer paired-secret" {
					t.Error("missing paired credential")
				}
				nonce := r.Header.Get("X-WeKnora-Nonce")
				if nonce == "" || nonces[nonce] {
					t.Error("machine request lacks fresh nonce")
				}
				nonces[nonce] = true
				canonical, err := machineCanonicalRequest(r.Method, r.URL.EscapedPath(), r.URL.RawQuery, raw, r.Header.Get("X-WeKnora-Timestamp"), nonce, r.Header.Get("X-WeKnora-Key-Id"))
				if err != nil {
					t.Error(err)
				}
				key := sha256.Sum256([]byte("paired-secret"))
				mac := hmac.New(sha256.New, key[:])
				_, _ = mac.Write([]byte(canonical))
				if !hmac.Equal(mac.Sum(nil), mustDecodeHex(t, r.Header.Get("X-WeKnora-Signature"))) {
					t.Error("signature did not bind actual request")
				}
				if r.URL.Path == apiPath+"/capabilities" {
					_, _ = w.Write([]byte(`{"protocol_version":"1","instance_id":"instance-1"}`))
					return
				}
				if r.URL.Path != apiPath+"/bindings/binding-1/authorize-manager" || r.Method != http.MethodPost {
					t.Error("unexpected manager path/method")
				}
				managerRequests++
				var actual ManagerAuthorizationScope
				if json.Unmarshal(raw, &actual) != nil || actual != scope {
					t.Error("signed scope changed")
				}
				response := map[string]any{}
				if err := json.Unmarshal(raw, &response); err != nil {
					t.Error(err)
				}
				response["binding_id"] = "binding-1"
				response["allow"] = true
				response["reason"] = "authorized"
				response["root_file_id"] = "42"
				response["rule_id"] = "actual-rule"
				response["rule_revision"] = "1"
				response["publication_epoch"] = "2"
				response["policy_revision"] = strings.Repeat("3", 64)
				response["checked_at"] = time.Now().Unix()
				if tc.mutate != nil {
					tc.mutate(response)
				}
				w.WriteHeader(tc.status)
				_ = json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			config := authorizationConfig(t, server.URL)
			config.ResourceIDs = []string{"binding-1"}
			// Two calls prove ALLOW is never reused without another source decision.
			for i := 0; i < 2; i++ {
				decision, err := AuthorizeCurrentManager(context.Background(), config, "binding-1", scope)
				if tc.want == nil {
					if err != nil || decision.RootFileID != "42" {
						t.Fatalf("expected fresh allow: %v", err)
					}
					if tc.name == "new binding starts at epoch zero" && decision.PublicationEpoch != "0" {
						t.Fatal("initial publication epoch was changed")
					}
				} else if !errors.Is(err, tc.want) {
					t.Fatalf("expected %v, got %v", tc.want, err)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if managerRequests != 2 || len(nonces) != 4 {
				t.Fatalf("requests=%d signed nonces=%d", managerRequests, len(nonces))
			}
		})
	}
}
