package nextcloud

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// Some Nextcloud proxies suffix the entity tag with "-gzip" when the Go
// transport offers compression. A manifest tag identifies the original file,
// so the content request must use that same representation and tag.
func TestContentRequestsIdentityRepresentationForManifestETag(t *testing.T) {
	const tag = "source-etag-42"
	var compressionOffered atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings/dev-published/manifest":
			writeJSON(w, testManifest(tag))
		case apiPath + "/bindings/dev-published/files/41/content":
			if strings.Contains(strings.ToLower(r.Header.Get("Accept-Encoding")), "gzip") {
				compressionOffered.Store(true)
				w.Header().Set("ETag", `"`+tag+`-gzip"`)
			} else {
				w.Header().Set("ETag", `"`+tag+`"`)
			}
			w.Header().Set("Content-Type", "text/markdown")
			_, _ = w.Write([]byte("hello"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	cfg, err := parseConfig(testConfig(t, server.URL))
	if err != nil {
		t.Fatal(err)
	}
	client := newClient(cfg)
	manifest, _, err := client.manifest(context.Background(), "dev-published")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest) != 1 {
		t.Fatalf("manifest count=%d", len(manifest))
	}
	if _, _, err := client.content(context.Background(), "dev-published", manifest[0]); err != nil {
		t.Fatal(err)
	}
	if compressionOffered.Load() {
		t.Fatal("content request offered gzip, changing the strong source ETag")
	}
}
