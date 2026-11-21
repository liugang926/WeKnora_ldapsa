package nextcloudwebdav

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

func davEntry(href string, folder bool, id, etag string, size int) string {
	typ := ""
	if folder {
		typ = "<d:collection/>"
	}
	return fmt.Sprintf(`<d:response>
  <d:href>%s</d:href>
  <d:propstat>
    <d:prop>
      <d:resourcetype>%s</d:resourcetype>
      <oc:fileid>%s</oc:fileid><d:getetag>%s</d:getetag><d:getcontentlength>%d</d:getcontentlength>
    </d:prop>
    <d:status>HTTP/1.1 200 OK</d:status>
  </d:propstat>
</d:response>`, href, typ, id, etag, size)
}

func TestDirectorySyncAndReadRevocation(t *testing.T) {
	const root = "/nc/remote.php/dav/files/tester"
	const folder = "/选定"
	const sub = folder + "/sub"
	const file = sub + "/测试.txt"
	data := "hello"
	etag := `"v1"`
	requests := 0
	revoked := false
	hidden := false
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if u, p, ok := r.BasicAuth(); !ok || u != "tester" || p != "test-pass" {
			t.Error("invalid authorization")
			w.WriteHeader(401)
			return
		}
		p, ok := strings.CutPrefix(r.URL.Path, root)
		if !ok {
			t.Error("outside DAV root")
			w.WriteHeader(404)
			return
		}
		entry := func(p string, dir bool, id, etag string, size int) string {
			return davEntry((&url.URL{Path: root + p}).EscapedPath(), dir, id, etag, size)
		}
		switch r.Method {
		case "PROPFIND":
			p = strings.TrimSuffix(p, "/")
			if p != folder && p != sub {
				t.Errorf("out of selection %s", p)
				w.WriteHeader(404)
				return
			}
			if revoked {
				w.WriteHeader(403)
				return
			}
			body := entry(p+"/", true, "1", "", 0)
			if p == folder {
				body += entry(sub+"/", true, "2", "", 0)
			}
			if p == sub && !hidden {
				body += entry(file, false, "42", etag, len(data))
				body += entry(sub+"/not-imported.rar", false, "43", etag, 100)
			}
			w.WriteHeader(http.StatusMultiStatus)
			const prefix = `<d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">`
			if _, err := fmt.Fprint(w, prefix+body+`</d:multistatus>`); err != nil {
				t.Error(err)
			}
		case "GET":
			if p != file || r.Header.Get("If-Match") != etag {
				t.Error("unexpected or unpinned file read")
				w.WriteHeader(412)
				return
			}
			requests++
			w.Header().Set("ETag", etag)
			if _, err := fmt.Fprint(w, data); err != nil {
				t.Error(err)
			}
		default:
			t.Error("unexpected method", r.Method)
			w.WriteHeader(405)
		}
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL + "/nc")
	c := client{
		http: srv.Client(), base: *u, davRoot: root,
		username: "tester", password: "test-pass", namespace: "instance",
	}
	ctx := context.Background()
	var cursor *types.SyncCursor
	sync := func(want int) *types.FetchedItem {
		t.Helper()
		h := new(collector)
		next, err := c.sync(ctx, []string{folder}, cursor, h, 100)
		if err != nil || len(h.items) != want {
			t.Fatalf("sync count=%d err=%v", len(h.items), err)
		}
		raw, _ := json.Marshal(next)
		cursor = nil
		if err := json.Unmarshal(raw, &cursor); err != nil {
			t.Fatal(err)
		}
		if want == 0 {
			return nil
		}
		return &h.items[0]
	}
	first := sync(1)
	if string(first.Content) != data || first.SourceResourceID != folder ||
		first.URL != srv.URL+"/nc/f/42" || first.Metadata["webdav_path"] != file {
		t.Fatal(first)
	}
	sync(0)
	etag = `"v2"`
	data = "edited"
	updated := sync(1)
	if updated.ExternalID != first.ExternalID || string(updated.Content) != data || requests != 2 {
		t.Fatalf("inconsistent update %+v GETs=%d", updated, requests)
	}
	revoked = true
	h := new(collector)
	if _, err := c.sync(ctx, []string{folder}, cursor, h, 100); err == nil || len(h.items) != 0 {
		t.Fatal("revoked listing must fail without changing the KB")
	}
	revoked = false
	hidden = true
	sync(0)
}

func TestScopeAndHref(t *testing.T) {
	roots, err := selectedRoots([]string{"/a/u", "/aa", "/a", "/a"})
	if err != nil || strings.Join(roots, ",") != "/a,/aa" {
		t.Fatal(roots, err)
	}
	if _, err := selectedRoots(nil); err == nil {
		t.Fatal("unexpected whole-account fallback")
	}
	ids, err := NewConnector().ResolveResourceAncestors(context.Background(), nil, []string{"/a/b/c", "/a/z"})
	if err != nil || strings.Join(ids, ",") != "/,/a,/a/b" {
		t.Fatal(ids, err)
	}
	u, _ := url.Parse("https://cloud.example/nc")
	c := client{base: *u, davRoot: "/nc/remote.php/dav/files/alice"}
	for _, href := range []string{
		"https://foreign.example" + c.davRoot + "/x",
		"//cloud.example" + c.davRoot + "/x",
		c.davRoot + "2/x",
		c.davRoot + "/%2e%2e/foreign",
		c.davRoot + "/child%2fgrandchild",
		c.davRoot + "/x?token=never",
	} {
		if _, err := c.relative(href); err == nil {
			t.Errorf("unexpected href %s", href)
		}
	}
}

func TestCredentialConfiguration(t *testing.T) {
	for _, endpoint := range []string{
		"http://example.test",
		"https://user@example.test",
		"https://example.test?token=bad",
		"https://example.test/base/../x",
	} {
		ds := &types.DataSourceConfig{Credentials: map[string]interface{}{
			"base_url": endpoint, "username": "user", "app_password": "not-for-logs",
		}}
		if _, err := newClient(ds); err == nil || strings.Contains(err.Error(), "not-for-logs") {
			t.Fatal("unsafe configuration")
		}
	}
}
