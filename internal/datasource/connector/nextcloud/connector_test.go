package nextcloud

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

func allowHTTPTestOrigin(t *testing.T, base string) {
	t.Helper()
	if strings.HasPrefix(base, "http://") {
		t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
		if strings.HasPrefix(base, "http://127.0.0.1:") {
			t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", base)
		}
	}
}

func testConfig(t *testing.T, base string) *types.DataSourceConfig {
	allowHTTPTestOrigin(t, base)
	return &types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		Settings:    map[string]interface{}{"base_url": base},
		Credentials: map[string]interface{}{"token": "test-token"},
		ResourceIDs: []string{"dev-published"},
	}
}

func testManifest(tag string) map[string]interface{} {
	return map[string]interface{}{
		"generation": "generation-1", "complete": true, "next_cursor": nil,
		"items": []map[string]interface{}{{
			"file_id": 41, "etag": tag, "name": "Notes.md", "path": "/Published/Notes.md",
			"mime_type": "text/markdown", "size": 5, "mtime": 1700000000,
			"url": "https://files.example/index.php/f/41",
		}},
	}
}

func writeJSON(w http.ResponseWriter, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func TestConnectorContractAndIncrementalETag(t *testing.T) {
	tag := "etag-1"
	body := "hello"
	var contentRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q", got)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Header.Get("X-WeKnora-Key-Id") != "default" ||
			r.Header.Get("X-WeKnora-Nonce") == "" || r.Header.Get("X-WeKnora-Signature") == "" {
			t.Errorf("missing signed machine headers on %s", r.URL.Path)
		}
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings":
			writeJSON(w, map[string]interface{}{"bindings": []map[string]interface{}{{
				"id": "dev-published", "name": "Published", "root_file_id": 7,
			}}})
		case apiPath + "/bindings/dev-published/manifest":
			writeJSON(w, testManifest(tag))
		case apiPath + "/bindings/dev-published/changes":
			// The connector falls back to complete scans against older app versions.
			http.NotFound(w, r)
		case apiPath + "/bindings/dev-published/files/41/content":
			contentRequests++
			if got := r.Header.Get("If-Match"); got != `"`+tag+`"` {
				t.Errorf("If-Match = %q, want %q", got, `"`+tag+`"`)
			}
			w.Header().Set("Content-Type", "text/markdown")
			w.Header().Set("ETag", `"`+tag+`"`)
			_, _ = w.Write([]byte(body))
		default:
			t.Errorf("unexpected request path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	ctx := context.Background()
	ds := testConfig(t, server.URL)
	connector := NewConnector()
	if got := connector.Type(); got != types.ConnectorTypeNextcloud {
		t.Fatalf("Type() = %q", got)
	}
	if err := connector.Validate(ctx, ds); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	resources, err := connector.ListResources(ctx, ds, "")
	if err != nil || len(resources) != 1 || resources[0].ExternalID != "dev-published" ||
		resources[0].Name != "Published" {
		t.Fatalf("ListResources = %+v, %v", resources, err)
	}
	children, err := connector.ListResources(ctx, ds, "dev-published")
	if err != nil || len(children) != 0 {
		t.Fatalf("ListResources(children) = %+v, %v", children, err)
	}

	all, err := connector.FetchAll(ctx, ds, ds.ResourceIDs)
	if err != nil || len(all) != 1 || string(all[0].Content) != "hello" {
		t.Fatalf("FetchAll = %+v, %v", all, err)
	}
	if all[0].ExternalID != "nextcloud:instance-1:41" || all[0].SourceResourceID != "dev-published" ||
		all[0].ContentType != "text/markdown" || all[0].Metadata["nextcloud_etag"] != tag {
		t.Fatalf("unexpected FetchedItem: %+v", all[0])
	}

	first, cursor, err := connector.FetchIncremental(ctx, ds, nil)
	if err != nil || cursor == nil || len(first) != 1 {
		t.Fatalf("first incremental = %+v, %+v, %v", first, cursor, err)
	}
	second, next, err := connector.FetchIncremental(ctx, ds, cursor)
	// An older app without the changes endpoint cannot prove a scoped hint
	// history. Reconcile its complete content even when metadata is unchanged.
	if err != nil || next == nil || len(second) != 1 || contentRequests != 3 {
		t.Fatalf("unsupported changes fallback = %+v, %+v, %v; downloads=%d", second, next, err, contentRequests)
	}
	tag, body = "etag-2", "world"
	changed, _, err := connector.FetchIncremental(ctx, ds, next)
	if err != nil || len(changed) != 1 || string(changed[0].Content) != "world" || contentRequests != 4 {
		t.Fatalf("changed incremental = %+v, %v; downloads=%d", changed, err, contentRequests)
	}
}

func TestValidateRejectsUnpairedOrMultipleBindings(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings":
			writeJSON(w, map[string]interface{}{"bindings": []map[string]interface{}{{
				"id": "dev-published", "name": "Published", "root_file_id": 7,
			}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	connector := NewConnector()
	for _, selection := range [][]string{nil, {"dev-published", "another"}, {"unsafe/id"}} {
		cfg := testConfig(t, server.URL)
		cfg.ResourceIDs = selection
		if err := connector.Validate(context.Background(), cfg); err == nil {
			t.Fatalf("Validate accepted selection %q", selection)
		}
	}
	if requests != 0 {
		t.Fatalf("invalid selections triggered %d source requests", requests)
	}
	cfg := testConfig(t, server.URL)
	cfg.ResourceIDs = []string{"other"}
	if err := connector.Validate(context.Background(), cfg); err == nil {
		t.Fatal("Validate accepted a binding absent from the source registry")
	}
}

func TestInspectPairingReturnsLiveIdentity(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Header.Get("Authorization") != "Bearer test-token" ||
			r.Header.Get("X-WeKnora-Key-Id") != "default" ||
			r.Header.Get("X-WeKnora-Nonce") == "" ||
			r.Header.Get("X-WeKnora-Signature") == "" {
			t.Error("pairing inspection did not sign the machine request")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-live"})
		case apiPath + "/bindings":
			writeJSON(w, map[string]interface{}{"bindings": []map[string]interface{}{{
				"id": "dev-published", "name": "Published", "root_file_id": 7,
			}}})
		default:
			t.Errorf("unexpected pairing inspection endpoint %q", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cfg := testConfig(t, server.URL)
	cfg.Settings["base_url"] = server.URL + "/"
	identity, err := InspectPairing(context.Background(), cfg)
	if err != nil {
		t.Fatalf("InspectPairing: %v", err)
	}
	if identity != (PairingIdentity{InstanceID: "instance-live", BindingID: "dev-published", BaseURL: server.URL}) {
		t.Fatalf("InspectPairing identity = %+v", identity)
	}
	if requests != 2 {
		t.Fatalf("pairing inspection sent %d requests, want 2", requests)
	}

	cfg.ResourceIDs = []string{"unpublished"}
	if _, err := InspectPairing(context.Background(), cfg); err == nil {
		t.Fatal("InspectPairing accepted a binding absent from the live registry")
	}
	requests = 0
	cfg.Type = types.ConnectorTypeRSS
	if _, err := InspectPairing(context.Background(), cfg); err == nil || requests != 0 {
		t.Fatal("InspectPairing accepted a non-Nextcloud source")
	}
}

func TestIncompleteManifestNeverDownloadsOrDeletes(t *testing.T) {
	var contentRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings/dev-published/manifest":
			writeJSON(w, map[string]interface{}{
				"generation": "g1", "complete": false, "next_cursor": nil,
				"items": []interface{}{testManifest("etag-1")["items"].([]map[string]interface{})[0]},
			})
		case apiPath + "/bindings/dev-published/files/41/content":
			contentRequests++
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	old := &types.SyncCursor{ConnectorCursor: map[string]interface{}{
		"instance_id": "instance-1",
		"files": map[string]interface{}{"dev-published": map[string]interface{}{
			"99": map[string]interface{}{"etag": "old", "name": "Old.md", "path": "/Old.md"},
		}},
	}}
	items, next, err := NewConnector().FetchIncremental(context.Background(), testConfig(t, server.URL), old)
	if err == nil || next != nil || len(items) != 0 || contentRequests != 0 {
		t.Fatalf("incomplete manifest = %+v, %+v, %v; downloads=%d", items, next, err, contentRequests)
	}
}

func TestDeletionNeedsTwoCompleteScansAndTombstoneRetries(t *testing.T) {
	present := true
	manifestStatus := http.StatusOK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings/dev-published/manifest":
			if manifestStatus != http.StatusOK {
				http.Error(w, "temporary outage", manifestStatus)
				return
			}
			manifest := testManifest("etag-1")
			if !present {
				manifest["items"] = []map[string]interface{}{}
			}
			writeJSON(w, manifest)
		case apiPath + "/bindings/dev-published/files/41/content":
			w.Header().Set("ETag", "etag-1")
			_, _ = w.Write([]byte("hello"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	connector := NewConnector()
	config := testConfig(t, server.URL)
	ctx := context.Background()
	items, cursor, err := connector.FetchIncremental(ctx, config, nil)
	if err != nil || len(items) != 1 || items[0].IsDeleted || cursor == nil {
		t.Fatalf("initial import = %+v, %+v, %v", items, cursor, err)
	}

	present = false
	items, firstMissing, err := connector.FetchIncremental(ctx, config, cursor)
	if err != nil || len(items) != 0 || firstMissing == nil {
		t.Fatalf("first missing scan = %+v, %+v, %v", items, firstMissing, err)
	}
	state, err := decodeCursor(firstMissing)
	if err != nil || !state.Missing["dev-published"]["41"] || state.Files["dev-published"]["41"].ETag != "etag-1" {
		t.Fatalf("first missing cursor = %+v, %v", state, err)
	}

	// An incomplete or failed scan is never evidence of disappearance and
	// must not advance the pending-deletion cursor.
	manifestStatus = http.StatusServiceUnavailable
	items, failedCursor, err := connector.FetchIncremental(ctx, config, firstMissing)
	if err == nil || len(items) != 0 || failedCursor != nil {
		t.Fatalf("failed scan = %+v, %+v, %v", items, failedCursor, err)
	}
	manifestStatus = http.StatusOK
	items, confirmed, err := connector.FetchIncremental(ctx, config, firstMissing)
	if err != nil || confirmed == nil || len(items) != 1 || !items[0].IsDeleted ||
		items[0].ExternalID != "nextcloud:instance-1:41" || items[0].SourceResourceID != "dev-published" {
		t.Fatalf("confirmed deletion = %+v, %+v, %v", items, confirmed, err)
	}
	state, err = decodeCursor(confirmed)
	if err != nil || state.Tombstones["dev-published"]["41"].Name != "Notes.md" ||
		len(state.Files["dev-published"]) != 0 {
		t.Fatalf("confirmed cursor = %+v, %v", state, err)
	}

	// The downstream importer does not acknowledge individual deletes.
	// Re-emit confirmed tombstones until the file returns.
	items, retryCursor, err := connector.FetchIncremental(ctx, config, confirmed)
	if err != nil || retryCursor == nil || len(items) != 1 || !items[0].IsDeleted {
		t.Fatalf("retry tombstone = %+v, %+v, %v", items, retryCursor, err)
	}
	items, _, err = connector.FetchIncremental(ctx, config, firstMissing)
	if err != nil || len(items) != 1 || !items[0].IsDeleted {
		t.Fatalf("uncommitted tombstone replay = %+v, %v", items, err)
	}

	present = true
	items, restored, err := connector.FetchIncremental(ctx, config, retryCursor)
	if err != nil || restored == nil || len(items) != 1 || items[0].IsDeleted || string(items[0].Content) != "hello" {
		t.Fatalf("restored file = %+v, %+v, %v", items, restored, err)
	}
	state, err = decodeCursor(restored)
	if err != nil || len(state.Tombstones["dev-published"]) != 0 {
		t.Fatalf("restored cursor retains tombstone: %+v, %v", state, err)
	}
}

func TestFullSyncWithCursorRefetchesAndReconcilesDeletion(t *testing.T) {
	present := true
	downloads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings/dev-published/manifest":
			manifest := testManifest("etag-1")
			if !present {
				manifest["items"] = []map[string]interface{}{}
			}
			writeJSON(w, manifest)
		case apiPath + "/bindings/dev-published/files/41/content":
			downloads++
			w.Header().Set("ETag", "etag-1")
			_, _ = w.Write([]byte("hello"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	connector := NewConnector()
	config := testConfig(t, server.URL)
	ctx := context.Background()
	_, cursor, err := connector.FetchIncremental(ctx, config, nil)
	if err != nil || cursor == nil {
		t.Fatalf("initial sync: cursor=%+v err=%v", cursor, err)
	}
	items, cursor, err := connector.FetchAllFromCursor(ctx, config, config.ResourceIDs, cursor)
	if err != nil || cursor == nil || len(items) != 1 || items[0].IsDeleted || downloads != 2 {
		t.Fatalf("full re-fetch = %+v, %+v, %v; downloads=%d", items, cursor, err, downloads)
	}
	present = false
	items, cursor, err = connector.FetchAllFromCursor(ctx, config, config.ResourceIDs, cursor)
	if err != nil || cursor == nil || len(items) != 0 {
		t.Fatalf("first full absence = %+v, %+v, %v", items, cursor, err)
	}
	items, cursor, err = connector.FetchAllFromCursor(ctx, config, config.ResourceIDs, cursor)
	if err != nil || cursor == nil || len(items) != 1 || !items[0].IsDeleted {
		t.Fatalf("confirmed full deletion = %+v, %+v, %v", items, cursor, err)
	}
}

func TestNextcloudConnectorRequiresExactlyOneBinding(t *testing.T) {
	for _, ids := range [][]string{nil, {}, {"a", "b"}, {"a", "a"}} {
		if _, err := selectedBindings(ids); err == nil {
			t.Fatalf("selectedBindings(%q) accepted a non-single selection", ids)
		}
	}
	selected, err := selectedBindings([]string{"a"})
	if err != nil || len(selected) != 1 || selected[0] != "a" {
		t.Fatalf("selectedBindings(a) = %q, %v", selected, err)
	}
}

func TestReappearanceResetsMissingObservation(t *testing.T) {
	present := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings/dev-published/manifest":
			manifest := testManifest("etag-1")
			if !present {
				manifest["items"] = []map[string]interface{}{}
			}
			writeJSON(w, manifest)
		case apiPath + "/bindings/dev-published/files/41/content":
			w.Header().Set("ETag", "etag-1")
			_, _ = w.Write([]byte("hello"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	connector := NewConnector()
	config := testConfig(t, server.URL)
	ctx := context.Background()
	_, cursor, err := connector.FetchIncremental(ctx, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	present = false
	items, cursor, err := connector.FetchIncremental(ctx, config, cursor)
	if err != nil || len(items) != 0 {
		t.Fatalf("first absence = %+v, %v", items, err)
	}
	present = true
	items, cursor, err = connector.FetchIncremental(ctx, config, cursor)
	if err != nil || len(items) != 1 || items[0].IsDeleted || string(items[0].Content) != "hello" {
		t.Fatalf("reappearance = %+v, %v", items, err)
	}
	state, err := decodeCursor(cursor)
	if err != nil || len(state.Missing["dev-published"]) != 0 {
		t.Fatalf("missing observation not reset: %+v, %v", state, err)
	}
	present = false
	items, cursor, err = connector.FetchIncremental(ctx, config, cursor)
	if err != nil || len(items) != 0 {
		t.Fatalf("new first absence = %+v, %v", items, err)
	}
	items, _, err = connector.FetchIncremental(ctx, config, cursor)
	if err != nil || len(items) != 1 || !items[0].IsDeleted {
		t.Fatalf("new second absence = %+v, %v", items, err)
	}
}

func TestUnsupportedRenameWithdrawsPreviouslyImportedFile(t *testing.T) {
	name := "Notes.md"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings/dev-published/manifest":
			manifest := testManifest("etag-1")
			manifest["items"].([]map[string]interface{})[0]["name"] = name
			writeJSON(w, manifest)
		case apiPath + "/bindings/dev-published/files/41/content":
			w.Header().Set("ETag", "etag-1")
			_, _ = w.Write([]byte("hello"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	connector := NewConnector()
	config := testConfig(t, server.URL)
	ctx := context.Background()
	_, cursor, err := connector.FetchIncremental(ctx, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	name = "Notes.bin"
	items, cursor, err := connector.FetchIncremental(ctx, config, cursor)
	if err != nil || len(items) != 0 {
		t.Fatalf("first unsupported scan = %+v, %v", items, err)
	}
	items, _, err = connector.FetchIncremental(ctx, config, cursor)
	if err != nil || len(items) != 1 || !items[0].IsDeleted || items[0].ExternalID != "nextcloud:instance-1:41" {
		t.Fatalf("confirmed unsupported rename = %+v, %v", items, err)
	}
}

func TestInstanceChangeDoesNotApplyOldDeletionEvidence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "new-instance"})
		case apiPath + "/bindings/dev-published/manifest":
			writeJSON(w, map[string]interface{}{
				"generation": "empty", "complete": true, "next_cursor": nil,
				"items": []interface{}{},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	old := &types.SyncCursor{ConnectorCursor: map[string]interface{}{
		"instance_id": "old-instance",
		"files": map[string]interface{}{"dev-published": map[string]interface{}{
			"41": map[string]interface{}{"etag": "etag-1", "name": "Notes.md"},
		}},
		"missing": map[string]interface{}{"dev-published": map[string]interface{}{"41": true}},
		"tombstones": map[string]interface{}{"dev-published": map[string]interface{}{
			"42": map[string]interface{}{"etag": "etag-2", "name": "Old.md"},
		}},
	}}
	items, cursor, err := NewConnector().FetchIncremental(context.Background(), testConfig(t, server.URL), old)
	if err == nil || cursor != nil || len(items) != 0 {
		t.Fatalf("changed instance must stop before import: %+v, %+v, %v", items, cursor, err)
	}
}

func TestIncompleteCursorCannotDiscardDeletionEvidence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == apiPath+"/capabilities" {
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	for _, old := range []*types.SyncCursor{
		{
			ConnectorCursor: map[string]interface{}{
				"files": map[string]interface{}{"dev-published": map[string]interface{}{}},
			},
		},
		{ConnectorCursor: map[string]interface{}{"instance_id": "instance-1", "files": map[string]interface{}{}}},
	} {
		items, cursor, err := NewConnector().FetchIncremental(context.Background(), testConfig(t, server.URL), old)
		if err == nil || cursor != nil || len(items) != 0 {
			t.Fatalf("incomplete cursor must stop before manifest: %+v, %+v, %v", items, cursor, err)
		}
	}
}

func TestPaginatedManifestCompletesBeforeDownload(t *testing.T) {
	var pages, downloads int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings/dev-published/manifest":
			pages++
			if r.URL.Query().Get("cursor") == "" {
				writeJSON(w, map[string]interface{}{
					"generation": "g1", "complete": false,
					"next_cursor": "page-2", "items": testManifest("etag-1")["items"],
				})
			} else if r.URL.Query().Get("cursor") == "page-2" {
				writeJSON(w, map[string]interface{}{
					"generation": "g1", "complete": true,
					"next_cursor": nil, "items": []interface{}{},
				})
			} else {
				t.Errorf("unexpected cursor %q", r.URL.Query().Get("cursor"))
			}
		case apiPath + "/bindings/dev-published/files/41/content":
			if pages != 2 {
				t.Errorf("download before complete manifest: pages=%d", pages)
			}
			downloads++
			w.Header().Set("ETag", "etag-1")
			_, _ = w.Write([]byte("hello"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	items, cursor, err := NewConnector().FetchIncremental(context.Background(), testConfig(t, server.URL), nil)
	if err != nil || len(items) != 1 || cursor == nil || pages != 2 || downloads != 1 {
		t.Fatalf("paged sync = %+v, %+v, %v; pages=%d downloads=%d", items, cursor, err, pages, downloads)
	}
}

func TestChangedContentETagRejectsCursor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings/dev-published/manifest":
			writeJSON(w, testManifest("etag-1"))
		case apiPath + "/bindings/dev-published/files/41/content":
			w.Header().Set("ETag", `"etag-2"`)
			_, _ = w.Write([]byte("newer"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	items, cursor, err := NewConnector().FetchIncremental(context.Background(), testConfig(t, server.URL), nil)
	if err == nil || !strings.Contains(err.Error(), "ETag changed") || len(items) != 0 || cursor != nil {
		t.Fatalf("ETag race = %+v, %+v, %v", items, cursor, err)
	}
}

func TestRedirectAndOversizeAreRejected(t *testing.T) {
	var redirected int
	target := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		redirected++
	}))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings/dev-published/manifest":
			writeJSON(w, testManifest("etag-1"))
		case apiPath + "/bindings/dev-published/files/41/content":
			if r.URL.Query().Get("oversize") == "1" {
				w.Header().Set("Content-Length", fmt.Sprint(maxContentBytes+1))
				w.Header().Set("ETag", "etag-1")
				_, _ = w.Write([]byte("hello"))
			} else {
				http.Redirect(w, r, target.URL+"/stolen", http.StatusFound)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	_, _, err := NewConnector().FetchIncremental(context.Background(), testConfig(t, server.URL), nil)
	if err == nil || redirected != 0 {
		t.Fatalf("redirect followed or accepted: err=%v target calls=%d", err, redirected)
	}

	// Exercise the Content-Length guard directly; the endpoint is fixed and
	// cannot take caller query parameters, so this second server sends oversize.
	large := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings/dev-published/manifest":
			writeJSON(w, testManifest("etag-1"))
		case apiPath + "/bindings/dev-published/files/41/content":
			w.Header().Set("Content-Length", fmt.Sprint(maxContentBytes+1))
			w.Header().Set("ETag", "etag-1")
			_, _ = w.Write([]byte("hello"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer large.Close()
	items, cursor, err := NewConnector().FetchIncremental(context.Background(), testConfig(t, large.URL), nil)
	if err == nil || !strings.Contains(err.Error(), "download limit") || len(items) != 0 || cursor != nil {
		t.Fatalf("oversize = %+v, %+v, %v", items, cursor, err)
	}
}

func TestBaseURLValidationAllowsExplicitPrivateHost(t *testing.T) {
	t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", "")
	for _, raw := range []string{
		"http://user:secret@nextcloud:80", "http://nextcloud/#fragment",
		"http://nextcloud/?q=x", "file:///tmp/nextcloud", "http://nextcloud/%2e%2e",
		"http://nextcloud/a%2fb", "http://nextcloud//other",
	} {
		if _, err := parseConfig(testConfig(t, raw)); err == nil {
			t.Errorf("unsafe URL accepted: %q", raw)
		}
	}
	if _, err := parseConfig(testConfig(t, "http://nextcloud:80")); err != nil {
		t.Errorf("explicit Docker private URL rejected: %v", err)
	}
}

func TestNextcloudOriginRequiresOperatorApprovalBeforeBearerRequest(t *testing.T) {
	t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "")
	t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", "")
	requests := 0
	attacker := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.WriteHeader(http.StatusOK)
	}))
	defer attacker.Close()
	source := &types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		Settings:    map[string]interface{}{"base_url": attacker.URL},
		Credentials: map[string]interface{}{"token": "hidden-machine-token"},
		ResourceIDs: []string{"dev-published"},
	}
	if err := NewConnector().Validate(context.Background(), source); err == nil {
		t.Fatal("unapproved HTTPS origin accepted")
	}
	if requests != 0 {
		t.Fatalf("sent Bearer to unapproved HTTPS origin: %d requests", requests)
	}
	for _, allowed := range []string{"https://different.example", attacker.URL + ".evil"} {
		t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", allowed)
		if _, err := parseConfig(source); err == nil {
			t.Fatalf("origin %q matched different approval %q", attacker.URL, allowed)
		}
	}
	t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", attacker.URL)
	if _, err := parseConfig(source); err != nil {
		t.Fatalf("exact approved HTTPS origin rejected: %v", err)
	}
}

func TestNextcloudHTTPOnlyForExplicitDevEndpoint(t *testing.T) {
	t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", "")
	t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "")
	plain := &types.DataSourceConfig{
		Settings:    map[string]interface{}{"base_url": "http://nextcloud"},
		Credentials: map[string]interface{}{"token": "test-token"},
	}
	if _, err := parseConfig(plain); err == nil {
		t.Fatal("development HTTP accepted without explicit flag")
	}
	t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
	if _, err := parseConfig(plain); err != nil {
		t.Fatalf("exact Docker test endpoint rejected: %v", err)
	}
	for _, raw := range []string{"http://nextcloud:8080", "http://nextcloud/other", "http://evil.example"} {
		plain.Settings["base_url"] = raw
		if _, err := parseConfig(plain); err == nil {
			t.Fatalf("unapproved development HTTP target accepted: %s", raw)
		}
	}
}
