package nextcloud

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func testChangesPage(cursor string, hints []map[string]interface{}, more bool) map[string]interface{} {
	return map[string]interface{}{
		"binding_id": "dev-published", "items": hints, "next_cursor": cursor,
		"has_more": more, "hint_only": true, "rescan_required": false,
	}
}

func TestChangesSkipUnchangedManifestAndReconcileHint(t *testing.T) {
	tag := "etag-1"
	hintPending := false
	manifestCalls, contentCalls := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings/dev-published/changes":
			cursor := r.URL.Query().Get("cursor")
			if cursor == "" {
				writeJSON(w, testChangesPage("c0", nil, false))
			} else if cursor == "c0" && hintPending {
				writeJSON(w, testChangesPage("c1", []map[string]interface{}{{
					"event_id": "1", "file_id": 41, "type": "upsert",
				}}, false))
			} else if cursor == "c0" || cursor == "c1" {
				writeJSON(w, testChangesPage(cursor, nil, false))
			} else {
				t.Errorf("unexpected changes cursor %q", cursor)
			}
		case apiPath + "/bindings/dev-published/manifest":
			manifestCalls++
			writeJSON(w, testManifest(tag))
		case apiPath + "/bindings/dev-published/files/41/content":
			contentCalls++
			w.Header().Set("ETag", tag)
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
	if err != nil || len(items) != 1 || cursor == nil || manifestCalls != 1 || contentCalls != 1 {
		t.Fatalf("first sync = %+v, %+v, %v; manifest=%d content=%d", items, cursor, err, manifestCalls, contentCalls)
	}
	state, _ := decodeCursor(cursor)
	if state.Changes["dev-published"] != "c0" || state.LastReconcileAt == 0 {
		t.Fatalf("first cursor omitted changes checkpoint: %+v", state)
	}
	items, cursor, err = connector.FetchIncremental(ctx, config, cursor)
	if err != nil || len(items) != 0 || manifestCalls != 1 || contentCalls != 1 {
		t.Fatalf("empty feed still scanned: %+v, %v; manifest=%d content=%d", items, err, manifestCalls, contentCalls)
	}
	tag, hintPending = "etag-2", true
	items, cursor, err = connector.FetchIncremental(ctx, config, cursor)
	if err != nil || len(items) != 1 || items[0].Metadata["nextcloud_etag"] != tag ||
		manifestCalls != 2 || contentCalls != 2 {
		t.Fatalf("hint was not reconciled: %+v, %v; manifest=%d content=%d", items, err, manifestCalls, contentCalls)
	}
	state, _ = decodeCursor(cursor)
	if state.Changes["dev-published"] != "c1" {
		t.Fatalf("hint cursor not recorded: %+v", state)
	}
	items, _, err = connector.FetchIncremental(ctx, config, cursor)
	if err != nil || len(items) != 0 || manifestCalls != 2 {
		t.Fatalf("settled feed still scanned: %+v, %v; manifest=%d", items, err, manifestCalls)
	}
}

func TestChangesRefreshOnlyTouchedContentAndFallbackForBroadHint(t *testing.T) {
	hintType := ""
	body41 := "hello"
	manifestCalls := 0
	contentCalls := map[int]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings/dev-published/changes":
			switch r.URL.Query().Get("cursor") {
			case "":
				writeJSON(w, testChangesPage("c0", nil, false))
			case "c0":
				if hintType != "upsert" {
					t.Errorf("unexpected first hint state %q", hintType)
				}
				writeJSON(w, testChangesPage("c1", []map[string]interface{}{{
					"event_id": "1", "file_id": 41, "type": "upsert",
				}}, false))
			case "c1":
				if hintType != "targeted_reconcile" {
					t.Errorf("unexpected targeted reconcile state %q", hintType)
				}
				writeJSON(w, testChangesPage("c2", []map[string]interface{}{{
					"event_id": "2", "file_id": 41, "type": "reconcile",
				}}, false))
			case "c2":
				if hintType != "reconcile" {
					t.Errorf("unexpected broad hint state %q", hintType)
				}
				writeJSON(w, testChangesPage("c3", []map[string]interface{}{{
					"event_id": "3", "type": "reconcile",
				}}, false))
			default:
				t.Errorf("unexpected changes cursor %q", r.URL.Query().Get("cursor"))
			}
		case apiPath + "/bindings/dev-published/manifest":
			manifestCalls++
			manifest := testManifest("same-etag")
			manifest["items"] = append(manifest["items"].([]map[string]interface{}), map[string]interface{}{
				"file_id": 42, "etag": "same-etag", "name": "Other.md", "path": "/Published/Other.md",
				"mime_type": "text/markdown", "size": 5, "mtime": 1700000000,
				"url": "https://files.example/index.php/f/42",
			})
			writeJSON(w, manifest)
		case apiPath + "/bindings/dev-published/files/41/content":
			contentCalls[41]++
			w.Header().Set("ETag", "same-etag")
			_, _ = w.Write([]byte(body41))
		case apiPath + "/bindings/dev-published/files/42/content":
			contentCalls[42]++
			w.Header().Set("ETag", "same-etag")
			_, _ = w.Write([]byte("other"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	connector := NewConnector()
	config := testConfig(t, server.URL)
	ctx := context.Background()
	items, cursor, err := connector.FetchIncremental(ctx, config, nil)
	if err != nil || len(items) != 2 || contentCalls[41] != 1 || contentCalls[42] != 1 {
		t.Fatalf("initial import = %+v, %v; downloads=%v", items, err, contentCalls)
	}
	// An opaque ETag can be reused. The explicit upsert still re-reads only
	// that file, while the complete manifest remains the deletion authority.
	hintType, body41 = "upsert", "world"
	items, cursor, err = connector.FetchIncremental(ctx, config, cursor)
	if err != nil || len(items) != 1 || string(items[0].Content) != "world" ||
		manifestCalls != 2 || contentCalls[41] != 2 || contentCalls[42] != 1 {
		t.Fatalf("targeted hint = %+v, %v; manifests=%d downloads=%v", items, err, manifestCalls, contentCalls)
	}
	// A file-scoped reconcile does not carry an ETag or path, but it must
	// re-read that file even when its opaque ETag is unchanged.
	hintType, body41 = "targeted_reconcile", "later"
	items, cursor, err = connector.FetchIncremental(ctx, config, cursor)
	if err != nil || len(items) != 1 || string(items[0].Content) != "later" ||
		manifestCalls != 3 || contentCalls[41] != 3 || contentCalls[42] != 1 {
		t.Fatalf("targeted reconcile = %+v, %v; manifests=%d downloads=%v", items, err, manifestCalls, contentCalls)
	}
	// An unscoped hint cannot identify safe skips, so every current file is
	// re-read after another complete manifest preflight.
	hintType = "reconcile"
	items, _, err = connector.FetchIncremental(ctx, config, cursor)
	if err != nil || len(items) != 2 || manifestCalls != 4 ||
		contentCalls[41] != 4 || contentCalls[42] != 2 {
		t.Fatalf("broad hint = %+v, %v; manifests=%d downloads=%v", items, err, manifestCalls, contentCalls)
	}
}

func TestScheduledContentAuditRecoversUnhintedSameMetadataChange(t *testing.T) {
	body := "first"
	manifestCalls, contentCalls := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings/dev-published/changes":
			writeJSON(w, testChangesPage("c0", nil, false))
		case apiPath + "/bindings/dev-published/manifest":
			manifestCalls++
			writeJSON(w, testManifest("same-etag"))
		case apiPath + "/bindings/dev-published/files/41/content":
			contentCalls++
			w.Header().Set("ETag", "same-etag")
			_, _ = w.Write([]byte(body))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	connector := NewConnector()
	config := testConfig(t, server.URL)
	ctx := context.Background()
	_, cursor, err := connector.FetchIncremental(ctx, config, nil)
	if err != nil || manifestCalls != 1 || contentCalls != 1 {
		t.Fatalf("initial import: %v, manifest=%d content=%d", err, manifestCalls, contentCalls)
	}
	// Simulate a source write that emitted no retained hint and reused every
	// manifest field. The independent audit deadline still requires content.
	body = "other"
	state, err := decodeCursor(cursor)
	if err != nil {
		t.Fatal(err)
	}
	state.LastContentAuditAt = time.Now().Add(-25 * time.Hour).Unix()
	cursor, err = encodeSyncCursor(state)
	if err != nil {
		t.Fatal(err)
	}
	items, next, err := connector.FetchIncremental(ctx, config, cursor)
	if err != nil || len(items) != 1 || string(items[0].Content) != body ||
		manifestCalls != 2 || contentCalls != 2 {
		t.Fatalf("scheduled content audit: %+v, %v; manifest=%d content=%d",
			items, err, manifestCalls, contentCalls)
	}
	updated, err := decodeCursor(next)
	if err != nil || updated.LastContentAuditAt <= state.LastContentAuditAt {
		t.Fatalf("content audit checkpoint not advanced: %+v, %v", updated, err)
	}
}

func TestMissingFileReappearanceAndPolicyChangeReloadContent(t *testing.T) {
	present := true
	contentCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings/dev-published/changes":
			writeJSON(w, testChangesPage("c0", nil, false))
		case apiPath + "/bindings/dev-published/manifest":
			manifest := testManifest("same-etag")
			if !present {
				manifest["items"] = []interface{}{}
			}
			writeJSON(w, manifest)
		case apiPath + "/bindings/dev-published/files/41/content":
			contentCalls++
			w.Header().Set("ETag", "same-etag")
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
	if err != nil || len(items) != 1 || contentCalls != 1 {
		t.Fatalf("initial import = %+v, %v; downloads=%d", items, err, contentCalls)
	}
	present = false
	state, _ := decodeCursor(cursor)
	state.LastReconcileAt = time.Now().Add(-25 * time.Hour).Unix()
	cursor, err = encodeSyncCursor(state)
	if err != nil {
		t.Fatal(err)
	}
	items, cursor, err = connector.FetchIncremental(ctx, config, cursor)
	if err != nil || len(items) != 0 {
		t.Fatalf("first absence = %+v, %v", items, err)
	}
	present = true
	items, cursor, err = connector.FetchIncremental(ctx, config, cursor)
	if err != nil || len(items) != 1 || items[0].IsDeleted || contentCalls != 2 {
		t.Fatalf("same-state reappearance was skipped: %+v, %v; downloads=%d", items, err, contentCalls)
	}
	config.MultimodalEnabled = true
	items, _, err = connector.FetchIncremental(ctx, config, cursor)
	if err != nil || len(items) != 1 || contentCalls != 3 {
		t.Fatalf("import policy change was skipped: %+v, %v; downloads=%d", items, err, contentCalls)
	}
}

func TestDeleteHintIsNeverDeletionEvidence(t *testing.T) {
	present := true
	deleteHint := false
	manifestCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings/dev-published/changes":
			if r.URL.Query().Get("cursor") == "c0" && deleteHint {
				writeJSON(w, testChangesPage("c1", []map[string]interface{}{{
					"event_id": "1", "file_id": 41, "type": "delete",
				}}, false))
			} else {
				cursor := r.URL.Query().Get("cursor")
				if cursor == "" {
					cursor = "c0"
				}
				writeJSON(w, testChangesPage(cursor, nil, false))
			}
		case apiPath + "/bindings/dev-published/manifest":
			manifestCalls++
			manifest := testManifest("etag-1")
			if !present {
				manifest["items"] = []interface{}{}
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
	deleteHint = true
	items, cursor, err := connector.FetchIncremental(ctx, config, cursor)
	// The hint is not deletion proof. A file still in the complete manifest
	// is re-read and remains an ordinary upsert, even with identical metadata.
	if err != nil || len(items) != 1 || items[0].IsDeleted ||
		string(items[0].Content) != "hello" || manifestCalls != 2 {
		t.Fatalf("delete hint hid present file: %+v, %v; manifest=%d", items, err, manifestCalls)
	}
	present = false
	// With no further hint, force a complete scan using the periodic deadline.
	state, _ := decodeCursor(cursor)
	state.LastReconcileAt = time.Now().Add(-25 * time.Hour).Unix()
	cursor, err = encodeSyncCursor(state)
	if err != nil {
		t.Fatal(err)
	}
	items, cursor, err = connector.FetchIncremental(ctx, config, cursor)
	if err != nil || len(items) != 0 {
		t.Fatalf("first complete absence = %+v, %v", items, err)
	}
	state, _ = decodeCursor(cursor)
	if !state.Missing["dev-published"]["41"] {
		t.Fatalf("first absence not tracked: %+v", state)
	}
	items, _, err = connector.FetchIncremental(ctx, config, cursor)
	if err != nil || len(items) != 1 || !items[0].IsDeleted || manifestCalls != 4 {
		t.Fatalf("second complete absence = %+v, %v; manifest=%d", items, err, manifestCalls)
	}
}

func TestExpiredChangesCursorNeedsSuccessfulManifestBeforeRecovery(t *testing.T) {
	expired := false
	afterFloorHint := false
	manifestStatus := http.StatusOK
	manifestCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings/dev-published/changes":
			cursor := r.URL.Query().Get("cursor")
			if cursor == "c0" && expired {
				w.WriteHeader(http.StatusConflict)
				writeJSON(w, map[string]interface{}{"rescan_required": true, "next_cursor": "floor5"})
			} else if cursor == "floor5" && afterFloorHint {
				writeJSON(w, testChangesPage("c6", []map[string]interface{}{{
					"event_id": "6", "file_id": 41, "type": "metadata",
				}}, false))
			} else if cursor == "floor5" {
				writeJSON(w, testChangesPage("floor5", nil, false))
			} else if cursor == "c6" {
				writeJSON(w, testChangesPage("c6", nil, false))
			} else {
				writeJSON(w, testChangesPage("c0", nil, false))
			}
		case apiPath + "/bindings/dev-published/manifest":
			manifestCalls++
			if manifestStatus != http.StatusOK {
				http.Error(w, "unavailable", manifestStatus)
				return
			}
			writeJSON(w, testManifest("etag-1"))
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
	_, old, err := connector.FetchIncremental(ctx, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	expired, manifestStatus = true, http.StatusServiceUnavailable
	items, failed, err := connector.FetchIncremental(ctx, config, old)
	if err == nil || len(items) != 0 || failed != nil {
		t.Fatalf("failed rescan advanced cursor: %+v, %+v, %v", items, failed, err)
	}
	manifestStatus = http.StatusOK
	items, recovered, err := connector.FetchIncremental(ctx, config, old)
	if err != nil || len(items) != 1 || items[0].IsDeleted || recovered == nil {
		t.Fatalf("successful rescan = %+v, %+v, %v", items, recovered, err)
	}
	state, _ := decodeCursor(recovered)
	if state.Changes["dev-published"] != "floor5" {
		t.Fatalf("recovery cursor missing after full scan: %+v", state)
	}
	afterFloorHint = true
	items, recovered, err = connector.FetchIncremental(ctx, config, recovered)
	if err != nil || len(items) != 1 || items[0].IsDeleted || manifestCalls != 4 {
		t.Fatalf("post-floor event was skipped: %+v, %v; manifest=%d", items, err, manifestCalls)
	}
	state, _ = decodeCursor(recovered)
	if state.Changes["dev-published"] != "c6" {
		t.Fatalf("post-floor event cursor missing: %+v", state)
	}
	items, _, err = connector.FetchIncremental(ctx, config, recovered)
	if err != nil || len(items) != 0 || manifestCalls != 4 {
		t.Fatalf("settled feed still scanned: %+v, %v; manifest=%d", items, err, manifestCalls)
	}
}

func TestChangesPagesAreBounded(t *testing.T) {
	changesCalls, manifestCalls := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings/dev-published/changes":
			changesCalls++
			writeJSON(w, testChangesPage("c"+strings.Repeat("x", changesCalls), []map[string]interface{}{{
				"event_id": strconv.Itoa(changesCalls), "file_id": 41, "type": "upsert",
			}}, true))
		case apiPath + "/bindings/dev-published/manifest":
			manifestCalls++
			writeJSON(w, testManifest("etag-1"))
		case apiPath + "/bindings/dev-published/files/41/content":
			w.Header().Set("ETag", "etag-1")
			_, _ = w.Write([]byte("hello"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	items, cursor, err := NewConnector().FetchIncremental(context.Background(), testConfig(t, server.URL), nil)
	if err != nil || len(items) != 1 || cursor == nil || changesCalls != maxChangePages || manifestCalls != 1 {
		t.Fatalf("bounded scan = %+v, %+v, %v; changes=%d manifest=%d", items, cursor, err, changesCalls, manifestCalls)
	}
}

func TestMalformedChangesPageDoesNotAdvanceCursor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings/dev-published/changes":
			writeJSON(w, testChangesPage("bad cursor", nil, false))
		case apiPath + "/bindings/dev-published/manifest":
			t.Error("manifest should not be scanned after malformed feed")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	items, cursor, err := NewConnector().FetchIncremental(context.Background(), testConfig(t, server.URL), nil)
	if err == nil || len(items) != 0 || cursor != nil {
		t.Fatalf("malformed feed = %+v, %+v, %v", items, cursor, err)
	}
}

func TestFullReconcileIntervalIsBounded(t *testing.T) {
	config := testConfig(t, "http://nextcloud")
	for _, value := range []interface{}{299, 604801, "bad", 600.5} {
		config.Settings["full_reconcile_interval_seconds"] = value
		if _, err := parseConfig(config); err == nil {
			t.Errorf("accepted invalid reconcile interval %v", value)
		}
	}
	config.Settings["full_reconcile_interval_seconds"] = 300
	parsed, err := parseConfig(config)
	if err != nil || parsed.reconcileInterval != 5*time.Minute {
		t.Fatalf("five-minute interval = %v, %v", parsed.reconcileInterval, err)
	}
}
