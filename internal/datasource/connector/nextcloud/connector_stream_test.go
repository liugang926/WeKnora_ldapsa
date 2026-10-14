package nextcloud

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type nextcloudCaptureStream struct {
	instanceID      string
	bindingID       string
	items           []types.FetchedItem
	checkpoints     int
	emitError       error
	observed        *atomic.Int32
	emitted         *atomic.Int32
	failed          map[string]string
	retryExternalID string
	requireManifest bool
}

func (h *nextcloudCaptureStream) FailedNextcloudCandidateETags() map[string]string {
	return h.failed
}

func (h *nextcloudCaptureStream) NextcloudRetryExternalID() string {
	return h.retryExternalID
}

func (h *nextcloudCaptureStream) NextcloudRequireManifest() bool {
	return h.requireManifest
}

func (h *nextcloudCaptureStream) ObserveNextcloudIdentity(_ context.Context, instanceID, bindingID string) error {
	h.instanceID, h.bindingID = instanceID, bindingID
	if h.observed != nil {
		h.observed.Add(1)
	}
	return nil
}

func (h *nextcloudCaptureStream) Emit(_ context.Context, item types.FetchedItem) error {
	if h.emitError != nil {
		return h.emitError
	}
	h.items = append(h.items, item)
	if h.emitted != nil {
		h.emitted.Add(1)
	}
	return nil
}

func (h *nextcloudCaptureStream) Checkpoint(context.Context, *types.SyncCursor) error {
	h.checkpoints++
	return nil
}

func streamBindingResponse() map[string]interface{} {
	return map[string]interface{}{"bindings": []map[string]interface{}{{
		"id": "dev-published", "name": "Published", "root_file_id": 7,
	}}}
}

func TestFetchStreamRetriesFailedFileWithoutETagChange(t *testing.T) {
	manifestCalls, contentCalls := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings":
			writeJSON(w, streamBindingResponse())
		case apiPath + "/bindings/dev-published/changes":
			writeJSON(w, testChangesPage("c0", nil, false))
		case apiPath + "/bindings/dev-published/manifest":
			manifestCalls++
			writeJSON(w, testManifest("same-etag"))
		case apiPath + "/bindings/dev-published/files/41/content":
			contentCalls++
			w.Header().Set("ETag", "same-etag")
			_, _ = w.Write([]byte("same bytes"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	connector, config := NewConnector(), testConfig(t, server.URL)
	first := &nextcloudCaptureStream{}
	cursor, err := connector.FetchStream(context.Background(), config, nil, first)
	require.NoError(t, err)
	require.Len(t, first.items, 1)
	require.Equal(t, 1, manifestCalls)
	require.Equal(t, 1, contentCalls)
	unchanged := &nextcloudCaptureStream{}
	cursor, err = connector.FetchStream(context.Background(), config, cursor, unchanged)
	require.NoError(t, err)
	require.Empty(t, unchanged.items)
	require.Equal(t, 1, manifestCalls)
	require.Equal(t, 1, contentCalls)
	retry := &nextcloudCaptureStream{failed: map[string]string{"41": "same-etag"}}
	_, err = connector.FetchStream(context.Background(), config, cursor, retry)
	require.NoError(t, err)
	require.Len(t, retry.items, 1)
	require.Equal(t, "same-etag", retry.items[0].Metadata["nextcloud_etag"])
	require.Equal(t, 2, manifestCalls)
	require.Equal(t, 2, contentCalls)
}

func TestFetchStreamRetryDoesNotReadChangedNeighborContent(t *testing.T) {
	phase, blockedNeighborReads := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings":
			writeJSON(w, streamBindingResponse())
		case apiPath + "/bindings/dev-published/changes":
			if r.URL.Query().Get("cursor") == "" {
				writeJSON(w, testChangesPage("c0", nil, false))
			} else if phase > 0 {
				writeJSON(w, testChangesPage("c1", []map[string]interface{}{{
					"event_id": "1", "file_id": 42, "type": "upsert",
				}}, false))
			} else {
				writeJSON(w, testChangesPage("c0", nil, false))
			}
		case apiPath + "/bindings/dev-published/manifest":
			manifest := testManifest("etag-a")
			neighborETag := "etag-b-old"
			if phase > 0 {
				neighborETag = "etag-b-new"
			}
			manifest["items"] = append(manifest["items"].([]map[string]interface{}), map[string]interface{}{
				"file_id": 42, "etag": neighborETag, "name": "Neighbor.md", "path": "/Published/Neighbor.md",
				"mime_type": "text/markdown", "size": 8, "mtime": 1700000000,
				"url": "https://files.example/index.php/f/42",
			})
			writeJSON(w, manifest)
		case apiPath + "/bindings/dev-published/files/41/content":
			w.Header().Set("ETag", "etag-a")
			_, _ = w.Write([]byte("failed A"))
		case apiPath + "/bindings/dev-published/files/42/content":
			if phase == 1 {
				blockedNeighborReads++
				http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
				return
			}
			if phase == 2 {
				w.Header().Set("ETag", "etag-b-new")
			} else {
				w.Header().Set("ETag", "etag-b-old")
			}
			_, _ = w.Write([]byte("neighbor"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	connector, config := NewConnector(), testConfig(t, server.URL)
	initial := &nextcloudCaptureStream{}
	cursor, err := connector.FetchStream(context.Background(), config, nil, initial)
	require.NoError(t, err)
	require.Len(t, initial.items, 2)

	phase = 1
	retry := &nextcloudCaptureStream{
		failed: map[string]string{"41": "etag-a"}, retryExternalID: "nextcloud:instance-1:41",
	}
	_, err = connector.FetchStream(context.Background(), config, cursor, retry)
	require.NoError(t, err)
	require.Len(t, retry.items, 1)
	require.Equal(t, "41", retry.items[0].Metadata["nextcloud_file_id"])
	require.Zero(t, blockedNeighborReads, "retry must skip B before its failing content GET")

	// The service retains this pre-retry cursor. Its next ordinary sync still
	// sees B's changed ETag and fetches B when the endpoint recovers.
	phase = 2
	ordinary := &nextcloudCaptureStream{}
	_, err = connector.FetchStream(context.Background(), config, cursor, ordinary)
	require.NoError(t, err)
	require.Len(t, ordinary.items, 1)
	require.Equal(t, "42", ordinary.items[0].Metadata["nextcloud_file_id"])
}

func TestFetchStreamEventRetryProvesFreshManifestAfterSettledChanges(t *testing.T) {
	manifestCalls, contentCalls := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings":
			writeJSON(w, streamBindingResponse())
		case apiPath + "/bindings/dev-published/changes":
			writeJSON(w, testChangesPage("c1", nil, false))
		case apiPath + "/bindings/dev-published/manifest":
			manifestCalls++
			writeJSON(w, testManifest("etag-a"))
		case apiPath + "/bindings/dev-published/files/41/content":
			contentCalls++
			w.Header().Set("ETag", "etag-a")
			_, _ = w.Write([]byte("candidate A"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	connector, config := NewConnector(), testConfig(t, server.URL)
	cursor, err := connector.FetchStream(context.Background(), config, nil, &nextcloudCaptureStream{})
	require.NoError(t, err)
	require.Equal(t, 1, manifestCalls)
	require.Equal(t, 1, contentCalls)
	state, err := decodeCursor(cursor)
	require.NoError(t, err)
	state.LastReconcileAt = time.Now().UTC().Add(-5 * time.Minute).Unix()
	state.LastContentAuditAt = time.Now().UTC().Unix()
	cursor, err = encodeSyncCursor(state)
	require.NoError(t, err)

	// A normal sync may skip the fresh manifest when the changes cursor was
	// already consumed; an event retry must produce proof from this sync run.
	_, err = connector.FetchStream(context.Background(), config, cursor, &nextcloudCaptureStream{})
	require.NoError(t, err)
	require.Equal(t, 1, manifestCalls)
	startedAt := time.Now().UTC()
	event := &nextcloudCaptureStream{requireManifest: true}
	confirmed, err := connector.FetchStream(context.Background(), config, cursor, event)
	require.NoError(t, err)
	require.Empty(t, event.items, "unchanged content need not be downloaded")
	require.Equal(t, 2, manifestCalls)
	require.Equal(t, 1, contentCalls)
	confirmedState, err := decodeCursor(confirmed)
	require.NoError(t, err)
	require.False(t, time.Unix(confirmedState.LastReconcileAt, 0).Before(startedAt.Add(-time.Second)),
		"event reconciliation must satisfy applied cursor proof")
	require.False(t, confirmed.LastSyncTime.Before(startedAt))
}

func TestFetchStreamEmitsOneContentAtATimeAfterCompleteManifest(t *testing.T) {
	var pages, emitted, observed atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings":
			writeJSON(w, streamBindingResponse())
		case apiPath + "/bindings/dev-published/changes":
			http.NotFound(w, r)
		case apiPath + "/bindings/dev-published/manifest":
			pages.Add(1)
			if r.URL.Query().Get("cursor") == "" {
				writeJSON(w, map[string]interface{}{
					"generation": "g1", "complete": false,
					"next_cursor": "page-2", "items": testManifest("etag-41")["items"],
				})
				return
			}
			second := map[string]interface{}{
				"file_id": 42, "etag": "etag-42", "name": "Other.md",
				"path": "/Published/Other.md", "mime_type": "text/markdown", "size": 5,
				"mtime": 1700000000, "url": "https://files.example/index.php/f/42",
			}
			writeJSON(w, map[string]interface{}{
				"generation": "g1", "complete": true,
				"next_cursor": nil, "items": []map[string]interface{}{second},
			})
		case apiPath + "/bindings/dev-published/files/41/content":
			if pages.Load() != 2 || observed.Load() != 1 || emitted.Load() != 0 {
				t.Errorf("first download before preflight: pages=%d observed=%d emitted=%d",
					pages.Load(), observed.Load(), emitted.Load())
			}
			w.Header().Set("ETag", "etag-41")
			_, _ = w.Write([]byte("first"))
		case apiPath + "/bindings/dev-published/files/42/content":
			if emitted.Load() != 1 {
				t.Errorf("second content fetched before first item was emitted")
			}
			w.Header().Set("ETag", "etag-42")
			_, _ = w.Write([]byte("other"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	h := &nextcloudCaptureStream{observed: &observed, emitted: &emitted}
	next, err := NewConnector().FetchStream(context.Background(), testConfig(t, server.URL), nil, h)
	require.NoError(t, err)
	require.NotNil(t, next)
	require.Equal(t, "instance-1", h.instanceID)
	require.Equal(t, "dev-published", h.bindingID)
	require.Len(t, h.items, 2)
	require.Equal(t, "first", string(h.items[0].Content))
	require.Equal(t, "other", string(h.items[1].Content))
	require.Zero(t, h.checkpoints)
}

func TestFetchStreamIncompleteManifestNeverEmits(t *testing.T) {
	var contentCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings":
			writeJSON(w, streamBindingResponse())
		case apiPath + "/bindings/dev-published/changes":
			http.NotFound(w, r)
		case apiPath + "/bindings/dev-published/manifest":
			if r.URL.Query().Get("cursor") == "" {
				writeJSON(w, map[string]interface{}{
					"generation": "g1", "complete": false,
					"next_cursor": "page-2", "items": testManifest("etag-1")["items"],
				})
			} else {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
			}
		case apiPath + "/bindings/dev-published/files/41/content":
			contentCalls.Add(1)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	h := &nextcloudCaptureStream{}
	next, err := NewConnector().FetchStream(context.Background(), testConfig(t, server.URL), nil, h)
	require.Error(t, err)
	require.Nil(t, next)
	require.Empty(t, h.items)
	require.Zero(t, contentCalls.Load())
	require.Zero(t, h.checkpoints)
}

func TestFetchStreamWithdrawnBindingStopsBeforeManifest(t *testing.T) {
	var manifestCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings":
			writeJSON(w, map[string]interface{}{"bindings": []interface{}{}})
		case apiPath + "/bindings/dev-published/manifest":
			manifestCalls.Add(1)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	h := &nextcloudCaptureStream{}
	next, err := NewConnector().FetchStream(context.Background(), testConfig(t, server.URL), nil, h)
	require.ErrorContains(t, err, "binding is unavailable")
	require.Nil(t, next)
	require.Empty(t, h.instanceID)
	require.Empty(t, h.items)
	require.Zero(t, manifestCalls.Load())
}

func TestFetchFullStreamRetainsTwoCompleteScanDeletionEvidence(t *testing.T) {
	var present atomic.Bool
	present.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings":
			writeJSON(w, streamBindingResponse())
		case apiPath + "/bindings/dev-published/changes":
			http.NotFound(w, r)
		case apiPath + "/bindings/dev-published/manifest":
			manifest := testManifest("etag-1")
			if !present.Load() {
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
	first := &nextcloudCaptureStream{}
	cursor, err := connector.FetchStream(ctx, config, nil, first)
	require.NoError(t, err)
	require.Len(t, first.items, 1)
	present.Store(false)
	missing := &nextcloudCaptureStream{}
	firstMissing, err := connector.FetchFullStream(ctx, config, cursor, missing)
	require.NoError(t, err)
	require.Empty(t, missing.items)
	state, err := decodeCursor(firstMissing)
	require.NoError(t, err)
	require.True(t, state.Missing["dev-published"]["41"])
	confirmed := &nextcloudCaptureStream{}
	secondMissing, err := connector.FetchFullStream(ctx, config, firstMissing, confirmed)
	require.NoError(t, err)
	require.Len(t, confirmed.items, 1)
	require.True(t, confirmed.items[0].IsDeleted)
	state, err = decodeCursor(secondMissing)
	require.NoError(t, err)
	require.Equal(t, "etag-1", state.Tombstones["dev-published"]["41"].ETag)
	require.Zero(t, confirmed.checkpoints)
}

func TestFetchStreamEmitFailureStopsWithoutCursor(t *testing.T) {
	var secondDownload atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings":
			writeJSON(w, streamBindingResponse())
		case apiPath + "/bindings/dev-published/changes":
			http.NotFound(w, r)
		case apiPath + "/bindings/dev-published/manifest":
			second := map[string]interface{}{
				"file_id": 42, "etag": "etag-42", "name": "Other.md",
				"path": "/Published/Other.md", "mime_type": "text/markdown", "size": 5,
				"mtime": 1700000000, "url": "https://files.example/index.php/f/42",
			}
			writeJSON(w, map[string]interface{}{
				"generation": "g1", "complete": true,
				"next_cursor": nil,
				"items": []interface{}{
					testManifest("etag-41")["items"].([]map[string]interface{})[0],
					second,
				},
			})
		case apiPath + "/bindings/dev-published/files/41/content":
			w.Header().Set("ETag", "etag-41")
			_, _ = w.Write([]byte("first"))
		case apiPath + "/bindings/dev-published/files/42/content":
			secondDownload.Add(1)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	h := &nextcloudCaptureStream{emitError: errors.New("ingest failed")}
	next, err := NewConnector().FetchStream(context.Background(), testConfig(t, server.URL), nil, h)
	require.ErrorContains(t, err, "ingest failed")
	require.Nil(t, next)
	require.Zero(t, secondDownload.Load())
	require.Zero(t, h.checkpoints)
}
