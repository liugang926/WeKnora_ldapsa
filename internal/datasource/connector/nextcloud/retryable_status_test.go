package nextcloud

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/stretchr/testify/require"
)

func TestNextcloudSourceReadStatusClassification(t *testing.T) {
	status := http.StatusOK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))
	defer server.Close()
	cfg, err := parseConfig(testConfig(t, server.URL))
	require.NoError(t, err)
	client := newClient(cfg)
	for _, scenario := range []struct {
		name      string
		status    int
		ifMatch   bool
		retryable bool
		invalid   bool
	}{
		{"stale content", 412, true, true, false},
		{"deleted after manifest", 404, true, true, false},
		{"missing manifest", 404, false, false, false},
		{"rate limited", 429, false, true, false},
		{"upstream unavailable", 503, false, true, false},
		{"unauthorized", 401, true, false, true},
		{"forbidden", 403, true, false, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			status = scenario.status
			var err error
			if scenario.ifMatch {
				_, err = client.get(context.Background(), "/bindings/dev-published/files/41/content", nil, "old-etag")
			} else {
				_, err = client.get(context.Background(), "/bindings/dev-published/manifest", nil)
			}
			require.Error(t, err)
			require.Equal(t, scenario.retryable, errors.Is(err, datasource.ErrRetryableSource))
			require.Equal(t, scenario.invalid, errors.Is(err, datasource.ErrInvalidCredentials))
		})
	}
}

func TestNextcloudResponseReadFailureClassification(t *testing.T) {
	for _, scenario := range []struct {
		name          string
		kind          string
		body          string
		contentLength int64
		wantRetryable bool
	}{
		{"truncated manifest", "json", `{}`, 10, true},
		{"truncated changes", "changes", `{}`, 10, true},
		{"truncated content", "content", "abc", 10, true},
		{"malformed JSON", "json", `{`, 1, false},
		{"oversized JSON", "json", "", maxJSONResponseBytes + 1, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Length", strconv.FormatInt(scenario.contentLength, 10))
				w.Header().Set("ETag", `"etag-1"`)
				_, _ = w.Write([]byte(scenario.body))
			}))
			defer server.Close()
			cfg, err := parseConfig(testConfig(t, server.URL))
			require.NoError(t, err)
			client := newClient(cfg)
			var readErr error
			switch scenario.kind {
			case "changes":
				_, readErr = client.changes(context.Background(), "dev-published", "")
			case "content":
				_, _, readErr = client.content(context.Background(), "dev-published",
					manifestItem{FileID: 41, ETag: "etag-1", Size: 3})
			default:
				var page manifestPage
				readErr = client.getJSON(context.Background(), "/bindings/dev-published/manifest", nil, &page)
			}
			require.Error(t, readErr)
			require.Equal(t, scenario.wantRetryable, errors.Is(readErr, datasource.ErrRetryableSource))
		})
	}
}

func TestNextcloudManifestGenerationMismatchIsRetryable(t *testing.T) {
	var pages atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pages.Add(1)
		if r.URL.Query().Get("cursor") == "" {
			writeJSON(w, map[string]interface{}{
				"generation": "first", "complete": false,
				"next_cursor": "page-2", "items": []interface{}{},
			})
			return
		}
		writeJSON(w, map[string]interface{}{
			"generation": "second", "complete": true, "next_cursor": nil,
			"items": []interface{}{},
		})
	}))
	defer server.Close()
	cfg, err := parseConfig(testConfig(t, server.URL))
	require.NoError(t, err)
	_, _, err = newClient(cfg).manifest(context.Background(), "dev-published")
	require.ErrorIs(t, err, datasource.ErrRetryableSource)
	require.EqualValues(t, 2, pages.Load())
}

func TestNextcloudContentETagMismatchIsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("If-Match"); got != `"etag-1"` {
			t.Errorf("If-Match = %q, want quoted etag-1", got)
		}
		w.Header().Set("ETag", `"etag-2"`)
		_, _ = w.Write([]byte("hello"))
	}))
	defer server.Close()
	cfg, err := parseConfig(testConfig(t, server.URL))
	require.NoError(t, err)
	_, _, err = newClient(cfg).content(context.Background(), "dev-published", manifestItem{
		FileID: 41,
		ETag:   "etag-1", Size: 5,
	})
	require.ErrorIs(t, err, datasource.ErrRetryableSource)
}
