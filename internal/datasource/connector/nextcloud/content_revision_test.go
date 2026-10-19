package nextcloud

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNextcloudSameETagHigherContentRevisionFetchesNewSnapshot(t *testing.T) {
	revision := "0"
	body := "old body"
	downloads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case apiPath + "/capabilities":
			writeJSON(w, map[string]string{"protocol_version": "1", "instance_id": "instance-1"})
		case apiPath + "/bindings/dev-published/manifest":
			page := testManifest("etag-collision")
			page["items"].([]map[string]interface{})[0]["source_content_revision"] = revision
			writeJSON(w, page)
		case apiPath + "/bindings/dev-published/files/41/content":
			downloads++
			if revision != "0" {
				require.Equal(t, revision, r.URL.Query().Get("content_revision"))
			}
			w.Header().Set("ETag", "etag-collision")
			w.Header().Set("X-Nextcloud-Content-Revision", revision)
			writeNextcloudTestResponse(t, w, body)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	config := testConfig(t, server.URL)
	connector := NewConnector()
	items, cursor, err := connector.FetchIncremental(context.Background(), config, nil)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, "0", items[0].Metadata["nextcloud_content_revision"])
	revision = "1"
	body = "new body"
	items, _, err = connector.FetchIncremental(context.Background(), config, cursor)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, "new body", string(items[0].Content))
	require.Equal(t, "1", items[0].Metadata["nextcloud_content_revision"])
	artifact, err := VerifyOriginalFetchedArtifact(&items[0])
	require.NoError(t, err)
	require.Equal(t, "1", artifact.ContentRevision)
	items[0].Metadata["nextcloud_content_revision"] = "2"
	_, err = VerifyOriginalFetchedArtifact(&items[0])
	require.Error(t, err, "the private original-artifact issuer binds the independent content revision")
	require.Equal(t, 2, downloads)
}

func TestNextcloudDownloadContentRevisionMismatchRejectsArtifact(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", "same")
		w.Header().Set("X-Nextcloud-Content-Revision", "2")
		writeNextcloudTestResponse(t, w, "newer bytes")
	}))
	defer server.Close()
	cfg, err := parseConfig(testConfig(t, server.URL))
	require.NoError(t, err)
	_, _, err = newClient(
		cfg,
	).content(context.Background(), "dev-published", manifestItem{FileID: 41, ETag: "same", ContentRevision: "1", Name: "Notes.md", Size: 11})
	require.Error(t, err)
}
