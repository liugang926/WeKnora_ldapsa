package nextcloud

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFetchedItemSeparatesMachineContentFromHumanCitation(t *testing.T) {
	const machineURL = "https://source.example/index.php/apps/integration_weknora/api/v1/bindings/b/files/77/content"
	const humanURL = "https://files.example/index.php/f/77"
	item := fetchedItem("instance", "binding", "generation", manifestItem{
		FileID: 77, ETag: "etag-1", Name: "file.md", Path: "file.md",
		URL: machineURL, HumanURL: humanURL,
	}, []byte("content"), "text/markdown")
	require.Equal(t, machineURL, item.URL)
	require.Equal(t, humanURL, item.Metadata["nextcloud_human_url"])
}
