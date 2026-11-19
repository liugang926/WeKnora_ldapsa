package dingtalk

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Tencent/WeKnora/internal/types"
)

func TestEmptyUploadRemainsPresentAndCanRefill(t *testing.T) {
	ctx := context.Background()
	api := uploadedDocumentsFixture()
	doc := binaryNode("upload", "instructions.txt", "txt")
	api.nodes["root"] = []node{doc}
	cfg := testConfigWithUploadedFiles("space")
	c := testConnector(api)

	// Even an already imported revision can become empty during a full sync.
	before, err := encodeCursor(&cursorState{
		Version: cursorVersion, Resources: map[string]map[string]string{"space": {doc.ID: doc.revision()}},
	})
	require.NoError(t, err)
	items, next, err := c.FetchAllFromCursor(ctx, cfg, cfg.ResourceIDs, &types.SyncCursor{ConnectorCursor: before})
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.True(t, IsEmptyUpload(&items[0]))
	require.False(t, items[0].IsDeleted)
	present, err := CursorContainsAnyDocument(next, []string{doc.ID})
	require.NoError(t, err)
	require.True(t, present)

	// An error from the same file is never an empty-file success.
	api.dlErrors = map[string]error{doc.ID: errDocumentTooLarge}
	items, _, err = c.FetchIncremental(ctx, cfg, next)
	require.Error(t, err)
	require.Len(t, items, 1)
	require.False(t, IsEmptyUpload(&items[0]))

	delete(api.dlErrors, doc.ID)
	api.downloads[doc.ID] = []byte("ready")
	items, _, err = c.FetchIncremental(ctx, cfg, next)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.False(t, IsEmptyUpload(&items[0]))
	require.Equal(t, []byte("ready"), items[0].Content)
	_, err = VerifyOriginalFetchedArtifact(&items[0])
	require.NoError(t, err)
}
