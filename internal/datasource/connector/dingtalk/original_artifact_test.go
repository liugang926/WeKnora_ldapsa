package dingtalk

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func artifactFixture() (*Connector, *fakeAPI, *types.DataSourceConfig) {
	api := &fakeAPI{workspaces: []workspace{{ID: "w", RootNodeID: "root"}}, nodes: map[string][]node{
		"root": {{ID: "folder", Type: "FOLDER", WorkspaceID: "w"}},
		"folder": {{
			ID: "doc", Type: "FILE", Category: "ALIDOC", Extension: "adoc",
			WorkspaceID: "w", Name: "Synthetic", ModifiedTimestamp: 1234,
		}},
	}, blocks: map[string][]json.RawMessage{"doc": {rawJSON(
		`{"blockType":"paragraph","children":[{"elementType":"text","text":"synthetic text"}]}`,
	)}}}
	return testConnector(api), api, testConfig("w")
}

func TestOriginalArtifactActualFetchAndIncremental(t *testing.T) {
	c, api, cfg := artifactFixture()
	items, cursor, err := c.FetchAllFromCursor(context.Background(), cfg, cfg.ResourceIDs, nil)
	require.NoError(t, err)
	require.Len(t, items, 1)
	input, err := VerifyOriginalFetchedArtifact(&items[0])
	require.NoError(t, err)
	require.Equal(t, "w", input.WorkspaceID)
	require.Equal(t, "doc", input.DocumentID)
	require.Equal(t, "1234", input.Revision)
	require.NoError(t, VerifyOriginalArtifactConfiguration(cfg, input))
	input.DocumentID = "modified copy"
	again, err := VerifyOriginalFetchedArtifact(&items[0])
	require.NoError(t, err)
	require.Equal(t, "doc", again.DocumentID)
	skipped, _, err := c.FetchIncremental(context.Background(), cfg, cursor)
	require.NoError(t, err)
	require.Empty(t, skipped)
	api.nodes["folder"][0].ModifiedTimestamp = 5678
	updated, next, err := c.FetchIncremental(context.Background(), cfg, cursor)
	require.NoError(t, err)
	require.Len(t, updated, 1)
	changed, err := VerifyOriginalFetchedArtifact(&updated[0])
	require.NoError(t, err)
	require.Equal(t, "5678", changed.Revision)
	api.nodes["folder"] = nil
	deleted, _, err := c.FetchIncremental(context.Background(), cfg, next)
	require.NoError(t, err)
	require.Len(t, deleted, 1)
	require.True(t, deleted[0].IsDeleted)
	require.Nil(t, deleted[0].OriginalArtifact)
	_, err = VerifyOriginalFetchedArtifact(&deleted[0])
	require.Error(t, err)
}

func TestOriginalArtifactRejectsForgeryAndMutation(t *testing.T) {
	c, _, cfg := artifactFixture()
	items, err := c.FetchAll(context.Background(), cfg, cfg.ResourceIDs)
	require.NoError(t, err)
	require.Len(t, items, 1)
	original := items[0]
	for name, mutate := range map[string]func(*types.FetchedItem){
		"bytes":     func(i *types.FetchedItem) { i.Content = []byte("other") },
		"empty":     func(i *types.FetchedItem) { i.Content = nil },
		"external":  func(i *types.FetchedItem) { i.ExternalID = "other" },
		"title":     func(i *types.FetchedItem) { i.Title = "other" },
		"filename":  func(i *types.FetchedItem) { i.FileName = "other.md" },
		"metadata":  func(i *types.FetchedItem) { i.Metadata["extra"] = "other" },
		"workspace": func(i *types.FetchedItem) { i.Metadata["workspace_id"] = "other" },
		"revision":  func(i *types.FetchedItem) { i.UpdatedAt = i.UpdatedAt.Add(1) },
		"scope":     func(i *types.FetchedItem) { i.SourceResourceID = "other" },
		"delete":    func(i *types.FetchedItem) { i.IsDeleted = true },
		"forged":    func(i *types.FetchedItem) { i.OriginalArtifact = map[string]any{"complete": true} },
		"descriptor": func(i *types.FetchedItem) {
			i.OriginalArtifact = original.OriginalArtifact.(originalFetchedArtifact).input
		},
		"missing": func(i *types.FetchedItem) { i.OriginalArtifact = nil },
	} {
		t.Run(name, func(t *testing.T) {
			item := original
			item.Metadata = map[string]string{}
			for k, v := range original.Metadata {
				item.Metadata[k] = v
			}
			mutate(&item)
			_, err := VerifyOriginalFetchedArtifact(&item)
			require.Error(t, err)
		})
	}
	body, err := json.Marshal(original)
	require.NoError(t, err)
	var roundTripped types.FetchedItem
	require.NoError(t, json.Unmarshal(body, &roundTripped))
	_, err = VerifyOriginalFetchedArtifact(&roundTripped)
	require.Error(t, err)
	_, err = VerifyOriginalFetchedArtifact(nil)
	require.Error(t, err)
	for _, name := range []string{"secret", "operator", "client", "selection", "settings", "type"} {
		t.Run(name, func(t *testing.T) {
			changed := testConfig("w")
			switch name {
			case "secret":
				changed.Credentials["client_secret"] = "other"
			case "operator":
				changed.Credentials["operator_id"] = "other"
			case "client":
				changed.Credentials["client_id"] = "other"
			case "selection":
				changed.ResourceIDs = []string{"other"}
			case "settings":
				changed.Settings = map[string]interface{}{"changed": true}
			case "type":
				changed.Type = "other"
			}
			input, err := VerifyOriginalFetchedArtifact(&original)
			require.NoError(t, err)
			require.Error(t, VerifyOriginalArtifactConfiguration(changed, input))
		})
	}
}

func TestOriginalArtifactPartialFailureAndEmptyRevision(t *testing.T) {
	c, api, cfg := artifactFixture()
	api.nodes["folder"][0].ModifiedTimestamp = 0
	other := api.nodes["folder"][0]
	other.ID = "second"
	api.nodes["folder"] = append(api.nodes["folder"], other)
	api.blocks["second"] = api.blocks["doc"]
	items, err := c.FetchAll(context.Background(), cfg, cfg.ResourceIDs)
	require.NoError(t, err)
	require.Len(t, items, 2)
	first, err := VerifyOriginalFetchedArtifact(&items[0])
	require.NoError(t, err)
	second, err := VerifyOriginalFetchedArtifact(&items[1])
	require.NoError(t, err)
	require.Empty(t, first.Revision)
	require.Equal(t, first.ArtifactSHA256, second.ArtifactSHA256)
	require.NotEqual(t, first.DocumentID, second.DocumentID)
	swapped := items[1]
	swapped.OriginalArtifact = items[0].OriginalArtifact
	_, err = VerifyOriginalFetchedArtifact(&swapped)
	require.Error(t, err)
	api.blockErrors = map[string]error{"second": errors.New("synthetic read failure")}
	partial, err := c.FetchAll(context.Background(), cfg, cfg.ResourceIDs)
	require.Error(t, err)
	require.Len(t, partial, 2)
	for i := range partial {
		_, verifyErr := VerifyOriginalFetchedArtifact(&partial[i])
		if partial[i].ExternalID == "second" {
			require.Error(t, verifyErr)
			require.Nil(t, partial[i].OriginalArtifact)
		} else {
			require.NoError(t, verifyErr)
		}
	}
}

func TestOriginalArtifactUsesActualSelection(t *testing.T) {
	c, _, cfg := artifactFixture()
	cfg.ResourceIDs = []string{"not-selected"}
	items, err := c.FetchAll(context.Background(), cfg, []string{"w"})
	require.NoError(t, err)
	require.Len(t, items, 1)
	input, err := VerifyOriginalFetchedArtifact(&items[0])
	require.NoError(t, err)
	require.Error(t, VerifyOriginalArtifactConfiguration(cfg, input))
	cfg.ResourceIDs = []string{"w", "w"}
	require.NoError(t, VerifyOriginalArtifactConfiguration(cfg, input))
}

func TestOriginalArtifactInvocationBinding(t *testing.T) {
	c, api, cfg := artifactFixture()
	api.nodes["folder"][0].Name = "客户 / 合同: 版本1"
	ctx, invocation := BeginOriginalFetch(context.Background())
	items, err := c.FetchAll(ctx, cfg, cfg.ResourceIDs)
	require.NoError(t, err)
	require.Len(t, items, 1)
	for n := 0; n < 2; n++ {
		_, err = VerifyOriginalFetchedArtifactForInvocation(invocation, &items[0])
		require.NoError(t, err)
	}
	_, other := BeginOriginalFetch(ctx)
	_, err = VerifyOriginalFetchedArtifactForInvocation(other, &items[0])
	require.Error(t, err)
	_, err = VerifyOriginalFetchedArtifactForInvocation(&OriginalFetchInvocation{}, &items[0])
	require.Error(t, err)
	legacy, err := c.FetchAll(context.Background(), cfg, cfg.ResourceIDs)
	require.NoError(t, err)
	_, err = VerifyOriginalFetchedArtifactForInvocation(invocation, &legacy[0])
	require.Error(t, err)
	encoded, err := json.Marshal(items[0])
	require.NoError(t, err)
	var decoded types.FetchedItem
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	_, err = VerifyOriginalFetchedArtifactForInvocation(invocation, &decoded)
	require.Error(t, err)
}
