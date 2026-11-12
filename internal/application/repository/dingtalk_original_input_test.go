package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/datasource/connector/dingtalk"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestDingTalkOriginalRequiresConnectorIssuer(t *testing.T) {
	store := &KnowledgeInputProvenanceStore{}
	item := &types.FetchedItem{
		Content: []byte(
			"external body"),
		OriginalArtifact: dingtalk.OriginalFetchedArtifactInput{
			WorkspaceID: "workspace",
			DocumentID:  "document",
		},
	}
	_, err := store.BindDingTalkOriginalArtifact(context.Background(), item)
	require.Error(t, err, "no captured source/run")
	ctx := context.WithValue(context.Background(),
		dingTalkFetchKey{},
		dingTalkFetch{
			Tenant: 7,
			KB:     "kb",
			Source: "source",
			Run:    "run",
		})
	_, err = store.BindDingTalkOriginalArtifact(ctx, item)
	require.Error(t, err, "a public descriptor is not an issuer")
	item.OriginalArtifact = nil
	_, err = store.BindDingTalkOriginalArtifact(ctx, item)
	require.Error(t, err, "metadata and body without the issuer cannot mint a receipt")
}
