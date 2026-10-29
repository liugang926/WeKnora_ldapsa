package handler

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestNextcloudReadAuthorityRequiresExplicitOptIn(t *testing.T) {
	knowledge := &types.Knowledge{Channel: types.ConnectorTypeNextcloud}

	t.Setenv("WEKNORA_NEXTCLOUD_READ_AUTHORITY", "")
	require.Error(t, checkKnowledgePublication(context.Background(), nil, knowledge),
		"the default deployment must retain the live Nextcloud publication boundary")
	handler := &KnowledgeHandler{}
	_, err := handler.beginNextcloudRead(context.Background(), nil, []*types.Knowledge{knowledge})
	require.Error(t, err, "the default deployment must retain Nextcloud read leases")

	t.Setenv("WEKNORA_NEXTCLOUD_READ_AUTHORITY", "  WeKnora  ")
	require.NoError(t, checkKnowledgePublication(context.Background(), nil, knowledge))
	leases, err := handler.beginNextcloudRead(context.Background(), nil, []*types.Knowledge{knowledge})
	require.NoError(t, err)
	require.Nil(t, leases)
}

func TestChunkNextcloudReadAuthorityRequiresExplicitOptIn(t *testing.T) {
	knowledge := &types.Knowledge{Channel: types.ConnectorTypeNextcloud}
	handler := &ChunkHandler{}

	t.Setenv("WEKNORA_NEXTCLOUD_READ_AUTHORITY", "")
	_, err := handler.beginNextcloudRead(context.Background(), nil, knowledge)
	require.Error(t, err)

	t.Setenv("WEKNORA_NEXTCLOUD_READ_AUTHORITY", "weknora")
	leases, err := handler.beginNextcloudRead(context.Background(), nil, knowledge)
	require.NoError(t, err)
	require.Nil(t, leases)
}
