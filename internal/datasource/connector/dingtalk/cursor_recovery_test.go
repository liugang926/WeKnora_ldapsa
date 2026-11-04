package dingtalk

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestCursorRecoveryOnlyRefetchesUnconfirmedLocalDeletion(t *testing.T) {
	for _, version := range []int{1, cursorVersion} {
		key := "resources"
		if version == 1 {
			key = "workspaces"
		}
		cursor := &types.SyncCursor{ConnectorCursor: map[string]interface{}{"version": version,
			key: map[string]map[string]string{"selected": {"still-remote": "unchanged-revision"}}}}
		local, err := CursorContainsAnyDocument(cursor, []string{"still-remote"})
		require.NoError(t, err)
		require.True(t, local)
		remote, err := CursorContainsAnyDocument(cursor, []string{"confirmed-removed"})
		require.NoError(t, err)
		require.False(t, remote, "a remote tombstone must not force permanent full body fetches")
	}
	_, err := CursorContainsAnyDocument(&types.SyncCursor{ConnectorCursor: map[string]interface{}{"version": -1}}, []string{"doc"})
	require.Error(t, err)
}
