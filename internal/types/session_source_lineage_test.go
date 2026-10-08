package types

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSessionTextLineageDefaultSerializationRedactsRawRepositoryText(t *testing.T) {
	empty, _ := NewCompleteSourceLineage()
	for _, proof := range []*SourceLineage{nil, UnknownSourceLineage(), empty} {
		row := Session{ID: "session", Title: "private title", Description: "private description", TitleSourceLineage: proof, DescriptionSourceLineage: proof}
		payload, err := json.Marshal(row)
		require.NoError(t, err)
		require.NotContains(t, string(payload), "private title")
		require.NotContains(t, string(payload), "private description")
		require.NotContains(t, string(payload), "source_lineage")
	}
	row := Session{ID: "session", Title: "authorized", TitleSourceLineage: empty, TitleAuthorized: true}
	payload, err := json.Marshal(SessionListItem{Session: row, IMPlatform: "feishu"})
	require.NoError(t, err)
	require.Contains(t, string(payload), `"title":"authorized"`)
	require.Contains(t, string(payload), `"im_platform":"feishu"`)
	row.TitleSourceLineage = nil
	payload, err = json.Marshal(row)
	require.NoError(t, err)
	require.NotContains(t, string(payload), "authorized")
}
