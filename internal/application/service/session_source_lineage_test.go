package service

import (
	"context"
	"encoding/json"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSessionTextLineageReadKeepsPrivateRawAndNeverRecertifiesUnknown(t *testing.T) {
	empty, _ := types.NewCompleteSourceLineage()
	svc := &sessionService{}
	for _, proof := range []*types.SourceLineage{nil, types.UnknownSourceLineage(), empty} {
		row := &types.Session{Title: "raw title", Description: "raw description", TitleSourceLineage: proof, DescriptionSourceLineage: proof}
		visible, err := svc.authorizeSessionText(context.Background(), row)
		require.NoError(t, err)
		payload, err := json.Marshal(visible)
		require.NoError(t, err)
		if proof == empty {
			require.Contains(t, string(payload), "raw title")
		} else {
			require.Empty(t, visible.Title)
			require.Empty(t, visible.Description)
			require.NotContains(t, string(payload), "raw title")
		}
		require.Equal(t, "raw title", row.Title, "private repository object was mutated by read redaction")
	}
}
