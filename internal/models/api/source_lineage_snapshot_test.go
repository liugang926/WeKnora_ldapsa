package api

import (
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestRAGSourceLineageWireSnapshotDetachesNestedValues(t *testing.T) {
	proof, _ := types.NewCompleteSourceLineage()
	messages := []Message{
		{
			Role:              "user",
			Content:           "original",
			Images:            []string{"image"},
			ReasoningMetadata: types.ProviderMetadata{"nested": json.RawMessage(`{"value":"original"}`)},
			SourceLineage:     proof,
			Kind:              MessageKindCompactionSummary,
			TurnID:            "turn",
		},
	}
	thinking := true
	options := &Options{
		Thinking: &thinking,
		Format:   json.RawMessage(`{"schema":"original"}`),
		Tools: []Tool{
			{Type: "function", Function: FunctionDef{Name: "tool", Parameters: json.RawMessage(`{"original":true}`)}},
		},
	}
	frozenMessages, err := FreezeSourceModelMessages(messages)
	require.NoError(t, err)
	frozenOptions, err := FreezeSourceModelOptions(options)
	require.NoError(t, err)
	messages[0].Images[0] = "changed"
	messages[0].ReasoningMetadata["nested"][2] = 'X'
	thinking = false
	options.Format[2] = 'X'
	options.Tools[0].Function.Parameters[2] = 'X'
	require.Equal(t, "image", frozenMessages[0].Images[0])
	require.JSONEq(t, `{"value":"original"}`, string(frozenMessages[0].ReasoningMetadata["nested"]))
	require.True(t, *frozenOptions.Thinking)
	require.JSONEq(t, `{"schema":"original"}`, string(frozenOptions.Format))
	require.JSONEq(t, `{"original":true}`, string(frozenOptions.Tools[0].Function.Parameters))
	require.Equal(t, proof, frozenMessages[0].SourceLineage)
	require.Equal(t, "turn", frozenMessages[0].TurnID)
	require.Equal(t, MessageKindCompactionSummary, frozenMessages[0].Kind)
	payload, err := json.Marshal(frozenMessages)
	require.NoError(t, err)
	require.NotContains(t, string(payload), "source_lineage")
}
