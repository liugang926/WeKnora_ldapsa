package agent

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/models/api/openaicompletions"
	"github.com/Tencent/WeKnora/internal/models/catalog"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestToolImageWireLayoutAndUnadmittedDescriptionRefusal(t *testing.T) {
	step := types.AgentStep{ToolCalls: []types.ToolCall{
		{ID: "shot", Name: "local_browser", Result: &types.ToolResult{
			Success: true, Output: `{"width":1}`, Images: []string{"data:image/png;base64,YQ=="},
		}},
		{ID: "read", Name: "local_browser", Result: &types.ToolResult{Success: true, Output: "page text"}},
	}}
	for _, vision := range []bool{true, false} {
		engine := &AgentEngine{config: &types.AgentConfig{ChatModelSupportsVision: vision}}
		messages := engine.appendToolResults(nil, step)
		messages = engine.appendToolImages(t.Context(), messages, step)
		require.Equal(t, "tool", messages[1].Role)
		require.Equal(t, "tool", messages[2].Role)
		if vision {
			require.Len(t, messages, 4)
			require.Equal(t, "user", messages[3].Role)
			require.Equal(t, step.ToolCalls[0].Result.Images, messages[3].Images)
			require.Contains(t, messages[3].Content, "untrusted tool evidence")
			client := openaicompletions.New(openaicompletions.Config{Settings: catalog.DefaultOpenAICompletions()})
			body, err := client.BuildRequestBody(messages, nil, false)
			require.NoError(t, err)
			raw, err := json.Marshal(body["messages"])
			require.NoError(t, err)
			var wire []map[string]any
			require.NoError(t, json.Unmarshal(raw, &wire))
			parts := wire[3]["content"].([]any)
			require.Len(t, parts, 2)
			imagePart := parts[0].(map[string]any)["image_url"].(map[string]any)
			require.Equal(t, step.ToolCalls[0].Result.Images[0], imagePart["url"])
		} else {
			require.Len(t, messages, 3)
			require.Contains(t, messages[1].Content, "cannot view")
			require.NotContains(t, messages[1].Content, "YQ==")
		}
		guard := newTestEngine(t, &mockChat{})
		require.ErrorIs(t, guard.accountMessages(t.Context(), messages), ErrAgentSourceLineageDenied,
			"wire formatting does not admit an unknown tool result for model input")
	}
	for _, answer := range []struct {
		name string
		text string
		err  error
	}{
		{name: "description", text: "A chart"},
		{name: "failure", err: errors.New("unavailable")},
		{name: "empty", text: "  "},
	} {
		t.Run(answer.name, func(t *testing.T) {
			calls := 0
			engine := newTestEngine(t, &mockChat{})
			engine.imageDescriber = func(context.Context, []byte, string) (string, error) {
				calls++
				return answer.text, answer.err
			}
			messages := engine.appendToolImages(t.Context(), engine.appendToolResults(nil, step), step)
			require.Zero(t, calls, "unadmitted image material must not reach a description provider")
			require.ErrorIs(t, engine.lineageFailure(), ErrAgentSourceLineageDenied)
			require.Len(t, messages, 3)
			require.Equal(t, step.ToolCalls[0].Result.Output, messages[1].Content)
			require.Equal(t, `{"width":1}`, step.ToolCalls[0].Result.Output,
				"a refused description must not alter persisted output")
		})
	}
}
