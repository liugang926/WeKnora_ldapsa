package agent

import (
	"context"
	"encoding/json"
	"testing"

	agenttools "github.com/Tencent/WeKnora/internal/agent/tools"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestAgentLineageDuringAuthorizationMutationCannotChangeActualWireSnapshot(t *testing.T) {
	for _, mutation := range []string{"description", "type", "shared schema bytes", "image pointer", "reasoning metadata", "literal positive"} {
		t.Run(mutation, func(t *testing.T) {
			model := &mockChat{responses: []mockResponse{lineageAnswer("safe response")}}
			e := newTestEngine(t, model)
			e.toolRegistry = agenttools.NewToolRegistry()
			tool := agenttools.NewSequentialThinkingTool()
			e.toolRegistry.RegisterTool(tool)
			opts := &chat.ChatOptions{
				Tools:          e.buildToolsForLLM(),
				PromptCacheKey: "server-session",
				CacheRetention: chat.CacheRetentionNone,
			}
			originalParameters := append(json.RawMessage(nil), tool.Parameters()...)
			t.Cleanup(func() { copy(tool.Parameters(), originalParameters) })
			trustedDescription := opts.Tools[0].Function.Description
			messages := []chat.Message{
				{
					Role:          "user",
					Content:       "trusted raw text",
					SourceLineage: independentAgentInput(),
					MultiContent: []chat.MessageContentPart{
						{Type: "image_url", ImageURL: &chat.ImageURL{URL: "trusted-image"}},
					},
					ReasoningMetadata: types.ProviderMetadata{"provider": json.RawMessage(`{"trusted":"original"}`)},
				},
			}
			mutated := false
			e.SetSourceLineageVerifier(func(_ context.Context, l *types.SourceLineage) error {
				if !mutated {
					mutated = true
					switch mutation {
					case "description":
						opts.Tools[0].Function.Description = "unknown source body"
					case "type":
						opts.Tools[0].Type = "unknown source body"
					case "shared schema bytes":
						opts.Tools[0].Function.Parameters[0] = ' '
					case "image pointer":
						messages[0].MultiContent[0].ImageURL.URL = "unknown-source-image"
					case "reasoning metadata":
						copy(messages[0].ReasoningMetadata["provider"], []byte(`{"trusted":"poisoned"}`))
					}
				}
				return l.RequireComplete()
			})
			_, err := e.streamLLMToEventBus(context.Background(), messages, opts, nil)
			require.NoError(t, err)
			require.Equal(t, 1, model.callCount)
			require.Equal(t, "function", model.opts[0].Tools[0].Type)
			require.Equal(t, trustedDescription, model.opts[0].Tools[0].Function.Description)
			require.Equal(t, originalParameters, model.opts[0].Tools[0].Function.Parameters)
			require.Equal(t, "server-session", model.opts[0].PromptCacheKey)
			require.Equal(t, chat.CacheRetentionNone, model.opts[0].CacheRetention)
			require.Equal(t, "trusted-image", model.calls[0][0].MultiContent[0].ImageURL.URL)
			require.JSONEq(t, `{"trusted":"original"}`, string(model.calls[0][0].ReasoningMetadata["provider"]))
		})
	}
}

func TestAgentLineageOpaqueFormatInputIsUnknownBeforeModel(t *testing.T) {
	model := &mockChat{responses: []mockResponse{lineageAnswer("must not call")}}
	e, _ := lineageEngine(t, model, nil, grantChecker(map[types.SourceIdentity]bool{}))
	_, err := e.streamLLMToEventBus(
		context.Background(),
		[]chat.Message{{Role: "user", Content: "raw human", SourceLineage: independentAgentInput()}},
		&chat.ChatOptions{Format: json.RawMessage(`{"description":"opaque source body"}`)},
		nil,
	)
	require.ErrorIs(t, err, ErrAgentSourceLineageDenied)
	require.Zero(t, model.callCount)
}

func TestAgentLineageUnsupportedProviderArtifactCannotSilentlyBecomeEmpty(t *testing.T) {
	model := &mockChat{responses: []mockResponse{lineageAnswer("must not call")}}
	e, _ := lineageEngine(t, model, nil, grantChecker(map[types.SourceIdentity]bool{}))
	_, err := e.streamLLMToEventBus(
		context.Background(),
		[]chat.Message{
			{
				Role:              "assistant",
				Content:           "source free claimed",
				SourceLineage:     independentAgentInput(),
				ReasoningMetadata: types.ProviderMetadata{"provider": json.RawMessage(`{"opaque_source_body":`)},
			},
		},
		nil,
		nil,
	)
	require.ErrorIs(t, err, ErrAgentSourceLineageDenied)
	require.Zero(t, model.callCount)
	require.Error(t, e.SourceLineageSnapshot().RequireComplete())
}
