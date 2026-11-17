package chatpipeline

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/models/api"
	"github.com/Tencent/WeKnora/internal/models/api/openaicompletions"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// Exercise the real protocol reader: OpenAI returns usage in a separate
// choices:[] envelope after finish_reason and before the [DONE] sentinel.
func TestFinalAnswerUsageAfterProtocolFinish(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	for _, tc := range []struct {
		name    string
		trailer string
		want    *types.TokenUsage
	}{
		{"counted", `{"prompt_tokens":8,"completion_tokens":3,"total_tokens":11,` +
			`"prompt_tokens_details":{"cached_tokens":4}}`,
			&types.TokenUsage{PromptTokens: 8, CompletionTokens: 3, TotalTokens: 11}},
		{"reported zero", `{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}`,
			&types.TokenUsage{ProviderReported: true}},
		{"missing counters", `{}`, &types.TokenUsage{}},
		{"unreported", "", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			releaseSentinel := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				const finish = `{"choices":[{"delta":{"content":"answer"},"finish_reason":"stop"}]}`
				_, _ = io.WriteString(w, "data: "+finish+"\n\n")
				if tc.trailer != "" {
					_, _ = io.WriteString(w, "data: "+`{"choices":[],"usage":`+tc.trailer+"}\n\n")
				}
				if tc.want != nil && (tc.want.TotalTokens > 0 || tc.want.ProviderReported) {
					// The usage arrived, but the proxy holds the sentinel until
					// the client has finished. Do not rely on a wall-clock race.
					w.(http.Flusher).Flush()
					select {
					case <-r.Context().Done():
						return
					case <-releaseSentinel:
					}
				}
				_, _ = io.WriteString(w, "data: [DONE]\n\n")
			}))
			defer server.Close()
			defer close(releaseSentinel)
			model := openaicompletions.New(openaicompletions.Config{
				Endpoint: api.Endpoint{BaseURL: server.URL + "/v1", Model: "test"},
				Settings: api.DefaultOpenAICompletions(),
			})
			bus := &syncEventBus{}
			turn := &types.ChatManage{}
			turn.UserContent = "question"
			turn.EventBus = bus
			plugin := &PluginChatCompletionStream{modelService: &stubModelService{model: model}}
			next := func() *PluginError { return nil }
			require.Nil(t, plugin.OnEvent(context.Background(), types.CHAT_COMPLETION_STREAM, turn, next))
			require.Eventually(t, func() bool {
				events := bus.finalAnswerEvents()
				return len(events) > 0 && events[len(events)-1].Done
			}, 2*time.Second, 5*time.Millisecond)
			events := bus.finalAnswerEvents()
			require.Len(t, events, 2)
			require.NotEmpty(t, events[0].Content)
			require.Equal(t, "answer", events[0].Content+events[1].Content)
			require.False(t, events[0].Done)
			require.Nil(t, events[0].Usage)
			got := events[1].Usage
			if tc.want == nil {
				require.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			require.Equal(t, tc.want.PromptTokens, got.PromptTokens)
			require.Equal(t, tc.want.CompletionTokens, got.CompletionTokens)
			require.Equal(t, tc.want.TotalTokens, got.TotalTokens)
			require.Equal(t, tc.want.ProviderReported, got.ProviderReported)
			if got.TotalTokens > 0 {
				require.Equal(t, 4, got.CacheReadTokens)
			}
		})
	}
}
