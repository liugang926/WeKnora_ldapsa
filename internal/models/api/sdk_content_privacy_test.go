package api_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Tencent/WeKnora/internal/logprivacy"
	"github.com/Tencent/WeKnora/internal/models/api"
	"github.com/Tencent/WeKnora/internal/models/api/anthropicmessages"
	"github.com/Tencent/WeKnora/internal/models/api/googlegenai"
	"github.com/Tencent/WeKnora/internal/models/api/openaicompletions"
	"github.com/Tencent/WeKnora/internal/models/api/openairesponses"
	"github.com/Tencent/WeKnora/internal/models/catalog"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

type privacySDKChat interface {
	Chat(context.Context, []api.Message, *api.Options) (*types.ChatResponse, error)
	ChatStream(context.Context, []api.Message, *api.Options) (<-chan types.StreamResponse, error)
}

type privacySDKWire struct {
	name      string
	newClient func(api.Endpoint) privacySDKChat
	response  string
	stream    string
}

func sourcePrivacySDKWires() []privacySDKWire {
	return []privacySDKWire{
		{
			name: "openaicompletions",
			newClient: func(endpoint api.Endpoint) privacySDKChat {
				return openaicompletions.New(openaicompletions.Config{Endpoint: endpoint, Settings: catalog.DefaultOpenAICompletions()})
			},
			response: `{"choices":[{"index":0,"message":{"role":"assistant","content":"LABEL_RESPONSE","reasoning_content":"LABEL_REASONING"},"finish_reason":"stop"}]}`,
			stream:   "data: " + `{"choices":[{"index":0,"delta":{"reasoning_content":"LABEL_REASONING","content":"LABEL_RESPONSE","tool_calls":[{"index":0,"id":"LABEL_TOOL_ID","type":"function","function":{"name":"LABEL_TOOL_NAME","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}` + "\n\ndata: [DONE]\n\n",
		},
		{
			name: "openairesponses",
			newClient: func(endpoint api.Endpoint) privacySDKChat {
				return openairesponses.New(openairesponses.Config{Endpoint: endpoint, Settings: catalog.DefaultOpenAIResponses()})
			},
			response: `{"id":"controlled-response","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"LABEL_RESPONSE"}]}]}`,
			stream: "data: " + `{"type":"response.reasoning_summary_text.delta","item_id":"reasoning","output_index":0,"summary_index":0,"delta":"LABEL_REASONING"}` + "\n\n" +
				"data: " + `{"type":"response.output_text.delta","item_id":"message","output_index":1,"delta":"LABEL_RESPONSE"}` + "\n\n" +
				"data: " + `{"type":"response.output_item.added","output_index":2,"item":{"type":"function_call","id":"function","call_id":"LABEL_TOOL_ID","name":"LABEL_TOOL_NAME","arguments":""}}` + "\n\n" +
				"data: " + `{"type":"response.output_item.done","output_index":2,"item":{"type":"function_call","id":"function","call_id":"LABEL_TOOL_ID","name":"LABEL_TOOL_NAME","arguments":"{}","status":"completed"}}` + "\n\n" +
				"data: " + `{"type":"response.completed","response":{"id":"controlled-response","status":"completed","output":[]}}` + "\n\n",
		},
		{
			name: "anthropicmessages",
			newClient: func(endpoint api.Endpoint) privacySDKChat {
				return anthropicmessages.New(anthropicmessages.Config{Endpoint: endpoint, Settings: catalog.DefaultAnthropicMessages()})
			},
			response: `{"id":"controlled-response","type":"message","role":"assistant","content":[{"type":"text","text":"LABEL_RESPONSE"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`,
			stream: "data: " + `{"type":"message_start","message":{"usage":{"input_tokens":1,"output_tokens":0}}}` + "\n\n" +
				"data: " + `{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}` + "\n\n" +
				"data: " + `{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"LABEL_REASONING"}}` + "\n\n" +
				"data: " + `{"type":"content_block_stop","index":0}` + "\n\n" +
				"data: " + `{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}` + "\n\n" +
				"data: " + `{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"LABEL_RESPONSE"}}` + "\n\n" +
				"data: " + `{"type":"content_block_stop","index":1}` + "\n\n" +
				"data: " + `{"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"LABEL_TOOL_ID","name":"LABEL_TOOL_NAME","input":{}}}` + "\n\n" +
				"data: " + `{"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{}"}}` + "\n\n" +
				"data: " + `{"type":"content_block_stop","index":2}` + "\n\n" +
				"data: " + `{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}}` + "\n\n" +
				"data: " + `{"type":"message_stop"}` + "\n\n",
		},
		{
			name: "googlegenai",
			newClient: func(endpoint api.Endpoint) privacySDKChat {
				return googlegenai.New(googlegenai.Config{Endpoint: endpoint, Settings: catalog.DefaultGoogleGenerativeAI()})
			},
			response: `{"candidates":[{"content":{"role":"model","parts":[{"text":"LABEL_RESPONSE"}]},"finishReason":"STOP"}]}`,
			stream:   "data: " + `{"candidates":[{"content":{"role":"model","parts":[{"text":"LABEL_REASONING","thought":true},{"text":"LABEL_RESPONSE"},{"functionCall":{"id":"LABEL_TOOL_ID","name":"LABEL_TOOL_NAME","args":{}}}]},"finishReason":"STOP"}]}` + "\n\n",
		},
	}
}

func TestSourceContentSDKActualHTTPStreamAndNonstream(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	for _, wire := range sourcePrivacySDKWires() {
		t.Run(wire.name, func(t *testing.T) {
			var mu sync.Mutex
			var requests []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				requests = append(requests, string(body))
				mu.Unlock()
				label := "PUBLIC_ORDINARY"
				if strings.Contains(string(body), "PRIVATE_SOURCE") {
					label = "PRIVATE_SOURCE"
				}
				response := wire.response
				if r.Header.Get("Accept") == "text/event-stream" {
					w.Header().Set("Content-Type", "text/event-stream")
					response = wire.stream
				} else {
					w.Header().Set("Content-Type", "application/json")
				}
				_, _ = w.Write([]byte(strings.ReplaceAll(response, "LABEL", label)))
			}))
			defer server.Close()
			client := wire.newClient(api.Endpoint{URL: server.URL, Model: "controlled-model", Auth: api.BearerAuth("owned-sdk-fixture"), Client: server.Client()})
			for _, mode := range []string{"private-marker", "private-lineage", "ordinary"} {
				t.Run(mode, func(t *testing.T) {
					var logs bytes.Buffer
					log := logrus.New()
					log.SetOutput(&logs)
					log.SetLevel(logrus.DebugLevel)
					ctx := context.WithValue(context.Background(), types.LoggerContextKey, logrus.NewEntry(log))
					label := "PUBLIC_ORDINARY"
					if mode != "ordinary" {
						label = "PRIVATE_SOURCE"
						if mode == "private-marker" {
							ctx = logprivacy.SuppressContent(ctx)
						} else {
							ctx = types.WithRAGSourceLineage(ctx, types.NewRAGSourceLineage(nil))
						}
					}
					dir := filepath.Join(t.TempDir(), "raw-not-created")
					t.Setenv("WEKNORA_LLM_STREAM_RAW_DUMP_DIR", dir)
					messages := []api.Message{{Role: "user", Content: label + "_PROMPT"}}
					response, err := client.Chat(ctx, messages, nil)
					require.NoError(t, err)
					require.Equal(t, label+"_RESPONSE", response.Content)
					stream, err := client.ChatStream(ctx, messages, nil)
					require.NoError(t, err)
					var answer, reasoning strings.Builder
					var sawTool bool
					for chunk := range stream {
						require.NotEqual(t, types.ResponseTypeError, chunk.ResponseType)
						if chunk.ResponseType == types.ResponseTypeAnswer {
							answer.WriteString(chunk.Content)
						}
						if chunk.ResponseType == types.ResponseTypeThinking {
							reasoning.WriteString(chunk.Content)
						}
						for _, tool := range chunk.ToolCalls {
							if tool.Function.Name == label+"_TOOL_NAME" {
								sawTool = true
							}
						}
					}
					require.Equal(t, label+"_RESPONSE", answer.String())
					require.Equal(t, label+"_REASONING", reasoning.String())
					require.True(t, sawTool, "protected logging must preserve model tool output")
					mu.Lock()
					current := append([]string(nil), requests[len(requests)-2:]...)
					mu.Unlock()
					for _, request := range current {
						require.Contains(t, request, label+"_PROMPT", "privacy affects diagnostics, never outbound model input")
					}
					if mode != "ordinary" {
						require.NotContains(t, logs.String(), "PRIVATE_SOURCE")
						require.Contains(t, logs.String(), "content logging suppressed")
						require.NoDirExists(t, dir)
					} else {
						require.Contains(t, logs.String(), "PUBLIC_ORDINARY_PROMPT")
						require.Contains(t, logs.String(), "PUBLIC_ORDINARY_RESPONSE")
						files, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
						require.NoError(t, err)
						require.Len(t, files, 1)
						data, err := os.ReadFile(files[0])
						require.NoError(t, err)
						require.Contains(t, string(data), "PUBLIC_ORDINARY_PROMPT")
						require.Contains(t, string(data), "PUBLIC_ORDINARY_RESPONSE")
					}
				})
			}
		})
	}
}
