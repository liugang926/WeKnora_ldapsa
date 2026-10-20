package chat

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Tencent/WeKnora/internal/logprivacy"
	"github.com/Tencent/WeKnora/internal/models/utils/ollama"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestSourceContentOllamaActualHTTPStreamErrorKeepsCallerAndSuppressesLog(t *testing.T) {
	for _, mode := range []string{"private-marker", "private-lineage", "ordinary"} {
		t.Run(mode, func(t *testing.T) {
			label := "PUBLIC_ORDINARY"
			if mode != "ordinary" {
				label = "PRIVATE_SOURCE"
			}
			var mu sync.Mutex
			var requestBody []byte
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/":
					w.WriteHeader(http.StatusOK)
				case "/api/tags":
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"models":[{"name":"controlled:latest","model":"controlled:latest"}]}`))
				case "/api/chat":
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					requestBody = body
					mu.Unlock()
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(`{"error":"` + label + `_PROVIDER_ERROR"}`))
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			t.Setenv("OLLAMA_BASE_URL", server.URL)
			service, err := ollama.GetOllamaService()
			require.NoError(t, err)
			client, err := NewOllamaChat(&ChatConfig{ModelName: "controlled", ModelID: "controlled"}, service)
			require.NoError(t, err)
			var logs bytes.Buffer
			log := logrus.New()
			log.SetOutput(&logs)
			log.SetLevel(logrus.DebugLevel)
			ctx := context.WithValue(context.Background(), types.LoggerContextKey, logrus.NewEntry(log))
			if mode == "private-marker" {
				ctx = logprivacy.SuppressContent(ctx)
			}
			if mode == "private-lineage" {
				ctx = types.WithRAGSourceLineage(ctx, types.NewRAGSourceLineage(nil))
			}
			stream, err := client.ChatStream(ctx, []Message{{Role: "user", Content: label + "_PROMPT"}}, nil)
			require.NoError(t, err, "native Ollama reports this HTTP error through the stream")
			var last types.StreamResponse
			for response := range stream {
				last = response
			}
			require.Equal(t, types.ResponseTypeError, last.ResponseType)
			require.True(t, last.Done)
			require.Contains(
				t,
				last.Content,
				label+"_PROVIDER_ERROR",
				"the original native provider error still reaches the caller",
			)
			mu.Lock()
			body := append([]byte(nil), requestBody...)
			mu.Unlock()
			require.Contains(t, string(body), label+"_PROMPT")
			var request map[string]any
			require.NoError(t, json.Unmarshal(body, &request))
			require.Equal(t, true, request["stream"])
			require.Equal(t, "controlled", request["model"])
			if mode != "ordinary" {
				require.NotContains(t, logs.String(), "PRIVATE_SOURCE")
				require.Empty(t, logs.String(), "protected diagnostics never reach the caller's content logger")
			} else {
				require.True(
					t,
					strings.Contains(logs.String(), "PUBLIC_ORDINARY_PROVIDER_ERROR"),
					"ordinary error diagnostics remain available",
				)
			}
		})
	}
}
