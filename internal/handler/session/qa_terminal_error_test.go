package session

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestLiveSSEProducerErrorClosesWithoutSuccessOrHeldSourceTail(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		t.Run(map[bool]string{false: "producer_refusal", true: "source_revoked"}[revoke], func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			service := &liveHistoryMessageService{}
			streams := &stagedLiveStream{}
			if revoke {
				service.denyAt = 1
				lineage, err := types.NewCompleteSourceLineage()
				require.NoError(t, err)
				streams.events = []interfaces.StreamEvent{{
					SourceLineage: lineage, ID: "diagnostic", Type: types.ResponseTypeError, Done: true,
					Content: "private response body token=SECRET",
					Data:    map[string]interface{}{"error": "private response body"},
				}}
			} else {
				ctx := context.Background()
				recorder := &capturingStreamManager{}
				producer := NewAgentStreamHandler(ctx, "session", "assistant", "request", 1, time.Now(),
					&types.Message{}, recorder, nil, nil, nil, nil)
				require.NoError(t, producer.handleError(ctx, event.Event{
					Type: event.EventError, SessionID: "session", RequestID: "request",
					Data: event.ErrorData{Error: "private response body token=SECRET"},
				}))
				require.Len(t, recorder.events, 1)
				streams.events = recorder.events
			}
			h := &Handler{messageService: service, streamManager: streams}
			r := gin.New()
			r.GET("/stream", func(c *gin.Context) {
				ctx := c.Request.Context()
				rewriter := publicStreamRewriter()
				rewriter.Push(ctx, "answer:held", "private held source ![", false, nil)
				h.handleAgentEventsForSSE(
					ctx,
					c,
					"session",
					"assistant",
					"request",
					event.NewEventBus(),
					true,
					rewriter,
					&types.Message{Role: "assistant", RequestID: "request"},
				)
			})
			server := httptest.NewServer(r)
			defer server.Close()
			client := &http.Client{Timeout: 2 * time.Second}
			started := time.Now()
			response, err := client.Get(server.URL + "/stream")
			require.NoError(t, err)
			defer func() { require.NoError(t, response.Body.Close()) }()
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err, "real HTTP body must end, not wait for client timeout")
			require.Less(t, time.Since(started), time.Second)
			text := string(body)
			require.Contains(t, text, `"done":true`)
			require.Equal(t, 1, strings.Count(text, `"response_type":"error"`))
			require.NotContains(t, text, "SECRET")
			require.NotContains(t, text, "private response body")
			require.NotContains(t, text, "private held source")
			require.NotContains(t, text, `"response_type":"complete"`)
			if revoke {
				require.Positive(t, service.checks)
			} else {
				require.Contains(t, text, "generation_failed")
			}
		})
	}
}
