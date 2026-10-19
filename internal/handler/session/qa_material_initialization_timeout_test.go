package session

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	applicationservice "github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/storageurl"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestLiveSSEProducerErrorInitializationTimeoutNeverWritesUnadmittedControl(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &Handler{
		messageService: &liveHistoryMessageService{},
		streamManager: &stagedLiveStream{
			events: []interfaces.StreamEvent{{Type: types.ResponseTypeAgentQuery, Content: "UNADMITTED QUERY SECRET"}},
		},
	}
	message := &types.Message{Role: "assistant", RequestID: "request"}
	router := gin.New()
	router.GET("/stream", func(c *gin.Context) {
		ctx, attach, check, closeRead := applicationservice.BeginMessageMaterialHTTPBoundary(c.Request.Context())
		defer closeRead()
		require.ErrorIs(t, check(), applicationservice.ErrMessageMaterialHTTPInitializing)
		phase := &liveMaterialWritePhase{
			MaterialCheck:          check,
			InitializationDeadline: time.Now().Add(150 * time.Millisecond),
		}
		ctx = context.WithValue(attach(ctx), liveMaterialWritePhaseKey{}, phase)
		c.Writer = &nativeSavedReplayWriter{ResponseWriter: c.Writer, ctx: ctx, check: check}
		rewriter := storageurl.NewStreamRewriter(storageurl.NewRequestRewriter(ctx, storageurl.ModeHandle, nil, nil))
		h.handleAgentEventsForSSE(
			ctx,
			c,
			"session",
			"assistant",
			"request",
			event.NewEventBus(),
			false,
			rewriter,
			message,
		)
	})
	server := httptest.NewServer(router)
	defer server.Close()
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get(server.URL + "/stream")
	require.NoError(t, err)
	defer func() { require.NoError(t, response.Body.Close()) }()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), `"response_type":"error"`)
	require.Contains(t, string(body), `"done":true`)
	require.NotContains(t, string(body), "UNADMITTED")
	require.NotContains(t, string(body), `"response_type":"agent_query"`)
	require.False(t, message.IsCompleted)
}
