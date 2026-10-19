package session

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	applicationservice "github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/storageurl"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type terminalMaterialStream struct {
	interfaces.StreamManager
	mu     sync.Mutex
	events []interfaces.StreamEvent
	scopes [][2]string
}

func (s *terminalMaterialStream) AppendEvent(
	_ context.Context,
	session, message string,
	frame interfaces.StreamEvent,
) error {
	// Use the transport's JSON shape, retaining no caller-owned event maps.
	raw, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	var copied interfaces.StreamEvent
	if err := json.Unmarshal(raw, &copied); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, copied)
	s.scopes = append(s.scopes, [2]string{session, message})
	return nil
}

func (s *terminalMaterialStream) GetEvents(
	_ context.Context,
	_, _ string,
	offset int,
) ([]interfaces.StreamEvent, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]interfaces.StreamEvent(nil), s.events[offset:]...), len(s.events), nil
}

func TestAgentStreamHandlerInternalErrorRejectsForeignRunScope(t *testing.T) {
	streams := &terminalMaterialStream{}
	bus := event.NewEventBus()
	message := &types.Message{ID: "assistant", SessionID: "session", RequestID: "request", Role: "assistant"}
	producer := NewAgentStreamHandler(
		context.Background(),
		"session",
		"assistant",
		"request",
		7,
		time.Now(),
		message,
		streams,
		bus,
		nil,
		nil,
		nil,
	)
	producer.Subscribe()
	for _, control := range [][2]string{{"foreign-session", "request"}, {"session", "foreign-request"}} {
		require.ErrorIs(
			t,
			bus.Emit(
				context.Background(),
				event.Event{
					Type:      event.EventError,
					SessionID: control[0],
					RequestID: control[1],
					Data:      event.ErrorData{Error: "foreign private error"},
				},
			),
			access.ErrNextcloudPublicationDenied,
		)
	}
	require.Empty(t, streams.events)
	require.False(t, message.IsCompleted)
}

func TestLiveSSEProducerErrorWithRefusedOriginalMaterialHolder(t *testing.T) {
	for _, condition := range []string{"missing_original", "unknown_prefix", "source_revoked"} {
		t.Run(condition, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			streams := &terminalMaterialStream{}
			message := &types.Message{ID: "assistant", SessionID: "session", RequestID: "request", Role: "assistant"}
			service := &liveHistoryMessageService{}
			if condition == "source_revoked" {
				service.denyAt = 1
			}
			h := &Handler{messageService: service, streamManager: streams}
			producerResult := make(chan error, 1)
			r := gin.New()
			r.GET("/stream", func(c *gin.Context) {
				// This is the actual original-material owner used by executeQA,
				// not a check stub. It has no admitted originals and is not Ready.
				ctx, attach, check, closeRead := applicationservice.BeginMessageMaterialHTTPBoundary(
					c.Request.Context(),
				)
				defer closeRead()
				require.ErrorIs(t, check(), access.ErrNextcloudPublicationUnavailable)
				ctx = attach(ctx)
				c.Writer = &nativeSavedReplayWriter{ResponseWriter: c.Writer, ctx: ctx, check: check}
				bus := event.NewEventBus()
				producer := NewAgentStreamHandler(
					ctx,
					"session",
					"assistant",
					"request",
					7,
					time.Now(),
					message,
					streams,
					bus,
					nil,
					nil,
					nil,
				)
				producer.Subscribe()
				if condition == "unknown_prefix" {
					require.ErrorIs(
						t,
						bus.Emit(
							ctx,
							event.Event{
								Type:          event.EventAgentThought,
								SourceLineage: types.UnknownSourceLineage(),
								Data:          event.AgentThoughtData{Content: "private refused body"},
							},
						),
						access.ErrNextcloudPublicationDenied,
					)
				}
				if condition == "source_revoked" {
					require.ErrorIs(
						t,
						service.CheckLiveMessagePublication(ctx, message),
						access.ErrNextcloudPublicationDenied,
					)
				}
				// executeQA emits this event after the service fails, without a
				// generated-body receipt; do not fake assistant completion to close.
				producerResult <- bus.Emit(ctx, event.Event{ID: "private diagnostic event ID", Type: event.EventError, SessionID: "session", RequestID: "request", Data: event.ErrorData{Error: "private diagnostic token=SECRET", Stage: "private source stage"}})
				setSSEHeaders(c)
				rewriter := storageurl.NewStreamRewriter(
					storageurl.NewRequestRewriter(ctx, storageurl.ModePublic, nil, nil),
				)
				rewriter.Push(ctx, "held", "private held source ![", false, nil)
				h.handleAgentEventsForSSE(ctx, c, "session", "assistant", "request", bus, true, rewriter, message)
			})
			server := httptest.NewServer(r)
			defer server.Close()
			client := &http.Client{Timeout: 2 * time.Second}
			started := time.Now()
			response, err := client.Get(server.URL + "/stream")
			require.NoError(t, err, "original refusal must close via a static terminal, not client timeout")
			defer func() { require.NoError(t, response.Body.Close()) }()
			raw, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Less(t, time.Since(started), time.Second)
			require.NoError(t, <-producerResult)
			require.Contains(t, string(raw), `"response_type":"error"`)
			require.Contains(t, string(raw), `"done":true`)
			require.Contains(t, string(raw), `"error_code":"generation_failed"`)
			require.NotContains(t, string(raw), `"response_type":"complete"`)
			for _, forbidden := range []string{"SECRET", "private diagnostic", "private source stage", "private refused body", "private held source"} {
				require.NotContains(t, string(raw), forbidden)
			}
			require.False(t, message.IsCompleted)
			require.Empty(t, message.Content)
			require.Error(
				t,
				message.SourceLineage.RequireComplete(),
				"static control must not mint an assistant body origin",
			)
			streams.mu.Lock()
			defer streams.mu.Unlock()
			require.Len(t, streams.events, 1)
			require.Equal(t, [][2]string{{"session", "assistant"}}, streams.scopes)
			require.Equal(t, "request", streams.events[0].ID)
			stored, err := json.Marshal(streams.events[0])
			require.NoError(t, err)
			require.NotContains(t, string(stored), "SECRET")
			require.NotContains(t, string(stored), "private")
			require.Error(
				t,
				streams.events[0].SourceLineage.RequireComplete(),
				"the code-origin terminal is not source-body authority",
			)
		})
	}
}
