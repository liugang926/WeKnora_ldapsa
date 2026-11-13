package session

import (
	"bufio"
	"context"
	"encoding/json"
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

func TestLiveDeltaBoundaries(t *testing.T) {
	lineage := controlledLiveSSEFixtureLineage(t, "first")
	first := interfaces.StreamEvent{
		ID: "segment", Type: types.ResponseTypeAnswer, Content: "a", SourceLineage: lineage,
		Data: map[string]interface{}{"event_id": "segment"},
	}
	for name, change := range map[string]func(*interfaces.StreamEvent){
		"done":      func(e *interfaces.StreamEvent) { e.Done = true },
		"thinking":  func(e *interfaces.StreamEvent) { e.Type = types.ResponseTypeThinking },
		"stop":      func(e *interfaces.StreamEvent) { e.Type = types.ResponseType(event.EventStop) },
		"anotherID": func(e *interfaces.StreamEvent) { e.ID = "new" },
		"metadata": func(e *interfaces.StreamEvent) {
			e.Data = map[string]interface{}{"event_id": "segment", "truncated": true}
		},
		"usage":   func(e *interfaces.StreamEvent) { e.Usage = &types.TokenUsage{} },
		"unknown": func(e *interfaces.StreamEvent) { e.SourceLineage = nil },
		"revision": func(e *interfaces.StreamEvent) {
			changed := lineage.Sources[0]
			changed.Revision++
			var err error
			e.SourceLineage, err = types.NewCompleteSourceLineage(changed)
			require.NoError(t, err)
		},
		"splitUTF8": func(e *interfaces.StreamEvent) { e.Content = string([]byte{0xff}) },
		"limit":     func(e *interfaces.StreamEvent) { e.Content = strings.Repeat("b", maxLiveDeltaBytes) },
	} {
		t.Run(name, func(t *testing.T) {
			second := first
			change(&second)
			got, consumed := nextLiveFrame([]interfaces.StreamEvent{first, second, first})
			require.Equal(t, 1, consumed)
			require.Equal(t, first, got)
		})
	}
}

type liveDeltaRecorder struct {
	*httptest.ResponseRecorder
	flushes int
}

func (w *liveDeltaRecorder) Flush() {
	w.flushes++
	w.ResponseRecorder.Flush()
}

func TestLiveDeltaBacklogWritesAndRevocation(t *testing.T) {
	lineage := controlledLiveSSEFixtureLineage(t, "document")
	events := make([]interfaces.StreamEvent, 0, 258)
	for range 256 {
		events = append(events, interfaces.StreamEvent{
			ID: "answer", Data: map[string]interface{}{"event_id": "answer"}, Type: types.ResponseTypeAnswer,
			Content: "12345678", SourceLineage: lineage,
		})
	}
	events = append(events,
		interfaces.StreamEvent{ID: "answer", Type: types.ResponseTypeAnswer, Done: true, SourceLineage: lineage},
		interfaces.StreamEvent{ID: "complete", Type: types.ResponseTypeComplete, Done: true, SourceLineage: lineage},
	)
	for _, tc := range []struct {
		name       string
		denyAt     int
		textDeltas int
		flushes    int
		lastType   types.ResponseType
		flagged    bool
	}{
		{"allowed", 0, 256, 4, types.ResponseTypeComplete, false},
		{"with_fallback_metadata", 0, 256, 258, types.ResponseTypeComplete, true},
		{"withdrawn_before_second_write", 2, 128, 2, types.ResponseTypeError, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pending := append([]interfaces.StreamEvent(nil), events...)
			if tc.flagged {
				for i := range pending[:256] {
					pending[i].Data = map[string]interface{}{"event_id": "answer", "is_fallback": true}
				}
			}
			svc := &liveHistoryMessageService{denyAt: tc.denyAt, expectedLineage: lineage}
			h := &Handler{messageService: svc, streamManager: &stubStreamManager{events: pending}}
			recorder := &liveDeltaRecorder{ResponseRecorder: httptest.NewRecorder()}
			c, _ := gin.CreateTestContext(recorder)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c.Request = httptest.NewRequest(http.MethodPost, "/knowledge-chat/session", nil).WithContext(ctx)
			h.handleAgentEventsForSSE(ctx, c, "session", "message", "turn", event.NewEventBus(), false,
				nil, &types.Message{Role: "assistant", RequestID: "turn"})
			var text strings.Builder
			var last types.ResponseType
			scanner := bufio.NewScanner(recorder.Body)
			for scanner.Scan() {
				line, ok := strings.CutPrefix(scanner.Text(), "data:")
				if !ok {
					continue
				}
				var frame types.StreamResponse
				require.NoError(t, json.Unmarshal([]byte(line), &frame))
				if frame.ResponseType == types.ResponseTypeAnswer {
					require.LessOrEqual(t, len(frame.Content), maxLiveDeltaBytes)
					text.WriteString(frame.Content)
				}
				last = frame.ResponseType
			}
			require.NoError(t, scanner.Err())
			require.Equal(t, strings.Repeat("12345678", tc.textDeltas), text.String())
			require.Equal(t, tc.lastType, last)
			require.Equal(t, tc.flushes, recorder.flushes)
			require.Equal(t, "12345678", pending[0].Content, "reconnect must read the original deltas")
		})
	}
}
