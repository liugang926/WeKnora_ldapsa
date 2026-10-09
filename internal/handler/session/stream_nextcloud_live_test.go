package session

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	agenttools "github.com/Tencent/WeKnora/internal/agent/tools"
	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/storageurl"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
)

type stagedLiveStream struct {
	interfaces.StreamManager
	events []interfaces.StreamEvent
}

func (s *stagedLiveStream) GetEvents(_ context.Context, _, _ string, offset int) ([]interfaces.StreamEvent, int,
	error,
) {
	if offset >= len(s.events) {
		return nil, offset, nil
	}
	return s.events[offset : offset+1], offset + 1, nil
}

type liveHistoryMessageService struct {
	interfaces.MessageService
	checks      int
	denyAt      int
	checkedRefs []string
	checkedKBs  []string
}

func (s *liveHistoryMessageService) CheckLiveMessagePublication(_ context.Context, message *types.Message) error {
	s.checks++
	s.checkedKBs = append(s.checkedKBs, message.ExecutionContext.KnowledgeBaseIDs...)
	for _, ref := range message.KnowledgeReferences {
		if ref != nil {
			s.checkedRefs = append(s.checkedRefs, ref.KnowledgeID)
		}
	}
	if s.denyAt > 0 && s.checks >= s.denyAt {
		return access.ErrNextcloudPublicationDenied
	}
	return nil
}

func TestLiveSSEStopsBeforeRevokedNextChunk(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &liveHistoryMessageService{denyAt: 3}
	h := &Handler{messageService: svc, streamManager: &stagedLiveStream{events: []interfaces.StreamEvent{
		{ID: "refs", Type: types.ResponseTypeReferences, Data: map[string]interface{}{"references": types.References{{
			KnowledgeID: "nextcloud-document", KnowledgeChannel: types.ConnectorTypeNextcloud,
		}}}},
		{ID: "answer", Type: types.ResponseTypeAnswer, Content: "authorized first chunk"},
		{ID: "answer", Type: types.ResponseTypeAnswer, Content: "revoked secret second chunk"},
	}}}
	bus := event.NewEventBus()
	stops := 0
	bus.On(event.EventStop, func(context.Context, event.Event) error {
		stops++
		return nil
	})
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	requestCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c.Request = httptest.NewRequest(http.MethodPost, "/knowledge-chat/session", nil).WithContext(requestCtx)
	h.handleAgentEventsForSSE(requestCtx, c, "session", "assistant", "turn", bus, false,
		storageurl.NewStreamRewriter(storageurl.NewRequestRewriter(requestCtx, storageurl.ModeHandle, nil, nil)),
		&types.Message{Role: "assistant", RequestID: "turn"})
	body := recorder.Body.String()
	if !strings.Contains(body, "authorized first chunk") || strings.Contains(body, "revoked secret second chunk") ||
		!strings.Contains(body, "Current source access changed") {
		t.Fatalf("live SSE did not stop before revoked chunk: %s", body)
	}
	if stops != 1 || svc.checks != 3 {
		t.Fatalf("revocation did not cancel generation once: stops=%d checks=%d", stops, svc.checks)
	}
}

func TestLiveSSEChecksKnowledgeToolResultIdentity(t *testing.T) {
	svc := &liveHistoryMessageService{}
	message := &types.Message{Role: "assistant"}
	events := []interfaces.StreamEvent{{Type: types.ResponseTypeToolResult, Data: map[string]interface{}{
		"tool_name": "read_document", "knowledge_id": "nextcloud-document",
		"output": "source excerpt",
	}}}
	if err := checkLiveStreamPublication(context.Background(), svc, message, events); err != nil {
		t.Fatal(err)
	}
	if len(message.KnowledgeReferences) != 1 || message.KnowledgeReferences[0].KnowledgeID != "nextcloud-document" {
		t.Fatalf("knowledge tool provenance not retained: %+v", message.KnowledgeReferences)
	}
	if err := checkLiveStreamPublication(context.Background(), svc, &types.Message{Role: "assistant"},
		[]interfaces.StreamEvent{{Type: types.ResponseTypeToolResult, Data: map[string]interface{}{
			"tool_name": "read_document", "output": "opaque excerpt without identity",
		}}}); err == nil {
		t.Fatal("knowledge tool output without document identity must fail closed")
	}
}

// Use the actual producer and sanitizer, followed by StreamManager's JSON
// roundtrip, so empty arrays, counts and retained document IDs have their real
// live-event shape rather than a hand-built approximation.
func producedKnowledgeToolResult(t *testing.T, data event.AgentToolResultData) interfaces.StreamEvent {
	t.Helper()
	data.Data = agenttools.SanitizeToolDataForPersist(data.ToolName, data.Data)
	streams := &capturingStreamManager{}
	h := NewAgentStreamHandler(context.Background(), "session", "assistant", "turn", 7,
		time.Now(), &types.Message{}, streams, nil, nil, nil, nil)
	if err := h.handleToolResult(context.Background(), event.Event{
		ID: "tool-result", Type: event.EventAgentToolResult, Data: data,
	}); err != nil {
		t.Fatal(err)
	}
	if len(streams.events) != 1 {
		t.Fatalf("expected one producer event, got %d", len(streams.events))
	}
	encoded, err := json.Marshal(streams.events[0])
	if err != nil {
		t.Fatal(err)
	}
	var decoded interfaces.StreamEvent
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

func emptySearchToolData() map[string]interface{} {
	return map[string]interface{}{
		"display_type": "search_results", "results": []interface{}{}, "count": 0,
		"knowledge_base_ids": []string{"manual-kb"}, "query": "synthetic no-answer query",
		"queries": []string{"synthetic no-answer query"}, "mode": "hybrid", "requested_mode": "hybrid",
		"rerank_rejected": 14,
	}
}

func TestLiveSSEAcceptsProducerContentlessKnowledgeToolResults(t *testing.T) {
	for _, tc := range []struct {
		name string
		data event.AgentToolResultData
	}{
		{"empty_search", event.AgentToolResultData{
			ToolName: "search_knowledge", Success: true,
			Data: emptySearchToolData(),
		}},
		{"empty_list", event.AgentToolResultData{
			ToolName: "list_documents", Success: true,
			Data: map[string]interface{}{
				"display_type": "document_info", "documents": []map[string]interface{}{},
				"knowledge_base_id": "manual-kb", "total_docs": int64(0), "page": 1, "page_size": 20,
			},
		}},
		{"failed_search", event.AgentToolResultData{
			ToolName: "search_knowledge", Success: false,
			Error: "query is required",
		}},
		{"failed_list", event.AgentToolResultData{
			ToolName: "list_documents", Success: false,
			Error: "knowledge_base_id is required",
		}},
		{"failed_read", event.AgentToolResultData{ToolName: "read_document", Success: false, Error: "id is required"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &liveHistoryMessageService{}
			message := &types.Message{Role: "assistant", ExecutionContext: types.MessageExecutionContext{
				KnowledgeBaseIDs: []string{"manual-kb"},
			}}
			evt := producedKnowledgeToolResult(t, tc.data)
			if err := checkLiveStreamPublication(context.Background(), svc, message,
				[]interfaces.StreamEvent{evt}); err != nil {
				t.Fatalf("contentless producer result stopped ordinary chat: %v", err)
			}
			if svc.checks != 1 || len(svc.checkedKBs) != 1 || svc.checkedKBs[0] != "manual-kb" {
				t.Fatalf("current grants were not checked: checks=%d KBs=%v", svc.checks, svc.checkedKBs)
			}
		})
	}
}

func TestLiveSSEProducerNonemptyKnowledgeResultsRetainIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		data map[string]interface{}
	}{
		{"search_knowledge", map[string]interface{}{
			"display_type": "search_results", "count": 1,
			"results": []map[string]interface{}{{"knowledge_id": "source-doc", "content": "synthetic excerpt"}},
		}},
		{"list_documents", map[string]interface{}{
			"display_type": "document_info",
			"documents":    []map[string]interface{}{{"knowledge_id": "source-doc", "title": "synthetic title"}},
		}},
		{"read_document", map[string]interface{}{
			"display_type": "knowledge_chunks_list", "knowledge_id": "source-doc",
			"chunks": []map[string]interface{}{{"knowledge_id": "source-doc", "content": "synthetic excerpt"}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evt := producedKnowledgeToolResult(t, event.AgentToolResultData{
				ToolName: tc.name, Success: true,
				Data: tc.data,
			})
			svc := &liveHistoryMessageService{}
			message := &types.Message{Role: "assistant"}
			if err := checkLiveStreamPublication(context.Background(), svc, message,
				[]interfaces.StreamEvent{evt}); err != nil {
				t.Fatal(err)
			}
			if len(svc.checkedRefs) != 1 || svc.checkedRefs[0] != "source-doc" {
				t.Fatalf("sanitized producer result lost identity: %v", svc.checkedRefs)
			}
		})
	}
}

func TestLiveSSERejectsUnknownEmptyOrOpaqueKnowledgeResults(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*interfaces.StreamEvent)
	}{
		{"missing_count", func(evt *interfaces.StreamEvent) { delete(evt.Data, "count") }},
		{"positive_count", func(evt *interfaces.StreamEvent) { evt.Data["count"] = 1 }},
		{"string_count", func(evt *interfaces.StreamEvent) { evt.Data["count"] = "0" }},
		{"missing_success", func(evt *interfaces.StreamEvent) { delete(evt.Data, "success") }},
		{"null_results", func(evt *interfaces.StreamEvent) { evt.Data["results"] = nil }},
		{"wrong_display", func(evt *interfaces.StreamEvent) { evt.Data["display_type"] = "unknown" }},
		{"unknown_payload", func(evt *interfaces.StreamEvent) {
			evt.Data["data"] = map[string]interface{}{"content": "opaque excerpt"}
		}},
		{"raw_output", func(evt *interfaces.StreamEvent) { evt.Data["output"] = "opaque excerpt" }},
		{"nonempty_without_id", func(evt *interfaces.StreamEvent) {
			evt.Data["results"] = []map[string]interface{}{{"content": "opaque excerpt"}}
		}},
		{"opaque_read", func(evt *interfaces.StreamEvent) { evt.Data["tool_name"] = "read_document" }},
		{"failure_with_output", func(evt *interfaces.StreamEvent) {
			evt.Data = map[string]interface{}{
				"tool_name": "read_document", "success": false,
				"error": "failed", "output": "opaque excerpt",
			}
			evt.Content = "failed"
		}},
		{"failure_with_unmatched_content", func(evt *interfaces.StreamEvent) {
			evt.Data = map[string]interface{}{"tool_name": "read_document", "success": false, "error": "failed"}
			evt.Content = "opaque excerpt"
		}},
		{"empty_list_missing_total", func(evt *interfaces.StreamEvent) {
			evt.Data = map[string]interface{}{
				"tool_name": "list_documents", "success": true,
				"display_type": "document_info", "documents": []interface{}{}, "knowledge_base_id": "manual-kb",
				"page": 1, "page_size": 20,
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evt := producedKnowledgeToolResult(t, event.AgentToolResultData{
				ToolName: "search_knowledge", Success: true, Data: emptySearchToolData(),
			})
			tc.mutate(&evt)
			err := checkLiveStreamPublication(context.Background(), &liveHistoryMessageService{},
				&types.Message{Role: "assistant"}, []interfaces.StreamEvent{evt})
			if !errors.Is(err, access.ErrNextcloudPublicationDenied) {
				t.Fatalf("unproven source output was not denied: %v", err)
			}
		})
	}
}

func TestLiveSSEEmptyResultStillRechecksRevokedEarlierReference(t *testing.T) {
	evt := producedKnowledgeToolResult(t, event.AgentToolResultData{
		ToolName: "search_knowledge", Success: true, Data: emptySearchToolData(),
	})
	svc := &liveHistoryMessageService{denyAt: 1}
	message := &types.Message{Role: "assistant", KnowledgeReferences: types.References{{KnowledgeID: "nextcloud-doc"}}}
	err := checkLiveStreamPublication(context.Background(), svc, message, []interfaces.StreamEvent{evt})
	if !errors.Is(err, access.ErrNextcloudPublicationDenied) || svc.checks != 1 ||
		len(svc.checkedRefs) != 1 || svc.checkedRefs[0] != "nextcloud-doc" {
		t.Fatalf("empty result bypassed current source check: err=%v checks=%d refs=%v", err, svc.checks,
			svc.checkedRefs)
	}
}

func TestLiveSSEEmptySearchContinuesOrdinaryChatAndPreservesRevocationStop(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, revoked := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary_chat", true: "earlier_reference_revoked"}[revoked], func(t *testing.T) {
			evt := producedKnowledgeToolResult(t, event.AgentToolResultData{
				ToolName: "search_knowledge", Success: true, Data: emptySearchToolData(),
			})
			svc := &liveHistoryMessageService{}
			message := &types.Message{
				Role: "assistant", RequestID: "turn",
				ExecutionContext: types.MessageExecutionContext{
					KnowledgeBaseIDs: []string{"manual-kb"},
				},
			}
			if revoked {
				svc.denyAt = 1
				message.KnowledgeReferences = types.References{{KnowledgeID: "nextcloud-doc"}}
			}
			h := &Handler{messageService: svc, streamManager: &stagedLiveStream{events: []interfaces.StreamEvent{
				evt,
				{ID: "answer", Type: types.ResponseTypeAnswer, Content: "synthetic no-evidence response"},
				{ID: "complete", Type: "complete", Done: true},
			}}}
			bus := event.NewEventBus()
			stops := 0
			bus.On(event.EventStop, func(context.Context, event.Event) error { stops++; return nil })
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			c.Request = httptest.NewRequest(http.MethodPost, "/knowledge-chat/session", nil).WithContext(ctx)
			h.handleAgentEventsForSSE(ctx, c, "session", "assistant", "turn", bus, false,
				storageurl.NewStreamRewriter(storageurl.NewRequestRewriter(ctx, storageurl.ModeHandle, nil,
					nil)), message)
			body := recorder.Body.String()
			if revoked {
				if stops != 1 || strings.Contains(body, "synthetic no-evidence response") ||
					!strings.Contains(body, "Current source access changed") {
					t.Fatalf("revoked earlier source escaped the empty-result checkpoint: %s", body)
				}
			} else if stops != 0 || !strings.Contains(body, "synthetic no-evidence response") ||
				strings.Contains(body, "Current source access changed") || svc.checks != 3 {
				t.Fatalf("ordinary no-result chat did not finish: stops=%d checks=%d body=%s", stops, svc.checks, body)
			}
		})
	}
}
