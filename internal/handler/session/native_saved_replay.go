package session

import (
	"bytes"
	"context"
	stderrors "errors"
	"fmt"

	agenttools "github.com/Tencent/WeKnora/internal/agent/tools"
	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/storageurl"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-contrib/sse"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type completedMessageMaterialReplay interface {
	BeginCompletedMessageMaterialReplay(context.Context, string, string) (*types.Message, func() error, func(), error)
}

// A nil native message means the service checked current control metadata and
// found a running turn. A completed row without an original receipt is an error.
func (h *Handler) tryNativeSavedReplay(
	ctx context.Context,
	c *gin.Context,
	sessionID, messageID string,
	rewriter *storageurl.StreamRewriter,
) bool {
	reader, ok := h.messageService.(completedMessageMaterialReplay)
	if !ok {
		return false
	}
	message, check, closeRead, err := reader.BeginCompletedMessageMaterialReplay(ctx, sessionID, messageID)
	if closeRead != nil {
		defer closeRead()
	}
	if err != nil {
		_ = c.Error(nativeSavedReplayHTTPError(err))
		return true
	}
	if message == nil {
		return false
	}
	if check == nil || closeRead == nil || !message.IsCompleted || message.Role != "assistant" ||
		message.ID != messageID ||
		message.SessionID != sessionID ||
		message.SourceLineage.RequireComplete() != nil {
		_ = c.Error(nativeSavedReplayHTTPError(access.ErrNextcloudPublicationUnavailable))
		return true
	}
	if err := check(); err != nil {
		_ = c.Error(nativeSavedReplayHTTPError(err))
		return true
	}
	if err := emitNativeSavedReplay(ctx, c, message, check, rewriter); err != nil && !c.Writer.Written() {
		_ = c.Error(nativeSavedReplayHTTPError(err))
	}
	return true
}

// Every value below comes from the typed original material capture. Transient
// Redis events, session titles, attachments of another message, and artifact
// rows have no receipt in this capture and supply no reconstructed fields.
func nativeSavedReplayEvents(message *types.Message) []interfaces.StreamEvent {
	events := []interfaces.StreamEvent{
		{
			ID:        "saved-query-" + message.ID,
			Type:      types.ResponseTypeAgentQuery,
			Timestamp: message.CreatedAt,
			Data: map[string]interface{}{
				"session_id":           message.SessionID,
				"assistant_message_id": message.ID,
				"assistant_created_at": message.CreatedAt,
			},
		},
	}
	appendEvent := func(id string, kind types.ResponseType, content string, done bool, data map[string]interface{}) {
		events = append(
			events,
			interfaces.StreamEvent{
				ID:        id,
				Type:      kind,
				Content:   content,
				Done:      done,
				Timestamp: message.CreatedAt,
				Data:      data,
			},
		)
	}
	for index, step := range message.AgentSteps {
		stepID := fmt.Sprintf("saved-step-%s-%d", message.ID, index)
		if step.Thought != "" {
			appendEvent(
				stepID+"-thought",
				types.ResponseTypeThinking,
				step.Thought,
				true,
				map[string]interface{}{"event_id": stepID + "-thought"},
			)
		}
		if step.ReasoningContent != "" && step.ReasoningContent != step.Thought {
			appendEvent(
				stepID+"-reasoning",
				types.ResponseTypeThinking,
				step.ReasoningContent,
				true,
				map[string]interface{}{"event_id": stepID + "-reasoning"},
			)
		}
		for callIndex, call := range step.ToolCalls {
			callID := fmt.Sprintf("%s-call-%d", stepID, callIndex)
			name := call.ExecutionName()
			appendEvent(
				callID,
				types.ResponseTypeToolCall,
				fmt.Sprintf("Calling tool: %s", name),
				false,
				map[string]interface{}{
					"tool_name":    name,
					"tool_call_id": call.ID,
					"arguments":    agenttools.SanitizeSandboxFileCallArgs(name, call.ExecutionArgs()),
				},
			)
			if call.Result != nil {
				result := call.Result
				data := agenttools.SanitizeToolResultForClient(name, result)
				data["tool_name"], data["tool_call_id"] = name, call.ID
				data["success"], data["error"], data["duration_ms"] = result.Success, result.Error, call.Duration
				appendEvent(
					callID+"-result",
					types.ResponseTypeToolResult,
					agenttools.StreamContentForToolResult(name, result.Success, result.Error, result.Data),
					false,
					data,
				)
			}
			if call.Reflection != "" {
				appendEvent(
					callID+"-reflection",
					types.ResponseTypeReflection,
					call.Reflection,
					true,
					map[string]interface{}{"event_id": callID + "-reflection", "tool_call_id": call.ID},
				)
			}
		}
	}
	if len(message.KnowledgeReferences) > 0 {
		appendEvent(
			"saved-references-"+message.ID,
			types.ResponseTypeReferences,
			"",
			false,
			map[string]interface{}{"references": message.KnowledgeReferences},
		)
	}
	answerID := "saved-answer-" + message.ID
	appendEvent(
		answerID,
		types.ResponseTypeAnswer,
		message.Content,
		true,
		map[string]interface{}{"event_id": answerID, "is_fallback": message.IsFallback},
	)
	appendEvent("saved-complete-"+message.ID, types.ResponseTypeComplete, "", true, map[string]interface{}{
		"final_content":     message.Content,
		"total_steps":       len(message.AgentSteps),
		"total_duration_ms": message.AgentDurationMs,
		"usage":             message.Usage,
	})
	events[len(events)-1].Usage = message.Usage
	return events
}

func emitNativeSavedReplay(
	ctx context.Context,
	c *gin.Context,
	message *types.Message,
	check func() error,
	rewriter *storageurl.StreamRewriter,
) error {
	prior := c.Writer
	writer := &nativeSavedReplayWriter{ResponseWriter: prior, ctx: ctx, check: check}
	c.Writer = writer
	defer func() { c.Writer = prior }()
	setSSEHeaders(c)
	for _, frame := range nativeSavedReplayEvents(message) {
		// Serialize the whole frame, including public resource rewrites, before
		// the final original-material check at the actual HTTP write boundary.
		response := buildStreamResponseFor(ctx, frame, message.RequestID, rewriter)
		var encoded bytes.Buffer
		if err := sse.Encode(&encoded, sse.Event{Event: "message", Data: response}); err != nil {
			return err
		}
		if _, err := writer.Write(encoded.Bytes()); err != nil {
			return err
		}
		writer.Flush()
		if writer.denied != nil {
			return writer.denied
		}
	}
	return nil
}

type nativeSavedReplayWriter struct {
	gin.ResponseWriter
	ctx    context.Context
	check  func() error
	denied error
}

func (w *nativeSavedReplayWriter) beforeWrite() error {
	if w.denied == nil {
		w.denied = w.ctx.Err()
	}
	if w.denied == nil {
		w.denied = w.check()
	}
	return w.denied
}

func (w *nativeSavedReplayWriter) deny(err error) {
	if w.Written() {
		return
	}
	app, _ := errors.IsAppError(nativeSavedReplayHTTPError(err))
	w.ResponseWriter.Header().Del("Content-Type")
	w.ResponseWriter.Header().Del("Content-Length")
	w.WriteHeader(app.HTTPCode)
	w.ResponseWriter.WriteHeaderNow()
}

func (w *nativeSavedReplayWriter) Write(body []byte) (int, error) {
	if err := w.beforeWrite(); err != nil {
		w.deny(err)
		return 0, err
	}
	return w.ResponseWriter.Write(body)
}

func (w *nativeSavedReplayWriter) WriteString(body string) (int, error) {
	return w.Write([]byte(body))
}

func (w *nativeSavedReplayWriter) WriteHeaderNow() {
	if err := w.beforeWrite(); err != nil {
		w.deny(err)
		return
	}
	w.ResponseWriter.WriteHeaderNow()
}

func (w *nativeSavedReplayWriter) Flush() {
	if err := w.beforeWrite(); err != nil {
		w.deny(err)
		return
	}
	w.ResponseWriter.Flush()
}

func nativeSavedReplayHTTPError(err error) error {
	if stderrors.Is(err, errors.ErrSessionNotFound) || stderrors.Is(err, gorm.ErrRecordNotFound) {
		return errors.NewNotFoundError("Session or message not found")
	}
	if stderrors.Is(err, repository.ErrKnowledgeInputChanged) || stderrors.Is(err, types.ErrSourceLineageUnknown) ||
		stderrors.Is(err, repository.ErrNextcloudContentGCUncovered) {
		return errors.NewServiceUnavailableError("Saved message original material is unavailable")
	}
	if stderrors.Is(err, access.ErrForbidden) || stderrors.Is(err, access.ErrNextcloudPublicationDenied) {
		return errors.NewForbiddenError("Current source access is required")
	}
	if stderrors.Is(err, access.ErrNextcloudPublicationUnavailable) {
		return errors.NewServiceUnavailableError("Cannot verify original saved message access")
	}
	return errors.NewServiceUnavailableError("Saved message original material is unavailable")
}
