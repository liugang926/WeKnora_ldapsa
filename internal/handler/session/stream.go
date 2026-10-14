package session

import (
	"context"
	stderrors "errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/storageurl"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	secutils "github.com/Tencent/WeKnora/internal/utils"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// ContinueStream godoc
// @Summary      继续流式响应
// @Description  继续获取正在进行的流式响应
// @Tags         问答
// @Accept       json
// @Produce      text/event-stream
// @Param        session_id     path      string  true   "会话ID"
// @Param        message_id     query     string  true   "消息ID"
// @Param        resource_urls  query     string  false  "文件引用形式，public 返回可加载直链"  Enums(handle, public)  default(handle)
// @Success      200            {object}  map[string]interface{}  "流式响应"
// @Failure      404         {object}  errors.AppError         "会话或消息不存在"
// @Security     Bearer
// @Security     ApiKeyAuth
// @Router       /sessions/continue-stream/{session_id} [get]
func (h *Handler) ContinueStream(c *gin.Context) {
	ctx := c.Request.Context()

	logger.Info(ctx, "Start continuing stream response processing")

	// Get session ID from URL parameter
	sessionID := secutils.SanitizeForLog(c.Param("session_id"))
	if sessionID == "" {
		logger.Error(ctx, "Session ID is empty")
		c.Error(errors.NewBadRequestError(errors.ErrInvalidSessionID.Error()))
		return
	}

	// Get message ID from query parameter
	messageID := secutils.SanitizeForLog(c.Query("message_id"))
	if messageID == "" {
		logger.Error(ctx, "Message ID is empty")
		c.Error(errors.NewBadRequestError("Missing message ID"))
		return
	}

	logger.Infof(ctx, "Continuing stream, session ID: %s, message ID: %s", sessionID, messageID)

	// Resolve before any SSE header is written so an invalid resource_urls value
	// is still reportable as a normal 400 JSON error.
	resourceRewriter, err := h.resolveStreamRewriter(c)
	if err != nil {
		logger.Warnf(ctx, "Rejected resource URL mode: %v", err)
		_ = c.Error(err)
		return
	}

	// Verify that the session exists and belongs to this tenant
	if _, err := h.sessionService.GetSession(ctx, sessionID); err != nil {
		if stderrors.Is(err, errors.ErrSessionNotFound) {
			logger.Warnf(ctx, "Session not found, ID: %s", sessionID)
			c.Error(errors.NewNotFoundError(err.Error()))
		} else {
			logger.ErrorWithFields(ctx, err, nil)
			c.Error(errors.NewInternalServerError(err.Error()))
		}
		return
	}

	// Get the incomplete message
	message, err := getMessageForStream(ctx, h.messageService, sessionID, messageID)
	if err != nil {
		if stderrors.Is(err, access.ErrNextcloudPublicationDenied) {
			_ = c.Error(errors.NewForbiddenError("Current Nextcloud file access is required"))
			return
		}
		if stderrors.Is(err, access.ErrNextcloudPublicationUnavailable) {
			_ = c.Error(errors.NewServiceUnavailableError("Cannot verify current Nextcloud file access"))
			return
		}
		if stderrors.Is(err, errors.ErrSessionNotFound) {
			// PR #1309 plumbed user-scope into messageService.GetMessage's
			// session existence check; non-owner / wrong-user lookups now
			// surface as ErrSessionNotFound. Map to 404 so clients can tell
			// "wrong URL" from a real 5xx instead of seeing a generic 500.
			logger.Warnf(ctx, "Session not found, ID: %s", sessionID)
			c.Error(errors.NewNotFoundError(err.Error()))
			return
		}
		if stderrors.Is(err, gorm.ErrRecordNotFound) {
			// The message_id doesn't exist (e.g. a wrong / non-persisted id, or an
			// expired replay buffer). That is a client error, not a server fault:
			// return 404 so callers read resource.not_found (a permanent condition
			// they must not retry) instead of a retryable 5xx. Mirrors the
			// ErrSessionNotFound branch above and the kb/doc/chunk not-found fix.
			logger.Warnf(ctx, "Message not found, session ID: %s, message ID: %s", sessionID, messageID)
			c.Error(errors.NewNotFoundError(err.Error()))
			return
		}
		logger.ErrorWithFields(ctx, err, nil)
		c.Error(errors.NewInternalServerError(err.Error()))
		return
	}

	if message == nil {
		logger.Warnf(ctx, "Incomplete message not found, session ID: %s, message ID: %s", sessionID, messageID)
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "Incomplete message not found",
		})
		return
	}

	// Get initial events from stream (offset 0)
	events, currentOffset, err := h.streamManager.GetEvents(ctx, sessionID, messageID, 0)
	if err != nil {
		logger.ErrorWithFields(ctx, err, nil)
		c.Error(errors.NewInternalServerError(fmt.Sprintf("Failed to get stream data: %s", err.Error())))
		return
	}

	if len(events) == 0 {
		logger.Warnf(ctx, "No events found in stream, session ID: %s, message ID: %s", sessionID, messageID)
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "No stream events found",
		})
		return
	}
	if message.IsCompleted {
		if err := checkReplayPublication(ctx, h.messageService, message, events); err != nil {
			_ = c.Error(replayPublicationHTTPError(err))
			return
		}
	} else {
		events, err = protectLiveStreamBatch(ctx, h.messageService, message, events)
		if err != nil {
			_ = c.Error(replayPublicationHTTPError(err))
			return
		}
	}

	logger.Infof(
		ctx, "Preparing to replay %d events and continue streaming, session ID: %s, message ID: %s",
		len(events), sessionID, messageID,
	)

	// Set headers for SSE
	setSSEHeaders(c)

	// Check if stream is already completed
	streamCompleted := false
	for _, evt := range events {
		if evt.Type == "complete" {
			streamCompleted = true
			break
		}
	}

	// Replay existing events, a segment's worth of chunks per frame
	replay := coalesceReplayEvents(events)
	logger.Debugf(ctx, "Replaying %d existing events as %d frames", len(events), len(replay))
	for index, evt := range replay {
		if index > 0 && (message.IsCompleted || liveBatchNeedsPublicationCheck([]interfaces.StreamEvent{evt})) {
			if err := checkReplayPublication(ctx, h.messageService, message, nil); err != nil {
				return
			}
		}
		emitStreamEvent(ctx, c, evt, message.RequestID, resourceRewriter)
	}

	// If stream is already completed, send final event and return
	if streamCompleted {
		logger.Infof(ctx, "Stream already completed, session ID: %s, message ID: %s", sessionID, messageID)
		sendCompletionEvent(c, message.RequestID)
		return
	}

	// Continue polling for new events
	logger.Debug(ctx, "Starting event update monitoring")
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-c.Request.Context().Done():
			logger.Debug(ctx, "Client connection closed")
			return

		case <-ticker.C:
			// Get new events from current offset
			newEvents, newOffset, err := h.streamManager.GetEvents(ctx, sessionID, messageID, currentOffset)
			if err != nil {
				logger.Errorf(ctx, "Failed to get new events: %v", err)
				// The held tail is still protected source output. A store error
				// does not bypass current authorization for its prior prefix.
				if err := checkReplayPublication(ctx, h.messageService, message, nil); err != nil {
					logger.Warnf(ctx, "Discarded held stream content after publication check failed: %v", err)
					return
				}
				if ctx.Err() != nil {
					return
				}
				flushHeldStreamContent(ctx, c, message.RequestID, resourceRewriter)
				return
			}
			if message.IsCompleted {
				err = checkReplayPublication(ctx, h.messageService, message, newEvents)
			} else {
				newEvents, err = protectLiveStreamBatch(ctx, h.messageService, message, newEvents)
			}
			if err != nil {
				logger.Warnf(ctx, "Stopped stream replay after source authorization changed: %v", err)
				return
			}

			// Send new events
			streamCompletedNow := false
			for index, evt := range newEvents {
				if index > 0 && (message.IsCompleted || liveBatchNeedsPublicationCheck([]interfaces.StreamEvent{evt})) {
					if err := checkReplayPublication(ctx, h.messageService, message, nil); err != nil {
						return
					}
				}
				// Check for completion event
				if evt.Type == "complete" {
					streamCompletedNow = true
				}

				emitStreamEvent(ctx, c, evt, message.RequestID, resourceRewriter)
			}

			// Update offset
			currentOffset = newOffset

			// If stream completed, send final event and exit
			if streamCompletedNow {
				logger.Infof(ctx, "Stream completed, session ID: %s, message ID: %s", sessionID, messageID)
				sendCompletionEvent(c, message.RequestID)
				return
			}
		}
	}
}

type replayPublicationVerifier interface {
	CheckMessagePublication(context.Context, *types.Message) error
}

func checkReplayPublication(
	ctx context.Context, service interfaces.MessageService,
	message *types.Message, events []interfaces.StreamEvent,
) error {
	if message == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	if !message.IsCompleted {
		return checkLiveStreamPublication(ctx, service, message, events)
	}
	verifier, ok := service.(replayPublicationVerifier)
	if !ok {
		return access.ErrNextcloudPublicationUnavailable
	}
	messageSnapshot := *message
	messageSnapshot.KnowledgeReferences = append(types.References(nil), message.KnowledgeReferences...)
	for _, streamEvent := range events {
		if streamEvent.Type == types.ResponseTypeReferences {
			response := buildStreamResponse(streamEvent, message.RequestID)
			messageSnapshot.KnowledgeReferences = append(messageSnapshot.KnowledgeReferences,
				response.KnowledgeReferences...)
		}
		// Old Redis frames may contain tool output not yet copied to the row.
		// Such a saved Agent replay remains globally guarded even without AgentID.
		if streamEvent.Type == types.ResponseTypeToolResult || streamEvent.Type == types.ResponseTypeToolCall {
			name, _ := streamEvent.Data["tool_name"].(string)
			id, _ := streamEvent.Data["tool_call_id"].(string)
			if !types.IsPipelineToolCallID(id) && !syntheticHistoryStreamCall(message, id) && name != "final_answer" {
				messageSnapshot.AgentSteps = append(messageSnapshot.AgentSteps,
					types.AgentStep{ToolCalls: []types.ToolCall{{ID: id, Name: name}}})
			}
		}
	}
	if err := verifier.CheckMessagePublication(ctx, &messageSnapshot); err != nil {
		return err
	}
	message.KnowledgeReferences = messageSnapshot.KnowledgeReferences
	message.AgentSteps = messageSnapshot.AgentSteps
	return nil
}

// KnowledgeQA records ragpipe-prefixed calls in the message, while its SSE
// frame carries the original ID. Match the persisted synthetic call so the
// ordinary fast-answer timeline is not mistaken for an opaque Agent round.
func syntheticHistoryStreamCall(message *types.Message, id string) bool {
	if message == nil || id == "" {
		return false
	}
	for _, step := range message.AgentSteps {
		for _, call := range step.ToolCalls {
			if call.ID == types.PipelineToolCallIDPrefix+id {
				return true
			}
		}
	}
	return false
}

type controlMessageLookup interface {
	GetMessageForControl(context.Context, string, string) (*types.Message, error)
}

func getMessageForControl(ctx context.Context, service interfaces.MessageService, sessionID,
	messageID string,
) (*types.Message, error) {
	if lookup, ok := service.(controlMessageLookup); ok {
		return lookup.GetMessageForControl(ctx, sessionID, messageID)
	}
	return service.GetMessage(ctx, sessionID, messageID)
}

type streamMessageLookup interface {
	GetMessageForStream(context.Context, string, string) (*types.Message, error)
}

func getMessageForStream(ctx context.Context, service interfaces.MessageService, sessionID,
	messageID string,
) (*types.Message, error) {
	if lookup, ok := service.(streamMessageLookup); ok {
		return lookup.GetMessageForStream(ctx, sessionID, messageID)
	}
	return service.GetMessage(ctx, sessionID, messageID)
}

func replayPublicationHTTPError(err error) error {
	if stderrors.Is(err, access.ErrNextcloudPublicationUnavailable) {
		return errors.NewServiceUnavailableError("Cannot verify current Nextcloud file access")
	}
	return errors.NewForbiddenError("Current Nextcloud file access is required")
}

// StopSession godoc
// @Summary      停止生成
// @Description  停止当前正在进行的生成任务
// @Tags         问答
// @Accept       json
// @Produce      json
// @Param        session_id  path      string              true  "会话ID"
// @Param        request     body      StopSessionRequest  true  "停止请求"
// @Success      200         {object}  map[string]interface{}  "停止成功"
// @Failure      404         {object}  errors.AppError         "会话或消息不存在"
// @Security     Bearer
// @Security     ApiKeyAuth
// @Router       /sessions/{session_id}/stop [post]
func (h *Handler) StopSession(c *gin.Context) {
	ctx := logger.CloneContext(c.Request.Context())
	sessionID := secutils.SanitizeForLog(c.Param("session_id"))

	if sessionID == "" {
		c.JSON(400, gin.H{"error": "Session ID is required"})
		return
	}

	// Parse request body to get message_id
	var req StopSessionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		logger.ErrorWithFields(ctx, err, map[string]interface{}{
			"session_id": sessionID,
		})
		c.JSON(400, gin.H{"error": "message_id is required"})
		return
	}

	assistantMessageID := secutils.SanitizeForLog(req.MessageID)
	logger.Infof(ctx, "Stop generation request for session: %s, message: %s", sessionID, assistantMessageID)

	// Get tenant ID from context
	tenantID, exists := c.Get(types.TenantIDContextKey.String())
	if !exists {
		logger.Error(ctx, "Failed to get tenant ID")
		c.JSON(401, gin.H{"error": "Unauthorized"})
		return
	}
	tenantIDUint := tenantID.(uint64)

	// Verify message ownership and status
	message, err := getMessageForControl(ctx, h.messageService, sessionID, assistantMessageID)
	if err != nil {
		logger.ErrorWithFields(ctx, err, map[string]interface{}{
			"session_id": sessionID,
			"message_id": assistantMessageID,
		})
		c.JSON(404, gin.H{"error": "Message not found"})
		return
	}

	// Verify message belongs to this session (double check)
	if message.SessionID != sessionID {
		logger.Warnf(ctx, "Message %s does not belong to session %s", assistantMessageID, sessionID)
		c.JSON(403, gin.H{"error": "Message does not belong to this session"})
		return
	}

	// Verify message belongs to the current tenant. Stopping generation mutates
	// an in-flight run, so use the strict owner scope: a tenant admin may read an
	// API-key session but must not be able to interrupt its (external) API calls.
	session, err := h.sessionService.GetOwnedSession(ctx, sessionID)
	if err != nil {
		logger.ErrorWithFields(ctx, err, map[string]interface{}{
			"session_id": sessionID,
		})
		c.JSON(404, gin.H{"error": "Session not found"})
		return
	}

	if session.TenantID != tenantIDUint {
		logger.Warnf(ctx, "Session %s does not belong to tenant %d", sessionID, tenantIDUint)
		c.JSON(403, gin.H{"error": "Access denied"})
		return
	}

	// Check if message is already completed (stopped)
	if message.IsCompleted {
		logger.Infof(ctx, "Message %s is already completed, no need to stop", assistantMessageID)
		c.JSON(200, gin.H{
			"success": true,
			"message": "Message already completed",
		})
		return
	}

	// Write stop event to StreamManager for distributed support
	stopEvent := interfaces.StreamEvent{
		ID:        fmt.Sprintf("stop-%d", time.Now().UnixNano()),
		Type:      types.ResponseType(event.EventStop),
		Content:   "",
		Done:      true,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"session_id": sessionID,
			"message_id": assistantMessageID,
			"reason":     "user_requested",
		},
	}

	if err := h.streamManager.AppendEvent(ctx, sessionID, assistantMessageID, stopEvent); err != nil {
		logger.ErrorWithFields(ctx, err, map[string]interface{}{
			"session_id": sessionID,
			"message_id": assistantMessageID,
		})
		c.JSON(500, gin.H{"error": "Failed to write stop event"})
		return
	}

	logger.Infof(ctx, "Stop event written successfully for session: %s, message: %s", sessionID, assistantMessageID)
	c.JSON(200, gin.H{
		"success": true,
		"message": "Generation stopped",
	})
}

// handleAgentEventsForSSE handles agent events for SSE streaming using an existing handler
// The handler is already subscribed to events and AgentQA is already running
// This function polls StreamManager and pushes events to SSE, allowing graceful handling of disconnections
// waitForTitle: if true, wait for title event after completion (for new sessions without title)
func (h *Handler) handleAgentEventsForSSE(
	ctx context.Context,
	c *gin.Context,
	sessionID, assistantMessageID, requestID string,
	eventBus *event.EventBus,
	waitForTitle bool,
	resourceRewriter *storageurl.StreamRewriter,
	liveMessages ...*types.Message,
) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	lastOffset := 0
	log := logger.GetLogger(ctx)

	log.Infof("Starting pull-based SSE streaming for session=%s, message=%s", sessionID, assistantMessageID)
	var liveMessage *types.Message
	if len(liveMessages) > 0 {
		liveMessage = liveMessages[0]
	}

	for {
		select {
		case <-c.Request.Context().Done():
			// Connection closed, exit gracefully without panic
			log.Infof(
				"Client disconnected, stopping SSE streaming for session=%s, message=%s",
				sessionID,
				assistantMessageID,
			)
			return

		case <-ticker.C:
			// Get new events from StreamManager using offset
			events, newOffset, err := h.streamManager.GetEvents(ctx, sessionID, assistantMessageID, lastOffset)
			if err != nil {
				log.Warnf("Failed to get events from stream: %v", err)
				continue
			}
			events, err = protectLiveStreamBatch(ctx, h.messageService, liveMessage, events)
			if err != nil {
				stopLiveStreamForPublication(ctx, c, eventBus, sessionID, assistantMessageID, requestID, err)
				return
			}

			// Send any new events
			streamCompleted := false
			titleReceived := false
			for index, evt := range events {
				// A batch can contain several answer frames. Recheck before
				// each later source-bearing frame so a grant withdrawn while
				// the batch is being sent stops further output.
				if index > 0 && liveBatchNeedsPublicationCheck([]interfaces.StreamEvent{evt}) {
					if err := checkLiveStreamPublication(ctx, h.messageService, liveMessage, nil); err != nil {
						stopLiveStreamForPublication(ctx, c, eventBus, sessionID, assistantMessageID, requestID, err)
						return
					}
				}
				// Check for stop event
				if evt.Type == types.ResponseType(event.EventStop) {
					log.Infof("Detected stop event, triggering stop via EventBus for session=%s", sessionID)

					// Emit stop event to the EventBus to trigger context cancellation
					if eventBus != nil {
						eventBus.Emit(ctx, event.Event{
							Type:      event.EventStop,
							SessionID: sessionID,
							Data: event.StopData{
								SessionID: sessionID,
								MessageID: assistantMessageID,
								Reason:    "user_requested",
							},
						})
					}

					// Release any buffered tail first: the answer generated
					// before the stop is still the user's content, and in
					// public resource URL mode part of it may be sitting in
					// the holdback buffer.
					flushHeldStreamContent(ctx, c, requestID, resourceRewriter)

					// Send stop notification to frontend
					c.SSEvent("message", &types.StreamResponse{
						ID:           requestID,
						ResponseType: "stop",
						Content:      "Generation stopped by user",
						Done:         true,
					})
					c.Writer.Flush()
					return
				}

				// Check for completion event
				if evt.Type == "complete" {
					streamCompleted = true
				}

				// Check for title event
				if evt.Type == types.ResponseTypeSessionTitle {
					titleReceived = true
				}

				// Check if connection is still alive before writing. Build the
				// payload only after this check: in public resource URL mode
				// building consumes the chunk into the holdback buffer, so an
				// early return here would drop it.
				if c.Request.Context().Err() != nil {
					log.Info("Connection closed during event sending, stopping")
					return
				}

				emitStreamEvent(ctx, c, evt, requestID, resourceRewriter)
			}

			// Update offset
			lastOffset = newOffset

			// Check if stream is completed - wait for title event only if needed and not already received
			if streamCompleted {
				if waitForTitle && !titleReceived {
					log.Infof("Stream completed for session=%s, message=%s, waiting for title event", sessionID, assistantMessageID)
					// Wait up to 3 seconds for title event after completion
					titleTimeout := time.After(3 * time.Second)
				titleWaitLoop:
					for {
						select {
						case <-titleTimeout:
							log.Info("Title wait timeout, closing stream")
							break titleWaitLoop
						case <-c.Request.Context().Done():
							log.Info("Connection closed while waiting for title")
							return
						default:
							// Check for new events (title event)
							events, newOff, err := h.streamManager.GetEvents(c.Request.Context(), sessionID, assistantMessageID, lastOffset)
							if err != nil {
								log.Warnf("Error getting events while waiting for title: %v", err)
								break titleWaitLoop
							}
							if len(events) > 0 {
								if err := checkLiveStreamPublication(ctx, h.messageService, liveMessage,
									events); err != nil {
									stopLiveStreamForPublication(ctx, c, eventBus, sessionID,
										assistantMessageID, requestID, err)
									return
								}
								for index, evt := range events {
									if index > 0 && liveBatchNeedsPublicationCheck([]interfaces.StreamEvent{evt}) {
										if err := checkLiveStreamPublication(ctx, h.messageService,
											liveMessage, nil); err != nil {
											stopLiveStreamForPublication(ctx, c, eventBus, sessionID,
												assistantMessageID, requestID, err)
											return
										}
									}
									emitStreamEvent(ctx, c, evt, requestID, resourceRewriter)
									// If we got the title, we can exit
									if evt.Type == types.ResponseTypeSessionTitle {
										log.Infof("Title event received: %s", evt.Content)
										break titleWaitLoop
									}
								}
								lastOffset = newOff
							} else {
								// No events, wait a bit before checking again
								time.Sleep(100 * time.Millisecond)
							}
						}
					}
				} else {
					log.Infof("Stream completed for session=%s, message=%s", sessionID, assistantMessageID)
				}
				sendCompletionEvent(c, requestID)
				return
			}
		}
	}
}

type livePublicationVerifier interface {
	CheckLiveMessagePublication(context.Context, *types.Message) error
}

func liveBatchNeedsPublicationCheck(events []interfaces.StreamEvent) bool {
	for _, evt := range events {
		switch evt.Type {
		case types.ResponseTypeAgentQuery, types.ResponseType(event.EventStop):
			continue
		default:
			return true
		}
	}
	return false
}

// Preparatory tool arguments can contain source-derived text. When a broad
// Nextcloud turn has not identified documents yet, omit only tool-call frames
// while retrieval continues. Content/result frames still fail closed. Once any
// reference is known, every later tool argument is checked against those refs.
func protectLiveStreamBatch(ctx context.Context, service interfaces.MessageService, message *types.Message,
	events []interfaces.StreamEvent,
) ([]interfaces.StreamEvent, error) {
	if !liveBatchNeedsPublicationCheck(events) {
		return events, nil
	}
	err := checkLiveStreamPublication(ctx, service, message, events)
	if err == nil {
		return events, nil
	}
	if !stderrors.Is(err, access.ErrNextcloudPublicationDenied) || message == nil ||
		len(message.KnowledgeReferences) > 0 {
		return nil, err
	}
	var filtered []interfaces.StreamEvent
	hasToolCall := false
	for _, evt := range events {
		switch evt.Type {
		case types.ResponseTypeToolCall:
			hasToolCall = true
		case types.ResponseTypeAgentQuery, types.ResponseType(event.EventStop):
			filtered = append(filtered, evt)
		default:
			return nil, err
		}
	}
	if !hasToolCall {
		return nil, err
	}
	return filtered, nil
}

func checkLiveStreamPublication(
	ctx context.Context, service interfaces.MessageService,
	message *types.Message, events []interfaces.StreamEvent,
) error {
	verifier, ok := service.(livePublicationVerifier)
	if !ok {
		return access.ErrNextcloudPublicationUnavailable
	}
	if message == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	messageSnapshot := *message
	messageSnapshot.KnowledgeReferences = append(types.References(nil), message.KnowledgeReferences...)
	seen := make(map[string]struct{}, len(messageSnapshot.KnowledgeReferences))
	for _, ref := range messageSnapshot.KnowledgeReferences {
		if ref != nil && ref.KnowledgeID != "" {
			seen[ref.KnowledgeID] = struct{}{}
		}
	}
	for _, streamEvent := range events {
		if streamEvent.Type == types.ResponseTypeReferences {
			response := buildStreamResponse(streamEvent, message.RequestID)
			for _, ref := range response.KnowledgeReferences {
				if ref == nil {
					continue
				}
				if ref.KnowledgeID == "" {
					messageSnapshot.KnowledgeReferences = append(messageSnapshot.KnowledgeReferences, ref)
					continue
				}
				if _, exists := seen[ref.KnowledgeID]; !exists {
					seen[ref.KnowledgeID] = struct{}{}
					messageSnapshot.KnowledgeReferences = append(messageSnapshot.KnowledgeReferences, ref)
				}
			}
		}
		if streamEvent.Type == types.ResponseTypeToolResult && isKnowledgeSourceTool(streamEvent.Data) {
			ids := make(map[string]struct{})
			collectStreamKnowledgeIDs(streamEvent.Data, ids, 0)
			if len(ids) == 0 && !contentlessKnowledgeToolResult(streamEvent) {
				return fmt.Errorf("%w: knowledge tool output lacks document identity",
					access.ErrNextcloudPublicationDenied)
			}
			for id := range ids {
				if _, exists := seen[id]; !exists {
					seen[id] = struct{}{}
					messageSnapshot.KnowledgeReferences = append(messageSnapshot.KnowledgeReferences,
						&types.SearchResult{KnowledgeID: id})
				}
			}
		}
	}
	if err := verifier.CheckLiveMessagePublication(ctx, &messageSnapshot); err != nil {
		return err
	}
	message.KnowledgeReferences = messageSnapshot.KnowledgeReferences
	return nil
}

// Empty search/list results and errors without a result payload contain no
// document to authorize. Accept only the structured contracts emitted by our
// tools, then still run the message verifier for earlier references and grants.
// Missing or opaque provenance must never be inferred from output text.
func contentlessKnowledgeToolResult(evt interfaces.StreamEvent) bool {
	data := evt.Data
	success, ok := data["success"].(bool)
	if !ok {
		return false
	}
	allowed := map[string]bool{
		"tool_name": true, "success": true, "error": true,
		"duration_ms": true, "tool_call_id": true,
	}
	if !success {
		// handleToolResult emits the diagnostic error as Content. No raw output
		// or structured tool payload is permitted on this contentless branch.
		diagnostic, ok := data["error"].(string)
		if !ok || evt.Content != diagnostic {
			return false
		}
	} else {
		name, _ := data["tool_name"].(string)
		switch name {
		case "search_knowledge":
			if data["display_type"] != "search_results" || !emptyKnowledgeResultList(data["results"]) ||
				!zeroKnowledgeResultCount(data["count"]) {
				return false
			}
			for _, key := range []string{
				"display_type", "results", "count", "knowledge_base_ids",
				"query", "queries", "mode", "requested_mode", "mode_fallbacks", "rerank_rejected",
			} {
				allowed[key] = true
			}
		case "list_documents":
			kbID, _ := data["knowledge_base_id"].(string)
			if data["display_type"] != "document_info" || !emptyKnowledgeResultList(data["documents"]) ||
				kbID == "" || !knowledgeResultCountAtLeast(data["total_docs"], -1) ||
				!knowledgeResultCountAtLeast(data["page"], 1) || !knowledgeResultCountAtLeast(data["page_size"], 1) {
				return false
			}
			for _, key := range []string{
				"display_type", "documents", "knowledge_base_id", "total_docs",
				"page", "page_size", "keyword", "next_page",
			} {
				allowed[key] = true
			}
		case "query_knowledge_graph":
			if evt.Content != "No relevant graph information found." || !emptyKnowledgeResultList(data["results"]) {
				return false
			}
			if _, ok := data["query"].(string); !ok {
				return false
			}
			if _, ok := data["knowledge_base_ids"]; !ok {
				return false
			}
			if _, ok := data["graph_configs"]; !ok {
				return false
			}
			if _, ok := data["graph_config"]; !ok {
				return false
			}
			if _, ok := data["errors"]; !ok {
				return false
			}
			for _, key := range []string{
				"knowledge_base_ids", "query", "results", "graph_configs",
				"graph_config", "errors",
			} {
				allowed[key] = true
			}

		default:
			return false
		}
	}
	for key := range data {
		if !allowed[key] {
			return false
		}
	}
	return true
}

func emptyKnowledgeResultList(value interface{}) bool {
	switch rows := value.(type) {
	case []interface{}:
		return rows != nil && len(rows) == 0
	case []map[string]interface{}:
		return rows != nil && len(rows) == 0
	default:
		return false
	}
}

func zeroKnowledgeResultCount(value interface{}) bool {
	switch count := value.(type) {
	case int:
		return count == 0
	case float64: // StreamManager JSON decoding.
		return count == 0
	default:
		return false
	}
}

func knowledgeResultCountAtLeast(value interface{}, minimum int64) bool {
	switch count := value.(type) {
	case int:
		return int64(count) >= minimum
	case int64:
		return count >= minimum
	case float64:
		return count >= float64(minimum) && count == float64(int64(count))
	default:
		return false
	}
}

func isKnowledgeSourceTool(data map[string]interface{}) bool {
	name, _ := data["tool_name"].(string)
	switch name {
	case "search_knowledge", "read_document", "list_documents", "query_knowledge_graph":
		return true
	default:
		return false
	}
}

func collectStreamKnowledgeIDs(value interface{}, ids map[string]struct{}, depth int) {
	if depth > 6 || len(ids) >= 1000 {
		return
	}
	switch data := value.(type) {
	case map[string]interface{}:
		if id, ok := data["knowledge_id"].(string); ok && id != "" {
			ids[id] = struct{}{}
		}
		for key, nested := range data {
			if key == "results" || key == "documents" || key == "chunks" || key == "document" || key == "data" {
				collectStreamKnowledgeIDs(nested, ids, depth+1)
			}
		}
	case []interface{}:
		for _, nested := range data {
			collectStreamKnowledgeIDs(nested, ids, depth+1)
		}
	case []map[string]interface{}:
		for _, nested := range data {
			collectStreamKnowledgeIDs(nested, ids, depth+1)
		}
	}
}

func stopLiveStreamForPublication(
	ctx context.Context, c *gin.Context, bus *event.EventBus,
	sessionID, messageID, requestID string, cause error,
) {
	logger.Warnf(ctx, "Stopping live answer after source authorization failed: %v", cause)
	if bus != nil {
		_ = bus.Emit(ctx, event.Event{
			Type: event.EventStop, SessionID: sessionID,
			Data: event.StopData{SessionID: sessionID, MessageID: messageID, Reason: "source_access_changed"},
		})
	}
	// Do not flush the resource URL holdback buffer: its tail may itself be
	// source-derived text from just before the publication was withdrawn.
	c.SSEvent("message", &types.StreamResponse{
		ID: requestID, ResponseType: types.ResponseTypeError,
		Content: "Current source access changed; generation stopped", Done: true,
	})
	c.Writer.Flush()
}
