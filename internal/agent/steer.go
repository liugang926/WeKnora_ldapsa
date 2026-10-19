package agent

import (
	"context"

	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
)

// sanitizeSteerContent uses the same fixed authored-text transfer as the
// original-material producer and its consume writer.
func sanitizeSteerContent(content string) string {
	return types.SteerCanonicalQuery(content)
}

// Only explicit in-package algorithm fixtures may exercise the old queue
// algorithm without original-material capabilities. Its implementation lives
// in a _test.go file; an external sink cannot supply this private marker.
type steerAlgorithmFixture interface {
	agentSteerAlgorithmFixture()
}

type originalSteerInputChecker interface {
	CheckOriginalSteerInputs(context.Context) error
}

// SetSteerSink is configured after the source verifier and round guard, before
// Execute. Retained original consume guards belong to the sink's producer/HTTP
// lifetime, including assistant persistence after EventAgentComplete.
func (e *AgentEngine) SetSteerSink(sink types.SteerSink) {
	e.steerSink = sink
	if sink == nil {
		return
	}
	if _, fixture := sink.(steerAlgorithmFixture); fixture {
		return
	}

	previousVerifier := e.lineageVerifier
	e.lineageVerifier = func(ctx context.Context, lineage *types.SourceLineage) error {
		if previousVerifier == nil {
			return e.rememberLineageFailure(types.ErrSourceLineageUnknown)
		}
		if err := previousVerifier(ctx, lineage); err != nil {
			return e.rememberLineageFailure(err)
		}
		return e.checkOriginalSteerSinkInputs(ctx, sink)
	}
	previousGuard := e.roundGuard
	e.roundGuard = func(ctx context.Context) error {
		if previousGuard != nil {
			if err := previousGuard(ctx); err != nil {
				return e.rememberLineageFailure(err)
			}
		}
		return e.checkOriginalSteerSinkInputs(ctx, sink)
	}

	// The accumulator captures its authorizer at construction. Rebind it with
	// the complete prior snapshot instead of losing sources already accounted
	// by the caller. Execute uses this composed verifier for its new turn too.
	previousLineage := e.SourceLineageSnapshot()
	e.turnLineage = types.NewRAGSourceLineage(e.lineageVerifier)
	if previousLineage != nil {
		_ = e.addLineage(previousLineage)
	}
}

func (e *AgentEngine) checkOriginalSteerInputs(ctx context.Context) error {
	return e.checkOriginalSteerSinkInputs(ctx, e.steerSink)
}

func (e *AgentEngine) checkOriginalSteerSinkInputs(ctx context.Context, sink types.SteerSink) error {
	if err := e.lineageFailure(); err != nil {
		return err
	}
	if sink == nil {
		return nil
	}
	if _, fixture := sink.(steerAlgorithmFixture); fixture {
		return nil
	}
	if _, actual := sink.(types.OriginalSteerSink); !actual {
		return e.rememberLineageFailure(types.ErrSourceLineageUnknown)
	}
	if checker, ok := sink.(originalSteerInputChecker); ok {
		return e.rememberLineageFailure(checker.CheckOriginalSteerInputs(ctx))
	}
	return nil
}

// drainSteerMessages is called at the round boundary, after context
// compression and before the next LLM call, so injected text:
//   - lands in the protected tail (CompressContext never removes anything
//     after the last user message),
//   - is included in lastSentMsgCount's delta-token accounting,
//   - and is visible to the very next callLLMWithRetry.
//
// Typed original persistence, lineage and retained guard checks all happen
// before append. A rejected capability poisons the turn so a later model or
// publication boundary cannot continue with an unaccounted input.
func (e *AgentEngine) drainSteerMessages(
	ctx context.Context,
	state *types.AgentState,
	messagesPtr *[]chat.Message,
	sessionID, messageID string,
) int {
	if e.steerSink == nil {
		return 0
	}
	actualSink, actual := e.steerSink.(types.OriginalSteerSink)
	_, fixture := e.steerSink.(steerAlgorithmFixture)
	if !fixture {
		if err := e.checkOriginalSteerInputs(ctx); err != nil {
			return 0
		}
	}
	events, _, err := e.steerSink.PollSteer(ctx, sessionID, messageID, 0)
	if err != nil {
		logger.Warnf(ctx, "[Agent] Steer poll failed at round %d: %v", state.CurrentRound+1, err)
		if !fixture {
			e.rememberLineageFailure(err)
		}
		return 0
	}
	if len(events) == 0 {
		return 0
	}

	injected := 0
	for _, evt := range events {
		content := sanitizeSteerContent(getString(evt, "content"))
		if content == "" {
			continue
		}
		steerID := getString(evt, "id")
		var userMessageID string
		var lineage *types.SourceLineage
		if actual {
			var check func() error
			var close func()
			userMessageID, lineage, check, close, err = actualSink.PersistSteerMaterial(
				ctx,
				sessionID,
				messageID,
				steerID,
				content,
				types.MentionedItemsFromRaw(evt["mentioned_items"]),
				getString(evt, "channel"),
			)
			if err != nil {
				e.rememberLineageFailure(err)
				return injected
			}
			if userMessageID == "" || check == nil || close == nil || lineage.RequireComplete() != nil {
				e.rememberLineageFailure(types.ErrSourceLineageUnknown)
				return injected
			}
			// Keep a private receipt for the exact input, then validate the live
			// consume guard. The sink retains and closes it after persistence;
			// closing here would invalidate the next model/output boundary.
			lineage, err = types.UnionSourceLineage(lineage)
			if err != nil {
				e.rememberLineageFailure(err)
				return injected
			}
			if err := check(); err != nil {
				e.rememberLineageFailure(err)
				return injected
			}
			if err := e.addLineage(lineage); err != nil {
				return injected
			}
			if err := e.checkLineage(ctx); err != nil {
				return injected
			}
		} else if fixture {
			userMessageID = e.steerSink.PersistSteerMessage(ctx, sessionID, messageID, steerID, content,
				types.MentionedItemsFromRaw(evt["mentioned_items"]), getString(evt, "channel"))
			if userMessageID == "" {
				logger.Warnf(ctx, "[Agent] Steer persist failed for %s, leaving event pending", steerID)
				continue
			}
			lineage = independentAgentInput()
		} else {
			e.rememberLineageFailure(types.ErrSourceLineageUnknown)
			return injected
		}
		*messagesPtr = append(*messagesPtr, chat.Message{
			Role: "user", Content: types.SteerMessageContent(content), SourceLineage: lineage,
		})
		state.PendingSteerMessages = append(state.PendingSteerMessages, userMessageID)
		err = e.emitEvent(ctx, event.Event{
			ID:        generateEventID("injected"),
			Type:      event.EventUserMessageInjected,
			SessionID: sessionID,
			Data: event.UserMessageInjectedData{
				SteerID:       steerID,
				Content:       content,
				MessageID:     messageID,
				UserMessageID: userMessageID,
			},
		})
		if actual && err != nil {
			return injected
		}
		injected++
	}
	if injected > 0 {
		logger.Infof(ctx, "[Agent][Round-%d] Injected %d steered user message(s) into the turn",
			state.CurrentRound+1, injected)
	}
	return injected
}

func getString(evt map[string]interface{}, key string) string {
	return types.MapString(evt, key)
}
