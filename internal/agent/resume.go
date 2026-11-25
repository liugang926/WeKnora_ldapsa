package agent

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
)

// Resume continues a run that was suspended on a prompt, now answered. It
// rebuilds the transcript from the snapshot, finishes the interrupted round
// with the answer delivered to the blocked call, then carries on with the
// ReAct loop. The run may suspend again; completion is emitted either way.
func (e *AgentEngine) Resume(
	ctx context.Context,
	sessionID, messageID string,
	llmContext []chat.Message,
	snapshot []byte,
	answered *types.AgentPrompt,
) (*types.AgentState, error) {
	snap, err := DecodeRunSnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	ctx, finish := e.beginLineageTurn(ctx)
	defer finish()
	// Durable checkpoints come from the server. Re-authorize the whole prefix
	// before replay, including any side effects in an interrupted tool round.
	if !e.requireCompleteSourceLineage && (!snap.WeKnoraACL ||
		snap.SourceLineage.RequireComplete() != nil || len(snap.SourceLineage.Sources) != 0) {
		return nil, e.rememberLineageFailure(types.ErrSourceLineageUnknown)
	}
	if err := e.addLineage(snap.SourceLineage); err != nil {
		return nil, err
	}
	if err := e.checkLineage(ctx); err != nil {
		return nil, err
	}
	restoreResults := func(step *types.AgentStep) {
		for i := range step.ToolCalls {
			if result := step.ToolCalls[i].Result; result != nil {
				result.SourceLineage = snap.SourceLineage
			}
		}
	}
	for i := range snap.CompletedSteps {
		restoreResults(&snap.CompletedSteps[i])
	}
	restoreResults(&snap.PendingRound.Step)
	e.bindCollaborationInput(sessionID, messageID)
	p := snap.PendingRound
	wait := answered == nil && snap.CollaborationWait && e.collaboration != nil && e.collaboration.Child == nil
	if answered == nil && !wait {
		return nil, fmt.Errorf("resume needs an answered prompt or owned collaboration wait")
	}
	if !snap.BetweenRounds {
		if p.BlockedIndex < 0 || p.BlockedIndex >= len(p.Calls) || len(p.Step.ToolCalls) != p.BlockedIndex {
			return nil, fmt.Errorf("snapshot round is inconsistent: blocked %d of %d calls, %d done",
				p.BlockedIndex, len(p.Calls), len(p.Step.ToolCalls))
		}
		if wait && p.Calls[p.BlockedIndex].Name != "wait_agent" {
			return nil, fmt.Errorf("unsafe collaboration checkpoint")
		}
		if answered != nil && p.Calls[p.BlockedIndex].ID != answered.ToolCallID {
			return nil, fmt.Errorf("prompt %s answers call %s, run is blocked on %s",
				answered.ID, answered.ToolCallID, p.Calls[p.BlockedIndex].ID)
		}
	} else if !wait {
		return nil, fmt.Errorf("between-round resume requires collaboration ownership")
	}
	if e.collaboration != nil {
		var stop func()
		ctx, stop = e.collaboration.Start(ctx)
		defer stop()
	}
	logger.Infof(ctx, "[Agent] Resuming: session=%s, message=%s, prompt=%s, round=%d",
		sessionID, messageID, func() string {
			if answered != nil {
				return answered.ID
			}
			return "collaboration"
		}(), snap.CurrentRound+1)
	defer e.toolRegistry.Cleanup(ctx)

	state := &types.AgentState{
		RoundSteps:    append([]types.AgentStep{}, snap.CompletedSteps...),
		KnowledgeRefs: append([]*types.SearchResult{}, snap.KnowledgeRefs...),
		CurrentRound:  snap.CurrentRound,
		TurnUsage:     snap.TurnUsage,
	}
	systemPrompt := e.buildSystemPrompt(ctx)
	e.beginTurn(snap.Query, snap.ImageURLs, state)
	if err := e.restoreCollaborationDeliveries(ctx, snap); err != nil {
		return nil, err
	}
	for id, content := range snap.UserMessages {
		e.recordSteerMessage(id, content)
	}
	messages := e.buildMessagesWithLLMContext(systemPrompt, snap.Query, sessionID, llmContext, snap.ImageURLs)
	// The final user message is rebuilt from this authenticated checkpoint.
	messages[len(messages)-1].SourceLineage, err = types.UnionSourceLineage(snap.SourceLineage, e.promptSourceLineage())
	if err != nil {
		return nil, err
	}
	if err := e.accountMessages(ctx, messages); err != nil {
		return nil, err
	}
	appendUsers := func(step types.AgentStep) error {
		for _, id := range step.UserMessagesBefore {
			content, ok := snap.UserMessages[id]
			if !ok {
				return fmt.Errorf("missing steered message %s in run snapshot", id)
			}
			messages = append(messages, chat.Message{
				Role: "user", Content: types.SteerMessageContent(content), SourceLineage: snap.SourceLineage,
			})
		}
		return nil
	}
	appendBackground := func(step types.AgentStep) {
		for _, injection := range step.BackgroundInjections {
			messages = append(messages, chat.Message{Role: "user", Content: injection.Content})
		}
	}
	for i, step := range state.RoundSteps {
		e.replayCollaborationDeliveries(&messages, i, true)
		if err := appendUsers(step); err != nil {
			return nil, err
		}
		e.replayCollaborationDeliveries(&messages, i, false)
		appendBackground(step)
		messages = e.appendToolResults(messages, step)
		messages = e.appendToolImages(ctx, messages, step)
	}
	e.replayCollaborationDeliveries(&messages, len(state.RoundSteps), true)
	if err := appendUsers(p.Step); err != nil {
		return nil, err
	}
	e.replayCollaborationDeliveries(&messages, len(state.RoundSteps), false)
	appendBackground(p.Step)
	if err := e.accountMessages(ctx, messages); err != nil {
		return nil, err
	}
	if e.toolRegistry != nil {
		e.toolRegistry.RestoreMCPExposure(snap.MCPToolRefs)
		e.toolRegistry.RestoreMCPSourceBindings(snap.MCPSourceBindings)
		e.toolRegistry.RememberMCPHistory(messages)
		// These calls were issued before suspension, including the first call
		// to a tool whose definition was only exposed by a describe round.
		e.toolRegistry.RememberMCPHistory(pendingCallHistory(p))
		e.toolRegistry.RefreshMCPTools(ctx)
	}
	tools := e.buildToolsForLLM()

	if wait && snap.BetweenRounds {
		if err := e.collaboration.ClearWait(ctx); err != nil {
			return nil, err
		}
	}
	if !snap.BetweenRounds {
		startTime := time.Now()
		step, suspended := e.finishPendingRound(ctx, p, answered, state.CurrentRound, sessionID, messageID)
		if err := e.lineageFailure(); err != nil {
			return nil, err
		}
		if suspended != nil {
			logger.Infof(ctx, "[Agent][Round-%d] Suspended again waiting on prompt %s",
				state.CurrentRound+1, suspended.PromptID)
			state.Suspended = suspended
			state.SuspendedStep = &step
			if e.collaboration != nil {
				e.collaboration.End(true)
			}
			e.emitCompletionEvent(context.WithoutCancel(ctx), state, sessionID, messageID, startTime)
			state.SourceLineage, err = e.verifiedLineage(ctx)
			return state, err
		}
		state.RoundSteps = append(state.RoundSteps, step)
		messages = e.appendToolResults(messages, step)
		messages = e.appendToolImages(ctx, messages, step)
		state.CurrentRound++
	}

	if _, err := e.executeLoop(ctx, state, snap.Query, messages, tools, sessionID, messageID); err != nil {
		if errors.Is(ctx.Err(), context.Canceled) && errors.Is(err, context.Canceled) {
			return nil, err
		}
		logger.Errorf(ctx, "[Agent] Resumed execution failed: %v", err)
		if emitErr := e.emitEvent(ctx, event.Event{
			ID:        generateEventID("error"),
			Type:      event.EventError,
			SessionID: sessionID,
			Data: event.ErrorData{
				Error:     err.Error(),
				Stage:     "agent_execution",
				SessionID: sessionID,
			},
		}); emitErr != nil {
			logger.Warnf(ctx, "[Agent] Failed to emit resumed execution error: %v", emitErr)
		}
		return nil, err
	}
	state.SourceLineage, err = e.verifiedLineage(ctx)
	return state, err
}

func pendingCallHistory(p PendingRound) []chat.Message {
	calls := make([]chat.ToolCall, len(p.Calls))
	for i, c := range p.Calls {
		calls[i] = chat.ToolCall{ID: c.ID, Type: "function", Function: chat.FunctionCall{
			Name: c.Name, Arguments: c.Arguments,
		}}
	}
	return []chat.Message{{Role: "assistant", ToolCalls: calls}}
}

// finishPendingRound runs the interrupted round from its blocked call on,
// sequentially. The blocked call receives the answer; a later call may raise
// a prompt of its own and suspend the round again.
func (e *AgentEngine) finishPendingRound(
	ctx context.Context, p PendingRound, answered *types.AgentPrompt,
	iteration int, sessionID, messageID string,
) (types.AgentStep, *types.PromptSuspension) {
	step := p.Step
	step.ToolCalls = append([]types.ToolCall(nil), p.Step.ToolCalls...)
	calls := make([]types.LLMToolCall, len(p.Calls))
	for i, c := range p.Calls {
		calls[i] = types.LLMToolCall{
			ID:                c.ID,
			Type:              "function",
			Function:          types.FunctionCall{Name: c.Name, Arguments: c.Arguments},
			UnresolvedHandles: c.UnresolvedHandles,
			ProviderMetadata:  c.ProviderMetadata,
		}
	}

	round := iteration + 1
	e.beginRound(step, calls)
	defer e.endRound()
	for i, done := range step.ToolCalls {
		e.recordCall(i, done)
	}
	for i := p.BlockedIndex; i < len(calls); i++ {
		var answer *types.AgentPrompt
		if i == p.BlockedIndex {
			answer = answered
		}
		toolCall := e.runToolCallAnswered(ctx, calls[i], i, iteration, round, sessionID, messageID, answer)
		if s := suspensionOf(toolCall); s != nil {
			return step, s
		}
		e.recordCall(i, toolCall)
		step.ToolCalls = append(step.ToolCalls, toolCall)
		e.emitToolOutcome(ctx, toolCall, iteration, sessionID)
	}
	return step, nil
}
