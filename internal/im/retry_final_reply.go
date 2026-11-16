package im

import (
	"context"
	"errors"

	"github.com/Tencent/WeKnora/internal/storageurl"
	"github.com/Tencent/WeKnora/internal/types"
)

type completedReplyReader interface {
	BeginCompletedMessageMaterialReplay(context.Context, string, string) (*types.Message, func() error, func(), error)
}

// A platform can take seconds to reject a card update. Reopen the completed
// turn for this receiver and render its stable resource references again.
func (s *Service) retryFinalReply(
	ctx context.Context, msg *IncomingMessage, adapter Adapter, sessionID, answer string, qaErr error,
	resolver *storageurl.FileServiceResolver,
) error {
	scope, scoped := types.IMChannelScopeFromContext(ctx)
	if scoped {
		if err := scope.Check(ctx, scope.TenantID,
			types.GroupResourceTypeAgent, scope.AgentID, types.ResourceActionUse); err != nil {
			return err
		}
	}
	if qaErr != nil {
		return adapter.SendReply(ctx, msg, &ReplyMessage{Content: imQAFailureReply(qaErr), IsFinal: true})
	}
	if !scoped {
		content := formatIMOutboundAnswerOrFallback(ctx, answer, resolver)
		return adapter.SendReply(ctx, msg, &ReplyMessage{Content: content, IsFinal: true})
	}

	reader, ok := s.messageService.(completedReplyReader)
	entry, _ := ctx.Value(inflightOwnerKey{}).(*inflightEntry)
	if !ok || entry == nil {
		return types.ErrSourceLineageUnknown
	}
	entry.mu.Lock()
	boundSession, messageID := entry.sessionID, entry.assistantMessageID
	entry.mu.Unlock()
	if boundSession != sessionID || messageID == "" {
		return types.ErrSourceLineageUnknown
	}
	saved, recheck, closeRead, err := reader.BeginCompletedMessageMaterialReplay(ctx, sessionID, messageID)
	if closeRead != nil {
		defer closeRead()
	}
	if err != nil {
		return err
	}
	if saved == nil || recheck == nil || closeRead == nil || saved.ID != messageID || saved.SessionID != sessionID ||
		saved.Role != "assistant" || !saved.IsCompleted || saved.AgentID != scope.AgentID || saved.Content != answer ||
		saved.SourceLineage.RequireComplete() != nil {
		return errors.New("IM completed reply changed")
	}

	content := formatIMOutboundAnswerOrFallback(ctx, saved.Content, resolver)
	// This rechecks the original body, current owner, live agent/KB grants and
	// source revisions after storage resolution and immediately before delivery.
	if err := recheck(); err != nil {
		return err
	}
	return adapter.SendReply(ctx, msg, &ReplyMessage{Content: content, IsFinal: true})
}
