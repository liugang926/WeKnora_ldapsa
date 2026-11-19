// SPDX-License-Identifier: Apache-2.0

package im

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/google/uuid"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
)

// This snapshot is accounting metadata, never an authorization principal.
type (
	turnUsage    struct{ source, actorID, eventKey string }
	turnUsageKey struct{}
)

func dingTalkEventKey(channelID, upstreamID string) string {
	if channelID == "" || upstreamID == "" {
		return ""
	}
	sum := sha256.Sum256([]byte("dingtalk\x00" + channelID + "\x00" + upstreamID))
	return hex.EncodeToString(sum[:])
}

// Snapshot the binding once, before queueing, so a later rebind cannot change
// who owns the reply. A failed lookup leaves the question in the unknown bucket.
func (s *Service) withTurnUsage(ctx context.Context, ch *IMChannel, msg *IncomingMessage) context.Context {
	if ch.Platform != string(PlatformDingtalk) || msg.Platform != PlatformDingtalk {
		return ctx
	}
	origin := turnUsage{source: ch.Platform, eventKey: dingTalkEventKey(ch.ID, msg.MessageID)}
	if origin.eventKey == "" {
		// No upstream ID: identify this one delivery without merging two legitimate questions.
		origin.eventKey = uuid.NewString()
	}
	if s.db != nil && msg.SenderCorpID != "" && msg.SenderStaffID != "" && msg.UserID == msg.SenderStaffID {
		var ids []string
		err := s.db.WithContext(ctx).Model(&types.DingTalkIdentity{}).
			Where("corp_id = ? AND dingtalk_user_id = ? AND status = ? AND user_id IS NOT NULL",
				msg.SenderCorpID, msg.SenderStaffID, types.DingTalkIdentityActive).
			Limit(2).Pluck("user_id", &ids).Error
		if err != nil {
			logger.Warnf(ctx, "[IM] Could not resolve usage attribution; retaining unattributed turn")
		} else if len(ids) == 1 {
			origin.actorID = ids[0]
		}
	}
	return context.WithValue(ctx, turnUsageKey{}, origin)
}

func stampIMTurn(ctx context.Context, m *types.Message) *types.Message {
	if origin, ok := ctx.Value(turnUsageKey{}).(turnUsage); ok {
		m.UsageSource = origin.source
		m.UsageActorUserID = origin.actorID
		m.UsageEventKey = origin.eventKey
	}
	return m
}

// Record the accepted question before queueing or preparing attachments. It holds
// no transcript and stays visible across soft deletion just like message usage;
// physical deletion of its session purges this record as well.
// A duplicate delivery keeps the first sender snapshot and must not run QA again.
func (s *Service) recordTurnSubmission(ctx context.Context, sessionID string) bool {
	origin, ok := ctx.Value(turnUsageKey{}).(turnUsage)
	if !ok || s.db == nil {
		return false
	}
	result := s.db.WithContext(ctx).Exec(`INSERT INTO im_usage_submissions
		(source, session_id, event_key, actor_user_id, created_at) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(event_key) DO NOTHING`, origin.source, sessionID, origin.eventKey, origin.actorID, time.Now().UTC())
	if result.Error != nil {
		// Messages retain the same attribution and key as a fallback if this
		// ancillary write failed; accounting must never block the conversation.
		logger.Warnf(ctx, "[IM] Could not record question before queueing")
		return false
	}
	return result.RowsAffected == 0
}
