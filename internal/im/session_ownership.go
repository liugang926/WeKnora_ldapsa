package im

import (
	"context"
	"errors"
	"fmt"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Validate ownership through the exact durable mapping without changing history.
// New IM sessions receive their owner atomically in CreateSession.
func (s *Service) ensureChannelSessionOwner(ctx context.Context, mapping *ChannelSession) error {
	scope, ok := types.IMChannelScopeFromContext(ctx)
	if !ok || mapping == nil || scope.OwnerID == "" || mapping.TenantID != scope.TenantID || mapping.IMChannelID != scope.ChannelID || mapping.AgentID != scope.AgentID {
		return fmt.Errorf("invalid IM session owner scope")
	}
	if err := scope.Check(ctx, scope.TenantID, types.GroupResourceTypeAgent, scope.AgentID, types.ResourceActionUse); err != nil {
		return err
	}
	fresh, err := s.GetChannelByID(scope.ChannelID)
	if err != nil {
		return err
	}
	if imChannelSessionOwner(fresh, &IncomingMessage{UserID: mapping.UserID, ChatID: mapping.ChatID, ThreadID: mapping.ThreadID}) != scope.OwnerID {
		return fmt.Errorf("IM session mapping belongs to another subject")
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current ChannelSession
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND tenant_id = ? AND im_channel_id = ? AND platform = ? AND user_id = ? AND chat_id = ? AND thread_id = ? AND session_id = ? AND agent_id = ?", mapping.ID, mapping.TenantID, mapping.IMChannelID, mapping.Platform, mapping.UserID, mapping.ChatID, mapping.ThreadID, mapping.SessionID, mapping.AgentID).First(&current).Error; err != nil {
			return err
		}
		var count int64
		if err := tx.Model(&ChannelSession{}).Where("session_id = ?", mapping.SessionID).Count(&count).Error; err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("IM session mapping is not unique")
		}
		var session types.Session
		if err := tx.Select("id", "tenant_id", "user_id").Where("id = ? AND tenant_id = ?", mapping.SessionID, mapping.TenantID).First(&session).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if session.UserID == scope.OwnerID {
			return nil
		}
		if session.UserID != "" {
			return fmt.Errorf("IM session belongs to another owner")
		}
		// Never adopt an existing unowned session. Changing only sessions.user_id
		// invalidates immutable message receipts and their owner revisions. Even an
		// apparently empty session may have retained or concurrently written history.
		return fmt.Errorf("legacy IM session has no owner; automatic adoption denied")
	})
}
