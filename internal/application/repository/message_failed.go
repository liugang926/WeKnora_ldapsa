package repository

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// MarkAgentReplyFailed records a terminal control on the saved assistant
// placeholder. Nothing from a failed producer's in-memory answer is written.
// The existing material receipt is advanced in the same transaction: PostgreSQL
// revisions also cover run_state, even though the body has not changed.
func (r *messageRepository) MarkAgentReplyFailed(ctx context.Context, sessionID, messageID, requestID string) error {
	owner := types.SessionOwnerIDFromContext(ctx)
	tenant, ok := types.TenantIDFromContext(ctx)
	if !ok || tenant == 0 || owner == "" || sessionID == "" || messageID == "" || requestID == "" {
		return types.ErrSourceLineageUnknown
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		owned := tx.Model(&types.Session{}).Select("id").
			Where("id=? AND tenant_id=? AND user_id=?", sessionID, tenant, owner)
		match := func() *gorm.DB {
			return tx.Model(&types.Message{}).
				Where("id=? AND session_id IN (?) AND request_id=? AND role='assistant'", messageID, owned, requestID)
		}
		var saved types.Message
		if err := match().Clauses(clause.Locking{Strength: "UPDATE"}).Take(&saved).Error; err != nil {
			return err
		}
		if saved.IsCompleted || saved.RunState != "" {
			return nil
		}

		saved.RunState = types.MessageRunStateFailed
		var origin *messageGenerationOrigin
		if r.materialStore != nil {
			bound, err := r.materialStore.WithMessageProducerInput(ctx, &saved, false)
			if err != nil {
				return err
			}
			origin, err = r.materialStore.beginMessageProducerInTx(tx.WithContext(bound), &saved, false)
			if err != nil {
				return err
			}
		}
		result := match().Where("is_completed=? AND COALESCE(run_state,'')=''", false).
			Update("run_state", types.MessageRunStateFailed)
		if result.Error != nil || result.RowsAffected == 0 {
			return result.Error
		}
		if origin != nil {
			return r.materialStore.sealMessageMaterialInTx(tx, origin, &saved)
		}
		return nil
	})
}
