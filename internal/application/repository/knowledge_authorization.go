package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
)

// FailKnowledgeAuthorization records a terminal permission failure with an
// optimistic binding/version check. A rejected old task must never alter a
// document that was moved, deleted, or submitted again while it waited.
func (r *knowledgeRepository) FailKnowledgeAuthorization(
	ctx context.Context, before *types.Knowledge, summary bool,
) error {
	if before == nil || before.ID == "" || before.KnowledgeBaseID == "" || before.TenantID == 0 {
		return fmt.Errorf("authorization failure scope missing")
	}
	changes := map[string]any{}
	switch before.ParseStatus {
	case types.ParseStatusPending, types.ParseStatusProcessing, types.ParseStatusFinalizing:
		changes["parse_status"] = types.ParseStatusFailed
		changes["pending_subtasks_count"] = 0
		changes["error_message"] = "processing permission revoked"
	}
	if summary {
		changes["summary_status"] = types.SummaryStatusFailed
	}
	if len(changes) == 0 {
		return nil
	}
	changes["updated_at"] = time.Now().UTC().Truncate(time.Microsecond)
	result := r.db.WithContext(ctx).Model(&types.Knowledge{}).
		Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND parse_status = ? AND updated_at = ?",
			before.ID, before.TenantID, before.KnowledgeBaseID, before.ParseStatus, before.UpdatedAt).
		Updates(changes)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("knowledge changed during authorization failure")
	}
	return nil
}
