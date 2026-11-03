package service

import (
	"context"
	"fmt"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"time"
)

// waitForEvaluationKnowledge reads persisted state; indexing alone does not
// satisfy the strict snapshot reader while background enrichment is pending.
func waitForEvaluationKnowledge(ctx context.Context, repo interfaces.KnowledgeRepository, tenant uint64, kbID, id string, timeout, interval time.Duration) error {
	if ctx == nil || repo == nil || tenant == 0 || kbID == "" || id == "" || timeout <= 0 || interval <= 0 {
		return fmt.Errorf("invalid evaluation readiness scope")
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := waitCtx.Err(); err != nil {
			return fmt.Errorf("evaluation knowledge readiness: %w", err)
		}
		k, err := repo.GetKnowledgeByID(waitCtx, tenant, id)
		if err != nil {
			return fmt.Errorf("read evaluation knowledge readiness: %w", err)
		}
		if k == nil || k.ID != id || k.TenantID != tenant || k.KnowledgeBaseID != kbID || k.DeletedAt.Valid {
			return fmt.Errorf("evaluation knowledge is unavailable or changed")
		}
		if k.EnableStatus != "enabled" {
			return fmt.Errorf("evaluation knowledge is disabled")
		}
		switch k.ParseStatus {
		case types.ParseStatusCompleted:
			if k.PendingSubtasksCount != 0 {
				return fmt.Errorf("evaluation knowledge completed without readable state")
			}
			return nil
		case types.ParseStatusPending, types.ParseStatusProcessing, types.ParseStatusFinalizing:
			// The synchronous index exists, but enrichment is not finalized yet.
		default:
			return fmt.Errorf("evaluation knowledge processing ended in state %q", k.ParseStatus)
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("evaluation knowledge readiness: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}
