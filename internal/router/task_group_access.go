package router

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
)

// Enrichment is queued separately from document admission. Recheck each stage
// so a grant revoked while a task waits cannot authorize another model call,
// chunk write or index update. Both Redis workers and Lite use this wrapper.
func taskGroupAccessMiddleware(
	groupAccess interfaces.GroupAccessService,
	kbs interfaces.KnowledgeBaseService,
	knowledgeRepo interfaces.KnowledgeRepository,
	chunkRepo interfaces.ChunkRepository,
	tracker service.SpanTracker,
) asynq.MiddlewareFunc {
	return func(next asynq.Handler) asynq.Handler {
		return asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
			switch task.Type() {
			case types.TypeChunkExtract, types.TypeDataTableSummary, types.TypeImageMultimodal,
				types.TypeKnowledgePostProcess, types.TypeQuestionGeneration, types.TypeSummaryGeneration,
				types.TypeKnowledgeAutoTag, types.TypeKnowledgeBaseProfile:
			default:
				return next.ProcessTask(ctx, task)
			}
			var payload struct {
				TenantID        uint64              `json:"tenant_id"`
				KnowledgeBaseID string              `json:"knowledge_base_id"`
				KnowledgeID     string              `json:"knowledge_id"`
				ChunkID         string              `json:"chunk_id"`
				Initiator       types.TaskInitiator `json:"initiator"`
				Attempt         int                 `json:"attempt"`
			}
			if err := json.Unmarshal(task.Payload(), &payload); err != nil {
				return fmt.Errorf("invalid enrichment task: %v: %w", err, asynq.SkipRetry)
			}
			ctx = types.WithTaskAuthorization(ctx, payload.TenantID, payload.Initiator)
			if groupAccess == nil {
				return next.ProcessTask(ctx, task)
			}
			kbID := payload.KnowledgeBaseID
			var taskKnowledge *types.Knowledge
			if payload.ChunkID != "" && payload.KnowledgeID == "" {
				chunk, err := chunkRepo.GetChunkByID(ctx, payload.TenantID, payload.ChunkID)
				if err != nil {
					return err
				}
				if chunk == nil || chunk.TenantID != payload.TenantID ||
					(kbID != "" && chunk.KnowledgeBaseID != kbID) {
					return fmt.Errorf("enrichment chunk binding changed: %w", asynq.SkipRetry)
				}
				kbID = chunk.KnowledgeBaseID
			}
			if payload.KnowledgeID != "" {
				knowledge, err := knowledgeRepo.GetKnowledgeByID(ctx, payload.TenantID, payload.KnowledgeID)
				if err != nil {
					return err
				}
				if knowledge == nil || knowledge.TenantID != payload.TenantID ||
					(kbID != "" && knowledge.KnowledgeBaseID != kbID) {
					return fmt.Errorf("enrichment document binding changed: %w", asynq.SkipRetry)
				}
				kbID = knowledge.KnowledgeBaseID
				taskKnowledge = knowledge
			}
			if payload.TenantID == 0 || kbID == "" {
				return fmt.Errorf("enrichment KB scope missing: %w", asynq.SkipRetry)
			}
			kb, err := kbs.GetKnowledgeBaseByID(ctx, kbID)
			if err != nil {
				return err
			}
			if kb == nil || kb.TenantID != payload.TenantID || kb.ID != kbID {
				return fmt.Errorf("enrichment KB binding changed: %w", asynq.SkipRetry)
			}
			permission, err := groupAccess.EffectivePermission(
				ctx, kb.TenantID, types.GroupResourceTypeKnowledgeBase, kb.ID,
				types.ResourceActionEdit, time.Now().UTC(),
			)
			if err != nil {
				return err
			}
			if !permission.Allowed {
				if taskKnowledge != nil && (tracker == nil ||
					tracker.LatestAttempt(ctx, taskKnowledge.ID) <= payload.Attempt) {
					dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
					defer cancel()
					if err := knowledgeRepo.FailKnowledgeAuthorization(
						dctx, taskKnowledge, task.Type() == types.TypeSummaryGeneration,
					); err != nil {
						return err
					}
					if tracker != nil && payload.Attempt > 0 {
						tracker.FinalizeAttempt(dctx, taskKnowledge.ID, payload.Attempt, types.SpanStatusFailed,
							nil, "AUTHORIZATION_REVOKED", "processing permission revoked")
					}
				}
				return fmt.Errorf("%w: enrichment permission revoked (%s): %w",
					service.ErrResourceAccessDenied, permission.Reason, asynq.SkipRetry)
			}
			return next.ProcessTask(ctx, task)
		})
	}
}
