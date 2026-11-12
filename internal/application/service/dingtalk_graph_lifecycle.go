package service

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/tracing/langfuse"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
)

type dingTalkGraphRepository interface {
	SeedDingTalkGraphFinalizing(context.Context,
		string,
		int,
		[]types.ExtractChunkPayload,
		*types.TaskPendingOp) (bool,
		error)
	DingTalkGraphState(context.Context, types.ExtractChunkPayload) (string, *types.GraphData, string, error)
	PrepareDingTalkGraph(context.Context, types.ExtractChunkPayload, *types.GraphData) (*types.GraphData, error)
	MarkDingTalkGraphOutcome(context.Context, types.ExtractChunkPayload, bool, string, string) error
}

func (s *KnowledgePostProcessService) planDingTalkGraphs(
	ctx context.Context,
	kb *types.KnowledgeBase,
	id string,
	tenant uint64,
	attempt int,
	chunks []*types.Chunk) ([]types.ExtractChunkPayload,
	error,
) {
	var tasks []types.ExtractChunkPayload
	if s.knowledgeInputs == nil {
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	for i, c := range chunks {
		snapshot,
			err := s.knowledgeInputs.CaptureKnowledgeBuildInputs(
			ctx,
			types.KnowledgeBuildInputCaptureRequest{
				Projection:      types.KnowledgeBuildProjectionGraph,
				TenantID:        tenant,
				KnowledgeBaseID: kb.ID,
				KnowledgeID:     id,
				ChunkIDs: []string{
					c.ID,
				},
				Expected: repository.KnowledgeBuildInputProofFromContext(
					ctx),
			})
		if err != nil {
			return nil, err
		}
		if snapshot == nil || snapshot.Proof == nil {
			return nil, types.ErrGraphSourceReceiptUnknown
		}
		p := types.ExtractChunkPayload{
			OriginalInput: snapshot.Proof,
			TenantID:      tenant,
			KnowledgeID:   id,
			ChunkID:       c.ID,
			Attempt:       attempt,
			ChunkIndex:    i,
			ModelID:       kb.SummaryModelID,
			Initiator: types.TaskInitiatorFromContext(
				ctx),
		}
		langfuse.InjectTracing(ctx, &p)
		p.GraphTaskID = repository.DingTalkGraphTaskID(p)
		tasks = append(tasks, p)
	}
	return tasks, nil
}

func enqueueDingTalkGraph(ctx context.Context, queue interfaces.TaskEnqueuer, p types.ExtractChunkPayload) error {
	if queue == nil {
		return errors.New("DingTalk graph queue unavailable")
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	// Payload is immutable; Unique protects an active extraction, but expires
	// independently of archived task IDs so SQL recovery cannot be wedged by one.
	uniqueFor := 35 * time.Minute
	if p.GraphSettlementOnly {
		uniqueFor = time.Minute
	}
	task,
		err := newKnowledgeQueueTask(ctx,
		nil,
		types.TypeChunkExtract,
		raw,
		asynq.Queue(types.QueueGraph),
		asynq.MaxRetry(3),
		asynq.Timeout(30*time.Minute),
		asynq.Unique(uniqueFor))
	if err != nil {
		return err
	}
	_, err = queue.Enqueue(task)
	if errors.Is(err, asynq.ErrDuplicateTask) {
		return nil
	}
	return err
}

// RecoverDingTalkGraphs redelivers durable pending graph tasks.
func RecoverDingTalkGraphs(ctx context.Context,
	store *repository.KnowledgeInputProvenanceStore,
	queue interfaces.TaskEnqueuer,
	id string) (int,
	error,
) {
	if store == nil || queue == nil {
		return 0, nil
	}
	rows, err := store.PendingDingTalkGraphs(ctx, id)
	if err != nil {
		return 0, err
	}
	count := 0
	var errs []error
	for _, row := range rows {
		var saved repository.DingTalkGraphRecord
		if json.Unmarshal(row.Payload,
			&saved) != nil ||
			saved.Payload.OriginalInput == nil ||
			saved.Payload.GraphTaskID != row.DedupKey ||
			row.TenantID != saved.Payload.TenantID ||
			row.ScopeID != saved.Payload.KnowledgeID {

			errs = append(errs, types.ErrGraphSourceReceiptUnknown, store.NoteDingTalkGraphDelivery(ctx, row.ID, false))
			continue
		}
		p := saved.Payload
		p.GraphSettlementOnly = row.Op == "result_committed" ||
			row.Op == "terminal_failed" ||
			row.Op == "result_skipped"
		// Full input and current actor verification is repeated by the worker and
		// inside every write transaction. A queue delivery never creates evidence.
		taskCtx := types.WithTaskAuthorization(ctx, p.TenantID, p.Initiator)
		taskCtx, e := repository.RestoreKnowledgeBuildActorIfRequired(taskCtx, store, p.OriginalInput)
		if e == nil {
			e = store.RecheckKnowledgeBuildInputs(taskCtx, p.OriginalInput)
		}
		if e == nil {
			e = enqueueDingTalkGraph(taskCtx, queue, p)
		}
		// Rotate failed rows as well, so a stale/deleted document cannot starve the
		// bounded recovery batch. This changes no result or settlement evidence.
		noteErr := store.NoteDingTalkGraphDelivery(ctx, row.ID, e == nil)
		if e != nil {
			errs = append(errs, e)
			continue
		}
		if noteErr != nil {
			errs = append(errs, noteErr)
			continue
		}
		count++
	}
	return count, errors.Join(errs...)
}
