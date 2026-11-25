package service

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// flakyFinalizeRepo fails the first `failures` FinalizeSubtask calls.
type flakyFinalizeRepo struct {
	interfaces.KnowledgeRepository
	mu        sync.Mutex
	failures  int
	calls     int
	successes int
}

func (r *flakyFinalizeRepo) FinalizeSubtask(context.Context, string, int) (int, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.calls <= r.failures {
		return 0, false, errors.New("serialization failure")
	}
	r.successes++
	return 0, false, nil
}

// The release is the only thing that drains a finished task's slot, so one
// transient DB error must not strand the row in "finalizing".
func TestFinalizeSubtaskDetachedRetriesTransientFailure(t *testing.T) {
	repo := &flakyFinalizeRepo{failures: subtaskSlotReleaseAttempts - 1}

	err := finalizeSubtaskDetached(context.Background(), repo, "k-1", "summary", nil, false, true, 0)
	require.NoError(t, err)

	assert.Equal(t, subtaskSlotReleaseAttempts, repo.calls)
	assert.Equal(t, 1, repo.successes)
}

func TestReleaseSubtaskSlotGivesUpAfterBoundedRetries(t *testing.T) {
	repo := &flakyFinalizeRepo{failures: subtaskSlotReleaseAttempts + 5}

	err := releaseSubtaskSlot(context.Background(), repo, "k-1", 0)

	require.Error(t, err)
	assert.Equal(t, subtaskSlotReleaseAttempts, repo.calls)
}

// The release rides a detached context: a worker whose own context is
// already cancelled (shutdown) must still drain its slot.
func TestReleaseSubtaskSlotIgnoresCallerCancellation(t *testing.T) {
	repo := &flakyFinalizeRepo{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.NoError(t, releaseSubtaskSlot(ctx, repo, "k-1", 0))
	assert.Equal(t, 1, repo.successes)
}

type shortfallReleaseRepo struct {
	wikiEnqueueFailureKnowledgeRepo
	flaky flakyFinalizeRepo
}

func (r *shortfallReleaseRepo) FinalizeSubtask(ctx context.Context, id string, attempt int) (int, bool, error) {
	if _, _, err := r.flaky.FinalizeSubtask(ctx, id, attempt); err != nil {
		return 0, false, err
	}
	return r.wikiEnqueueFailureKnowledgeRepo.FinalizeSubtask(ctx, id, attempt)
}

// Post-process releases one slot per subtask it planned but could not
// enqueue. A release that fails for good must not abandon the rest: each
// remaining slot has no other owner.
func TestKnowledgePostProcessShortfallReleaseContinuesPastFailure(t *testing.T) {
	// NEO4J off: the two planned graph-extract slots are never enqueued.
	t.Setenv("NEO4J_ENABLE", "false")
	const knowledgeID = "knowledge-shortfall"
	const kbID = "kb-shortfall"
	repo := &shortfallReleaseRepo{
		wikiEnqueueFailureKnowledgeRepo: wikiEnqueueFailureKnowledgeRepo{knowledge: &types.Knowledge{
			ID: knowledgeID, TenantID: 7, KnowledgeBaseID: kbID, ParseStatus: types.ParseStatusProcessing,
		}},
		// The first release exhausts its retries; the second succeeds.
		flaky: flakyFinalizeRepo{failures: subtaskSlotReleaseAttempts},
	}
	chunk := func(id, content string) *types.Chunk {
		return &types.Chunk{
			ID: id, TenantID: 7, KnowledgeID: knowledgeID, KnowledgeBaseID: kbID,
			ChunkType: types.ChunkTypeText, Content: content,
		}
	}
	queue := &wikiEnqueueFailureTaskQueue{}
	kb := &types.KnowledgeBase{
		ID: kbID, TenantID: 7,
		IndexingStrategy: types.IndexingStrategy{GraphEnabled: true},
		ExtractConfig:    &types.ExtractConfig{Enabled: true},
	}
	svc := &KnowledgePostProcessService{knowledgeRepo: repo, taskEnqueuer: queue}
	f := attachPostProcessOriginalInputs(t, svc, &repo.wikiEnqueueFailureKnowledgeRepo, kb, []*types.Chunk{
		chunk("c-1", "Contract between Alice and Bob."),
		chunk("c-2", "Payment is due in thirty days."),
	})

	require.NoError(t, svc.Handle(f.build, postProcessOriginalTask(t, f)))
	assert.Equal(t, 3, repo.expectedSubtasks, "summary plus two graph slots")
	assert.Equal(t, []string{types.TypeSummaryGeneration}, queue.taskTypes)
	assertPostProcessQueuedOriginals(t, f, queue)
	assert.Equal(t, subtaskSlotReleaseAttempts+1, repo.flaky.calls)
	assert.Equal(t, 1, repo.flaky.successes, "the second shortfall slot is still released")
	var stored types.Knowledge
	require.NoError(t, f.db.Where("id = ?", knowledgeID).Take(&stored).Error)
	assert.Equal(t, 2, stored.PendingSubtasksCount, "the queued summary and the failed release retain their slots")
}

func TestPostProcessSummarySkipsMissingChunkReceipt(t *testing.T) {
	f := newEnrichmentTestFixture(t, false)
	deleted := f.db.Exec("DELETE FROM knowledge_input_material_receipts WHERE row_kind = 'chunk' AND row_id = ?",
		f.chunks[0].ID)
	require.NoError(t, deleted.Error)
	require.EqualValues(t, 1, deleted.RowsAffected)

	queued := f.post.enqueueSummaryGenerationTask(f.build, types.KnowledgePostProcessPayload{
		TenantID: f.k.TenantID, KnowledgeBaseID: f.k.KnowledgeBaseID, KnowledgeID: f.k.ID,
	}, 0)
	require.False(t, queued)
	require.Empty(t, f.queue.tasks)
}
