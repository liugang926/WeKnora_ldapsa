//go:build sqlite_fts5

package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

// The queue payloads, originals and producer receipts come from the real
// committed-history/DocumentProcess/post pipeline; no assistant proof is made
// by these tests. Only the already controlled fixture model is used.
func historyActualQueuedTask(t *testing.T, f *historyVectorActualFixture, kind, childID string) *asynq.Task {
	t.Helper()
	for _, task := range f.m.f.queue.tasks {
		if task.Type() != kind {
			continue
		}
		var header struct {
			KnowledgeID string `json:"knowledge_id"`
		}
		require.NoError(t, json.Unmarshal(task.Payload(), &header))
		if header.KnowledgeID == childID {
			return task
		}
	}
	t.Fatalf("real queue is missing %s for the produced child", kind)
	return nil
}

func historyActualRunEnrichmentEntry(t *testing.T, f *historyVectorActualFixture, ctx context.Context, task *asynq.Task) error {
	t.Helper()
	switch task.Type() {
	case types.TypeKnowledgePostProcess:
		return f.m.f.post.Handle(ctx, task)
	case types.TypeSummaryGeneration:
		return f.m.f.s.ProcessSummaryGeneration(ctx, task)
	case types.TypeQuestionGeneration:
		return f.m.f.s.ProcessQuestionGeneration(ctx, task)
	default:
		t.Fatalf("unexpected actual worker entry %s", task.Type())
		return nil
	}
}

func historyActualReadLeases(t *testing.T, f *historyVectorActualFixture) int64 {
	t.Helper()
	var count int64
	require.NoError(t, f.m.f.db.Table("original_body_leases").Where("kind='read' AND released_at_ms IS NULL AND expires_at_ms>?", time.Now().UnixMilli()).Count(&count).Error)
	return count
}

func TestHistoryKnowledgeTaskActualSQLFreshSummaryQuestionAndOwnedCleanup(t *testing.T) {
	f := newHistoryVectorActualFixture(t, historyVectorActualOptions{Questions: true})
	for _, child := range f.children {
		historyActualQueuedTask(t, f, types.TypeSummaryGeneration, child.Knowledge.ID)
		historyActualQueuedTask(t, f, types.TypeQuestionGeneration, child.Knowledge.ID)
		var current types.Knowledge
		require.NoError(t, f.m.f.db.Where("id=?", child.Knowledge.ID).Take(&current).Error)
		require.Equal(t, types.ParseStatusCompleted, current.ParseStatus)
		var chunks []*types.Chunk
		require.NoError(t, f.m.f.db.Where("knowledge_id=? AND chunk_type=?", child.Knowledge.ID, types.ChunkTypeText).Find(&chunks).Error)
		require.NotEmpty(t, chunks)
		foundQuestions := false
		for _, chunk := range chunks {
			metadata, err := chunk.DocumentMetadata()
			require.NoError(t, err)
			if metadata == nil || len(metadata.GeneratedQuestions) == 0 {
				continue
			}
			foundQuestions = true
			require.Equal(t, chunk.ContentRevision, metadata.GeneratedQuestionsRevision)
			for _, question := range metadata.GeneratedQuestions {
				require.NotEmpty(t, question.ID)
				require.Equal(t, "What is the actual body of this document?", question.Question,
					"persist the controlled question provider's actual response")
				require.True(t, metadata.IsQuestionCurrent(question, chunk.ContentRevision))
			}
		}
		require.True(t, foundQuestions, "the actual question producer must persist nonempty questions")
	}
	// Two actual children must reach both summary and question providers, each
	// with its newly restored real original guard, from fresh Background ctx.
	require.GreaterOrEqual(t, len(f.models.enrichmentGuards), 4)
	for _, guard := range f.models.enrichmentGuards {
		require.ErrorIs(t, guard.Context().Err(), context.Canceled)
		require.Error(t, guard.Recheck(), "completed worker guards must not remain reusable")
	}
	require.Zero(t, f.activeLeases(t, ""))
	require.Zero(t, historyActualReadLeases(t, f))
}

func TestHistoryKnowledgeTaskActualSQLChangedOriginalRefusesBeforeModel(t *testing.T) {
	for _, mode := range []string{"source-revoked", "original-body-changed"} {
		t.Run(mode, func(t *testing.T) {
			f := newHistoryVectorActualFixture(t, historyVectorActualOptions{Questions: true, PauseEnrichment: true})
			childID := f.children[0].Knowledge.ID
			tasks := []*asynq.Task{
				historyActualQueuedTask(t, f, types.TypeKnowledgePostProcess, childID),
				historyActualQueuedTask(t, f, types.TypeSummaryGeneration, childID),
				historyActualQueuedTask(t, f, types.TypeQuestionGeneration, childID),
			}
			switch mode {
			case "source-revoked":
				f.m.allow.Store(false)
			case "original-body-changed":
				// This mutation does not create an admission. The existing real
				// receipt/body/version must fail, even with source grants intact.
				require.NoError(t, f.m.f.db.Model(&types.Message{}).Where("id=?", f.questions[0].ID).Update("content", "unadmitted replacement of an original question").Error)
			}
			beforeCalls := f.m.f.model.calls
			beforeLookups := len(f.models.enrichmentGuards)
			for _, task := range tasks {
				require.Error(t, historyActualRunEnrichmentEntry(t, f, context.Background(), task), task.Type())
				require.Equal(t, beforeCalls, f.m.f.model.calls, "refused originals must never reach any model")
				require.Len(t, f.models.enrichmentGuards, beforeLookups, "refused originals must stop before model lookup")
				require.Zero(t, f.activeLeases(t, ""), "failed opens must release physical leases")
				require.Zero(t, historyActualReadLeases(t, f), "failed opens must release original body leases")
			}
		})
	}
}

func TestHistoryKnowledgeTaskActualSQLClosesOnlyItsAcquiredGuard(t *testing.T) {
	f := newHistoryVectorActualFixture(t, historyVectorActualOptions{Questions: true, PauseEnrichment: true})
	proof := f.children[0].Proof
	request, err := f.messages.materialStore.KnowledgeHistoryMaterialRequest(f.base, proof)
	require.NoError(t, err)
	require.NotNil(t, request)
	borrowed, err := f.messages.beginMessageMaterialRead(f.base, *request)
	require.NoError(t, err)
	defer borrowed.Close()
	caller := context.WithValue(borrowed.Context(), historyMaterialGuardContextKey{}, borrowed)
	worker, closeWorker, err := f.m.f.s.BeginKnowledgeHistoryTaskMaterial(caller, proof)
	require.NoError(t, err)
	owned := worker.Value(historyMaterialGuardContextKey{}).(*MessageMaterialGuard)
	require.NotSame(t, borrowed, owned)
	require.Equal(t, caller.Value(types.CallerContextKey), worker.Value(types.CallerContextKey))
	closeWorker()
	require.ErrorIs(t, owned.Context().Err(), context.Canceled)
	require.NoError(t, borrowed.Context().Err())
	require.NoError(t, borrowed.Recheck(), "worker cleanup must not close its caller's sibling guard")
	borrowed.Close()
	beforeCalls := f.m.f.model.calls
	for _, kind := range []string{types.TypeKnowledgePostProcess, types.TypeSummaryGeneration, types.TypeQuestionGeneration} {
		task := historyActualQueuedTask(t, f, kind, proof.KnowledgeID)
		require.Error(t, historyActualRunEnrichmentEntry(t, f, caller, task), "a closed original guard context must stop the entry")
	}
	require.Equal(t, beforeCalls, f.m.f.model.calls)
	require.Zero(t, f.activeLeases(t, ""))
	require.Zero(t, historyActualReadLeases(t, f))
	// A partial authenticated context is not a fresh queue actor. It must
	// never be replaced with the immutable history owner to make the read work.
	partial := types.WithPrincipal(context.Background(), types.Principal{Type: types.PrincipalWebUser, ID: "another-current-principal"})
	_, closePartial, err := f.m.f.s.BeginKnowledgeHistoryTaskMaterial(partial, proof)
	require.Error(t, err)
	closePartial()
	require.Zero(t, f.activeLeases(t, ""))
	// WithExecutionTenant captures a zero Caller in a fresh context. It is
	// not authenticated and must never be accepted as the admission owner.
	zeroCaller := types.WithExecutionTenant(context.Background(), 7)
	captured, ok := zeroCaller.Value(types.CallerContextKey).(types.Caller)
	require.True(t, ok)
	require.Zero(t, captured.TenantID)
	require.Empty(t, captured.UserID)
	beforeZeroCalls, beforeZeroLookups := f.m.f.model.calls, len(f.models.enrichmentGuards)
	_, closeZero, err := f.m.f.s.BeginKnowledgeHistoryTaskMaterial(zeroCaller, proof)
	require.ErrorIs(t, err, types.ErrSourceLineageUnknown)
	require.NotNil(t, closeZero)
	closeZero()
	require.Equal(t, beforeZeroCalls, f.m.f.model.calls)
	require.Len(t, f.models.enrichmentGuards, beforeZeroLookups)
	require.Zero(t, f.activeLeases(t, ""))
	require.Zero(t, historyActualReadLeases(t, f))
}
