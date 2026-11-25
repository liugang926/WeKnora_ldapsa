package service

import (
	"context"
	"encoding/json"
	"testing"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

type supersedingGraphChat struct {
	graphReplyChat
	duringCall func()
}

func (c supersedingGraphChat) Chat(
	ctx context.Context, messages []chat.Message, opts *chat.ChatOptions,
) (*types.ChatResponse, error) {
	c.duringCall()
	return c.graphReplyChat.Chat(ctx, messages, opts)
}

func TestChunkExtractCompletionDoesNotDrainNewParseAttempt(t *testing.T) {
	svc, store, db, _, ctx := knowledgeInputServiceFixture(t)
	require.NoError(t, db.Exec(spanTrackerTestDDL).Error)
	kb := &types.KnowledgeBase{
		ID: "graph-kb", TenantID: 7, Name: "attempt fixture", SummaryModelID: "graph-reply",
		ExtractConfig: &types.ExtractConfig{Enabled: true},
	}
	require.NoError(t, store.CreateKnowledgeBaseWithInput(apprepo.WithIndependentKnowledgeInput(ctx), kb))
	_, proof := graphAdmittedInput(t, db, store, types.NameSpace{KnowledgeBase: kb.ID, Knowledge: "doc"})
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", "doc").Updates(map[string]interface{}{
		"parse_status": types.ParseStatusFinalizing, "pending_subtasks_count": 1,
	}).Error)
	lineage, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	template, err := types.BindGraphPromptTemplateOrigin(
		&types.PromptTemplateStructured{Description: "extract"},
		lineage,
	)
	require.NoError(t, err)
	tracker := NewSpanTracker(apprepo.NewKnowledgeSpanRepository(db), db)
	_, first, err := tracker.OpenAttempt(ctx, "doc", "")
	require.NoError(t, err)
	require.Equal(t, 1, first)
	model := supersedingGraphChat{
		graphReplyChat: graphReplyChat{resp: &types.ChatResponse{
			Content: "No entities to extract.", FinishReason: "stop",
		}},
		duringCall: func() {
			_, next, err := tracker.OpenAttempt(ctx, "doc", "")
			require.NoError(t, err)
			require.Equal(t, 2, next)
			require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", "doc").Updates(map[string]interface{}{
				"parse_status": types.ParseStatusProcessing, "pending_subtasks_count": 0,
			}).Error)
			seeded, err := svc.repo.SetFinalizing(ctx, "doc", 2)
			require.NoError(t, err)
			require.True(t, seeded)
		},
	}
	extractor := &ChunkExtractService{
		buildInputs: store, template: template,
		modelService:  &stubModelService{chatModel: &model},
		knowledgeRepo: svc.repo, spanTracker: tracker,
	}
	payload, err := json.Marshal(types.ExtractChunkPayload{
		TenantID: kb.TenantID, ChunkID: "doc-chunk", KnowledgeID: "doc",
		ModelID: kb.SummaryModelID, Attempt: first, OriginalInput: proof,
	})
	require.NoError(t, err)
	require.NoError(t, extractor.Handle(ctx, asynq.NewTask(types.TypeChunkExtract, payload)))
	current, err := svc.repo.GetKnowledgeByID(ctx, kb.TenantID, "doc")
	require.NoError(t, err)
	require.Equal(t, 2, current.PendingSubtasksCount,
		"the old handler completed after the new attempt seeded its slots")
	require.Equal(t, types.ParseStatusFinalizing, current.ParseStatus)
}

type supersedingSummaryQueue struct{ duringCall func() }

func (q supersedingSummaryQueue) Enqueue(task *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	if task.Type() == types.TypeSummaryGeneration {
		q.duringCall()
	}
	return &asynq.TaskInfo{ID: "queued", Type: task.Type()}, nil
}

func TestPostProcessShortfallUsesResolvedParseAttempt(t *testing.T) {
	t.Setenv("NEO4J_ENABLE", "false")
	knowledge := &types.Knowledge{
		ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", Type: "file", ParseStatus: types.ParseStatusProcessing,
	}
	kb := &types.KnowledgeBase{
		ID: "kb", TenantID: 7, IndexingStrategy: types.IndexingStrategy{GraphEnabled: true},
		ExtractConfig: &types.ExtractConfig{Enabled: true},
	}
	chunks := []*types.Chunk{
		{ID: "c1", ChunkType: types.ChunkTypeText, Content: "Alice pays Bob."},
		{ID: "c2", ChunkType: types.ChunkTypeText, Content: "Payment is due."},
	}
	post := &KnowledgePostProcessService{}
	f := attachPostProcessOriginalInputs(t, post, &wikiEnqueueFailureKnowledgeRepo{knowledge: knowledge}, kb, chunks)
	post.knowledgeRepo = f.s.repo
	ctx, db := f.build, f.db
	tracker := postProcessOriginalSpanTracker(t, db)
	post.spanTracker = tracker
	_, first, err := tracker.OpenAttempt(ctx, "doc", "")
	require.NoError(t, err)
	require.Equal(t, 1, first)
	queued := false
	post.taskEnqueuer = supersedingSummaryQueue{duringCall: func() {
		queued = true
		_, attempt, err := tracker.OpenAttempt(ctx, "doc", "")
		require.NoError(t, err)
		require.Equal(t, 2, attempt)
		require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", "doc").Updates(map[string]interface{}{
			"parse_status": types.ParseStatusProcessing, "pending_subtasks_count": 0,
		}).Error)
		seeded, err := f.s.repo.SetFinalizing(ctx, "doc", 2)
		require.NoError(t, err)
		require.True(t, seeded)
	}}
	// A delivery without an explicit attempt still owns attempt one's slots.
	task := postProcessOriginalTask(t, f)
	require.NoError(t, post.Handle(ctx, task))
	require.True(t, queued)
	current, err := f.s.repo.GetKnowledgeByID(ctx, 7, "doc")
	require.NoError(t, err)
	require.Equal(t, 2, current.PendingSubtasksCount, "un-enqueued slots still belong to the resolved old attempt")
	require.Equal(t, types.ParseStatusFinalizing, current.ParseStatus)
	oldStatus, code, _ := rootSpan(t, db, "doc", first)
	require.Equal(t, types.SpanStatusCancelled, oldStatus)
	require.Equal(t, "ATTEMPT_SUPERSEDED", code)
	newStatus, _, _ := rootSpan(t, db, "doc", 2)
	require.Equal(t, types.SpanStatusRunning, newStatus)
}
