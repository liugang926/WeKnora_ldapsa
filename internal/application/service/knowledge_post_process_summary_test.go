package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type optionalSummaryKnowledgeRepo struct {
	wikiEnqueueFailureKnowledgeRepo
	completionErr     error
	completionSkipped bool
}

func (r *optionalSummaryKnowledgeRepo) UpdateKnowledgeColumn(
	ctx context.Context, id string, column string, value interface{},
) error {
	return r.wikiEnqueueFailureKnowledgeRepo.UpdateKnowledgeColumn(ctx, id, column, value)
}

func (r *optionalSummaryKnowledgeRepo) CompleteProcessingWithoutSubtasks(ctx context.Context, id string) (bool, error) {
	if r.completionErr != nil {
		return false, r.completionErr
	}
	if r.completionSkipped {
		return false, nil
	}
	if r.KnowledgeRepository != nil {
		completed, err := r.KnowledgeRepository.CompleteProcessingWithoutSubtasks(ctx, id)
		if err != nil || !completed {
			return completed, err
		}
	}
	r.knowledge.ParseStatus = types.ParseStatusCompleted
	r.knowledge.SummaryStatus = types.SummaryStatusNone
	return true, nil
}

func TestKnowledgePostProcessOptionalSummary(t *testing.T) {
	t.Setenv("NEO4J_ENABLE", "true")
	for _, tt := range []struct {
		name      string
		summary   *bool
		question  bool
		graph     bool
		wiki      bool
		wantTasks []string
	}{
		{name: "legacy defaults generate summary", wantTasks: []string{types.TypeSummaryGeneration}},
		{
			name:      "explicitly enabled",
			summary:   processConfigBoolPtr(true),
			wantTasks: []string{types.TypeSummaryGeneration},
		},
		{name: "disabled completes without enrichment", summary: processConfigBoolPtr(false)},
		{
			name:      "disabled preserves question generation",
			summary:   processConfigBoolPtr(false),
			question:  true,
			wantTasks: []string{types.TypeQuestionGeneration},
		},
		{
			name:      "disabled preserves graph extraction",
			summary:   processConfigBoolPtr(false),
			graph:     true,
			wantTasks: []string{types.TypeChunkExtract},
		},
		{
			name:      "disabled preserves wiki generation",
			summary:   processConfigBoolPtr(false),
			wiki:      true,
			wantTasks: []string{types.TypeWikiIngest},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			knowledge := &types.Knowledge{
				ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", ParseStatus: types.ParseStatusProcessing,
			}
			require.NoError(t, knowledge.SetProcessOverrides(&types.KnowledgeProcessOverrides{
				SummaryEnabled: tt.summary,
			}))
			repo := &optionalSummaryKnowledgeRepo{
				wikiEnqueueFailureKnowledgeRepo: wikiEnqueueFailureKnowledgeRepo{knowledge: knowledge},
			}
			queue := &wikiEnqueueFailureTaskQueue{}
			kb := &types.KnowledgeBase{
				ID: "kb", TenantID: 7,
				IndexingStrategy: types.IndexingStrategy{
					VectorEnabled: true, GraphEnabled: tt.graph, WikiEnabled: tt.wiki,
				},
				QuestionGenerationConfig: &types.QuestionGenerationConfig{Enabled: tt.question},
				ExtractConfig:            &types.ExtractConfig{Enabled: tt.graph},
			}
			service := &KnowledgePostProcessService{
				knowledgeRepo: repo,
				kbService:     &wikiEnqueueFailureKBService{kb: kb},
				chunkRepo: &wikiEnqueueFailureChunkRepo{chunks: []*types.Chunk{{
					ID: "chunk", TenantID: 7, KnowledgeID: "doc", KnowledgeBaseID: "kb",
					ChunkType: types.ChunkTypeText, Content: "Document content for summary and enrichment.",
				}}},
				taskEnqueuer: queue,
				pendingRepo:  &wikiEnqueueFailurePendingRepo{knowledgeRepo: &repo.wikiEnqueueFailureKnowledgeRepo},
			}
			f := attachPostProcessOriginalInputs(t, service, &repo.wikiEnqueueFailureKnowledgeRepo, kb,
				service.chunkRepo.(*wikiEnqueueFailureChunkRepo).chunks)
			require.NoError(t, service.Handle(f.build, postProcessOriginalTask(t, f)))
			assertPostProcessQueuedOriginals(t, f, queue)
			require.Equal(t, tt.wantTasks, queue.taskTypes)
			require.Equal(t, len(tt.wantTasks), repo.expectedSubtasks)
			if len(tt.wantTasks) == 0 {
				require.Equal(t, types.ParseStatusCompleted, knowledge.ParseStatus)
			} else {
				require.Equal(t, types.ParseStatusFinalizing, knowledge.ParseStatus)
			}
			if tt.summary != nil && !*tt.summary {
				require.Equal(t, types.SummaryStatusNone, knowledge.SummaryStatus)
			} else {
				require.Equal(t, types.SummaryStatusPending, knowledge.SummaryStatus)
			}
		})
	}
}

func TestKnowledgePostProcessWithoutSummaryCompletionFailure(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		skipped bool
	}{
		{name: "database failure is retryable", err: errors.New("database unavailable")},
		{name: "concurrent cancellation skips auto tagging", skipped: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			knowledge := &types.Knowledge{
				ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", ParseStatus: types.ParseStatusProcessing,
			}
			require.NoError(t, knowledge.SetProcessOverrides(&types.KnowledgeProcessOverrides{
				SummaryEnabled: processConfigBoolPtr(false),
			}))
			repo := &optionalSummaryKnowledgeRepo{
				wikiEnqueueFailureKnowledgeRepo: wikiEnqueueFailureKnowledgeRepo{knowledge: knowledge},
				completionErr:                   tc.err, completionSkipped: tc.skipped,
			}
			queue := &wikiEnqueueFailureTaskQueue{}
			svc := &KnowledgePostProcessService{
				knowledgeRepo: repo,
				kbService: &wikiEnqueueFailureKBService{kb: &types.KnowledgeBase{
					ID: "kb", TenantID: 7, Type: types.KnowledgeBaseTypeDocument,
					AutoTagConfig: &types.AutoTagConfig{Enabled: true},
				}},
				chunkRepo: &wikiEnqueueFailureChunkRepo{chunks: []*types.Chunk{{
					ID: "chunk", TenantID: 7, KnowledgeID: "doc", KnowledgeBaseID: "kb",
					ChunkType: types.ChunkTypeText, Content: "Document content.",
				}}},
				taskEnqueuer: queue,
			}
			f := attachPostProcessOriginalInputs(t, svc, &repo.wikiEnqueueFailureKnowledgeRepo,
				svc.kbService.(*wikiEnqueueFailureKBService).kb, svc.chunkRepo.(*wikiEnqueueFailureChunkRepo).chunks)
			task := postProcessOriginalTask(t, f)
			err := svc.Handle(f.build, task)
			require.ErrorIs(t, err, tc.err)
			require.Empty(t, queue.taskTypes)
			require.Equal(t, types.ParseStatusProcessing, knowledge.ParseStatus)
			var handoffs int64
			require.NoError(t, f.db.Model(&types.TaskPendingOp{}).
				Where("task_type = ? AND scope_id = ?", types.TypeKnowledgeAutoTag, knowledge.ID).
				Count(&handoffs).Error)
			require.Zero(t, handoffs, "failed or skipped completion must not own child work")
			if tc.err != nil {
				repo.completionErr = nil
				require.NoError(t, svc.Handle(f.build, task))
				require.Equal(t, types.ParseStatusCompleted, knowledge.ParseStatus)
				require.Equal(t, []string{types.TypeKnowledgeAutoTag}, queue.taskTypes)
				assertPostProcessQueuedOriginals(t, f, queue)
				var op types.TaskPendingOp
				require.NoError(t, f.db.Where("task_type = ? AND scope_id = ?",
					types.TypeKnowledgeAutoTag, knowledge.ID).Take(&op).Error)
				require.Equal(t, "auto_tag_completion", op.Op)
				require.NotEmpty(t, op.DedupKey)
				require.JSONEq(t, string(op.Payload), string(queue.tasks[0].Payload()))
			}
		})
	}
}
