package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestFAQPostgresActualImportProgressResultAndABA(t *testing.T) {
	s, store, db, ctx := faqPostgresActualFixture(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	batch := &types.FAQBatchUpsertPayload{
		Mode: types.FAQBatchModeAppend,
		Entries: []types.FAQEntryPayload{
			{StandardQuestion: "PostgreSQL actual import question", Answers: []string{"actual PostgreSQL answer"}},
		},
	}
	task, err := s.UpsertFAQEntries(
		faqInputContext(ctx, t, types.FAQInputOperation{Kind: "import", Batch: batch}),
		"kb",
		batch,
	)
	require.NoError(t, err)
	queue := s.task.(*knowledgeInputQueue)
	require.Len(t, queue.tasks, 1)
	workerCtx, workerCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer workerCancel()
	require.NoError(t, s.ProcessFAQImport(workerCtx, queue.tasks[0]))
	progress, err := s.GetFAQImportProgress(ctx, task)
	require.NoError(t, err)
	require.Equal(t, types.FAQImportStatusCompleted, progress.Status)
	require.Equal(t, 1, progress.SuccessCount)
	var payload types.FAQImportPayload
	require.NoError(t, json.Unmarshal(queue.tasks[0].Payload(), &payload))
	request := types.FAQImportOutputReadRequest{
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     payload.KnowledgeID,
		Kind:            "result",
	}
	snapshot, err := store.CaptureFAQImportOutputInputs(ctx, request)
	require.NoError(t, err)
	require.Equal(t, 1, snapshot.Result.SuccessCount)
	require.NoError(
		t,
		s.UpdateLastFAQImportResultDisplayStatus(
			faqInputContext(ctx, t, types.FAQInputOperation{Kind: "display-status", DisplayStatus: "close"}),
			"kb",
			"close",
		),
	)
	progress, err = s.GetFAQImportProgress(ctx, task)
	require.NoError(t, err)
	require.Equal(t, "close", progress.DisplayStatus)
	require.NoError(t, db.Model(&types.User{}).Where("id='actual-faq-author'").Update("is_active", false).Error)
	_, err = s.GetFAQImportProgress(ctx, task)
	require.Error(t, err)
	require.NoError(t, db.Model(&types.User{}).Where("id='actual-faq-author'").Update("is_active", true).Error)
	current, err := store.CaptureFAQImportOutputInputs(ctx, request)
	require.NoError(t, err)
	var knowledge types.Knowledge
	require.NoError(t, db.Where("id=?", payload.KnowledgeID).Take(&knowledge).Error)
	saved := knowledge.LastFAQImportResult
	require.NoError(
		t,
		db.Model(&knowledge).
			Update("last_faq_import_result", types.JSON(`{"task_id":"foreign","success_count":999}`)).
			Error,
	)
	require.NoError(t, db.Model(&knowledge).Update("last_faq_import_result", saved).Error)
	require.Error(t, store.RecheckFAQImportOutputInputs(ctx, current))
}
