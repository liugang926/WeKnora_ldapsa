package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestEvaluationRunsPersistAndIsolateTenants(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+uuid.NewString()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, db.Exec(`CREATE TABLE evaluation_runs (
		task_id TEXT PRIMARY KEY, tenant_id INTEGER NOT NULL, dataset_id TEXT NOT NULL,
		status INTEGER NOT NULL, detail_json TEXT NOT NULL, started_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL)`).Error)
	repo := NewEvaluationRunRepository(db)
	ctx := context.Background()
	started := time.Now().UTC().Truncate(time.Second)
	detail := &types.EvaluationDetail{Task: &types.EvaluationTask{
		ID: "evaluation_7_test", TenantID: 7, DatasetID: "fixture", StartTime: started,
		Status: types.EvaluationStatueRunning, Total: 2,
	}}
	require.NoError(t, repo.Save(ctx, detail))
	detail.Task.Status = types.EvaluationStatueSuccess
	detail.Task.Finished = 2
	detail.Metric = &types.MetricResult{RetrievalMetrics: types.RetrievalMetrics{Recall: 0.75}}
	require.NoError(t, repo.Save(ctx, detail))

	// Recreate the repository to prove the result does not depend on process memory.
	restarted := NewEvaluationRunRepository(db)
	loaded, err := restarted.Get(ctx, 7, detail.Task.ID)
	require.NoError(t, err)
	require.Equal(t, types.EvaluationStatueSuccess, loaded.Task.Status)
	require.Equal(t, 2, loaded.Task.Finished)
	require.InDelta(t, 0.75, loaded.Metric.RetrievalMetrics.Recall, 0.001)
	_, err = restarted.Get(ctx, 8, detail.Task.ID)
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)
	listed, err := restarted.List(ctx, 7, 20)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	listed, err = restarted.List(ctx, 8, 20)
	require.NoError(t, err)
	require.Empty(t, listed)
}

func TestEvaluationCaseHumanReviewsPersistAndStayTenantScoped(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+uuid.NewString()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, db.Exec(`CREATE TABLE evaluation_runs (
		task_id TEXT PRIMARY KEY, tenant_id INTEGER NOT NULL, dataset_id TEXT NOT NULL,
		status INTEGER NOT NULL, detail_json TEXT NOT NULL, started_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL)`).Error)
	repo := NewEvaluationRunRepository(db)
	ctx := context.Background()
	detail := &types.EvaluationDetail{
		Task: &types.EvaluationTask{
			ID: "evaluation_7_review", TenantID: 7, DatasetID: "fixture",
			StartTime: time.Now().UTC(), Status: types.EvaluationStatueRunning,
		},
		Cases: []*types.EvaluationCaseResult{
			{QuestionID: 1, ReferenceAnswer: "fictional answer"},
			{QuestionID: 2, ReferenceAnswer: ""},
		},
	}
	require.NoError(t, repo.Save(ctx, detail))
	answered := types.EvaluationCaseReviewInput{
		Faithfulness: "pass", CitationAccuracy: "fail", Abstention: "not_applicable",
	}
	_, err = repo.ReviewCase(ctx, 7, detail.Task.ID, 1, "reviewer-1", answered)
	require.ErrorIs(t, err, types.ErrEvaluationReviewNotReady)
	detail.Task.Status = types.EvaluationStatueSuccess
	require.NoError(t, repo.Save(ctx, detail))
	_, err = repo.ReviewCase(ctx, 8, detail.Task.ID, 1, "reviewer-1", answered)
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)
	_, err = repo.ReviewCase(ctx, 7, detail.Task.ID, 3, "reviewer-1", answered)
	require.ErrorIs(t, err, types.ErrEvaluationCaseNotFound)
	_, err = repo.ReviewCase(ctx, 7, detail.Task.ID, 2, "reviewer-1", answered)
	require.ErrorIs(t, err, types.ErrEvaluationReviewInvalid)
	_, err = repo.ReviewCase(ctx, 7, detail.Task.ID, 1, "", answered)
	require.ErrorIs(t, err, types.ErrEvaluationReviewInvalid)

	updated, err := repo.ReviewCase(ctx, 7, detail.Task.ID, 1, "reviewer-1", answered)
	require.NoError(t, err)
	require.Equal(t, "reviewer-1", updated.Cases[0].Review.ReviewedBy)
	require.Equal(t, "pass", updated.Cases[0].Review.Faithfulness)
	require.Empty(t, updated.Cases[0].ReviewHistory)
	noAnswer := types.EvaluationCaseReviewInput{
		Faithfulness: "not_applicable", CitationAccuracy: "not_applicable", Abstention: "pass",
	}
	_, err = repo.ReviewCase(ctx, 7, detail.Task.ID, 2, "reviewer-2", noAnswer)
	require.NoError(t, err)
	answered.CitationAccuracy = "pass"
	_, err = repo.ReviewCase(ctx, 7, detail.Task.ID, 1, "reviewer-3", answered)
	require.NoError(t, err)

	// A new repository instance sees both judgments and the correction trail.
	restarted := NewEvaluationRunRepository(db)
	loaded, err := restarted.Get(ctx, 7, detail.Task.ID)
	require.NoError(t, err)
	require.Equal(t, "pass", loaded.Cases[0].Review.CitationAccuracy)
	require.Equal(t, "reviewer-3", loaded.Cases[0].Review.ReviewedBy)
	require.Len(t, loaded.Cases[0].ReviewHistory, 1)
	require.Equal(t, "fail", loaded.Cases[0].ReviewHistory[0].CitationAccuracy)
	require.Equal(t, "pass", loaded.Cases[1].Review.Abstention)
	_, err = restarted.Get(ctx, 8, detail.Task.ID)
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)
}
