package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// EvaluationRunRepository persists evaluation results for tenant-scoped history.
type EvaluationRunRepository struct{ db *gorm.DB }

// NewEvaluationRunRepository creates the database-backed evaluation repository.
func NewEvaluationRunRepository(db *gorm.DB) *EvaluationRunRepository {
	return &EvaluationRunRepository{db: db}
}

type evaluationRunRow struct {
	TaskID     string    `gorm:"column:task_id"`
	TenantID   uint64    `gorm:"column:tenant_id"`
	DatasetID  string    `gorm:"column:dataset_id"`
	Status     int       `gorm:"column:status"`
	DetailJSON string    `gorm:"column:detail_json"`
	StartedAt  time.Time `gorm:"column:started_at"`
	UpdatedAt  time.Time `gorm:"column:updated_at"`
}

// Save inserts or updates an evaluation run and its full result snapshot.
func (r *EvaluationRunRepository) Save(ctx context.Context, detail *types.EvaluationDetail) error {
	if detail == nil || detail.Task == nil || detail.Task.ID == "" || detail.Task.TenantID == 0 {
		return errors.New("invalid evaluation run")
	}
	encoded, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("encode evaluation run: %w", err)
	}
	row := evaluationRunRow{
		TaskID:     detail.Task.ID,
		TenantID:   detail.Task.TenantID,
		DatasetID:  detail.Task.DatasetID,
		Status:     int(detail.Task.Status),
		DetailJSON: string(encoded),
		StartedAt:  detail.Task.StartTime,
		UpdatedAt:  time.Now().UTC(),
	}
	return r.db.WithContext(ctx).Table("evaluation_runs").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"status", "detail_json", "updated_at"}),
	}).Create(&row).Error
}

// Get loads one evaluation run owned by the given tenant.
func (r *EvaluationRunRepository) Get(
	ctx context.Context, tenantID uint64, taskID string,
) (*types.EvaluationDetail, error) {
	if tenantID == 0 || taskID == "" {
		return nil, gorm.ErrRecordNotFound
	}
	var row evaluationRunRow
	if err := r.db.WithContext(ctx).Table("evaluation_runs").
		Where("tenant_id = ? AND task_id = ?", tenantID, taskID).Take(&row).Error; err != nil {
		return nil, err
	}
	return decodeEvaluationRun(row)
}

// List returns the most recent evaluation runs owned by the given tenant.
func (r *EvaluationRunRepository) List(
	ctx context.Context, tenantID uint64, limit int,
) ([]*types.EvaluationDetail, error) {
	if tenantID == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	var rows []evaluationRunRow
	if err := r.db.WithContext(ctx).Table("evaluation_runs").Where("tenant_id = ?", tenantID).
		Order("started_at DESC, task_id DESC").Limit(limit).Find(&rows).Error; err != nil {
		return nil, err
	}
	results := make([]*types.EvaluationDetail, 0, len(rows))
	for _, row := range rows {
		detail, err := decodeEvaluationRun(row)
		if err != nil {
			return nil, err
		}
		results = append(results, detail)
	}
	return results, nil
}

// ReviewCase records an explicit human judgment in the durable run snapshot.
// The optimistic compare-and-swap preserves concurrent reviews of different
// questions without adding a migration that would race the Nextcloud branch's
// pending migration versions.
func (r *EvaluationRunRepository) ReviewCase(
	ctx context.Context, tenantID uint64, taskID string, questionID int,
	reviewerID string, input types.EvaluationCaseReviewInput,
) (*types.EvaluationDetail, error) {
	if tenantID == 0 || taskID == "" || reviewerID == "" || questionID < 0 {
		return nil, types.ErrEvaluationReviewInvalid
	}
	for attempt := 0; attempt < 5; attempt++ {
		var row evaluationRunRow
		if err := r.db.WithContext(ctx).Table("evaluation_runs").
			Where("tenant_id = ? AND task_id = ?", tenantID, taskID).Take(&row).Error; err != nil {
			return nil, err
		}
		detail, err := decodeEvaluationRun(row)
		if err != nil {
			return nil, err
		}
		if detail.Task.Status != types.EvaluationStatueSuccess {
			return nil, types.ErrEvaluationReviewNotReady
		}
		var target *types.EvaluationCaseResult
		for _, result := range detail.Cases {
			if result != nil && result.QuestionID == questionID {
				target = result
				break
			}
		}
		if target == nil {
			return nil, types.ErrEvaluationCaseNotFound
		}
		if !validEvaluationReview(input, target.ReferenceAnswer == "") {
			return nil, types.ErrEvaluationReviewInvalid
		}
		if target.Review != nil {
			target.ReviewHistory = append(target.ReviewHistory, *target.Review)
		}
		now := time.Now().UTC()
		target.Review = &types.EvaluationCaseReview{
			Faithfulness: input.Faithfulness, CitationAccuracy: input.CitationAccuracy,
			Abstention: input.Abstention, ReviewedBy: reviewerID, ReviewedAt: now,
		}
		encoded, err := json.Marshal(detail)
		if err != nil {
			return nil, fmt.Errorf("encode reviewed evaluation run: %w", err)
		}
		updated := r.db.WithContext(ctx).Table("evaluation_runs").
			Where("tenant_id = ? AND task_id = ? AND status = ? AND detail_json = ?",
				tenantID, taskID, int(types.EvaluationStatueSuccess), row.DetailJSON).
			Updates(map[string]any{"detail_json": string(encoded), "updated_at": now})
		if updated.Error != nil {
			return nil, updated.Error
		}
		if updated.RowsAffected == 1 {
			return detail, nil
		}
	}
	return nil, types.ErrEvaluationReviewConflict
}

func validEvaluationReview(input types.EvaluationCaseReviewInput, noAnswer bool) bool {
	judged := func(value string) bool { return value == "pass" || value == "fail" }
	if noAnswer {
		return input.Faithfulness == "not_applicable" &&
			input.CitationAccuracy == "not_applicable" && judged(input.Abstention)
	}
	return judged(input.Faithfulness) && judged(input.CitationAccuracy) &&
		input.Abstention == "not_applicable"
}

func decodeEvaluationRun(row evaluationRunRow) (*types.EvaluationDetail, error) {
	var detail types.EvaluationDetail
	if err := json.Unmarshal([]byte(row.DetailJSON), &detail); err != nil {
		return nil, fmt.Errorf("decode evaluation run %s: %w", row.TaskID, err)
	}
	if detail.Task == nil || detail.Task.ID != row.TaskID || detail.Task.TenantID != row.TenantID {
		return nil, errors.New("evaluation run identity mismatch")
	}
	return &detail, nil
}
