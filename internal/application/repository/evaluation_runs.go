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
