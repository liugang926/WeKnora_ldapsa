package repository

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type (
	dingTalkRecoveryGateKey struct{}
	dingTalkRecoveryGate    struct {
		snapshot   string
		queueCheck func(context.Context) error
	}
)

// WithDingTalkRecoveryGate attaches the live recovery authorization check.
func WithDingTalkRecoveryGate(ctx context.Context, snapshot string, check func(context.Context) error) context.Context {
	return context.WithValue(ctx, dingTalkRecoveryGateKey{}, dingTalkRecoveryGate{snapshot, check})
}

func dingTalkRecoveryEligibility(tx *gorm.DB, k *types.Knowledge) (*types.KnowledgeRecoveryEligibility, error) {
	result := &types.KnowledgeRecoveryEligibility{Action: "wait", Reason: "文档仍在处理，请稍后重试"}
	var material knowledgeInputMaterialReceipt
	if err := tx.Where("row_kind='knowledge' AND row_id=?", k.ID).Take(&material).Error; err != nil {
		return nil, err
	}
	var admission knowledgeInputAdmission
	if err := tx.Where("admission_id=? AND tenant_id=? AND knowledge_base_id=? AND knowledge_id=?",
		material.AdmissionID,
		k.TenantID,
		k.KnowledgeBaseID,
		k.ID).Take(&admission).Error; err != nil {
		return nil, err
	}
	if err := tx.Model(&types.KnowledgeProcessingSpan{}).Where(
		"knowledge_id=? AND kind=?",
		k.ID,
		types.SpanKindRoot).Select("COALESCE(MAX(attempt),0)").Scan(
		&result.Attempt).Error; err != nil {
		return nil, err
	}
	result.Generation = admission.BuildGeneration
	digest, err := KnowledgeInputDigest(k)
	if err != nil {
		return nil, err
	}
	result.Snapshot = types.GraphDigest([]any{digest, admission.AdmissionID, admission.BuildGeneration, result.Attempt})
	switch k.ParseStatus {
	case types.ParseStatusFinalizing, types.ParseStatusCompleted, types.ParseStatusFailed, types.ParseStatusCancelled:
	default:
		return result, nil
	}
	var outstanding int64
	if err := tx.Model(&DingTalkWorkIntent{}).Where(
		"tenant_id=? AND knowledge_base_id=? AND knowledge_id=? AND state<>?",
		k.TenantID,
		k.KnowledgeBaseID,
		k.ID,
		"done").Count(&outstanding).Error; err != nil {
		return nil, err
	}
	if outstanding > 0 {
		result.Reason = "同步处理任务尚未交付完成，请等待处理"
		return result, nil
	}
	var rows []types.TaskPendingOp
	if err := tx.Where("tenant_id=? AND ((task_type=? AND scope='knowledge' AND scope_id=?) OR "+
		"(task_type='wiki:ingest' AND scope='knowledge_base' AND scope_id=? AND "+
		"dedup_key=?))",
		k.TenantID,
		DingTalkGraphTaskType,
		k.ID,
		k.KnowledgeBaseID,
		k.ID).Find(&rows).Error; err != nil {
		return nil, err
	}
	for _, row := range rows {
		if row.TaskType == DingTalkGraphTaskType {
			var rec DingTalkGraphRecord
			if json.Unmarshal(row.Payload, &rec) != nil || rec.Payload.OriginalInput == nil {
				return nil, ErrKnowledgeInputChanged
			}
			if rec.Payload.OriginalInput.AdmissionID == admission.AdmissionID &&
				rec.Payload.Attempt == result.Attempt &&
				row.Op != "settled" {
				result.Reason = "图谱任务正在处理或等待自动补结算，请稍后重试"
				return result, nil
			}
		} else if row.Op == "ingest" {
			var wiki struct {
				OriginalInput *types.KnowledgeBuildInputProof `json:"original_input"`
				Attempt       int                             `json:"attempt"`
			}
			if json.Unmarshal(row.Payload, &wiki) != nil || wiki.OriginalInput == nil {
				return nil, ErrKnowledgeInputChanged
			}
			if wiki.OriginalInput.AdmissionID == admission.AdmissionID && wiki.Attempt == result.Attempt {
				result.Reason = "Wiki任务正在处理或等待恢复，请稍后重试"
				return result, nil
			}
		}
	}
	if k.FilePath == "" {
		result.Reason = "缺少本地原文，无法重新解析"
		return result, nil
	}
	result.Allowed = true
	result.Action = "reparse"
	result.Reason = "可以使用本地原文重新解析"
	return result, nil
}

// DingTalkRecoveryEligibility reads the current document recovery eligibility.
func (s *KnowledgeInputProvenanceStore) DingTalkRecoveryEligibility(
	ctx context.Context,
	k *types.Knowledge) (*types.KnowledgeRecoveryEligibility,
	error,
) {
	return dingTalkRecoveryEligibility(s.db.WithContext(ctx), k)
}

func checkDingTalkRecoveryGate(ctx context.Context, tx *gorm.DB, k *types.Knowledge) (bool, error) {
	gate, ok := ctx.Value(dingTalkRecoveryGateKey{}).(dingTalkRecoveryGate)
	if !ok {
		return false, nil
	} // Internal legacy callers retain the R14 contract.
	result, err := dingTalkRecoveryEligibility(tx, k)
	if err != nil {
		return false, err
	}
	if !result.Allowed || gate.snapshot == "" || gate.snapshot != result.Snapshot || gate.queueCheck == nil {
		return false, errors.New("恢复条件已变化，请刷新文档后重试")
	}
	if err := gate.queueCheck(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// FailDingTalkRecoveryHandoff makes only this exact failed pending generation retryable.
// It cannot overwrite a worker that already started or a later publication.
func (s *KnowledgeInputProvenanceStore) FailDingTalkRecoveryHandoff(
	ctx context.Context,
	p *types.KnowledgeBuildInputProof,
) error {
	if p == nil {
		return nil
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var k types.Knowledge
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
			"id=? AND tenant_id=?",
			p.KnowledgeID,
			p.TenantID).Take(&k).Error; err != nil {
			return err
		}
		var mr knowledgeInputMaterialReceipt
		if err := tx.Where("row_kind='knowledge' AND row_id=?", k.ID).Take(&mr).Error; err != nil {
			return err
		}
		if mr.AdmissionID != p.AdmissionID || k.ParseStatus != types.ParseStatusPending {
			return nil
		}
		return tx.Model(&k).Updates(map[string]any{
			"parse_status":  types.ParseStatusFailed,
			"error_message": "恢复任务提交失败，请重新发起解析",
			"updated_at":    time.Now(),
		}).Error
	})
}
