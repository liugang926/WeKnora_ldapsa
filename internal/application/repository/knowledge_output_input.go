package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
)

// CaptureKnowledgeOutputInputs starts from the exact current material receipt,
// never from the newest admission or a claimed channel. The second capture
// returns only actual rows still bound to that original immutable admission.
// Nothing in the first lookup is published or used as model input.
func (s *KnowledgeInputProvenanceStore) CaptureKnowledgeOutputInputs(
	ctx context.Context,
	request types.KnowledgeBuildInputCaptureRequest,
) (*types.KnowledgeBuildInputSnapshot, error) {
	if ctx == nil || s == nil || s.db == nil || request.Expected != nil || request.Projection != "" ||
		request.TenantID == 0 || request.TenantID > math.MaxInt64 ||
		request.KnowledgeBaseID == "" || request.KnowledgeID == "" {
		return nil, types.ErrSourceLineageUnknown
	}
	if len(request.ChunkIDs) > 4096 {
		return nil, types.ErrSourceLineageLimit
	}
	options := &sql.TxOptions{ReadOnly: true}
	switch s.db.Dialector.Name() {
	case "postgres":
		options.Isolation = sql.LevelRepeatableRead
	case "sqlite":
	default:
		return nil, ErrKnowledgeInputChanged
	}
	if _, nested := s.db.Statement.ConnPool.(gorm.TxCommitter); nested {
		return nil, ErrKnowledgeInputChanged
	}
	var original *types.KnowledgeBuildInputProof
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := s.checkFAQImportReadSetPlanInTx(tx); err != nil {
			return err
		}
		var k types.Knowledge
		if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?",
			request.KnowledgeID, request.TenantID, request.KnowledgeBaseID).Take(&k).Error; err != nil {
			return err
		}
		var receipt knowledgeInputMaterialReceipt
		if err := tx.Where("row_kind = ? AND row_id = ?", "knowledge", k.ID).Take(&receipt).Error; err != nil {
			return err
		}
		var admission knowledgeInputAdmission
		if err := tx.Where("admission_id = ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
			receipt.AdmissionID, k.TenantID, k.KnowledgeBaseID, k.ID).Take(&admission).Error; err != nil {
			return err
		}
		var lineage types.SourceLineage
		if err := json.Unmarshal([]byte(admission.SourceLineage), &lineage); err != nil {
			return err
		}
		if err := lineage.RequireComplete(); err != nil {
			return err
		}
		original = &types.KnowledgeBuildInputProof{
			Version: 1, TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID,
			AdmissionID: admission.AdmissionID, BuildGeneration: admission.BuildGeneration,
			InputDigest: admission.InputDigest, ConfigDigest: admission.ConfigDigest, SourceLineage: &lineage,
		}
		// Checks the receipt's exact revision/digest, current config and original
		// source generation before this admission may drive the second capture.
		_, _, _, err := s.admissionInTx(tx, &k, original)
		return err
	}, options)
	if err != nil {
		return nil, err
	}
	request.Expected = original
	return s.CaptureKnowledgeBuildInputs(ctx, request)
}

var _ interfaces.KnowledgeOutputInputReader = (*KnowledgeInputProvenanceStore)(nil)
