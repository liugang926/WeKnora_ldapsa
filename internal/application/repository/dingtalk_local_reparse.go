package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/Tencent/WeKnora/internal/datasource/connector/dingtalk"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

// A local processing generation uses the existing derived admission kind and
// an exact private receipt, never a new remote-issued receipt or a schema change.
const dingTalkLocalReparseReceiptTask = "dingtalk:local-reparse"

type dingTalkLocalReparseKey struct{}
type dingTalkLocalReparseReceipt struct {
	Generation          uint64 `json:"generation"`
	OriginalAdmissionID string `json:"original_admission_id"`
}

// AdmitDingTalkLocalReparse binds retained local bytes to current controls.
// Synchronization owns remote availability; current KB authority and immutable
// original file integrity still govern this explicit local-file operation.
func (s *KnowledgeInputProvenanceStore) AdmitDingTalkLocalReparse(ctx context.Context, before, proposed *types.Knowledge, expectedKB *types.KnowledgeBase, input []byte) (*types.KnowledgeBuildInputProof, error) {
	if before == nil || proposed == nil || expectedKB == nil || expectedKB.ID != before.KnowledgeBaseID || expectedKB.TenantID != before.TenantID || before.Channel != types.ConnectorTypeDingTalk || before.FilePath == "" || before.ID != proposed.ID || before.TenantID != proposed.TenantID || before.KnowledgeBaseID != proposed.KnowledgeBaseID {
		return nil, ErrKnowledgeInputChanged
	}
	// Resolve current actor/group authority before opening the data transaction:
	// group/share services use their own pool. Workers recheck this captured actor.
	authorized, err := s.AuthorizeCurrentKnowledgeInputWrite(ctx, expectedKB)
	if err != nil {
		return nil, err
	}
	var proof *types.KnowledgeBuildInputProof
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var actual types.Knowledge
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=? AND knowledge_base_id=?", before.ID, before.TenantID, before.KnowledgeBaseID).Take(&actual).Error; err != nil {
			return err
		}
		if actual.ParseStatus == types.ParseStatusDeleting {
			return ErrKnowledgeInputChanged
		}
		want, err := KnowledgeInputDigest(before)
		if err != nil {
			return err
		}
		got, err := KnowledgeInputDigest(&actual)
		if err != nil {
			return err
		}
		if want != got {
			return ErrKnowledgeInputChanged
		}
		recovering, err := checkDingTalkRecoveryGate(ctx, tx, &actual)
		if err != nil {
			return err
		}
		kb, err := loadKnowledgeBuildConfigurationInTx(tx.Clauses(clause.Locking{Strength: "SHARE"}), actual.TenantID, actual.KnowledgeBaseID)
		if err != nil {
			return err
		}
		desiredConfig, err := knowledgeConfigDigest(expectedKB, proposed)
		if err != nil {
			return err
		}
		actualConfig, err := knowledgeConfigDigest(kb, proposed)
		if err != nil {
			return err
		}
		if desiredConfig != actualConfig {
			return ErrKnowledgeInputChanged
		}
		bound := *s
		bound.db = tx
		// Generated material may be stale after an earlier failed parse. Verify the
		// independently retained original receipt and bytes, without a live source
		// lookup or requiring old generated metadata/config to match this request.
		var r dingTalkOriginalReceipt
		if err := tx.Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", actual.TenantID, actual.KnowledgeBaseID, actual.ID).Take(&r).Error; err != nil {
			return err
		}
		var original knowledgeInputAdmission
		if err := tx.Where("admission_id=? AND tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", r.AdmissionID, actual.TenantID, actual.KnowledgeBaseID, actual.ID).Take(&original).Error; err != nil {
			return err
		}
		var facts dingtalk.OriginalFetchedArtifactInput
		if err := json.Unmarshal([]byte(r.Facts), &facts); err != nil {
			return err
		}
		fd, err := KnowledgeInputDigest(facts)
		if err != nil {
			return err
		}
		digest, err := KnowledgeInputDigest(input)
		if err != nil {
			return err
		}
		sha := sha256.Sum256(input)
		if original.OriginKind != dingTalkOriginalKind || r.SourceRevision == 0 || r.SyncLogID == "" || r.DataSourceID == "" || r.RootDigest != original.RootDigest || r.InputDigest != original.InputDigest || fd != r.FactsDigest || digest != original.InputDigest || facts.ArtifactSHA256 != hex.EncodeToString(sha[:]) {
			return ErrKnowledgeInputChanged
		}
		var lineage types.SourceLineage
		if err := json.Unmarshal([]byte(original.SourceLineage), &lineage); err != nil {
			return err
		}
		if err := lineage.RequireComplete(); err != nil {
			return err
		}
		// Only explicit parsing overrides are persisted, never a stale object's
		// status or generated fields. Normal retention callbacks remain in use.
		if recovering {
			if err := tx.Model(&actual).Update("parse_status", types.ParseStatusPending).Error; err != nil {
				return err
			}
			actual.ParseStatus = types.ParseStatusPending
		}
		actual.Metadata = append(types.JSON(nil), proposed.Metadata...)
		if err := tx.Model(&types.Knowledge{}).Where("id=? AND tenant_id=?", actual.ID, actual.TenantID).Update("metadata", actual.Metadata).Error; err != nil {
			return err
		}
		if err := tx.Where("id=? AND tenant_id=?", actual.ID, actual.TenantID).Take(&actual).Error; err != nil {
			return err
		}
		proof, err = bound.admitInTx(context.WithValue(authorized, dingTalkLocalReparseKey{}, true), tx, &actual, kb, input, knowledgeInputOrigin{&lineage, "derived"})
		if err != nil {
			return err
		}
		payload, err := json.Marshal(dingTalkLocalReparseReceipt{proof.BuildGeneration, original.AdmissionID})
		if err != nil {
			return err
		}
		receipt := types.TaskPendingOp{TenantID: actual.TenantID, TaskType: dingTalkLocalReparseReceiptTask, Scope: "knowledge", ScopeID: actual.ID, DedupKey: proof.AdmissionID, Op: "admitted", Payload: payload, EnqueuedAt: time.Now().UTC()}
		if err := tx.Create(&receipt).Error; err != nil {
			return err
		}
		*proposed = actual
		return nil
	})
	return proof, err
}

// This predicate is deliberately not based on channel or a worker payload flag.
// Only an exact receipt committed by AdmitDingTalkLocalReparse selects the local
// completion path. General derived admissions retain their existing behavior.
func isDingTalkLocalReparseAdmission(tx *gorm.DB, a *knowledgeInputAdmission) (bool, error) {
	if a == nil || a.OriginKind != "derived" {
		return false, nil
	}
	var row types.TaskPendingOp
	err := tx.Where("task_type=? AND scope=? AND scope_id=? AND tenant_id=? AND dedup_key=? AND op=?", dingTalkLocalReparseReceiptTask, "knowledge", a.KnowledgeID, a.TenantID, a.AdmissionID, "admitted").Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var receipt dingTalkLocalReparseReceipt
	if json.Unmarshal(row.Payload, &receipt) != nil || receipt.Generation != a.BuildGeneration || receipt.OriginalAdmissionID == "" {
		return false, ErrKnowledgeInputChanged
	}
	var original knowledgeInputAdmission
	if err := tx.Where("admission_id=? AND knowledge_id=? AND knowledge_base_id=? AND tenant_id=? AND origin_kind=?", receipt.OriginalAdmissionID, a.KnowledgeID, a.KnowledgeBaseID, a.TenantID, dingTalkOriginalKind).Take(&original).Error; err != nil {
		return false, err
	}
	if original.InputDigest != a.InputDigest {
		return false, ErrKnowledgeInputChanged
	}
	return true, nil
}
