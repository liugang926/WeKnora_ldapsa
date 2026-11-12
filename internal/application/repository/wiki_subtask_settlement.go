package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type wikiSettlementTransactionKey struct{}

// SettleWikiPendingOp atomically acknowledges one durable Wiki op, drains its
// counter and performs the completion handoff. The receipt survives removal of
// the queue row, so a repeated op for the same parse attempt cannot drain a
// different enrichment slot. No schema change is needed: receipt rows occupy a
// separate task namespace and are never consumed as Wiki pending work.
func (r *knowledgeRepository) SettleWikiPendingOp(
	ctx context.Context, opID int64, knowledgeID string, attempt int, terminalError string, failures int,
) error {
	if opID <= 0 || knowledgeID == "" {
		return errors.New("wiki settlement requires a durable operation")
	}
	if KnowledgeBuildInputProofFromContext(ctx) != nil {
		if err := PrecheckKnowledgeBuildWrite(ctx); err != nil {
			return err
		}
	}
	var promoted bool
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Serialize receipts and reparses on the document before taking the queue row.
		var k types.Knowledge
		if err := tx.Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", knowledgeID).Take(&k).Error; err != nil {
			return err
		}
		var op types.TaskPendingOp
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", opID).Take(&op).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		var saved struct {
			KnowledgeID string `json:"knowledge_id"`
			Attempt     int    `json:"attempt"`
			Op          string `json:"op"`
		}
		if json.Unmarshal(op.Payload, &saved) != nil || saved.KnowledgeID != knowledgeID ||
			saved.Op != "ingest" || saved.Attempt != attempt || op.TaskType != "wiki:ingest" ||
			op.Scope != "knowledge_base" || op.ScopeID != k.KnowledgeBaseID || op.TenantID != k.TenantID ||
			op.DedupKey != knowledgeID {
			return errors.New("wiki settlement operation changed")
		}
		var latest int
		if err := tx.Model(&types.KnowledgeProcessingSpan{}).
			Where("knowledge_id = ? AND kind = ?", knowledgeID, types.SpanKindRoot).
			Select("COALESCE(MAX(attempt),0)").Scan(&latest).Error; err != nil {
			return err
		}
		if attempt == 0 {
			// Legacy payloads are tied to the parse which existed at enqueue time,
			// never to a newly started parse. Ambiguous/missing history fails closed.
			if op.EnqueuedAt.IsZero() {
				return errors.New("legacy wiki operation has no enqueue time")
			}
			if err := tx.Model(&types.KnowledgeProcessingSpan{}).
				Where("knowledge_id = ? AND kind = ? AND created_at <= ?",
					knowledgeID, types.SpanKindRoot, op.EnqueuedAt).
				Select("COALESCE(MAX(attempt),0)").Scan(&attempt).Error; err != nil {
				return err
			}
		}
		if attempt <= 0 {
			return errors.New("wiki operation parse attempt is unknown")
		}
		// Only the old row is acknowledged. Never drain a newer parse's counter.
		if attempt != latest || k.DeletedAt.Valid {
			return tx.Delete(&types.TaskPendingOp{}, opID).Error
		}
		key := strconv.Itoa(attempt)
		var receipts int64
		if err := tx.Model(&types.TaskPendingOp{}).
			Where("task_type = ? AND scope = ? AND scope_id = ? AND dedup_key = ?",
				"wiki:settled", "knowledge", knowledgeID, key).Count(&receipts).Error; err != nil {
			return err
		}
		if receipts == 0 {
			proof := KnowledgeBuildInputProofFromContext(ctx)
			if proof == nil || proof.KnowledgeID != knowledgeID || proof.TenantID != k.TenantID ||
				proof.KnowledgeBaseID != k.KnowledgeBaseID {
				return types.ErrSourceLineageUnknown
			}
			if k.ParseStatus == types.ParseStatusProcessing {
				return errors.New("wiki completion arrived before finalizing handoff")
			}
			if k.ParseStatus == types.ParseStatusFinalizing {
				nested := *r
				nested.db, nested.completionObserver = tx, nil
				var err error
				_, promoted, err = nested.FinalizeSubtask(
					context.WithValue(ctx, wikiSettlementTransactionKey{}, tx), knowledgeID,
				)
				if err != nil {
					return err
				}
			}
			receipt := types.TaskPendingOp{
				TenantID: k.TenantID, TaskType: "wiki:settled", Scope: "knowledge", ScopeID: knowledgeID,
				Op: "settled", DedupKey: key, Payload: json.RawMessage(fmt.Sprintf(`{"op_id":%d}`, opID)),
				EnqueuedAt: time.Now(),
			}
			if err := tx.Create(&receipt).Error; err != nil {
				return err
			}
		}
		if terminalError != "" {
			if err := tx.Create(&types.TaskDeadLetter{
				TenantID: k.TenantID, TaskType: op.TaskType, Scope: op.Scope, ScopeID: op.ScopeID,
				RelatedID: knowledgeID, Payload: op.Payload, LastError: terminalError, FailCount: failures,
			}).Error; err != nil {
				return err
			}
		}
		return tx.Delete(&types.TaskPendingOp{}, opID).Error
	})
	if err != nil {
		return err
	}
	// The handoff above is durable; observer dispatch is recoverable from its
	// outbox and must run outside the transaction, as in FinalizeSubtask.
	if promoted && r.completionObserver != nil {
		return r.completionObserver(ctx, knowledgeID)
	}
	return nil
}

// LoadKnowledgeOriginalInput reads an existing immutable admission through its
// original material receipt. It neither admits new input nor infers lineage
// from channel metadata. The caller must restore the original actor and recheck
// this proof before use; transaction writes revalidate it again.
func (s *KnowledgeInputProvenanceStore) LoadKnowledgeOriginalInput(
	ctx context.Context, tenant uint64, kb, id string,
) (*types.KnowledgeBuildInputProof, error) {
	var a knowledgeInputAdmission
	err := s.db.WithContext(ctx).Table("knowledge_input_admissions AS a").Select("a.*").
		Joins("JOIN knowledge_input_material_receipts r ON r.admission_id=a.admission_id "+
			"AND r.row_kind='knowledge' AND r.row_id=a.knowledge_id").
		Where("a.tenant_id=? AND a.knowledge_base_id=? AND a.knowledge_id=?", tenant, kb, id).Take(&a).Error
	if err != nil {
		return nil, err
	}
	var lineage types.SourceLineage
	if json.Unmarshal([]byte(a.SourceLineage), &lineage) != nil || lineage.RequireComplete() != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	return &types.KnowledgeBuildInputProof{
		Version: 1, TenantID: tenant, KnowledgeBaseID: kb, KnowledgeID: id, AdmissionID: a.AdmissionID,
		BuildGeneration: a.BuildGeneration, InputDigest: a.InputDigest, ConfigDigest: a.ConfigDigest,
		SourceLineage: &lineage,
	}, nil
}
