package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CommitKnowledgeMove changes the application rows and their retention receipts
// in one transaction. Remote vector engines use the existing idempotent move task.
func (r *knowledgeRepository) CommitKnowledgeMove(
	ctx context.Context, before, after *types.Knowledge, reuseChunks bool, finish func(context.Context) error,
) error {
	if before == nil || after == nil || before.ID == "" || before.ID != after.ID || before.TenantID != after.TenantID {
		return fmt.Errorf("invalid knowledge move")
	}
	source := &types.KnowledgeBase{ID: before.KnowledgeBaseID, TenantID: before.TenantID}
	target := &types.KnowledgeBase{ID: after.KnowledgeBaseID, TenantID: after.TenantID}
	if err := access.RequireKBTransfer(ctx, source, target, access.KBTransferMove); err != nil {
		return err
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var stored types.Knowledge
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", before.ID, source.TenantID, source.ID).
			First(&stored).Error
		if err != nil {
			return err
		}
		if !stored.UpdatedAt.Equal(before.UpdatedAt) || stored.ParseStatus != before.ParseStatus {
			return fmt.Errorf("knowledge changed during move")
		}
		var fields map[string]json.RawMessage
		var move struct {
			TaskID    string `json:"task_id"`
			SourceID  string `json:"source_id"`
			SourceKB  string `json:"source_kb"`
			TargetKB  string `json:"target_kb"`
			Operation string `json:"operation"`
			Phase     string `json:"phase"`
			Mode      string `json:"mode"`
		}
		mode := "reparse"
		if reuseChunks {
			mode = "reuse_vectors"
		}
		if json.Unmarshal(stored.Metadata, &fields) != nil ||
			json.Unmarshal(fields[types.KnowledgeTransferMetadataKey], &move) != nil ||
			move.TaskID == "" || move.TaskID != access.TransferTaskID(ctx) || move.Phase != "moving" ||
			move.Mode != mode || move.Operation != string(access.KBTransferMove) || move.SourceID != stored.ID ||
			move.SourceKB != source.ID || move.TargetKB != target.ID {
			return fmt.Errorf("knowledge move checkpoint changed")
		}
		retained := originalBodySchemaPresent(tx)
		if retained {
			// Purge and reconciliation acquire the same ledger lock before validating ownership.
			var journal originalBodyJournalHead
			if err := bodyInternal(tx).Clauses(clause.Locking{Strength: "UPDATE"}).
				First(&journal, "id=1").Error; err != nil {
				return err
			}
			if err := moveBodyHeads(tx, "knowledges", stored.ID, &stored, target.ID, false); err != nil {
				return err
			}
		}
		var chunks []types.Chunk
		if err := tx.Model(&types.Chunk{}).Clauses(clause.Locking{Strength: "UPDATE"}).
			Select("id,tenant_id,knowledge_base_id").Where("knowledge_id = ?", stored.ID).
			Order("id").Find(&chunks).Error; err != nil {
			return err
		}
		for _, chunk := range chunks {
			if !reuseChunks || chunk.TenantID != stored.TenantID ||
				(chunk.KnowledgeBaseID != source.ID && chunk.KnowledgeBaseID != target.ID) {
				return fmt.Errorf("knowledge chunks changed during move")
			}
			if chunk.KnowledgeBaseID == target.ID {
				// Older workers could commit chunks before committing the document.
				// A retry must still verify evidence already bound to the target.
				if retained {
					moved := stored
					moved.KnowledgeBaseID = target.ID
					if err := moveBodyHeads(tx, "chunks", chunk.ID, &moved, target.ID, true); err != nil {
						return err
					}
				}
				continue
			}
			if retained {
				if err := moveBodyHeads(tx, "chunks", chunk.ID, &stored, target.ID, false); err != nil {
					return err
				}
			}
			result := tx.Model(&types.Chunk{}).
				Where("id = ? AND tenant_id = ? AND knowledge_id = ? AND knowledge_base_id = ?",
					chunk.ID, stored.TenantID, stored.ID, source.ID).
				Updates(map[string]any{"knowledge_base_id": target.ID, "tag_id": ""})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return fmt.Errorf("chunk changed during move")
			}
		}
		moveRepo := *r
		moveRepo.db = tx
		if err := moveRepo.DeleteKnowledgeTagRelations(ctx, stored.ID); err != nil {
			return err
		}
		if err := moveRepo.UpdateKnowledgeForTransfer(ctx, before, after); err != nil {
			return err
		}
		if finish != nil {
			indexCtx := context.WithValue(ctx, knowledgeMoveTxKey{}, knowledgeMoveTx{repository: r.db, tx: tx})
			return finish(indexCtx)
		}
		return nil
	})
}

// CheckMoveOriginals validates the real row and all its chunks before destructive re-index cleanup.
func (r *knowledgeRepository) CheckMoveOriginals(ctx context.Context, k *types.Knowledge, target string) error {
	if k == nil || k.ID == "" {
		return ErrOriginalBodyUnavailable
	}
	sourceKB := &types.KnowledgeBase{ID: k.KnowledgeBaseID, TenantID: k.TenantID}
	targetKB := &types.KnowledgeBase{ID: target, TenantID: k.TenantID}
	if err := access.RequireKBTransfer(ctx, sourceKB, targetKB, access.KBTransferMove); err != nil {
		return err
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if !originalBodySchemaPresent(tx) {
			return nil
		}
		if err := moveBodyHeads(tx, "knowledges", k.ID, k, target, true); err != nil {
			return err
		}
		var ids []string
		if err := tx.Model(&types.Chunk{}).Where("knowledge_id=?", k.ID).Pluck("id", &ids).Error; err != nil {
			return err
		}
		for _, id := range ids {
			if err := moveBodyHeads(tx, "chunks", id, k, target, true); err != nil {
				return err
			}
		}
		return nil
	})
}

// Re-capture unchanged bytes under the destination domain while preserving all
// previous revisions. A source-domain payload can then retire independently.
func moveBodyHeads(
	tx *gorm.DB, table, key string, document *types.Knowledge, targetKB string, checkOnly bool,
) error {
	descriptor, _ := bodyDescriptor(table)
	row, err := bodySourceRow(tx, descriptor, key)
	if err != nil {
		return err
	}
	source, err := bodyScopeFromRow(tx, descriptor, key, row)
	if err != nil {
		return err
	}
	if source.Tenant != document.TenantID || source.Knowledge != document.ID || source.KB != document.KnowledgeBaseID {
		return ErrOriginalBodyUnavailable
	}
	if err := CheckOriginalBodyAvailableInTx(tx.Statement.Context, tx, table, key); err != nil {
		return err
	}
	destination := source
	destination.KB = targetKB
	store := NewOriginalBodyRetentionStore(tx)
	for _, field := range descriptor.Columns {
		raw, err := bodyFieldValue(row, field)
		if err != nil {
			return err
		}
		if checkOnly {
			var head originalBodyFieldHead
			err := bodyInternal(tx).Where("source_table=? AND row_key=? AND column_name=?",
				table, key, field).Take(&head).Error
			if errors.Is(err, gorm.ErrRecordNotFound) && (raw == "" || raw == "null") {
				continue
			}
			if err != nil {
				return err
			}
			if err := validateMoveBodyHead(tx, descriptor, key, field, head, source, raw); err != nil {
				return err
			}
		} else if err := store.captureFieldWithPreviousScope(
			tx, descriptor, key, field, raw, destination, &source,
		); err != nil {
			return err
		}
	}
	return nil
}

type knowledgeMoveTxKey struct{}

type knowledgeMoveTx struct {
	repository *gorm.DB
	tx         *gorm.DB
}

// KnowledgeMoveDB uses the application transaction only for an index
// backed by the same connection pool. Tenant-configured remote stores keep
// their own connection and retry-safe relocation.
func KnowledgeMoveDB(ctx context.Context, index *gorm.DB) *gorm.DB {
	move, ok := ctx.Value(knowledgeMoveTxKey{}).(knowledgeMoveTx)
	if ok && index.ConnPool == move.repository.ConnPool {
		return move.tx.WithContext(ctx)
	}
	return index.WithContext(ctx)
}

// KnowledgeMoveGraph binds the SQL part of a graph repository to the move transaction.
// Its normal authorization lookup must see the same rows even on a single-connection Lite database.
func KnowledgeMoveGraph(
	ctx context.Context, graph interfaces.RetrieveGraphRepository,
) interfaces.RetrieveGraphRepository {
	if scoped, ok := graph.(*ScopedSQLGraphRepository); ok {
		return NewScopedSQLGraphRepository(KnowledgeMoveDB(ctx, scoped.db), scoped.fallback)
	}
	return graph
}

// validateMoveBodyHead checks both the immutable slot revision and the actual inline producer bytes.
// An empty field can retain its last nonempty revision; moving it must never restore those bytes.
func validateMoveBodyHead(tx *gorm.DB, d originalBodyFieldDescriptor, key, col string,
	prior originalBodyFieldHead, source originalBodyScope, raw string,
) error {
	if prior.SlotID == "" || d.Mode != "inline" ||
		prior.SourceIdentitySHA256 != bodyIdentity(d.Table, key, col, source) ||
		prior.TenantID != source.Tenant || prior.KnowledgeBaseID != source.KB || prior.KnowledgeID != source.Knowledge {
		return ErrOriginalBodyUnavailable
	}
	var revision originalBodyFieldHead
	if err := bodyInternal(tx).Table("original_body_field_revisions").
		Where("slot_id=? AND revision=?", prior.SlotID, prior.Revision).Take(&revision).Error; err != nil {
		return err
	}
	if revision != prior {
		return ErrOriginalBodyUnavailable
	}
	var payload originalBodyPayload
	if err := bodyInternal(tx).Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id=? AND tenant_id=?", prior.BodyID, source.Tenant).Take(&payload).Error; err != nil {
		return err
	}
	ref, ok := bodyReference(prior.OriginalBodyRef)
	if !ok || ref.SlotID != prior.SlotID || ref.BodyID != payload.ID || ref.SHA256 != payload.BodySHA256 ||
		payload.State != "live" || (raw != "" && raw != "null" && payload.BodySHA256 != bodySHA([]byte(raw))) {
		return ErrOriginalBodyUnavailable
	}
	return nil
}
