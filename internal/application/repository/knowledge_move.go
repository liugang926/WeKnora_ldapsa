package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CommitKnowledgeMove changes the application rows and their retention receipts
// in one transaction. Remote vector engines use the existing idempotent move task.
func (r *knowledgeRepository) CommitKnowledgeMove(
	ctx context.Context, before, after *types.Knowledge, reuseChunks bool, finish, restore func(context.Context) error,
) error {
	if before == nil || after == nil || before.ID == "" || before.ID != after.ID || before.TenantID != after.TenantID {
		return fmt.Errorf("invalid knowledge move")
	}
	source := &types.KnowledgeBase{ID: before.KnowledgeBaseID, TenantID: before.TenantID}
	target := &types.KnowledgeBase{ID: after.KnowledgeBaseID, TenantID: after.TenantID}
	if err := access.RequireKBTransfer(ctx, source, target, access.KBTransferMove); err != nil {
		return err
	}
	var finished bool
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
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
		mode := "reparse"
		if reuseChunks {
			mode = "reuse_vectors"
		}
		move, ok := moveCheckpoint(&stored)
		if !ok ||
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
			err := finish(indexCtx)
			finished = err == nil
			return err
		}
		return nil
	})
	if err == nil || !finished {
		return err
	}
	committed, verifyErr := r.reconcileMoveCommit(ctx, before, after, restore)
	if committed && verifyErr == nil {
		return nil
	}
	return errors.Join(err, verifyErr)
}

func (r *knowledgeRepository) reconcileMoveCommit(ctx context.Context, before, after *types.Knowledge,
	restore func(context.Context) error,
) (bool, error) {
	// A driver can lose the commit acknowledgement even when the server committed.
	// Read the durable row under a fresh lock before deciding which side owns the index.
	verifyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	var committed bool
	verifyErr := r.db.WithContext(verifyCtx).Transaction(func(tx *gorm.DB) error {
		if tx.Name() == "sqlite" {
			// SQLite ignores FOR UPDATE. Reserve the writer before reading the checkpoint.
			if err := tx.Exec("UPDATE knowledges SET updated_at=updated_at WHERE 1=0").Error; err != nil {
				return err
			}
		}
		var actual types.Knowledge
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id=? AND tenant_id=?", before.ID, before.TenantID).Take(&actual).Error; err != nil {
			return err
		}
		switch {
		case sameMoveCheckpoint(&actual, after):
			committed = true
		case sameMoveCheckpoint(&actual, before):
			if restore != nil {
				move := knowledgeMoveTx{repository: r.db, tx: tx}
				return restore(context.WithValue(verifyCtx, knowledgeMoveTxKey{}, move))
			}
		default:
			return errors.New("cannot reconcile index: knowledge move checkpoint changed")
		}
		return nil
	})
	return committed, verifyErr
}

type storedMoveCheckpoint struct {
	TaskID    string `json:"task_id"`
	SourceID  string `json:"source_id"`
	SourceKB  string `json:"source_kb"`
	TargetKB  string `json:"target_kb"`
	Operation string `json:"operation"`
	Phase     string `json:"phase"`
	Mode      string `json:"mode"`
}

func moveCheckpoint(k *types.Knowledge) (storedMoveCheckpoint, bool) {
	var fields map[string]json.RawMessage
	var cp storedMoveCheckpoint
	if json.Unmarshal(k.Metadata, &fields) != nil ||
		json.Unmarshal(fields[types.KnowledgeTransferMetadataKey], &cp) != nil {
		return cp, false
	}
	return cp, cp.TaskID != ""
}

func sameMoveCheckpoint(actual, expected *types.Knowledge) bool {
	a, ok := moveCheckpoint(actual)
	b, valid := moveCheckpoint(expected)
	return ok && valid && a == b && actual.KnowledgeBaseID == expected.KnowledgeBaseID &&
		actual.ParseStatus == expected.ParseStatus && actual.UpdatedAt.Equal(expected.UpdatedAt)
}

// CompleteMoveSourceGraph locks the destination checkpoint before deleting the
// old graph. Repeated deletes are safe if cleanup succeeded but updating the phase failed.
func (r *knowledgeRepository) CompleteMoveSourceGraph(ctx context.Context, before, after *types.Knowledge,
	cleanup func(context.Context) error,
) error {
	if before == nil || after == nil || cleanup == nil {
		return errors.New("missing graph cleanup input")
	}
	from, ok := moveCheckpoint(before)
	to, valid := moveCheckpoint(after)
	if !ok || !valid || from.Phase != "source_graph_pending" || to.Phase != "done" ||
		from.Mode != "reuse_vectors" || from.Operation != string(access.KBTransferMove) ||
		from.SourceID != before.ID || from.TargetKB != before.KnowledgeBaseID ||
		from.TargetKB != after.KnowledgeBaseID ||
		from.TaskID != access.TransferTaskID(ctx) {
		return errors.New("invalid source graph cleanup checkpoint")
	}
	from.Phase = "done"
	if from != to {
		return errors.New("source graph cleanup target changed")
	}
	source := &types.KnowledgeBase{ID: from.SourceKB, TenantID: before.TenantID}
	target := &types.KnowledgeBase{ID: from.TargetKB, TenantID: before.TenantID}
	if err := access.RequireKBTransfer(ctx, source, target, access.KBTransferMove); err != nil {
		return err
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		moveRepo := *r
		moveRepo.db = tx
		// The existing CAS writes only if the exact destination row/version remains current.
		if err := moveRepo.UpdateKnowledgeForTransfer(ctx, before, after); err != nil {
			return err
		}
		moveCtx := context.WithValue(ctx, knowledgeMoveTxKey{}, knowledgeMoveTx{repository: r.db, tx: tx})
		return cleanup(moveCtx)
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
