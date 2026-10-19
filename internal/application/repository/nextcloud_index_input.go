package repository

import (
	"context"
	"fmt"
	"sort"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// LockNextcloudIndexKnowledgeBaseInTx takes the same entity-before-sentinel
// order as source/GC writers. Call before ValidateNextcloudBuildWrite, never
// after holding its content sentinel. Presented private inputs keep the exact
// original scope; legacy lease-only native callers retain their contract.
func LockNextcloudIndexKnowledgeBaseInTx(ctx context.Context, tx *gorm.DB, tenant uint64,
	kb, knowledge string,
) error {
	if ctx == nil || tx == nil {
		return ErrKnowledgeInputChanged
	}
	guard, present := ctx.Value(knowledgeBuildInputKey{}).(knowledgeBuildInputContext)
	if !present {
		if _, producer := ctx.Value(knowledgeProducerInputContextKey{}).(knowledgeProducerInputContext); producer {
			return ErrKnowledgeInputChanged
		}
		return nil
	}
	if guard.proof == nil || guard.proof.TenantID != tenant || guard.proof.KnowledgeBaseID != kb ||
		guard.proof.KnowledgeID != knowledge {
		return ErrKnowledgeInputChanged
	}
	if err := guard.admission.Err(); err != nil {
		return err
	}
	var row types.KnowledgeBase
	return tx.WithContext(ctx).Select("id").Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ? AND tenant_id = ?", kb, tenant).Take(&row).Error
}

// ValidateNextcloudIndexInputWriteInTx rechecks an actual worker's persisted
// original and producer/model inputs in the vector write transaction. Native
// callers without an original-input context retain their existing lease
// contract; a presented input context must pass every current-input check.
func ValidateNextcloudIndexInputWriteInTx(ctx context.Context, tx *gorm.DB, tenant uint64,
	kb, knowledge string,
) error {
	if ctx == nil || tx == nil {
		return ErrKnowledgeInputChanged
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := validateKnowledgeInputWrite(ctx, tx, tenant, kb, knowledge); err != nil {
		return err
	}
	producer, present := ctx.Value(knowledgeProducerInputContextKey{}).(knowledgeProducerInputContext)
	if !present {
		return nil
	}
	guard, bound := ctx.Value(knowledgeBuildInputKey{}).(knowledgeBuildInputContext)
	if !bound || guard.proof == nil || producer.proof == nil {
		return ErrKnowledgeInputChanged
	}
	// SQLite has already acquired its write transaction through the real
	// lease fence. PostgreSQL needs the precise version rows held to commit:
	// entity UPDATE triggers cannot finish a configuration/model mutation
	// between this check and vector insertion. Do not lock model/tenant entity
	// rows after their revisions: their UPDATE triggers use the reverse order.
	if tx.Name() != "postgres" {
		return nil
	}
	currentKB, err := loadKnowledgeBuildConfigurationInTx(tx, tenant, kb)
	if err != nil {
		return err
	}
	var current types.Knowledge
	if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", knowledge, tenant, kb).
		Take(&current).Error; err != nil {
		return err
	}
	ids, err := knowledgeProducerModelIDs(currentKB, &current, producer.proof.Purpose)
	if err != nil {
		return err
	}
	type revisionKey struct{ kind, id string }
	keys := []revisionKey{{"routing", kb}, {"tenant", fmt.Sprint(tenant)}}
	seen := map[string]bool{}
	for _, id := range ids {
		if id != "" && !seen[id] {
			seen[id] = true
			keys = append(keys, revisionKey{"model", id})
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].kind == keys[j].kind {
			return keys[i].id < keys[j].id
		}
		return keys[i].kind < keys[j].kind
	})
	for _, key := range keys {
		var row struct{ Revision uint64 }
		if err := tx.Table("knowledge_enrichment_row_versions").Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("row_kind = ? AND row_id = ?", key.kind, key.id).Take(&row).Error; err != nil {
			return err
		}
		if row.Revision == 0 {
			return ErrKnowledgeInputChanged
		}
	}
	// A mutation may have won before a revision lock was acquired. Re-read
	// every original, selected model, routing and tenant digest after the
	// complete lock set; an old MVCC observation cannot authorize this write.
	_, err = validateKnowledgeInputWrite(ctx, tx, tenant, kb, knowledge)
	return err
}
