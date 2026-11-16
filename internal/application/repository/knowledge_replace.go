package repository

import (
	"context"
	"fmt"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ReplaceKnowledge keeps the previous row live if inserting its replacement
// fails. No file, index, queue or other remote operation runs in this transaction.
func (r *knowledgeRepository) ReplaceKnowledge(
	ctx context.Context, before, after *types.Knowledge, tagIDs []string,
) error {
	if invalidKnowledgeReplacement(before, after) {
		return fmt.Errorf("invalid knowledge replacement")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := retireKnowledgeForReplacement(tx, before); err != nil {
			return err
		}
		repo := &knowledgeRepository{db: tx}
		if err := repo.CreateKnowledge(ctx, after); err != nil {
			return err
		}
		if len(tagIDs) > 0 {
			return repo.SetKnowledgeTags(ctx, after.ID, tagIDs)
		}
		return nil
	})
}

func invalidKnowledgeReplacement(before, after *types.Knowledge) bool {
	return before == nil || after == nil || before.ID == "" || after.ID == "" || before.ID == after.ID ||
		before.TenantID != after.TenantID || before.KnowledgeBaseID != after.KnowledgeBaseID
}

func retireKnowledgeForReplacement(tx *gorm.DB, before *types.Knowledge) error {
	// The primary-key lookup locks only the source row. Comparing the snapshot
	// prevents a second sync from replacing a row another writer has retired
	// or changed while this sync was preparing its replacement.
	result := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND updated_at = ?",
		before.ID, before.TenantID, before.KnowledgeBaseID, before.UpdatedAt).
		Delete(&types.Knowledge{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("knowledge changed during replacement: %s", before.ID)
	}
	return nil
}

// ReplaceKnowledgeWithInput publishes a freshly fetched file, its build proof and its tags
// atomically with retirement of the previous data-source document.
func (s *KnowledgeInputProvenanceStore) ReplaceKnowledgeWithInput(
	ctx context.Context, before, after *types.Knowledge, tagIDs []string, input any,
) (*types.KnowledgeBuildInputProof, error) {
	if ctx == nil || s == nil || s.db == nil || invalidKnowledgeReplacement(before, after) ||
		after.TenantID == 0 || after.KnowledgeBaseID == "" || after.Type != "file" {
		return nil, ErrKnowledgeInputChanged
	}
	origin, ok := ctx.Value(knowledgeInputOriginKey{}).(knowledgeInputOrigin)
	if !ok || origin.kind != "derived" || origin.lineage.RequireComplete() != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	data, ok := input.([]byte)
	if !ok || len(data) == 0 {
		return nil, ErrKnowledgeInputChanged
	}
	var proof *types.KnowledgeBuildInputProof
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := checkOrdinaryReplacementSource(tx, before, after); err != nil {
			return err
		}
		if err := retireKnowledgeForReplacement(tx, before); err != nil {
			return err
		}
		var err error
		proof, err = s.createKnowledgeWithInputInTx(ctx, tx, after, data, origin)
		if err != nil || len(tagIDs) == 0 {
			return err
		}
		ids := normalizedKnowledgeDisplayTagIDs(tagIDs)
		var scoped []string
		if err := tx.Model(&types.KnowledgeTag{}).Clauses(clause.Locking{Strength: "SHARE"}).
			Where("id IN ? AND tenant_id = ? AND knowledge_base_id = ?", ids, after.TenantID, after.KnowledgeBaseID).
			Pluck("id", &scoped).Error; err != nil {
			return err
		}
		if len(scoped) != len(ids) {
			return ErrKnowledgeInputChanged
		}
		repo := &knowledgeRepository{db: tx, displayAuthorityFactory: s.displayAuthorityFactory}
		return repo.SetKnowledgeTags(WithKnowledgeBuildInputProof(ctx, s, proof), after.ID, ids)
	})
	if err != nil {
		return nil, err
	}
	return proof, nil
}

func checkOrdinaryReplacementSource(tx *gorm.DB, before, after *types.Knowledge) error {
	meta := after.GetMetadata()
	if meta["datasource_id"] == "" || meta["external_id"] == "" ||
		after.Channel == types.ConnectorTypeNextcloud || after.Channel == types.ConnectorTypeDingTalk {
		return ErrKnowledgeInputChanged
	}
	var source types.DataSource
	if err := tx.Select("type").Clauses(clause.Locking{Strength: "SHARE"}).
		Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?",
			meta["datasource_id"], after.TenantID, after.KnowledgeBaseID).
		Take(&source).Error; err != nil {
		return err
	}
	if source.Type == "" || source.Type == types.ConnectorTypeDingTalk || source.Type == types.ConnectorTypeNextcloud {
		return ErrKnowledgeInputChanged
	}
	var current types.Knowledge
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", before.ID, before.TenantID, before.KnowledgeBaseID).
		Take(&current).Error; err != nil {
		return err
	}
	stored := current.GetMetadata()
	if current.Channel == types.ConnectorTypeDingTalk || stored["datasource_id"] != meta["datasource_id"] ||
		stored["external_id"] != meta["external_id"] {
		return ErrKnowledgeInputChanged
	}
	controlled, err := ragKnowledgeIsControlled(tx, &current)
	if err != nil {
		return err
	}
	if controlled {
		return ErrKnowledgeInputChanged
	}
	// A relabelled DingTalk row is still owned by its original publication.
	var originals int64
	if err := tx.Model(&knowledgeInputAdmission{}).Where("knowledge_id IN ? AND origin_kind = ?",
		[]string{before.ID, after.ID}, dingTalkOriginalKind).Count(&originals).Error; err != nil {
		return err
	}
	if originals != 0 {
		return ErrKnowledgeInputChanged
	}
	return nil
}
