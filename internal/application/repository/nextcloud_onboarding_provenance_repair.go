package repository

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ensureKnowledgeConfigurationMetadata repairs a narrowly defined legacy gap:
// the current control revision has no provenance row, but an earlier complete
// revision does. Initialization settings historically advanced the control
// revision without recording that save. The previous trusted configuration is
// used as the lineage, while the current validated configuration is admitted
// as a new human input and its original bodies are captured atomically.
//
// A knowledge base with no prior complete origin is never promoted.
func ensureKnowledgeConfigurationMetadata(
	ctx context.Context,
	tx *gorm.DB,
	tenant uint64,
	kbID string,
) (knowledgeConfigurationInputOrigin, *types.SourceLineage, error) {
	var origin knowledgeConfigurationInputOrigin
	var lineage *types.SourceLineage
	if current, lineage, err := onboardingConfigurationMetadata(tx, tenant, kbID); err == nil {
		return current, lineage, nil
	}

	err := tx.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var actual types.KnowledgeBase
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("tenant_id=? AND id=? AND deleted_at IS NULL", tenant, kbID).
			Take(&actual).Error; err != nil {
			return err
		}

		// Another candidate request may have repaired the same revision while
		// this transaction waited for the row lock.
		if current, actualLineage, err := onboardingConfigurationMetadata(tx, tenant, kbID); err == nil {
			origin = current
			lineage = actualLineage
			return actualLineage.RequireComplete()
		}

		revision, err := knowledgeConfigurationControlRevision(tx, kbID)
		if err != nil {
			return err
		}
		var previous knowledgeConfigurationInputOrigin
		err = tx.Where(
			"tenant_id=? AND knowledge_base_id=? AND config_revision<?",
			tenant,
			kbID,
			revision,
		).Order("config_revision DESC").Take(&previous).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return types.ErrSourceLineageUnknown
		}
		if err != nil {
			return err
		}
		var previousLineage types.SourceLineage
		if previous.InputDigest == "" ||
			json.Unmarshal([]byte(previous.SourceLineage), &previousLineage) != nil ||
			previousLineage.RequireComplete() != nil {
			return types.ErrSourceLineageUnknown
		}

		if err := admitKnowledgeConfigurationOrigin(
			tx,
			&actual,
			knowledgeInputOrigin{lineage: &previousLineage, kind: "human"},
		); err != nil {
			return err
		}
		actualOrigin, actualLineage, err := onboardingConfigurationMetadata(tx, tenant, kbID)
		if err != nil {
			return err
		}
		origin = actualOrigin
		lineage = actualLineage
		return nil
	})
	return origin, lineage, err
}
