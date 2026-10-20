package repository

import (
	"context"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// New creation copies every digest-bearing processing field, including enrichment,
// FAQ and wiki controls. Existing legal targets keep their actual configuration. Names,
// descriptions, generated profiles and runtime counters are never template input.
var onboardingProcessingFields = []string{
	"chunking_config",
	"image_processing_config",
	"embedding_model_id",
	"summary_model_id",
	"vlm_config",
	"asr_config",
	"extract_config",
	"faq_config",
	"question_generation_config",
	"auto_tag_config",
	"profile_config",
	"wiki_config",
	"indexing_strategy",
	"enrichment_templates",
}

func onboardingConfigurationProjection(tx *gorm.DB, tenant uint64, id string) (*types.KnowledgeBase, error) {
	var kb types.KnowledgeBase
	fields := append(
		[]string{"id", "tenant_id", "type", "is_temporary", "storage_backend_id", "vector_store_id"},
		onboardingProcessingFields...)
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Select(fields).
		Where("tenant_id=? AND id=? AND deleted_at IS NULL", tenant, id).
		Take(&kb).
		Error; err != nil {
		return nil, err
	}
	return &kb, nil
}

func onboardingBlankTargetMetadata(tx *gorm.DB, tenant uint64, id string) (*types.KnowledgeBase, error) {
	var row struct {
		ID                     string
		TenantID               uint64
		Type                   string
		IsTemporary            bool
		EverHadNextcloudSource bool
		HasProfile             bool
		VectorStoreID          *string
	}
	if err := tx.Table("knowledge_bases").
		Select(("id,tenant_id,type,is_temporary,ever_had_nextcloud_source,generated_profile" +
			" IS NOT NULL AS has_profile,vector_store_id")).
		Where("tenant_id=? AND id=? AND deleted_at IS NULL", tenant, id).
		Take(&row).
		Error; err != nil {
		return nil, ErrNextcloudKBBlankUnverified
	}
	if row.Type != types.KnowledgeBaseTypeDocument || row.IsTemporary || row.EverHadNextcloudSource || row.HasProfile {
		return nil, ErrNextcloudKBBlankUnverified
	}
	var n int64
	if err := tx.Table("nextcloud_kb_blank_witnesses").
		Where("tenant_id=? AND knowledge_base_id=? AND virgin=?", tenant, id, true).
		Count(&n).
		Error; err != nil ||
		n != 1 {
		return nil, ErrNextcloudKBBlankUnverified
	}
	for _, table := range []string{
		"knowledges",
		"data_sources",
		"wiki_pages",
		"chunks",
		"nextcloud_onboarding_operations",
	} {
		if err := tx.Table(table).
			Where("tenant_id=? AND knowledge_base_id=?", tenant, id).
			Count(&n).
			Error; err != nil ||
			n != 0 {
			return nil, ErrNextcloudKBBlankUnverified
		}
	}
	return &types.KnowledgeBase{
		ID:            row.ID,
		TenantID:      row.TenantID,
		Type:          row.Type,
		VectorStoreID: row.VectorStoreID,
	}, nil
}

func cloneOnboardingProcessingConfiguration(
	template *types.KnowledgeBase,
	id string,
	tenant uint64,
) *types.KnowledgeBase {
	return &types.KnowledgeBase{
		ID:                       id,
		TenantID:                 tenant,
		Type:                     types.KnowledgeBaseTypeDocument,
		ChunkingConfig:           template.ChunkingConfig,
		ImageProcessingConfig:    template.ImageProcessingConfig,
		EmbeddingModelID:         template.EmbeddingModelID,
		SummaryModelID:           template.SummaryModelID,
		VLMConfig:                template.VLMConfig,
		ASRConfig:                template.ASRConfig,
		StorageBackendID:         template.StorageBackendID,
		VectorStoreID:            template.VectorStoreID,
		ExtractConfig:            template.ExtractConfig,
		FAQConfig:                template.FAQConfig,
		QuestionGenerationConfig: template.QuestionGenerationConfig,
		AutoTagConfig:            template.AutoTagConfig,
		ProfileConfig:            template.ProfileConfig,
		WikiConfig:               template.WikiConfig,
		IndexingStrategy:         template.IndexingStrategy,
		EnrichmentTemplates:      template.EnrichmentTemplates,
	}
}

func writeOnboardingConfigurationClone(
	ctx context.Context,
	tx *gorm.DB,
	c types.NextcloudOnboardingCredential,
	req types.NextcloudOnboardingRequest,
	actor string,
	read *NextcloudOnboardingConfigurationRead,
) (*types.KnowledgeBase, error) {
	// Every original body read follows actual authority and committed physical
	// leases; this transaction never acquires/renews through a second connection.
	if err := read.checkInTx(ctx, tx); err != nil {
		return nil, err
	}
	if req.Mode == "existing" {
		if q := tx.Exec(
			("UPDATE knowledge_bases SET ever_had_nextcloud_source=ever_had_nextcloud_so" +
				"urce WHERE tenant_id=? AND id=? AND deleted_at IS NULL"),
			c.TenantID,
			req.KnowledgeBaseID,
		); q.Error != nil ||
			q.RowsAffected != 1 {
			return nil, ErrNextcloudKBBlankUnverified
		}
		if _, _, err := onboardingExistingTarget(tx, c.TenantID, req.KnowledgeBaseID); err != nil {
			return nil, err
		}
		if err := requireNextcloudTargetOpenInTx(tx, c.TenantID, req.KnowledgeBaseID); err != nil {
			return nil, err
		}
		actual, err := onboardingConfigurationProjection(tx, c.TenantID, req.KnowledgeBaseID)
		if err != nil {
			return nil, err
		}
		digest, err := knowledgeConfigurationOriginDigest(actual)
		if err != nil {
			return nil, err
		}
		if digest != read.origin.InputDigest {
			return nil, ErrKnowledgeInputChanged
		}
		if _, err := readKnowledgeConfigurationOrigin(tx.WithContext(ctx), actual); err != nil {
			return nil, err
		}
		if err := validateOnboardingTemplate(tx, actual); err != nil {
			return nil, err
		}
		if err := read.checkInTx(ctx, tx); err != nil {
			return nil, err
		}
		// PRD §5 preserves the target ID and every already legal configuration
		// field. No template bytes, name, storage selection or origin is rewritten.
		return actual, nil
	}
	template, err := onboardingConfigurationProjection(tx, c.TenantID, c.TemplateKBID)
	if err != nil {
		return nil, err
	}
	digest, err := knowledgeConfigurationOriginDigest(template)
	if err != nil {
		return nil, err
	}
	if digest != read.origin.InputDigest {
		return nil, ErrKnowledgeInputChanged
	}
	if err := validateOnboardingTemplate(tx, template); err != nil {
		return nil, err
	}
	lineage, err := readKnowledgeConfigurationOrigin(tx.WithContext(ctx), template)
	if err != nil {
		return nil, err
	}
	target := cloneOnboardingProcessingConfiguration(template, uuid.NewString(), c.TenantID)
	target.Name = req.Name
	target.CreatorID = actor
	target.CreatedAt = time.Now().UTC()
	target.UpdatedAt = target.CreatedAt
	create := tx
	if actor == "" {
		create = create.Omit("creator_id")
	}
	if err := create.Create(target).Error; err != nil {
		return nil, err
	}
	if err := admitOnboardingKnowledgeBaseName(tx, req, c, target); err != nil {
		return nil, err
	}
	actual, err := onboardingConfigurationProjection(tx, c.TenantID, target.ID)
	if err != nil {
		return nil, err
	}
	if err := admitKnowledgeConfigurationOrigin(
		tx,
		actual,
		knowledgeInputOrigin{lineage: lineage, kind: "derived"},
	); err != nil {
		return nil, err
	}
	if err := validateOnboardingTemplate(tx, actual); err != nil {
		return nil, err
	}
	if err := read.checkInTx(ctx, tx); err != nil {
		return nil, err
	}
	return target, nil
}
