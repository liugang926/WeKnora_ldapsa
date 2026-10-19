package repository

import (
	"context"
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// This projection is for a sealed, fresh connector/build purpose only. It is
// never an editable/full KB object: names, descriptions and generated profile
// bodies have separate original inputs and are not silently read here.
var knowledgeWorkerConfigurationColumns = []string{
	"id", "tenant_id", "type", "creator_id", "is_temporary", "ever_had_nextcloud_source",
	"chunking_config", "image_processing_config", "embedding_model_id", "summary_model_id",
	"vlm_config", "asr_config", "extract_config", "faq_config", "question_generation_config",
	"auto_tag_config", "profile_config", "wiki_config", "indexing_strategy", "enrichment_templates",
	"storage_provider_config", "storage_backend_id", "cos_config", "vector_store_id",
}

type scopedConfigurationProjector interface {
	ConfigurationProjection(context.Context) (*types.KnowledgeBase, error)
	ConfigurationProjectionInTx(context.Context, *gorm.DB) (*types.KnowledgeBase, error)
}

type scopedConfigurationProjection struct {
	kb     *types.KnowledgeBase
	digest string
}

// Normalize a detached copy only. The service's ordinary EnsureDefaults is a
// supported parser step; it cannot hide changes to effective model controls.
// EnrichmentTemplates is deliberately absent from KB JSON, so retain it in an
// explicit wrapper rather than dropping its real body while copying.
func knowledgeWorkerConfigurationProjectionDigest(kb *types.KnowledgeBase) (string, error) {
	if kb == nil {
		return "", ErrKnowledgeInputChanged
	}
	type wire struct {
		KB        *types.KnowledgeBase
		Templates *types.KnowledgeEnrichmentTemplates
	}
	encoded, err := json.Marshal(wire{kb, kb.EnrichmentTemplates})
	if err != nil {
		return "", err
	}
	var copy wire
	if err := json.Unmarshal(encoded, &copy); err != nil || copy.KB == nil {
		return "", ErrKnowledgeInputChanged
	}
	copy.KB.EnrichmentTemplates = copy.Templates
	copy.KB.EnsureDefaults()
	return KnowledgeInputDigest(wire{copy.KB, copy.KB.EnrichmentTemplates})
}

func checkScopedConfigurationProjections(slot *scopedOnboardingConfigurationReadSlot) error {
	if slot.err != nil {
		return slot.err
	}
	for _, held := range slot.projections {
		digest, err := knowledgeWorkerConfigurationProjectionDigest(held.kb)
		if err != nil || digest != held.digest {
			slot.err = ErrKnowledgeInputChanged
			return slot.err
		}
	}
	return nil
}

func pinScopedConfigurationProjection(slot *scopedOnboardingConfigurationReadSlot, kb *types.KnowledgeBase) error {
	if kb == nil || kb.ID != slot.kb || kb.TenantID != slot.tenant || kb.Name != "" || kb.Description != "" || kb.GeneratedProfile != nil {
		return ErrKnowledgeInputChanged
	}
	// The connector consumes its first projection for the whole sync. Each
	// later per-file value must match the actual original-upload receipt at
	// its writer, so a large manifest does not retain thousands of pointers.
	// Fresh model tasks retain every returned consumer projection until close.
	if !NextcloudBuildConfigurationReadRequired(slot.parent) && len(slot.projections) > 0 {
		return nil
	}
	digest, err := knowledgeWorkerConfigurationProjectionDigest(kb)
	if err != nil {
		return err
	}
	slot.projections = append(slot.projections, scopedConfigurationProjection{kb, digest})
	return nil
}

// A private read slot changes only exact worker getters. No slot means the
// ordinary public getter keeps its existing current authorization contract.
func scopedWorkerConfigurationProjection(ctx context.Context, kb string, tenant uint64) (*types.KnowledgeBase, bool, error) {
	if ctx == nil {
		return nil, false, nil
	}
	slot, _ := ctx.Value(scopedOnboardingConfigurationReadKey{}).(*scopedOnboardingConfigurationReadSlot)
	if slot == nil {
		return nil, false, nil
	}
	slot.mu.Lock()
	if slot.closed || slot.read == nil || kb != slot.kb || tenant != 0 && tenant != slot.tenant {
		slot.mu.Unlock()
		return nil, true, ErrKnowledgeInputChanged
	}
	if err := slot.parent.Err(); err != nil {
		slot.mu.Unlock()
		return nil, true, context.Cause(slot.parent)
	}
	if err := checkScopedConfigurationProjections(slot); err != nil {
		slot.mu.Unlock()
		return nil, true, err
	}
	projector, ok := slot.read.(scopedConfigurationProjector)
	slot.mu.Unlock()
	if !ok {
		return nil, true, types.ErrSourceLineageUnknown
	}
	projection, err := projector.ConfigurationProjection(ctx)
	if err != nil {
		return nil, true, err
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.closed || slot.parent.Err() != nil {
		return nil, true, ErrKnowledgeInputChanged
	}
	if err := checkScopedConfigurationProjections(slot); err != nil {
		return nil, true, err
	}
	if err := pinScopedConfigurationProjection(slot, projection); err != nil {
		return nil, true, err
	}
	return projection, true, nil
}

func loadKnowledgeBuildConfiguration(ctx context.Context, db *gorm.DB, tenant uint64, kb string) (*types.KnowledgeBase, error) {
	if current, handled, err := scopedWorkerConfigurationProjection(ctx, kb, tenant); handled {
		return current, err
	}
	var current types.KnowledgeBase
	err := db.WithContext(ctx).Where("id = ? AND tenant_id = ?", kb, tenant).Take(&current).Error
	return &current, err
}

// Original-input checks already own a pool-1 transaction. This port rechecks
// the same captured read in that transaction; it must never open a base-store
// transaction or recursively capture original inputs.
func loadKnowledgeBuildConfigurationInTx(tx *gorm.DB, tenant uint64, kb string) (*types.KnowledgeBase, error) {
	ctx := tx.Statement.Context
	slot, _ := ctx.Value(scopedOnboardingConfigurationReadKey{}).(*scopedOnboardingConfigurationReadSlot)
	if slot == nil {
		var current types.KnowledgeBase
		err := tx.Where("id = ? AND tenant_id = ?", kb, tenant).Take(&current).Error
		return &current, err
	}
	slot.mu.Lock()
	if slot.closed || slot.read == nil || slot.kb != kb || slot.tenant != tenant {
		slot.mu.Unlock()
		return nil, ErrKnowledgeInputChanged
	}
	if err := slot.parent.Err(); err != nil {
		slot.mu.Unlock()
		return nil, context.Cause(slot.parent)
	}
	if err := checkScopedConfigurationProjections(slot); err != nil {
		slot.mu.Unlock()
		return nil, err
	}
	projector, ok := slot.read.(scopedConfigurationProjector)
	slot.mu.Unlock()
	if !ok {
		return nil, types.ErrSourceLineageUnknown
	}
	current, err := projector.ConfigurationProjectionInTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	if current == nil || current.ID != kb || current.TenantID != tenant || current.Name != "" || current.Description != "" || current.GeneratedProfile != nil {
		return nil, ErrKnowledgeInputChanged
	}
	return current, nil
}

func scopedWorkerConfigurationFullSaveDenied(ctx context.Context, kb *types.KnowledgeBase) bool {
	if ctx == nil || kb == nil {
		return false
	}
	slot, _ := ctx.Value(scopedOnboardingConfigurationReadKey{}).(*scopedOnboardingConfigurationReadSlot)
	if slot == nil {
		return false
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if kb.ID == slot.kb && kb.TenantID == slot.tenant {
		return true
	}
	for _, held := range slot.projections {
		if held.kb == kb {
			return true
		}
	}
	return false
}
