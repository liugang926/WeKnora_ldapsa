package repository

import (
	"fmt"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

func knowledgeAutoTagModelInputs(tx *gorm.DB, kb *types.KnowledgeBase) (string, *types.Tenant, error) {
	if kb.AutoTagConfig == nil || !kb.AutoTagConfig.Enabled {
		return "", nil, nil
	}
	id := strings.TrimSpace(kb.AutoTagConfig.ModelID)
	if id == "" {
		id = strings.TrimSpace(kb.SummaryModelID)
	}
	if id == "" {
		return "", nil, nil
	}
	var model types.Model
	if err := tx.Where("id = ? AND (tenant_id = ? OR is_builtin = ?)", id, kb.TenantID, true).
		Take(&model).
		Error; err != nil {
		return "", nil, err
	}
	if model.Status != types.ModelStatusActive {
		return "", nil, ErrKnowledgeInputChanged
	}
	modelRevision, err := knowledgeEnrichmentRevision(tx, "model", id)
	if err != nil {
		return "", nil, err
	}
	var tenant types.Tenant
	if err := tx.Where("id = ?", kb.TenantID).Take(&tenant).Error; err != nil {
		return "", nil, err
	}
	tenantRevision, err := knowledgeEnrichmentRevision(tx, "tenant", fmt.Sprint(kb.TenantID))
	if err != nil {
		return "", nil, err
	}
	digest, err := knowledgeProducerTenantDigest(&tenant)
	if err != nil {
		return "", nil, err
	}
	envelope, err := KnowledgeInputDigest(struct {
		Model                         types.Model
		ModelRevision, TenantRevision uint64
		TenantDigest                  string
	}{model, modelRevision, tenantRevision, digest})
	return envelope, &tenant, err
}
