package repository

import (
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/datasource/connector/dingtalk"
	"gorm.io/gorm"
)

// loadDingTalkPublishedOriginal verifies immutable ownership of a published
// document. It deliberately does not authenticate its current generated fields:
// replacement content must come from the separately verified fresh artifact.
func loadDingTalkPublishedOriginal(tx *gorm.DB, p dingTalkPublication) (*knowledgeInputAdmission, error) {
	var r dingTalkOriginalReceipt
	if err := tx.Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=? AND data_source_id=?", p.TenantID, p.KnowledgeBaseID, p.KnowledgeID, p.DataSourceID).Take(&r).Error; err != nil {
		return nil, err
	}
	var a knowledgeInputAdmission
	if err := tx.Where("admission_id=? AND tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", r.AdmissionID, p.TenantID, p.KnowledgeBaseID, p.KnowledgeID).Take(&a).Error; err != nil {
		return nil, err
	}
	var facts dingtalk.OriginalFetchedArtifactInput
	if err := json.Unmarshal([]byte(r.Facts), &facts); err != nil {
		return nil, err
	}
	digest, err := KnowledgeInputDigest(facts)
	if err != nil {
		return nil, err
	}
	if a.OriginKind != dingTalkOriginalKind || r.SourceRevision == 0 || r.SyncLogID == "" || r.RootDigest != a.RootDigest || r.InputDigest != a.InputDigest || digest != r.FactsDigest || facts.DocumentID != p.ExternalID || facts.ItemDigest != p.ItemDigest || facts.SourceConfigDigest == "" || facts.ArtifactSHA256 == "" {
		return nil, ErrKnowledgeInputChanged
	}
	return &a, nil
}
