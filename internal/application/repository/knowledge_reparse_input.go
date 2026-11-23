package repository

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

// CheckKnowledgeOriginalConfiguration prevents a reparse from mutating its
// configuration before discovering that its immutable admission no longer fits.
func CheckKnowledgeOriginalConfiguration(
	proof *types.KnowledgeBuildInputProof, kb *types.KnowledgeBase, k *types.Knowledge,
) error {
	if proof == nil || kb == nil || k == nil || proof.TenantID != k.TenantID ||
		proof.KnowledgeBaseID != kb.ID || proof.KnowledgeID != k.ID {
		return types.ErrSourceLineageUnknown
	}
	digest, err := knowledgeConfigDigest(kb, k)
	if err != nil {
		return err
	}
	if digest != proof.ConfigDigest {
		return ErrKnowledgeInputChanged
	}
	return nil
}

// LoadWebDAVReparseInput reads only the retained derived admission bound to
// this document and its configured source in the same tenant and KB.
func (s *KnowledgeInputProvenanceStore) LoadWebDAVReparseInput(
	ctx context.Context, k *types.Knowledge,
) (*types.KnowledgeBuildInputProof, error) {
	if k == nil || k.Channel != types.ConnectorTypeNextcloudWebDAV || k.Type != "file" {
		return nil, types.ErrSourceLineageUnknown
	}
	meta := k.GetMetadata()
	if meta["datasource_id"] == "" || meta["external_id"] == "" {
		return nil, types.ErrSourceLineageUnknown
	}
	source := s.db.Model(&types.DataSource{}).Select("1").Where(
		"id = ? AND type = ? AND tenant_id = ? AND knowledge_base_id = ?",
		meta["datasource_id"], types.ConnectorTypeNextcloudWebDAV, k.TenantID, k.KnowledgeBaseID,
	)
	query := s.db.WithContext(ctx).Where("a.origin_kind = ? AND EXISTS (?)", "derived", source)
	return loadKnowledgeOriginalInput(query, k.TenantID, k.KnowledgeBaseID, k.ID)
}
