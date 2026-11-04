package repository

import "github.com/Tencent/WeKnora/internal/types"

// CheckKnowledgeOriginalConfiguration prevents a reparse from mutating its
// configuration before discovering that its immutable admission no longer fits.
func CheckKnowledgeOriginalConfiguration(proof *types.KnowledgeBuildInputProof, kb *types.KnowledgeBase, k *types.Knowledge) error {
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
