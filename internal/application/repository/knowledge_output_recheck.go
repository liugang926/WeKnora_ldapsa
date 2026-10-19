package repository

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// RecheckKnowledgeOutputInputs checks the entire original output set in one
// database snapshot. Per-row transactions cannot establish a coherent page.
func (s *KnowledgeInputProvenanceStore) RecheckKnowledgeOutputInputs(
	ctx context.Context,
	proofs []*types.KnowledgeBuildInputProof,
) error {
	if ctx == nil || s == nil || s.db == nil {
		return types.ErrSourceLineageUnknown
	}
	if len(proofs) > 4096 {
		return types.ErrSourceLineageLimit
	}
	options := &sql.TxOptions{ReadOnly: true}
	switch s.db.Name() {
	case "postgres":
		options.Isolation = sql.LevelRepeatableRead
	case "sqlite":
	default:
		return ErrKnowledgeInputChanged
	}
	if _, nested := s.db.Statement.ConnPool.(gorm.TxCommitter); nested {
		return ErrKnowledgeInputChanged
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, proof := range proofs {
			if proof == nil || proof.Projection != "" || proof.BodyDigest == "" ||
				proof.SourceLineage.RequireComplete() != nil ||
				len(proof.ChunkIDs) > 4096 {
				return types.ErrSourceLineageUnknown
			}
			var k types.Knowledge
			if err := tx.Where(
				"id = ? AND tenant_id = ? AND knowledge_base_id = ?",
				proof.KnowledgeID,
				proof.TenantID,
				proof.KnowledgeBaseID,
			).
				Take(&k).
				Error; err != nil {
				return err
			}
			admission, _, _, err := s.admissionInTx(tx, &k, proof)
			if err != nil {
				return err
			}
			kgDigest, err := knowledgeMaterialDigest(&k)
			if err != nil {
				return err
			}
			material := []string{admission.AdmissionID, admission.RootDigest, admission.ConfigDigest, kgDigest}
			ids := append([]string{}, proof.ChunkIDs...)
			sort.Strings(ids)
			for i, id := range ids {
				if id == "" || i > 0 && ids[i-1] == id {
					return ErrKnowledgeInputChanged
				}
				var chunk types.Chunk
				if err := tx.Where(
					"id = ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
					id,
					k.TenantID,
					k.KnowledgeBaseID,
					k.ID,
				).
					Take(&chunk).
					Error; err != nil {
					return err
				}
				if err := verifyKnowledgeInputMaterial(tx, admission, &k, &chunk); err != nil {
					return err
				}
				digest, err := knowledgeChunkDigest(&chunk)
				if err != nil {
					return err
				}
				revision, err := inputRowRevision(tx, "chunk", id)
				if err != nil {
					return err
				}
				material = append(material, fmt.Sprintf("%s:%d:%s", id, revision, digest))
			}
			revision, err := inputRowRevision(tx, "knowledge", k.ID)
			if err != nil {
				return err
			}
			material = append(material, fmt.Sprint(revision))
			digest, err := KnowledgeInputDigest(material)
			if err != nil {
				return err
			}
			if digest != proof.BodyDigest {
				return ErrKnowledgeInputChanged
			}
		}
		return nil
	}, options)
}
