package repository

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
)

func (s *KnowledgeInputProvenanceStore) captureKnowledgeOutputAuxiliaryInTx(
	tx *gorm.DB,
	proofs []*types.KnowledgeBuildInputProof,
	selections []types.KnowledgeOutputDisplaySelection,
) (*types.KnowledgeOutputAuxiliaryInputSnapshot, error) {
	if len(proofs) > 4096 || len(selections) > len(proofs) {
		return nil, types.ErrSourceLineageLimit
	}
	result := &types.KnowledgeOutputAuxiliaryInputSnapshot{
		Proofs:     proofs,
		Selections: selections,
		Knowledge:  []*types.Knowledge{},
	}
	byID := map[string]*types.Knowledge{}
	for _, proof := range proofs {
		if proof == nil || proof.Projection != "" || len(proof.ChunkIDs) != 0 || proof.BodyDigest == "" ||
			byID[proof.KnowledgeID] != nil {
			return nil, types.ErrSourceLineageUnknown
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
			return nil, err
		}
		a, _, lineage, err := s.admissionInTx(tx, &k, proof)
		if err != nil {
			return nil, err
		}
		actual, err := collectionProof(tx, &k, a, lineage)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(actual, proof) {
			return nil, ErrKnowledgeInputChanged
		}
		byID[k.ID] = &k
		result.Knowledge = append(result.Knowledge, &k)
	}
	var tagCandidates []*types.Knowledge
	seen := map[string]bool{}
	consumeNames := map[string]bool{}
	scopeSet := map[types.KnowledgeSearchScope]bool{}
	for _, selection := range selections {
		k := byID[selection.KnowledgeID]
		if k == nil || selection.TenantID != k.TenantID || selection.KnowledgeBaseID != k.KnowledgeBaseID ||
			seen[k.ID] ||
			!selection.Tags && !selection.KnowledgeBaseName {
			return nil, ErrKnowledgeInputChanged
		}
		seen[k.ID] = true
		scopeSet[types.KnowledgeSearchScope{TenantID: k.TenantID, KBID: k.KnowledgeBaseID}] = true
		if selection.Tags {
			tagCandidates = append(tagCandidates, k)
		}
		if selection.KnowledgeBaseName {
			consumeNames[k.KnowledgeBaseID] = true
		}
	}
	var scopes []types.KnowledgeSearchScope
	for scope := range scopeSet {
		scopes = append(scopes, scope)
	}
	sort.Slice(scopes, func(i, j int) bool {
		if scopes[i].TenantID != scopes[j].TenantID {
			return scopes[i].TenantID < scopes[j].TenantID
		}
		return scopes[i].KBID < scopes[j].KBID
	})
	var err error
	result.Auxiliary, err = captureKnowledgeCollectionAuxiliaryWithNames(tx, scopes, tagCandidates, consumeNames)
	return result, err
}

// CaptureKnowledgeOutputAuxiliary captures knowledge output auxiliary.
// Both original document receipts and the actual consumed joined producers
// are bound in one RR/SQLite snapshot. Current display strings cannot seal an
// older material body or widen the explicit document membership.
func (s *KnowledgeInputProvenanceStore) CaptureKnowledgeOutputAuxiliary(
	ctx context.Context,
	proofs []*types.KnowledgeBuildInputProof,
	selections []types.KnowledgeOutputDisplaySelection,
) (*types.KnowledgeOutputAuxiliaryInputSnapshot, error) {
	var result *types.KnowledgeOutputAuxiliaryInputSnapshot
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		var err error
		result, err = s.captureKnowledgeOutputAuxiliaryInTx(tx, proofs, selections)
		return err
	})
	return result, err
}

// RecheckKnowledgeOutputAuxiliary rechecks knowledge output auxiliary.
func (s *KnowledgeInputProvenanceStore) RecheckKnowledgeOutputAuxiliary(
	ctx context.Context,
	snapshot *types.KnowledgeOutputAuxiliaryInputSnapshot,
) error {
	if snapshot == nil {
		return types.ErrSourceLineageUnknown
	}
	return s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		current, err := s.captureKnowledgeOutputAuxiliaryInTx(tx, snapshot.Proofs, snapshot.Selections)
		if err != nil {
			return err
		}
		expected, err := json.Marshal(snapshot)
		if err != nil {
			return err
		}
		actual, err := json.Marshal(current)
		if err != nil {
			return err
		}
		if string(expected) != string(actual) {
			return ErrKnowledgeInputChanged
		}
		return nil
	})
}

var _ interfaces.KnowledgeOutputAuxiliaryInputReader = (*KnowledgeInputProvenanceStore)(nil)
