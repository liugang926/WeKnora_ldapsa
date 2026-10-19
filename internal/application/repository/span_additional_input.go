package repository

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

const spanAutoTagAdditionalKind = "auto_tag_classifier"
const autoTagProducerSpanName = "postprocess.auto_tag"

type spanAdditionalInputContextKey struct{}
type spanAdditionalInputContext struct {
	reader spanAdditionalOriginalInputVerifier
	input  *types.SpanAdditionalOriginalInput
}

// The matcher only accepts an original private persisted producer manifest.
// A current display value or a complete client-declared envelope is not proof.
type spanAdditionalOriginalInputVerifier interface {
	verifySpanAdditionalOriginalInput(*gorm.DB, *types.SpanAdditionalOriginalInput, *types.KnowledgeBuildInputProof, bool) (*types.SourceLineage, error)
	captureSpanAdditionalOriginalInput(context.Context, *gorm.DB, *types.SpanAdditionalOriginalInput, *types.KnowledgeBuildInputProof, bool) (*types.SourceLineage, error)
}

func (s *KnowledgeInputProvenanceStore) captureSpanAdditionalOriginalInput(ctx context.Context, tx *gorm.DB, input *types.SpanAdditionalOriginalInput, original *types.KnowledgeBuildInputProof, allowOwnResult bool) (*types.SourceLineage, error) {
	lineage, err := s.verifySpanAdditionalOriginalInput(tx, input, original, allowOwnResult)
	if err != nil {
		return nil, err
	}
	if s.displayAuthorityFactory == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	authority := s.displayAuthorityFactory(tx)
	if authority == nil || authority.db != tx {
		return nil, types.ErrSourceLineageUnknown
	}
	if _, err := authority.authorizeOriginalKnowledgeProducerWrite(ctx, input.AutoTag.Original.KnowledgeBase, original); err != nil {
		return nil, err
	}
	return lineage, nil
}

func WithSpanAutoTagOriginalInput(ctx context.Context, store *KnowledgeInputProvenanceStore, snapshot *types.KnowledgeAutoTagInputSnapshot) (context.Context, error) {
	if store == nil || snapshot == nil || snapshot.ManifestID == "" || snapshot.InputDigest == "" || snapshot.Original == nil || snapshot.Original.Proof == nil {
		return ctx, types.ErrSourceLineageUnknown
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return ctx, err
	}
	var copy types.KnowledgeAutoTagInputSnapshot
	if json.Unmarshal(data, &copy) != nil {
		return ctx, ErrKnowledgeInputChanged
	}
	input := &types.SpanAdditionalOriginalInput{Kind: spanAutoTagAdditionalKind, ID: copy.ManifestID, InputDigest: copy.InputDigest, AutoTag: &copy}
	return context.WithValue(ctx, spanAdditionalInputContextKey{}, spanAdditionalInputContext{reader: store, input: input}), nil
}

func spanAdditionalInputMatches(a, b *types.SpanAdditionalOriginalInput) error {
	if a == nil || b == nil {
		return types.ErrSourceLineageUnknown
	}
	left, err := KnowledgeInputDigest(a)
	if err != nil {
		return err
	}
	right, err := KnowledgeInputDigest(b)
	if err != nil {
		return err
	}
	if left != right {
		return ErrKnowledgeInputChanged
	}
	return nil
}

func (s *KnowledgeInputProvenanceStore) verifySpanAdditionalOriginalInput(tx *gorm.DB, input *types.SpanAdditionalOriginalInput, original *types.KnowledgeBuildInputProof, allowOwnResult bool) (*types.SourceLineage, error) {
	if input == nil || input.Kind != spanAutoTagAdditionalKind || input.AutoTag == nil || input.ID != input.AutoTag.ManifestID || input.InputDigest != input.AutoTag.InputDigest || input.AutoTag.Original == nil || input.AutoTag.Original.Proof == nil || original == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	before, err := KnowledgeInputDigest(original)
	if err != nil {
		return nil, err
	}
	after, err := KnowledgeInputDigest(input.AutoTag.Original.Proof)
	if err != nil || before != after {
		return nil, ErrKnowledgeInputChanged
	}
	return s.verifyKnowledgeAutoTagOriginalModelInputsInTx(tx, input.AutoTag, allowOwnResult)
}

func (s *KnowledgeInputProvenanceStore) verifyKnowledgeAutoTagOriginalModelInputsInTx(tx *gorm.DB, expected *types.KnowledgeAutoTagInputSnapshot, allowOwnResult bool) (*types.SourceLineage, error) {
	if expected == nil || expected.Original == nil || expected.Original.Proof == nil || expected.ManifestID == "" {
		return nil, types.ErrSourceLineageUnknown
	}
	var manifest knowledgeAutoTagManifest
	if err := tx.Where("manifest_id = ?", expected.ManifestID).Take(&manifest).Error; err != nil {
		return nil, err
	}
	var saved types.KnowledgeAutoTagInputSnapshot
	if json.Unmarshal([]byte(manifest.ActualInputs), &saved) != nil {
		return nil, ErrKnowledgeInputChanged
	}
	left, err := KnowledgeInputDigest(&saved)
	if err != nil {
		return nil, err
	}
	right, err := KnowledgeInputDigest(expected)
	if err != nil || left != right {
		return nil, ErrKnowledgeInputChanged
	}
	digest, err := autoTagSnapshotDigest(&saved)
	if err != nil || digest != manifest.InputDigest || digest != expected.InputDigest || saved.Original == nil || saved.Original.Proof == nil {
		return nil, ErrKnowledgeInputChanged
	}
	proof := saved.Original.Proof
	if manifest.TenantID != proof.TenantID || manifest.KnowledgeBaseID != proof.KnowledgeBaseID || manifest.KnowledgeID != proof.KnowledgeID || manifest.AdmissionID != proof.AdmissionID {
		return nil, ErrKnowledgeInputChanged
	}
	actual, err := s.captureKnowledgeAutoTagInTx(tx, proof)
	if err != nil {
		return nil, err
	}
	if err := acceptKnowledgeAutoPublicationTransition(tx, &saved, actual); err != nil {
		return nil, err
	}
	if allowOwnResult {
		// The exact root/chunk/config proof above checks every consumed body
		// and material revision. Later operational status/heartbeat fields are
		// not new classifier inputs and cannot replace the immutable bytes.
		actual.Original = saved.Original
	}
	currentDigest, err := autoTagSnapshotDigest(actual)
	if err != nil {
		return nil, err
	}
	if currentDigest != digest && allowOwnResult {
		// The classifier's own append is an output of this same original
		// execution. It must have an exact current atomic producer receipt.
		// No other relationship mutation can exempt the original membership.
		if err := verifyAutoTagOwnRelationResult(tx, &saved, actual); err != nil {
			return nil, err
		}
		actual.Relations = saved.Relations
		actual.RelationProof = saved.RelationProof
		currentDigest, err = autoTagSnapshotDigest(actual)
		if err != nil {
			return nil, err
		}
	}
	if currentDigest != digest {
		return nil, ErrKnowledgeInputChanged
	}
	return actual.SourceLineage, nil
}

func verifyAutoTagOwnRelationResult(tx *gorm.DB, saved, actual *types.KnowledgeAutoTagInputSnapshot) error {
	if saved.RelationProof == nil || actual.RelationProof == nil || saved.RelationProof.Revision == actual.RelationProof.Revision {
		return ErrKnowledgeInputChanged
	}
	k := saved.Original.Knowledge
	var receipt knowledgeDisplayInputOrigin
	if err := tx.Where("kind='relations' AND row_id = ? AND tenant_id = ? AND knowledge_base_id = ? AND revision = ?", k.ID, k.TenantID, k.KnowledgeBaseID, actual.RelationProof.Revision).Take(&receipt).Error; err != nil {
		return err
	}
	if receipt.OriginKind != "derived" {
		return ErrKnowledgeInputChanged
	}
	var request knowledgeDisplayRequest
	if json.Unmarshal([]byte(receipt.ActualInput), &request) != nil || request.Kind != "derived" || request.AutoTag == nil {
		return ErrKnowledgeInputChanged
	}
	left, err := KnowledgeInputDigest(request.AutoTag)
	if err != nil {
		return err
	}
	right, err := KnowledgeInputDigest(saved)
	if err != nil || left != right {
		return ErrKnowledgeInputChanged
	}
	present := map[string]bool{}
	for _, relation := range actual.Relations {
		present[relation.TagID] = true
	}
	for _, relation := range saved.Relations {
		if !present[relation.TagID] {
			return ErrKnowledgeInputChanged
		}
		delete(present, relation.TagID)
	}
	candidates := map[string]bool{}
	for _, tag := range saved.Tags {
		candidates[tag.ID] = true
	}
	if len(present) == 0 {
		return ErrKnowledgeInputChanged
	}
	for id := range present {
		if !candidates[id] {
			return ErrKnowledgeInputChanged
		}
	}
	if actual.RelationProof.SourceLineage.RequireComplete() != nil {
		return types.ErrSourceLineageUnknown
	}
	left, err = KnowledgeInputDigest(actual.RelationProof.SourceLineage)
	if err != nil {
		return err
	}
	right, err = KnowledgeInputDigest(saved.SourceLineage)
	if err != nil || left != right {
		return ErrKnowledgeInputChanged
	}
	return nil
}

var ErrSpanAdditionalInputUnknown = errors.New("span original additional input is unknown")
