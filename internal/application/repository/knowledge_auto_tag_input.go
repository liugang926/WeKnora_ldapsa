package repository

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type knowledgeAutoTagManifest struct {
	ManifestID                                                                          string `gorm:"primaryKey"`
	TenantID                                                                            uint64
	KnowledgeBaseID, KnowledgeID, AdmissionID, InputDigest, ActualInputs, SourceLineage string
}

func (knowledgeAutoTagManifest) TableName() string { return "knowledge_auto_tag_input_manifests" }

func (s *KnowledgeInputProvenanceStore) captureKnowledgeAutoTagInTx(tx *gorm.DB, proof *types.KnowledgeBuildInputProof) (*types.KnowledgeAutoTagInputSnapshot, error) {
	if proof == nil || proof.BodyDigest == "" || proof.Projection != "" {
		return nil, types.ErrSourceLineageUnknown
	}
	original, err := s.captureKnowledgeBuildSnapshotInTx(tx, types.KnowledgeBuildInputCaptureRequest{TenantID: proof.TenantID, KnowledgeBaseID: proof.KnowledgeBaseID, KnowledgeID: proof.KnowledgeID, ChunkIDs: proof.ChunkIDs, Expected: proof}, true)
	if err != nil {
		return nil, err
	}
	result := &types.KnowledgeAutoTagInputSnapshot{Original: &types.KnowledgeAutoTagOriginalInput{Knowledge: original.Knowledge, KnowledgeBase: original.KnowledgeBase, Chunks: original.Chunks, Proof: original.Proof}, Tags: []*types.KnowledgeTag{}, TagProofs: []*types.KnowledgeCollectionAuxiliaryProof{}, Relations: []*types.KnowledgeTagRelation{}}
	kb := original.KnowledgeBase
	k := original.Knowledge
	if err := tx.Model(&types.KnowledgeTag{}).Where("tenant_id = ? AND knowledge_base_id = ?", kb.TenantID, kb.ID).Count(&result.TotalTags).Error; err != nil {
		return nil, err
	}
	if err := tx.Where("tenant_id = ? AND knowledge_base_id = ?", kb.TenantID, kb.ID).Order("sort_order ASC,created_at DESC,seq_id DESC").Limit(500).Find(&result.Tags).Error; err != nil {
		return nil, err
	}
	result.TagListRevision, err = knowledgeDisplayRevision(tx, "tag-list", kb.ID)
	if err != nil {
		return nil, err
	}
	lineages := []*types.SourceLineage{proof.SourceLineage}
	for _, tag := range result.Tags {
		for _, field := range []string{"name", "sort_order"} {
			p, err := readKnowledgeDisplayOrigin(tx, "tag-"+field, tag.ID, tag.TenantID, tag.KnowledgeBaseID, tagDisplayFieldValue(tag, field))
			if err != nil {
				return nil, err
			}
			result.TagProofs = append(result.TagProofs, p)
			lineages = append(lineages, p.SourceLineage)
		}
	}
	if err := tx.Where("knowledge_id = ?", k.ID).Order("tag_id ASC").Find(&result.Relations).Error; err != nil {
		return nil, err
	}
	ids := []string{}
	for _, relation := range result.Relations {
		ids = append(ids, relation.TagID)
	}
	if len(ids) > 0 {
		result.RelationProof, err = readKnowledgeDisplayOrigin(tx, "relations", k.ID, k.TenantID, k.KnowledgeBaseID, ids)
		if err != nil {
			return nil, err
		}
		lineages = append(lineages, result.RelationProof.SourceLineage)
	} else {
		revision, err := knowledgeDisplayRevision(tx, "relations", k.ID)
		if err != nil {
			return nil, err
		}
		digest, err := KnowledgeInputDigest(knowledgeDisplayBody("relations", k.ID, k.TenantID, k.KnowledgeBaseID, ids))
		if err != nil {
			return nil, err
		}
		lineage, _ := types.NewCompleteSourceLineage()
		result.RelationProof = &types.KnowledgeCollectionAuxiliaryProof{Kind: "relations", ID: k.ID, TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, Revision: revision, Digest: digest, SourceLineage: lineage}
	}
	result.SourceLineage, err = types.UnionSourceLineage(lineages...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func autoTagSnapshotDigest(snapshot *types.KnowledgeAutoTagInputSnapshot) (string, error) {
	copy := *snapshot
	copy.ManifestID = ""
	copy.InputDigest = ""
	return KnowledgeInputDigest(&copy)
}

// CaptureKnowledgeAutoTagInputs persists the exact current material and every
// original influencing producer in one RR/SQLite transaction before the model.
func (s *KnowledgeInputProvenanceStore) CaptureKnowledgeAutoTagInputs(ctx context.Context, proof *types.KnowledgeBuildInputProof) (*types.KnowledgeAutoTagInputSnapshot, error) {
	if s.displayAuthorityFactory == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	if _, nested := s.db.Statement.ConnPool.(gorm.TxCommitter); nested {
		return nil, ErrKnowledgeInputChanged
	}
	options := &sql.TxOptions{}
	if s.db.Dialector.Name() == "postgres" {
		options.Isolation = sql.LevelRepeatableRead
	} else if s.db.Dialector.Name() != "sqlite" {
		return nil, ErrKnowledgeInputChanged
	}
	var result *types.KnowledgeAutoTagInputSnapshot
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		result, err = s.captureKnowledgeAutoTagInTx(tx, proof)
		if err != nil {
			return err
		}
		authority := s.displayAuthorityFactory(tx)
		if authority == nil || authority.db != tx {
			return types.ErrSourceLineageUnknown
		}
		if _, err := authority.AuthorizeCurrentKnowledgeInputWrite(ctx, result.Original.KnowledgeBase); err != nil {
			return err
		}
		result.ManifestID = uuid.NewString()
		result.InputDigest, err = autoTagSnapshotDigest(result)
		if err != nil {
			return err
		}
		data, err := json.Marshal(result)
		if err != nil {
			return err
		}
		lineage, err := json.Marshal(result.SourceLineage)
		if err != nil {
			return err
		}
		return tx.Create(&knowledgeAutoTagManifest{ManifestID: result.ManifestID, TenantID: proof.TenantID, KnowledgeBaseID: proof.KnowledgeBaseID, KnowledgeID: proof.KnowledgeID, AdmissionID: proof.AdmissionID, InputDigest: result.InputDigest, ActualInputs: string(data), SourceLineage: string(lineage)}).Error
	}, options)
	return result, err
}

func (s *KnowledgeInputProvenanceStore) recheckKnowledgeAutoTagInTx(tx *gorm.DB, ctx context.Context, expected *types.KnowledgeAutoTagInputSnapshot) (*types.SourceLineage, error) {
	if expected == nil || expected.ManifestID == "" || expected.Original == nil || expected.Original.Proof == nil {
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
	savedEnvelope, err := KnowledgeInputDigest(&saved)
	if err != nil {
		return nil, err
	}
	expectedEnvelope, err := KnowledgeInputDigest(expected)
	if err != nil || savedEnvelope != expectedEnvelope {
		return nil, ErrKnowledgeInputChanged
	}
	digest, err := autoTagSnapshotDigest(&saved)
	if err != nil || digest != manifest.InputDigest || expected.InputDigest != digest {
		return nil, ErrKnowledgeInputChanged
	}
	if saved.Original == nil || saved.Original.Proof == nil {
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
	currentDigest, err := autoTagSnapshotDigest(actual)
	if err != nil || currentDigest != digest {
		return nil, ErrKnowledgeInputChanged
	}
	if s.displayAuthorityFactory == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	authority := s.displayAuthorityFactory(tx)
	if authority == nil || authority.db != tx {
		return nil, types.ErrSourceLineageUnknown
	}
	if _, err := authority.AuthorizeCurrentKnowledgeInputWrite(ctx, actual.Original.KnowledgeBase); err != nil {
		return nil, err
	}
	return actual.SourceLineage, nil
}

func (s *KnowledgeInputProvenanceStore) RecheckKnowledgeAutoTagInputs(ctx context.Context, expected *types.KnowledgeAutoTagInputSnapshot) error {
	options := &sql.TxOptions{ReadOnly: true}
	if s.db.Dialector.Name() == "postgres" {
		options.Isolation = sql.LevelRepeatableRead
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error { _, err := s.recheckKnowledgeAutoTagInTx(tx, ctx, expected); return err }, options)
}

func WithKnowledgeAutoTagInput(ctx context.Context, reader *KnowledgeInputProvenanceStore, snapshot *types.KnowledgeAutoTagInputSnapshot) (context.Context, error) {
	if reader == nil || snapshot == nil || snapshot.Original == nil {
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
	return context.WithValue(ctx, knowledgeDisplayRequestKey{}, knowledgeDisplayRequest{Kind: "derived", Original: cloneKnowledgeInputProof(snapshot.Original.Proof), AutoTag: &copy, AutoTagReader: reader}), nil
}
