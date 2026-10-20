package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

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

func (s *KnowledgeInputProvenanceStore) captureKnowledgeAutoTagInTx(
	tx *gorm.DB,
	proof *types.KnowledgeBuildInputProof,
) (*types.KnowledgeAutoTagInputSnapshot, error) {
	if proof == nil || proof.BodyDigest == "" || proof.Projection != "" {
		return nil, types.ErrSourceLineageUnknown
	}
	original, err := s.captureKnowledgeBuildSnapshotInTx(
		tx,
		types.KnowledgeBuildInputCaptureRequest{
			TenantID:        proof.TenantID,
			KnowledgeBaseID: proof.KnowledgeBaseID,
			KnowledgeID:     proof.KnowledgeID,
			ChunkIDs:        proof.ChunkIDs,
			Expected:        proof,
		},
		true,
	)
	if err != nil {
		return nil, err
	}
	result := &types.KnowledgeAutoTagInputSnapshot{
		Original: &types.KnowledgeAutoTagOriginalInput{
			Knowledge:     original.Knowledge,
			KnowledgeBase: original.KnowledgeBase,
			Chunks:        original.Chunks,
			Proof:         original.Proof,
		},
		Tags:      []*types.KnowledgeTag{},
		TagProofs: []*types.KnowledgeCollectionAuxiliaryProof{},
		Relations: []*types.KnowledgeTagRelation{},
	}
	kb := original.KnowledgeBase
	k := original.Knowledge
	if k.Channel == types.ConnectorTypeNextcloud {
		result.PublicationRevision, err = knowledgeAutoPublicationRevision(tx, k.ID)
		if err != nil {
			return nil, err
		}
	}
	result.ModelDigest, result.Tenant, err = knowledgeAutoTagModelInputs(tx, kb)
	if err != nil {
		return nil, err
	}
	result.DataSourceName, err = s.originalUploadAutoTagNameInTx(tx, proof)
	if err != nil {
		return nil, err
	}
	if err := tx.Model(&types.KnowledgeTag{}).
		Where("tenant_id = ? AND knowledge_base_id = ?", kb.TenantID, kb.ID).
		Count(&result.TotalTags).
		Error; err != nil {
		return nil, err
	}
	if err := tx.Where("tenant_id = ? AND knowledge_base_id = ?", kb.TenantID, kb.ID).
		Order("sort_order ASC,created_at DESC,seq_id DESC").
		Limit(500).
		Find(&result.Tags).
		Error; err != nil {
		return nil, err
	}
	result.TagListRevision, err = knowledgeDisplayRevision(tx, "tag-list", kb.ID)
	if err != nil {
		return nil, err
	}
	lineages := []*types.SourceLineage{proof.SourceLineage}
	for _, tag := range result.Tags {
		for _, field := range []string{"name", "sort_order"} {
			p, err := readKnowledgeDisplayOrigin(
				tx,
				"tag-"+field,
				tag.ID,
				tag.TenantID,
				tag.KnowledgeBaseID,
				tagDisplayFieldValue(tag, field),
			)
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
		result.RelationProof, err = readKnowledgeDisplayOrigin(
			tx,
			"relations",
			k.ID,
			k.TenantID,
			k.KnowledgeBaseID,
			ids,
		)
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
		result.RelationProof = &types.KnowledgeCollectionAuxiliaryProof{
			Kind:            "relations",
			ID:              k.ID,
			TenantID:        k.TenantID,
			KnowledgeBaseID: k.KnowledgeBaseID,
			Revision:        revision,
			Digest:          digest,
			SourceLineage:   lineage,
		}
	}
	result.SourceLineage, err = types.UnionSourceLineage(lineages...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func autoTagSnapshotDigest(snapshot *types.KnowledgeAutoTagInputSnapshot) (string, error) {
	copied := *snapshot
	copied.ManifestID = ""
	copied.InputDigest = ""

	return KnowledgeInputDigest(&copied)
}

// CaptureKnowledgeAutoTagInputs persists the exact current material and every
// original influencing producer in one RR/SQLite transaction before the model.
func (s *KnowledgeInputProvenanceStore) CaptureKnowledgeAutoTagInputs(
	ctx context.Context,
	proof *types.KnowledgeBuildInputProof,
) (*types.KnowledgeAutoTagInputSnapshot, error) {
	if err := s.RecheckKnowledgeBuildInputs(ctx, proof); err != nil {
		return nil, err
	}
	if s.displayAuthorityFactory == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	if _, nested := s.db.Statement.ConnPool.(gorm.TxCommitter); nested {
		return nil, ErrKnowledgeInputChanged
	}
	options := &sql.TxOptions{}
	if s.db.Name() == "postgres" {
		options.Isolation = sql.LevelRepeatableRead
	} else if s.db.Name() != "sqlite" {
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
		if _, err := authority.authorizeOriginalKnowledgeProducerWrite(
			ctx,
			result.Original.KnowledgeBase,
			proof,
		); err != nil {
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
		return tx.Create(
			&knowledgeAutoTagManifest{
				ManifestID:      result.ManifestID,
				TenantID:        proof.TenantID,
				KnowledgeBaseID: proof.KnowledgeBaseID,
				KnowledgeID:     proof.KnowledgeID,
				AdmissionID:     proof.AdmissionID,
				InputDigest:     result.InputDigest,
				ActualInputs:    string(data),
				SourceLineage:   string(lineage),
			},
		).Error
	}, options)
	return result, err
}

func (s *KnowledgeInputProvenanceStore) recheckKnowledgeAutoTagInTx(
	ctx context.Context,
	tx *gorm.DB,
	expected *types.KnowledgeAutoTagInputSnapshot,
) (*types.SourceLineage, error) {
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
		return nil, fmt.Errorf("auto expected envelope: %w", ErrKnowledgeInputChanged)
	}
	digest, err := autoTagSnapshotDigest(&saved)
	if err != nil || digest != manifest.InputDigest || expected.InputDigest != digest {
		return nil, fmt.Errorf("auto expected model digest: %w", ErrKnowledgeInputChanged)
	}
	if saved.Original == nil || saved.Original.Proof == nil {
		return nil, ErrKnowledgeInputChanged
	}
	proof := saved.Original.Proof
	if manifest.TenantID != proof.TenantID || manifest.KnowledgeBaseID != proof.KnowledgeBaseID ||
		manifest.KnowledgeID != proof.KnowledgeID ||
		manifest.AdmissionID != proof.AdmissionID {
		return nil, ErrKnowledgeInputChanged
	}
	actual, err := s.captureKnowledgeAutoTagInTx(tx, proof)
	if err != nil {
		return nil, fmt.Errorf("auto current material: %w", err)
	}
	// A live producer uses this transient tenant to instantiate the model. It
	// is deliberately absent from persisted JSON, but must still equal the
	// actual routing context sealed by ModelDigest before/after the call.
	if expected.Tenant != nil {
		if actual.Tenant == nil {
			return nil, ErrKnowledgeInputChanged
		}
		frozenTenant, err := knowledgeProducerTenantDigest(expected.Tenant)
		if err != nil {
			return nil, err
		}
		currentTenant, err := knowledgeProducerTenantDigest(actual.Tenant)
		if err != nil || frozenTenant != currentTenant {
			return nil, ErrKnowledgeInputChanged
		}
	}
	if err := acceptKnowledgeAutoPublicationTransition(tx, &saved, actual); err != nil {
		return nil, fmt.Errorf("auto publication transition: %w", err)
	}
	if err := acceptKnowledgeAutoTagConfigurationProjection(&saved, actual); err != nil {
		return nil, fmt.Errorf("auto configuration projection: %w", err)
	}
	currentDigest, err := autoTagSnapshotDigest(actual)
	if err != nil || currentDigest != digest {
		return nil, fmt.Errorf("auto current digest: %w", ErrKnowledgeInputChanged)
	}
	if s.displayAuthorityFactory == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	authority := s.displayAuthorityFactory(tx)
	if authority == nil || authority.db != tx {
		return nil, types.ErrSourceLineageUnknown
	}
	if _, err := authority.authorizeOriginalKnowledgeProducerWrite(
		ctx,
		actual.Original.KnowledgeBase,
		proof,
	); err != nil {
		return nil, fmt.Errorf("auto producer authority: %w", err)
	}
	return actual.SourceLineage, nil
}

// Scoped Nextcloud worker reads intentionally project only model-relevant KB
// controls, omitting display metadata, timestamps and computed counters. Those
// fields are not Auto Tag inputs. Recheck the effective configuration first,
// then restore the sealed KB representation before hashing the model snapshot.
func acceptKnowledgeAutoTagConfigurationProjection(
	saved,
	actual *types.KnowledgeAutoTagInputSnapshot,
) error {
	if saved == nil || actual == nil || saved.Original == nil || actual.Original == nil ||
		saved.Original.Knowledge == nil || actual.Original.Knowledge == nil ||
		saved.Original.KnowledgeBase == nil || actual.Original.KnowledgeBase == nil ||
		saved.Original.Proof == nil || actual.Original.Proof == nil {
		return ErrKnowledgeInputChanged
	}
	savedKB, actualKB := saved.Original.KnowledgeBase, actual.Original.KnowledgeBase
	if savedKB.ID != actualKB.ID || savedKB.TenantID != actualKB.TenantID ||
		savedKB.Type != actualKB.Type ||
		saved.Original.Proof.ConfigDigest != actual.Original.Proof.ConfigDigest {
		return ErrKnowledgeInputChanged
	}
	savedConfig, err := knowledgeConfigDigest(savedKB, saved.Original.Knowledge)
	if err != nil {
		return err
	}
	actualConfig, err := knowledgeConfigDigest(actualKB, actual.Original.Knowledge)
	if err != nil || actualConfig != savedConfig {
		return ErrKnowledgeInputChanged
	}
	actual.Original.KnowledgeBase = savedKB
	return nil
}

// RecheckKnowledgeAutoTagInputs rechecks knowledge auto tag inputs.
func (s *KnowledgeInputProvenanceStore) RecheckKnowledgeAutoTagInputs(
	ctx context.Context,
	expected *types.KnowledgeAutoTagInputSnapshot,
) error {
	// The trusted producer's exact lease is fenced against retirement in this
	// transaction. Fence locking uses no-op SQL writes; it is not a public read.
	options := &sql.TxOptions{}
	if s.db.Name() == "postgres" {
		options.Isolation = sql.LevelRepeatableRead
	}
	return s.db.WithContext(ctx).
		Transaction(func(tx *gorm.DB) error {
			_, err := s.recheckKnowledgeAutoTagInTx(
				ctx,
				tx,
				expected,
			)
			return err
		}, options)
}

// WithKnowledgeAutoTagInput binds knowledge auto tag input.
func WithKnowledgeAutoTagInput(
	ctx context.Context,
	reader *KnowledgeInputProvenanceStore,
	snapshot *types.KnowledgeAutoTagInputSnapshot,
) (context.Context, error) {
	if reader == nil || snapshot == nil || snapshot.Original == nil {
		return ctx, types.ErrSourceLineageUnknown
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return ctx, err
	}
	var copied2 types.KnowledgeAutoTagInputSnapshot
	if json.Unmarshal(data, &copied2) != nil {
		return ctx, ErrKnowledgeInputChanged
	}
	return context.WithValue(
		ctx,
		knowledgeDisplayRequestKey{},
		knowledgeDisplayRequest{
			Kind:          "derived",
			Original:      cloneKnowledgeInputProof(snapshot.Original.Proof),
			AutoTag:       &copied2,
			AutoTagReader: reader,
		},
	), nil
}
