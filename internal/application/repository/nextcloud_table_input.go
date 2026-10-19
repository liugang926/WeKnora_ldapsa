package repository

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// NextcloudOriginalTableArtifactDigest returns only the immutable digest of
// the actual original upload. A consumer must compare its fetched file bytes
// against this value before admitting table/model inputs.
func (s *KnowledgeInputProvenanceStore) NextcloudOriginalTableArtifactDigest(
	ctx context.Context, proof *types.KnowledgeBuildInputProof,
) (string, error) {
	if s == nil || s.db == nil || ctx == nil || proof == nil || proof.SourceLineage.RequireComplete() != nil {
		return "", types.ErrSourceLineageUnknown
	}
	lease, scope, err := NextcloudBuildInputLease(ctx)
	if err != nil {
		return "", err
	}
	bound := KnowledgeBuildInputProofFromContext(ctx)
	if bound == nil || bound.AdmissionID != proof.AdmissionID || bound.InputDigest != proof.InputDigest ||
		bound.BuildGeneration != proof.BuildGeneration || scope.TenantID != proof.TenantID ||
		scope.KnowledgeBaseID != proof.KnowledgeBaseID || scope.KnowledgeID != proof.KnowledgeID ||
		uint64(lease.Epoch) != proof.BuildGeneration {
		return "", ErrKnowledgeInputChanged
	}
	if err := requireMachineConfigurationRead(ctx, proof.TenantID, proof.KnowledgeBaseID); err != nil {
		return "", err
	}
	if err := s.RecheckKnowledgeBuildInputs(ctx, proof); err != nil {
		return "", err
	}
	var digest string
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := NewNextcloudContentLeaseStore(s.db).ValidateBuildLeaseInTx(ctx, tx, lease, scope); err != nil {
			return err
		}
		if err := ValidateNextcloudMachineTaskWriteInTx(ctx, tx, scope.TenantID,
			scope.KnowledgeBaseID, scope.KnowledgeID); err != nil {
			return err
		}
		if err := scopedOnboardingConfigurationPurposeInTx(ctx, tx, scope.TenantID, scope.KnowledgeBaseID); err != nil {
			return err
		}
		if _, err := s.captureKnowledgeBuildSnapshotInTx(tx, types.KnowledgeBuildInputCaptureRequest{
			Projection: proof.Projection, TenantID: proof.TenantID, KnowledgeBaseID: proof.KnowledgeBaseID,
			KnowledgeID: proof.KnowledgeID, ChunkIDs: proof.ChunkIDs, Expected: proof,
		}, proof.BodyDigest != ""); err != nil {
			return err
		}
		var k types.Knowledge
		if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?",
			scope.KnowledgeID, scope.TenantID, scope.KnowledgeBaseID).Take(&k).Error; err != nil {
			return err
		}
		if k.Channel != types.ConnectorTypeNextcloud || k.GetMetadata()["datasource_id"] != scope.DataSourceID ||
			k.GetMetadata()["external_id"] != scope.ExternalID {
			return ErrKnowledgeInputChanged
		}
		receipt, err := s.verifyOriginalUploadInTx(tx, &k, nil, "", false)
		if err != nil {
			return err
		}
		if receipt.TenantID != scope.TenantID || receipt.KnowledgeBaseID != scope.KnowledgeBaseID ||
			receipt.KnowledgeID != scope.KnowledgeID || receipt.DataSourceID != scope.DataSourceID ||
			!recoveryHash.MatchString(receipt.ArtifactDigest) {
			return ErrKnowledgeInputChanged
		}
		digest = receipt.ArtifactDigest
		return nil
	})
	if err != nil {
		return "", err
	}
	return digest, nil
}
