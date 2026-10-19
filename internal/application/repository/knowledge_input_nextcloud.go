package repository

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

func knowledgeBuildSourceIdentity(tx *gorm.DB, k *types.Knowledge, epoch uint64) (types.SourceIdentity, error) {
	var zero types.SourceIdentity
	candidate, ready, err := (&knowledgeRepository{db: tx}).publicationCandidateMode(tx, k.ID, false, true)
	if err != nil {
		return zero, err
	}
	if !ready || candidate.version.TenantID != k.TenantID || candidate.version.KnowledgeBaseID != k.KnowledgeBaseID {
		return zero, ErrKnowledgeInputChanged
	}
	if k.ParseStatus == types.ParseStatusCancelled || k.ParseStatus == types.ParseStatusDeleting {
		return zero, ErrKnowledgeInputChanged
	}
	var kbFence, exact nextcloudContentFence
	if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ''", k.TenantID, k.KnowledgeBaseID).
		Take(&kbFence).
		Error; err != nil {
		return zero, err
	}
	if err := tx.Where(
		"tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
		k.TenantID,
		k.KnowledgeBaseID,
		k.ID,
	).
		Take(&exact).
		Error; err != nil {
		return zero, err
	}
	if kbFence.State != "open" || kbFence.Epoch <= 0 || exact.State != "open" || exact.Epoch <= 0 ||
		uint64(exact.Epoch) != epoch ||
		exact.DataSourceID != candidate.version.DataSourceID ||
		exact.ExternalID != candidate.version.ExternalID {
		return zero, ErrKnowledgeInputChanged
	}
	var pair NextcloudSourcePairing
	if err := tx.Where(("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND state = " +
		"'active'"), k.TenantID, k.KnowledgeBaseID, candidate.version.DataSourceID).
		Take(&pair).
		Error; err != nil {
		return zero, err
	}
	identity := types.SourceIdentity{
		Provider:        types.ConnectorTypeNextcloud,
		TenantID:        k.TenantID,
		KnowledgeBaseID: k.KnowledgeBaseID,
		KnowledgeID:     k.ID,
		DataSourceID:    candidate.version.DataSourceID,
		PairOperationID: pair.OperationID,
		InstanceID:      candidate.instance,
		BindingID:       candidate.binding,
		FileID:          strconv.FormatInt(candidate.fileID, 10),
		ExternalID:      candidate.version.ExternalID,
		Revision:        epoch,
		ETag:            candidate.version.DesiredETag,
		ContentRevision: types.NextcloudLineageContentRevision(candidate.version.DesiredContentRevision),
	}
	if err := identity.Validate(); err != nil {
		return zero, err
	}
	return identity, nil
}

// AdmitNextcloudBuildInputs only accepts the already admitted exact build lease
// and actual artifact read by this worker. The source pairing/config, original
// desired ETag and epoch are bound before parsing, never at graph-read time.
func (s *KnowledgeInputProvenanceStore) AdmitNextcloudBuildInputs(
	ctx context.Context,
	input any,
) (*types.KnowledgeBuildInputProof, error) {
	lease, scope, err := NextcloudBuildInputLease(ctx)
	if err != nil {
		return nil, err
	}
	var proof *types.KnowledgeBuildInputProof
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := ValidateNextcloudBuildWrite(
			ctx,
			tx,
			scope.TenantID,
			scope.KnowledgeBaseID,
			scope.KnowledgeID,
		); err != nil {
			return err
		}
		var k types.Knowledge
		if err := tx.Where(
			"id = ? AND tenant_id = ? AND knowledge_base_id = ?",
			scope.KnowledgeID,
			scope.TenantID,
			scope.KnowledgeBaseID,
		).
			Take(&k).
			Error; err != nil {
			return err
		}
		if path, strict := ctx.Value(nextcloudOriginalUploadBuildKey{}).(string); strict {
			artifact, ok := input.([]byte)
			if !ok {
				return ErrKnowledgeInputChanged
			}
			if _, err := s.verifyOriginalUploadInTx(tx, &k, artifact, path, true); err != nil {
				return err
			}
		}
		identity, err := knowledgeBuildSourceIdentity(tx, &k, uint64(lease.Epoch))
		if err != nil {
			return err
		}
		lineage, err := types.NewCompleteSourceLineage(identity)
		if err != nil {
			return err
		}
		var kb types.KnowledgeBase
		if err := tx.Where("id = ? AND tenant_id = ?", scope.KnowledgeBaseID, scope.TenantID).
			Take(&kb).
			Error; err != nil {
			return err
		}
		var old knowledgeInputAdmission
		query := tx.Where("knowledge_id = ? AND build_generation = ? AND origin_kind = 'nextcloud'", k.ID, lease.Epoch).
			Find(&old)
		if query.Error != nil {
			return query.Error
		}
		if query.RowsAffected == 1 {
			digest, err := KnowledgeInputDigest(input)
			if err != nil {
				return err
			}
			if digest != old.InputDigest {
				return ErrKnowledgeInputChanged
			}
			var saved types.SourceLineage
			if json.Unmarshal([]byte(old.SourceLineage), &saved) != nil {
				return ErrKnowledgeInputChanged
			}
			proof = &types.KnowledgeBuildInputProof{
				Version:         1,
				TenantID:        k.TenantID,
				KnowledgeBaseID: k.KnowledgeBaseID,
				KnowledgeID:     k.ID,
				AdmissionID:     old.AdmissionID,
				BuildGeneration: old.BuildGeneration,
				InputDigest:     old.InputDigest,
				ConfigDigest:    old.ConfigDigest,
				SourceLineage:   &saved,
			}
			_, _, _, err = s.admissionInTx(tx, &k, proof)
			return err
		}
		proof, err = s.admitInTx(ctx, tx, &k, &kb, input, knowledgeInputOrigin{lineage, "nextcloud"})
		return err
	})
	return proof, err
}
