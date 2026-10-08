package repository

import (
	"context"
	"github.com/Tencent/WeKnora/internal/types"
)

// This capability is separate from public reading. Only the exact admitted
// worker lease may verify its own unpublished original source generation.
type KnowledgeOriginalInputSourceVerifier interface {
	CheckKnowledgeOriginalInputSources(context.Context, *types.KnowledgeBuildInputProof) error
}

func ConfigureKnowledgeOriginalInputSourceVerifier(store *KnowledgeInputProvenanceStore, verifier KnowledgeOriginalInputSourceVerifier) {
	store.originalInputSources = verifier
}

func (s *KnowledgeInputProvenanceStore) ReadOriginalNextcloudBuildSource(ctx context.Context, proof *types.KnowledgeBuildInputProof) (*types.Knowledge, error) {
	if proof == nil || proof.SourceLineage.RequireComplete() != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	if _, err := s.snapshot(ctx, types.KnowledgeBuildInputCaptureRequest{Projection: proof.Projection, TenantID: proof.TenantID, KnowledgeBaseID: proof.KnowledgeBaseID, KnowledgeID: proof.KnowledgeID, ChunkIDs: proof.ChunkIDs, Expected: proof}, proof.BodyDigest != ""); err != nil {
		return nil, err
	}
	lease, scope, err := NextcloudBuildInputLease(ctx)
	if err != nil {
		return nil, err
	}
	if scope.TenantID != proof.TenantID || scope.KnowledgeBaseID != proof.KnowledgeBaseID || scope.KnowledgeID != proof.KnowledgeID || uint64(lease.Epoch) != proof.BuildGeneration {
		return nil, ErrKnowledgeInputChanged
	}
	if err := CheckNextcloudBuildLease(ctx); err != nil {
		return nil, err
	}
	var k types.Knowledge
	err = s.db.WithContext(ctx).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", proof.KnowledgeID, proof.TenantID, proof.KnowledgeBaseID).Take(&k).Error
	if err != nil {
		return nil, err
	}
	if k.Channel != types.ConnectorTypeNextcloud {
		return nil, ErrKnowledgeInputChanged
	}
	identity, err := knowledgeBuildSourceIdentity(s.db.WithContext(ctx), &k, proof.BuildGeneration)
	if err != nil {
		return nil, err
	}
	found := false
	for _, saved := range proof.SourceLineage.Sources {
		if saved == identity {
			found = true
		}
	}
	if !found {
		return nil, ErrKnowledgeInputChanged
	}
	return &k, nil
}
