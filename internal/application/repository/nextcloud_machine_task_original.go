package repository

import (
	"context"
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/types"
)

// The first document task predates a BuildInputProof. Reuse the existing
// immutable original-upload receipt; never manufacture a content admission or
// infer one from Nextcloud channel/metadata. Subsequent stages use their exact
// existing typed BuildInputProof (and consumer ProducerInput/TaskInput proofs).
func (s *KnowledgeInputProvenanceStore) machineTaskOriginal(ctx context.Context, kind string, raw []byte,
	p nextcloudMachinePayload,
) (NextcloudContentScope, string, uint64, string, error) {
	fail := func(err error) (NextcloudContentScope, string, uint64, string, error) {
		return NextcloudContentScope{}, "", 0, "", err
	}
	if kind != types.TypeDocumentProcess {
		lease, scope, err := NextcloudBuildInputLease(ctx)
		if err != nil || p.Original == nil || scope.TenantID != p.TenantID ||
			scope.KnowledgeBaseID != p.KnowledgeBaseID || scope.KnowledgeID != p.KnowledgeID ||
			uint64(lease.Epoch) != p.Original.BuildGeneration {
			return fail(ErrNextcloudMachineTaskInvalid)
		}
		if err := s.RecheckKnowledgeBuildInputs(ctx, p.Original); err != nil {
			return fail(err)
		}
		original, err := json.Marshal(p.Original)
		if err != nil {
			return fail(err)
		}
		return scope, string(original), p.Original.BuildGeneration, "build", nil
	}
	var document types.DocumentProcessPayload
	if json.Unmarshal(raw, &document) != nil || document.OriginalInput != nil {
		return fail(ErrNextcloudMachineTaskInvalid)
	}
	if err := s.VerifyNextcloudOriginalDocumentTask(ctx, document); err != nil {
		return fail(err)
	}
	var k types.Knowledge
	if err := s.db.WithContext(ctx).
		Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND deleted_at IS NULL",
			p.KnowledgeID, p.TenantID, p.KnowledgeBaseID).Take(&k).Error; err != nil {
		return fail(err)
	}
	if k.Channel != types.ConnectorTypeNextcloud {
		return fail(ErrNextcloudMachineTaskInvalid)
	}
	var metadata struct {
		DataSourceID string `json:"datasource_id"`
		ExternalID   string `json:"external_id"`
	}
	if json.Unmarshal(k.Metadata, &metadata) != nil || metadata.DataSourceID == "" || metadata.ExternalID == "" {
		return fail(ErrNextcloudMachineTaskInvalid)
	}
	scope := NextcloudContentScope{
		TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID,
		KnowledgeID: k.ID, DataSourceID: metadata.DataSourceID, ExternalID: metadata.ExternalID,
	}
	if _, present := ctx.Value(nextcloudBuildContextKey{}).(nextcloudBuildContext); present {
		_, actual, err := NextcloudBuildInputLease(ctx)
		if err != nil || actual != scope {
			return fail(ErrNextcloudMachineTaskInvalid)
		}
	} else {
		fetch, ok := ctx.Value(nextcloudOriginalFetchKey{}).(nextcloudOriginalFetch)
		if !ok || fetch.TenantID != scope.TenantID || fetch.KB != scope.KnowledgeBaseID ||
			fetch.DataSourceID != scope.DataSourceID {
			return fail(ErrNextcloudMachineTaskInvalid)
		}
	}
	receipt, err := s.verifyOriginalUploadInTx(s.db.WithContext(ctx), &k, nil, "", false)
	if err != nil || receipt.DataSourceID != scope.DataSourceID || receipt.SourceRevision == 0 ||
		receipt.ArtifactDigest == "" {
		if err == nil {
			err = ErrNextcloudMachineTaskInvalid
		}
		return fail(err)
	}
	digest, err := KnowledgeInputDigest(receipt)
	if err != nil {
		return fail(err)
	}
	return scope, digest, receipt.SourceRevision, "upload", nil
}
