//go:build sqlite_fts5

package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestNextcloudOriginalTableArtifactRejectsUnsealedUpload(t *testing.T) {
	f, build, _, _ := machineTaskRepositoryFixture(t)
	proof := KnowledgeBuildInputProofFromContext(build)
	require.NoError(t, f.store.RecheckKnowledgeBuildInputs(build, proof))
	digest, err := f.store.NextcloudOriginalTableArtifactDigest(build, proof)
	require.Error(t, err, "an actual build admission does not fabricate an original-upload receipt")
	require.Empty(t, digest)
	var count int64
	require.NoError(t, f.db.Model(&nextcloudOriginalUploadInput{}).Count(&count).Error)
	require.Zero(t, count, "a digest read cannot create an original upload")
}

func TestNextcloudOriginalTableArtifactRequiresExactProofAndHeldRead(t *testing.T) {
	f, build, _, _ := machineTaskRepositoryFixture(t)
	proof := KnowledgeBuildInputProofFromContext(build)
	for _, kind := range []string{"nil", "admission", "tenant", "kb", "knowledge", "generation"} {
		t.Run(kind, func(t *testing.T) {
			changed := *proof
			candidate := &changed
			switch kind {
			case "nil":
				candidate = nil
			case "admission":
				changed.AdmissionID = "unproved-origin"
			case "tenant":
				changed.TenantID++
			case "kb":
				changed.KnowledgeBaseID = "unrelated-kb"
			case "knowledge":
				changed.KnowledgeID = "unrelated-knowledge"
			case "generation":
				changed.BuildGeneration++
			}
			digest, err := f.store.NextcloudOriginalTableArtifactDigest(build, candidate)
			require.Error(t, err)
			require.Empty(t, digest)
		})
	}
	lease, scope, err := NextcloudBuildInputLease(build)
	require.NoError(t, err)
	leaseOnly := WithNextcloudBuildLease(context.Background(), NewNextcloudContentLeaseStore(f.db), lease, scope)
	leaseOnly = WithKnowledgeBuildInputProof(leaseOnly, f.store, proof)
	_, err = f.store.NextcloudOriginalTableArtifactDigest(leaseOnly, proof)
	require.Error(t, err, "a real lease alone cannot substitute for a held configuration/body read")
	CloseScopedOnboardingConfigurationReads(build)
	_, err = f.store.NextcloudOriginalTableArtifactDigest(build, proof)
	require.Error(t, err, "closed body leases cannot authorize a table artifact")
	_, err = f.store.NextcloudOriginalTableArtifactDigest(context.Background(), &types.KnowledgeBuildInputProof{})
	require.Error(t, err)
}
