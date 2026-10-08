package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestPostgresKnowledgeOutputInputOriginalBatchAndABA(t *testing.T) {
	store, db := knowledgeInputPostgresFixture(t)
	_, k, chunk, _ := createAdmittedManual(t, store, db)
	first, err := store.CaptureKnowledgeOutputInputs(context.Background(), types.KnowledgeBuildInputCaptureRequest{TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID, ChunkIDs: []string{chunk.ID}})
	require.NoError(t, err)
	second := *k
	second.ID = "second-output"
	_, err = store.CreateKnowledgeWithInput(WithIndependentKnowledgeInput(context.Background()), &second, "raw human body")
	require.NoError(t, err)
	other, err := store.CaptureKnowledgeOutputInputs(context.Background(), types.KnowledgeBuildInputCaptureRequest{TenantID: second.TenantID, KnowledgeBaseID: second.KnowledgeBaseID, KnowledgeID: second.ID})
	require.NoError(t, err)
	proofs := []*types.KnowledgeBuildInputProof{first.Proof, other.Proof}
	require.NoError(t, store.RecheckKnowledgeOutputInputs(context.Background(), proofs))
	require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", chunk.ID).Update("content", "changed actual material").Error)
	require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", chunk.ID).Update("content", chunk.Content).Error)
	require.Error(t, store.RecheckKnowledgeOutputInputs(context.Background(), proofs), "restored bytes cannot revive the original row revision")
	_, err = store.CaptureKnowledgeOutputInputs(context.Background(), types.KnowledgeBuildInputCaptureRequest{TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID, ChunkIDs: []string{chunk.ID}})
	require.Error(t, err, "a fresh output read cannot recertify an invalid material receipt")
}
