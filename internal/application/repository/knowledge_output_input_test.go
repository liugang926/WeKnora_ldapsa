package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestKnowledgeOutputInputFreshActualMaterialRetainsOriginalReceipt(t *testing.T) {
	store, db := knowledgeInputFixture(t)
	_, k, chunk, admission := createAdmittedManual(t, store, db)
	request := types.KnowledgeBuildInputCaptureRequest{TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID, ChunkIDs: []string{chunk.ID}}
	actual, err := store.CaptureKnowledgeOutputInputs(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, admission.AdmissionID, actual.Proof.AdmissionID)
	require.Equal(t, chunk.Content, actual.Chunks[0].Content)
	require.Equal(t, k.Title, actual.Knowledge.Title)
	require.NoError(t, actual.Proof.SourceLineage.RequireComplete())
	require.NotEmpty(t, actual.Proof.BodyDigest)
	require.NoError(t, store.RecheckKnowledgeBuildInputs(context.Background(), actual.Proof))
	// This capability does not remove the worker's original-input requirement.
	_, err = store.CaptureKnowledgeBuildInputs(context.Background(), request)
	require.ErrorIs(t, err, types.ErrSourceLineageUnknown)
	request.Expected = actual.Proof
	_, err = store.CaptureKnowledgeOutputInputs(context.Background(), request)
	require.ErrorIs(t, err, types.ErrSourceLineageUnknown)
}

func TestKnowledgeOutputInputUsesReceiptAdmissionRatherThanNewestOrigin(t *testing.T) {
	store, db := knowledgeInputFixture(t)
	_, k, _, admission := createAdmittedManual(t, store, db)
	var decoy knowledgeInputAdmission
	require.NoError(t, db.Where("admission_id = ?", admission.AdmissionID).Take(&decoy).Error)
	decoy.AdmissionID = "newer-unrelated-origin"
	decoy.BuildGeneration += 100
	var err error
	decoy.InputDigest, err = KnowledgeInputDigest("unrelated original artifact")
	require.NoError(t, err)
	require.NoError(t, db.Create(&decoy).Error)
	actual, err := store.CaptureKnowledgeOutputInputs(context.Background(), types.KnowledgeBuildInputCaptureRequest{TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID})
	require.NoError(t, err)
	require.Equal(t, admission.AdmissionID, actual.Proof.AdmissionID)
	require.Equal(t, admission.InputDigest, actual.Proof.InputDigest)
}

func TestKnowledgeOutputInputRejectsUnknownForeignAndMaterialABA(t *testing.T) {
	for _, change := range []string{"foreign_tenant", "foreign_kb", "orphan_chunk", "title_aba", "chunk_aba", "legacy_receipt"} {
		t.Run(change, func(t *testing.T) {
			store, db := knowledgeInputFixture(t)
			_, k, chunk, _ := createAdmittedManual(t, store, db)
			request := types.KnowledgeBuildInputCaptureRequest{TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID, ChunkIDs: []string{chunk.ID}}
			switch change {
			case "foreign_tenant":
				request.TenantID++
			case "foreign_kb":
				request.KnowledgeBaseID = "other"
			case "orphan_chunk":
				request.ChunkIDs = []string{"orphan"}
			case "title_aba":
				require.NoError(t, db.Model(k).Update("title", "changed").Error)
				require.NoError(t, db.Model(k).Update("title", "human title").Error)
			case "chunk_aba":
				require.NoError(t, db.Model(chunk).Update("content", "changed").Error)
				require.NoError(t, db.Model(chunk).Update("content", "actual parsed body").Error)
			case "legacy_receipt":
				require.NoError(t, db.Table("knowledge_input_material_receipts").Where("row_kind = ? AND row_id = ?", "knowledge", k.ID).Delete(&knowledgeInputMaterialReceipt{}).Error)
			}
			actual, err := store.CaptureKnowledgeOutputInputs(context.Background(), request)
			require.Error(t, err)
			require.Nil(t, actual)
		})
	}
}

func TestKnowledgeOutputInputBatchKeepsEveryOriginalProof(t *testing.T) {
	store, db := knowledgeInputFixture(t)
	_, k, chunk, _ := createAdmittedManual(t, store, db)
	actual, err := store.CaptureKnowledgeOutputInputs(context.Background(), types.KnowledgeBuildInputCaptureRequest{TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID, ChunkIDs: []string{chunk.ID}})
	require.NoError(t, err)
	second := *k
	second.ID = "second-manual"
	_, err = store.CreateKnowledgeWithInput(WithIndependentKnowledgeInput(context.Background()), &second, "second authored text")
	require.NoError(t, err)
	other, err := store.CaptureKnowledgeOutputInputs(context.Background(), types.KnowledgeBuildInputCaptureRequest{TenantID: second.TenantID, KnowledgeBaseID: second.KnowledgeBaseID, KnowledgeID: second.ID})
	require.NoError(t, err)
	proofs := []*types.KnowledgeBuildInputProof{actual.Proof, other.Proof}
	require.NoError(t, store.RecheckKnowledgeOutputInputs(context.Background(), proofs))
	require.NoError(t, db.Model(k).Update("title", "changed").Error)
	require.NoError(t, db.Model(k).Update("title", k.Title).Error)
	require.Error(t, store.RecheckKnowledgeOutputInputs(context.Background(), proofs))
	require.Error(t, store.RecheckKnowledgeOutputInputs(context.Background(), []*types.KnowledgeBuildInputProof{nil}))
}
