package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestMovedUploadRetainsOnlyVerifiedOriginal(t *testing.T) {
	store, db := knowledgeInputFixture(t)
	require.NoError(t, db.AutoMigrate(&types.TaskPendingOp{}))
	source := &types.KnowledgeBase{ID: "kb", TenantID: 7}
	target := &types.KnowledgeBase{ID: "other", TenantID: 7, Name: "other", EmbeddingModelID: "embedding"}
	require.NoError(t, store.CreateKnowledgeBaseWithInput(
		WithIndependentKnowledgeInput(context.Background()), target))
	ctx, err := access.WithKBTransferTask(context.Background(), source, target, 7, access.KBTransferMove, "move", false)
	require.NoError(t, err)
	k := &types.Knowledge{
		ID: "original", TenantID: 7, KnowledgeBaseID: source.ID, Channel: "web", Type: "file",
		FilePath: "resource://original", FileName: "notes.md", CustomMetadata: types.JSON(`{}`),
	}
	original := []byte("upload body")
	first, err := store.CreateKnowledgeWithInput(WithIndependentKnowledgeInput(context.Background()), k, original)
	require.NoError(t, err)
	state := func(phase string) types.JSON {
		return types.JSON(`{"_knowledge_transfer":{"task_id":"move","source_kb":"kb","target_kb":"other",` +
			`"source_id":"original",` +
			`"operation":"move","mode":"reparse","phase":"` + phase + `"}}`)
	}
	require.NoError(t, db.Model(k).Update("metadata", state("moving")).Error)
	_, err = store.MovedUploadInput(context.Background(), k, source, target, original, true)
	require.ErrorIs(t, err, access.ErrForbidden)
	_, err = store.MovedUploadInput(ctx, k, source, target, []byte("changed"), true)
	require.ErrorIs(t, err, ErrKnowledgeInputChanged)
	_, err = store.MovedUploadInput(ctx, k, source, target, original, true)
	require.NoError(t, err)
	require.NoError(t, db.Model(k).Updates(map[string]any{
		"knowledge_base_id": target.ID, "metadata": state("done"),
	}).Error)
	emitted, err := store.MovedUploadInput(ctx, k, source, target, original, false)
	require.NoError(t, err)
	require.NotEqual(t, first.AdmissionID, emitted.AdmissionID)
	require.Equal(t, first.InputDigest, emitted.InputDigest)
	require.Equal(t, target.ID, emitted.KnowledgeBaseID)
	replay, err := store.MovedUploadInput(ctx, k, source, target, original, false)
	require.NoError(t, err)
	require.Equal(t, emitted.AdmissionID, replay.AdmissionID)
	require.NoError(t, db.Model(k).Update("file_name", "changed.md").Error)
	_, err = store.MovedUploadInput(ctx, k, source, target, original, false)
	require.ErrorIs(t, err, ErrKnowledgeInputChanged)
	require.NoError(t, db.Where("row_kind='knowledge' AND row_id=?", k.ID).
		Delete(&knowledgeInputMaterialReceipt{}).Error)
	_, err = store.MovedUploadInput(ctx, k, source, target, original, false)
	require.Error(t, err)
}

func TestMovedUploadRejectsPurgedOriginal(t *testing.T) {
	db, _ := bodyProtocolFixture(t, false)
	source := &types.KnowledgeBase{ID: "kb", TenantID: 7}
	target := &types.KnowledgeBase{ID: "other", TenantID: 7}
	k := &types.Knowledge{
		ID: "upload", KnowledgeBaseID: source.ID, TenantID: 7, Channel: types.ChannelWeb,
		Type: "file", FilePath: "resource://upload", FileName: "receipt.md",
		Metadata: types.JSON(`{"_knowledge_transfer":{"task_id":"job","operation":"move","source_kb":"kb",` +
			`"target_kb":"other","source_id":"upload","phase":"moving","mode":"reparse"}}`),
	}
	require.NoError(t, db.Create(k).Error)
	var head originalBodyFieldHead
	require.NoError(t, db.Where("source_table='knowledges' AND row_key=? AND column_name='file_name'", k.ID).
		Take(&head).Error)
	require.NoError(t, db.Model(&originalBodyPayload{}).Where("id=?", head.BodyID).Update("state", "purged").Error)
	ctx, err := access.WithKBTransferTask(context.Background(), source, target, 7, access.KBTransferMove, "job", false)
	require.NoError(t, err)
	_, err = NewKnowledgeInputProvenanceStore(db).MovedUploadInput(ctx, k, source, target, []byte("bytes"), true)
	require.ErrorIs(t, err, ErrOriginalBodyUnavailable)
}
