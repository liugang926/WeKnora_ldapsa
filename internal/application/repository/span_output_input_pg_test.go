package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type spanSnapshotPauseKey struct{}

func TestPostgresSpanOriginalStartEndCoherentCaptureAndFinalMutation(t *testing.T) {
	store, db := knowledgeInputPostgresFixture(t)
	ctx, k, _, _ := createAdmittedManual(t, store, db)
	installSpanInputs(t, db)
	repo := NewKnowledgeSpanRepository(db)
	root := startOriginalSpan(ctx, t, repo, k.ID, 1, "root", "", types.SpanKindRoot)
	child := startOriginalSpan(ctx, t, repo, k.ID, 1, "child", root.SpanID, types.SpanKindGeneration)
	finishOriginalSpan(ctx, t, repo, child)
	baseline := captureOriginalSpans(t, store, k, 1)
	fired := false
	name := "span_original_changed_after_header"
	require.NoError(t, db.Callback().Query().After("gorm:query").Register(name, func(tx *gorm.DB) {
		if fired || tx.Statement.Context.Value(spanSnapshotPauseKey{}) != true || tx.Statement.Table != "knowledges" {
			return
		}
		fired = true
		require.NoError(
			t,
			db.Exec("UPDATE knowledge_processing_spans SET error_message='new error' WHERE span_id='child'").Error,
		)
		require.NoError(
			t,
			db.Exec(
				"UPDATE knowledge_processing_spans SET error_message='original error body' WHERE span_id='child'",
			).Error,
		)
	}))
	t.Cleanup(func() { _ = db.Callback().Query().Remove(name) })
	snapshot, err := store.CaptureSpanOutputInputs(
		context.WithValue(context.Background(), spanSnapshotPauseKey{}, true),
		types.SpanOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, Attempt: 1},
	)
	require.NoError(t, err)
	require.True(t, fired)
	require.Equal(
		t,
		baseline.Receipt,
		snapshot.Receipt,
		"one RR snapshot keeps original actual body and receipt coherent",
	)
	require.Equal(t, "original error body", snapshot.Rows[1].ErrorMessage)
	require.ErrorIs(
		t,
		store.RecheckSpanOutputInputs(context.Background(), snapshot),
		ErrKnowledgeInputChanged,
		"final whole attempt checkpoint sees committed body ABA",
	)
}
