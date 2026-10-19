package service

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestFAQActualFalseControlsSanitizedMaterialAndProjectionReject(t *testing.T) {
	s, store, db, ctx := faqActualInputFixture(t, "web")
	payload := &types.FAQEntryPayload{
		StandardQuestion: "  original whitespace question  ",
		SimilarQuestions: []string{"  actual similar question  "},
		Answers:          []string{"  actual answer  "},
		IsEnabled:        boolFAQ(false),
		IsRecommended:    boolFAQ(false),
	}
	entry, err := s.CreateFAQEntry(
		faqInputContext(ctx, t, types.FAQInputOperation{Kind: "create", Entry: payload}),
		"kb",
		payload,
	)
	require.NoError(t, err)
	require.False(t, entry.IsEnabled)
	require.False(t, entry.IsRecommended)
	require.Equal(t, []string{"actual answer"}, entry.Answers)
	var kb types.KnowledgeBase
	require.NoError(t, db.Where("id='kb'").Take(&kb).Error)
	patch := &types.FAQEntryFieldsBatchUpdate{
		ByID: map[int64]types.FAQEntryFieldsUpdate{entry.ID: {IsEnabled: boolFAQ(true)}},
	}
	operation := types.FAQInputOperation{Kind: "fields", Fields: patch}
	marked := faqInputContext(ctx, t, operation)
	marked, err = s.beginFAQInput(marked, &kb, operation)
	require.NoError(t, err)
	var actual types.Chunk
	require.NoError(t, db.Where("id=?", entry.ChunkID).Take(&actual).Error)
	actual.IsEnabled = true
	actual.Content = "unconsumed foreign body projected into this edit"
	require.Error(t, store.WriteFAQChunks(marked, []*types.Chunk{&actual}, false))
	read, err := s.GetFAQEntry(ctx, "kb", entry.ID)
	require.NoError(t, err)
	require.False(t, read.IsEnabled)
	require.Equal(t, entry.Answers, read.Answers)
}
