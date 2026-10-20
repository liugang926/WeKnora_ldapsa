package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestRAGSourceLineageMessageAtomicSaveCannotKeepStaleProof(t *testing.T) {
	db := newMessageSearchDB(t)
	repo := NewMessageRepository(db)
	empty, _ := types.NewCompleteSourceLineage()
	row := &types.Message{
		ID:            "answer",
		SessionID:     "session",
		Role:          "assistant",
		Content:       "old body",
		IsCompleted:   true,
		SourceLineage: empty,
	}
	require.NoError(t, db.Create(row).Error)
	// Explicit zero fields and NULL are part of the same answer save. GORM's
	// ordinary nonzero struct update must not retain the previous complete proof.
	row.Content, row.SourceLineage = "unknown replacement", nil
	require.NoError(t, repo.UpdateMessage(context.Background(), row))
	got, err := repo.GetMessage(context.Background(), "session", row.ID)
	require.NoError(t, err)
	require.Equal(t, "unknown replacement", got.Content)
	require.Nil(t, got.SourceLineage)
	row.Content, row.IsCompleted, row.SourceLineage = "", false, empty
	require.NoError(t, repo.UpdateMessage(context.Background(), row))
	got, err = repo.GetMessage(context.Background(), "session", row.ID)
	require.NoError(t, err)
	require.Empty(t, got.Content)
	require.False(t, got.IsCompleted)
	require.Equal(t, empty, got.SourceLineage)
}

func TestRAGSourceLineageMessageWriteFailureRollsBackBodyAndProof(t *testing.T) {
	db := newMessageSearchDB(t)
	repo := NewMessageRepository(db)
	empty, _ := types.NewCompleteSourceLineage()
	row := &types.Message{
		ID:            "answer",
		SessionID:     "session",
		Role:          "assistant",
		Content:       "old body",
		SourceLineage: empty,
	}
	require.NoError(t, db.Create(row).Error)
	injected := errors.New("failure after body/proof writes")
	updates := 0
	require.NoError(
		t,
		db.Callback().Update().After("gorm:update").Register("fail_second_atomic_update", func(tx *gorm.DB) {
			updates++
			if updates == 2 {
				_ = tx.AddError(injected)
			}
		}),
	)
	row.Content, row.SourceLineage, row.IsCompleted = "new unreadable orphan", types.UnknownSourceLineage(), true
	require.ErrorIs(t, repo.UpdateMessage(context.Background(), row), injected)
	require.Equal(t, 2, updates, "failure injection did not reach the final explicit write")
	got, err := repo.GetMessage(context.Background(), "session", row.ID)
	require.NoError(t, err)
	require.Equal(t, "old body", got.Content)
	require.False(t, got.IsCompleted)
	require.Equal(t, empty, got.SourceLineage)
}
