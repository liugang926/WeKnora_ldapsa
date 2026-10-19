package service

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestSessionTitleEventReviewAsyncExistingTitleKeepsOriginalProof(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.Session{}))
	proof := ragEntryProof(t)
	stored := &types.Session{
		TenantID:           7,
		UserID:             "reader",
		Title:              "existing source-derived title",
		TitleSourceLineage: proof,
	}
	require.NoError(t, db.Create(stored).Error)
	service := &sessionService{
		sessionRepo:        repository.NewSessionRepository(db),
		sourceLineageGuard: &parentBatchPolicy{},
	}
	bus := event.NewEventBus()
	emitted := make(chan event.Event, 1)
	bus.On(event.EventSessionTitle, func(_ context.Context, evt event.Event) error { emitted <- evt; return nil })
	// The caller's stale/generic DTO has no visible title, while GenerateTitle
	// sees the now-committed complete source-bearing title and returns it.
	service.GenerateTitleAsync(
		ragReadContext(),
		&types.Session{ID: stored.ID, TenantID: 7, UserID: "reader"},
		"raw user query",
		"unused-model",
		bus,
	)
	select {
	case evt := <-emitted:
		original, ok := evt.SourceLineage.(*types.SourceLineage)
		require.True(t, ok)
		require.Equal(t, proof, original)
		require.Equal(t, stored.Title, evt.Data.(event.SessionTitleData).Title)
	case <-time.After(3 * time.Second):
		t.Fatal("actual async title entry did not emit")
	}
}

func TestSessionTitleEventReviewCommittedSnapshotRejectsStaleOrRevokedText(t *testing.T) {
	for _, mode := range []string{"allowed original", "revoked", "legacy", "different text"} {
		t.Run(mode, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
			require.NoError(t, err)
			require.NoError(t, db.AutoMigrate(&types.Session{}))
			proof := ragEntryProof(t)
			row := &types.Session{
				TenantID:           7,
				UserID:             "reader",
				Title:              "returned original title",
				TitleSourceLineage: proof,
			}
			policy := &parentBatchPolicy{}
			if mode == "revoked" {
				policy.revokedA = true
			}
			if mode == "legacy" {
				row.TitleSourceLineage = nil
			}
			if mode == "different text" {
				row.Title = "later manual title"
				row.TitleSourceLineage, _ = types.NewCompleteSourceLineage()
			}
			require.NoError(t, db.Create(row).Error)
			service := &sessionService{sessionRepo: repository.NewSessionRepository(db), sourceLineageGuard: policy}
			original, err := service.committedSessionTitleEventLineage(ragReadContext(), row, "returned original title")
			if mode == "allowed original" {
				require.NoError(t, err)
				require.Equal(t, proof, original)
			} else {
				require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
				require.Nil(t, original)
			}
		})
	}
}
