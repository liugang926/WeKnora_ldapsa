package repository

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestSessionTitleReviewSameTextAndABACannotSatisfyOriginalSnapshot(t *testing.T) {
	for _, mutation := range []string{"same text", "ABA"} {
		t.Run(mutation, func(t *testing.T) {
			port, db := newSessionRepositoryForTest(t)
			repo := port.(*sessionRepository)
			sql, err := os.ReadFile("../../../migrations/sqlite/000052_session_text_source_lineage.up.sql")
			require.NoError(t, err)
			triggers := string(sql)[strings.Index(string(sql), "CREATE TRIGGER"):]
			require.NoError(t, db.Exec(triggers).Error)
			row := &types.Session{TenantID: 7, UserID: "reader", Title: "original"}
			require.NoError(t, db.Create(row).Error)
			expected := types.SessionTitleSnapshot{Text: row.Title, Revision: row.TitleRevision}
			if mutation == "ABA" {
				require.NoError(t, db.Model(&types.Session{}).Where("id = ?", row.ID).Update("title", "other").Error)
			}
			require.NoError(t, db.Model(&types.Session{}).Where("id = ?", row.ID).Update("title", "original").Error)
			empty, _ := types.NewCompleteSourceLineage()
			written, err := repo.SaveGeneratedSessionTitle(context.Background(), 7, "reader", row.ID, expected, "async replacement", empty)
			require.NoError(t, err)
			require.Zero(t, written)
			stored, err := repo.Get(context.Background(), 7, "reader", row.ID)
			require.NoError(t, err)
			require.Equal(t, "original", stored.Title)
			require.Greater(t, stored.TitleRevision, expected.Revision)
			require.Nil(t, stored.TitleSourceLineage)
		})
	}
}

func TestSessionTitleReviewPostgresOriginalProofAndRevisionCAS(t *testing.T) {
	db := leaseTestPostgres(t)
	require.NoError(t, db.AutoMigrate(&types.Session{}))
	script, err := os.ReadFile("../../../migrations/versioned/000133_session_text_source_lineage.up.sql")
	require.NoError(t, err)
	function := string(script)[strings.Index(string(script), "CREATE OR REPLACE FUNCTION"):strings.Index(string(script), "CREATE TRIGGER")]
	trigger := string(script)[strings.Index(string(script), "CREATE TRIGGER"):strings.Index(string(script), "COMMENT ON")]
	require.NoError(t, db.Exec(function).Error)
	require.NoError(t, db.Exec(trigger).Error)
	repo := NewSessionRepository(db).(*sessionRepository)
	empty, _ := types.NewCompleteSourceLineage()
	for _, initial := range []*types.SourceLineage{nil, empty} {
		row := &types.Session{TenantID: 7, UserID: "reader", Title: "original", TitleSourceLineage: initial}
		require.NoError(t, db.Create(row).Error)
		expected := types.SessionTitleSnapshot{Text: row.Title, Revision: row.TitleRevision, SourceLineage: initial}
		// Wrong proof must not satisfy the compare even at the correct revision.
		wrong := expected
		if initial == nil {
			wrong.SourceLineage = empty
		} else {
			wrong.SourceLineage = nil
		}
		written, err := repo.SaveGeneratedSessionTitle(context.Background(), 7, "reader", row.ID, wrong, "wrong proof", empty)
		require.NoError(t, err)
		require.Zero(t, written)
		written, err = repo.SaveGeneratedSessionTitle(context.Background(), 7, "other owner", row.ID, expected, "wrong owner", empty)
		require.NoError(t, err)
		require.Zero(t, written)
		require.NoError(t, db.Model(&types.Session{}).Where("id = ?", row.ID).Update("title", row.Title).Error)
		written, err = repo.SaveGeneratedSessionTitle(context.Background(), 7, "reader", row.ID, expected, "stale async title", empty)
		require.NoError(t, err)
		require.Zero(t, written)
		current, err := repo.Get(context.Background(), 7, "reader", row.ID)
		require.NoError(t, err)
		require.Equal(t, expected.Revision+1, current.TitleRevision)
		fresh := types.SessionTitleSnapshot{Text: current.Title, Revision: current.TitleRevision, SourceLineage: current.TitleSourceLineage}
		written, err = repo.SaveGeneratedSessionTitle(context.Background(), 7, "reader", row.ID, fresh, "fresh async title", empty)
		require.NoError(t, err)
		require.EqualValues(t, 1, written)
		current, err = repo.Get(context.Background(), 7, "reader", row.ID)
		require.NoError(t, err)
		require.Equal(t, fresh.Revision+1, current.TitleRevision)
		require.Equal(t, "fresh async title", current.Title)
		require.NoError(t, current.TitleSourceLineage.RequireComplete())
	}
}
