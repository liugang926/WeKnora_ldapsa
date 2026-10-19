package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestSessionTextLineageAtomicTitleWritePreservesLegacyAndManualCAS(t *testing.T) {
	port, db := newSessionRepositoryForTest(t)
	repo := port.(*sessionRepository)
	row := &types.Session{TenantID: 7, UserID: "reader", Title: "legacy title"}
	require.NoError(t, db.Create(row).Error)
	empty, _ := types.NewCompleteSourceLineage()
	written, err := repo.SaveGeneratedSessionTitle(
		context.Background(),
		7,
		"reader",
		row.ID,
		types.SessionTitleSnapshot{Text: "wrong title"},
		"new title",
		empty,
	)
	require.NoError(t, err)
	require.Zero(t, written)
	got, err := repo.Get(context.Background(), 7, "reader", row.ID)
	require.NoError(t, err)
	require.Equal(t, "legacy title", got.Title)
	require.Nil(t, got.TitleSourceLineage)
	written, err = repo.SaveGeneratedSessionTitle(
		context.Background(),
		7,
		"reader",
		row.ID,
		types.SessionTitleSnapshot{Text: "legacy title"},
		"new title",
		empty,
	)
	require.NoError(t, err)
	require.EqualValues(t, 1, written)
	got, err = repo.Get(context.Background(), 7, "reader", row.ID)
	require.NoError(t, err)
	require.Equal(t, "new title", got.Title)
	require.Equal(t, empty, got.TitleSourceLineage)
	require.NoError(
		t,
		db.Exec(
			`CREATE TRIGGER reject_title_proof BEFORE UPDATE OF title_source_lineage ON sessions BEGIN SELECT RAISE(ABORT, 'injected title proof failure'); END`,
		).Error,
	)
	_, err = repo.SaveGeneratedSessionTitle(
		context.Background(),
		7,
		"reader",
		row.ID,
		types.SessionTitleSnapshot{Text: got.Title, Revision: got.TitleRevision, SourceLineage: got.TitleSourceLineage},
		"orphan title",
		empty,
	)
	require.ErrorContains(t, err, "injected title proof failure")
	got, err = repo.Get(context.Background(), 7, "reader", row.ID)
	require.NoError(t, err)
	require.Equal(t, "new title", got.Title)
	require.Equal(t, empty, got.TitleSourceLineage)
}

func TestSessionTextLineageKeywordCandidatesExcludeUnknownWithoutBackfill(t *testing.T) {
	port, db := newSessionRepositoryForTest(t)
	repo := port.(*sessionRepository)
	require.NoError(t, db.AutoMigrate(&testIMChannelSession{}))
	empty, _ := types.NewCompleteSourceLineage()
	for _, proof := range []*types.SourceLineage{nil, types.UnknownSourceLineage(), empty} {
		require.NoError(
			t,
			db.Create(
				&types.Session{TenantID: 7, UserID: "reader", Title: "matching title", TitleSourceLineage: proof},
			).Error,
		)
	}
	rows, total, err := repo.QueryPaged(
		context.Background(),
		&types.SessionListQuery{TenantID: 7, UserID: "reader", Keyword: "matching", Page: 1, PageSize: 10},
	)
	require.NoError(t, err)
	require.EqualValues(t, 1, total)
	require.Len(t, rows, 1)
	require.Equal(t, empty, rows[0].TitleSourceLineage)
	var unknown int64
	require.NoError(t, db.Model(&types.Session{}).Where("title_source_lineage IS NULL").Count(&unknown).Error)
	require.EqualValues(t, 1, unknown)
}
