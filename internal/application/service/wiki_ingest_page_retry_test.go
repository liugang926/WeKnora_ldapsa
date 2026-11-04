package service

import (
	"context"
	"errors"
	"testing"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type pageRetryU4Service struct {
	interfaces.WikiPageService
	reads, writes int
	err           error
	conflicts     int
	cancel        context.CancelFunc
}

func (s *pageRetryU4Service) GetPageBySlug(context.Context, string, string) (*types.WikiPage, error) {
	s.reads++
	return &types.WikiPage{ID: "page", Slug: "summary/page", Version: s.reads}, nil
}

func (s *pageRetryU4Service) UpdatePage(_ context.Context, page *types.WikiPage) (*types.WikiPage, error) {
	s.writes++
	if page.Version != s.reads {
		panic("retry reused stale page")
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.writes <= s.conflicts {
		return nil, apprepo.ErrWikiPageConflict
	}
	if s.err != nil {
		return nil, s.err
	}
	return page, nil
}

func TestWikiPageRetryU4(t *testing.T) {
	for _, tc := range []struct {
		name      string
		conflicts int
		err       error
		cancel    bool
		want      int
		success   bool
	}{
		{name: "reload after conflict", conflicts: 1, want: 2, success: true},
		{name: "bounded exhaustion", conflicts: 9, want: 3},
		{name: "non conflict", err: errors.New("storage failure"), want: 1},
		{name: "cancel during conflict", conflicts: 9, cancel: true, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			pages := &pageRetryU4Service{conflicts: tc.conflicts, err: tc.err}
			if tc.cancel {
				pages.cancel = cancel
			}
			s := &wikiIngestService{wikiService: pages}
			changed, _, _, err := s.reduceSlugUpdates(ctx, nil, "kb", "summary/page", []SlugUpdate{{Type: "summary", DocTitle: "doc", SummaryBody: "body"}}, 1, nil, nil)
			require.Equal(t, tc.want, pages.writes)
			require.Equal(t, tc.want, pages.reads)
			require.Equal(t, tc.success, changed)
			if tc.success {
				require.NoError(t, err)
			} else if tc.cancel {
				require.ErrorIs(t, err, context.Canceled)
			} else if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			} else {
				require.ErrorIs(t, err, apprepo.ErrWikiPageConflict)
			}
		})
	}
	t.Run("already canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		pages := &pageRetryU4Service{}
		s := &wikiIngestService{wikiService: pages}
		_, _, _, err := s.reduceSlugUpdates(ctx, nil, "kb", "summary/page", []SlugUpdate{{Type: "summary"}}, 1, nil, nil)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, pages.reads)
	})
}
