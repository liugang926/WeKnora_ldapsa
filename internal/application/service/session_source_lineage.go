package service

import (
	"context"
	"errors"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
)

func (s *sessionService) authorizeSessionText(ctx context.Context, session *types.Session) (*types.Session, error) {
	if session == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	copied := *session
	copied.TitleAuthorized, copied.DescriptionAuthorized = false, false
	authorize := func(text *string, lineage *types.SourceLineage, approved *bool) error {
		if lineage.RequireComplete() != nil {
			*text = ""
			return nil
		}
		if len(lineage.Sources) == 0 {
			*approved = true
			return nil
		} // Existing owner/session checks still apply.
		if s.sourceLineageGuard == nil {
			return access.ErrNextcloudPublicationUnavailable
		}
		if err := s.sourceLineageGuard.Check(ctx, lineage, access.LineageReadScope{}); err != nil {
			if errors.Is(err, access.ErrNextcloudPublicationDenied) {
				*text = ""
				return nil
			}
			return err
		}
		*approved = true
		return nil
	}
	if err := authorize(&copied.Title, copied.TitleSourceLineage, &copied.TitleAuthorized); err != nil {
		return nil, err
	}
	if err := authorize(
		&copied.Description,
		copied.DescriptionSourceLineage,
		&copied.DescriptionAuthorized,
	); err != nil {
		return nil, err
	}
	var reads []access.SourceLineageRead
	if copied.TitleAuthorized && len(copied.TitleSourceLineage.Sources) > 0 {
		reads = append(reads, access.SourceLineageRead{Lineage: copied.TitleSourceLineage})
	}
	if copied.DescriptionAuthorized && len(copied.DescriptionSourceLineage.Sources) > 0 {
		reads = append(reads, access.SourceLineageRead{Lineage: copied.DescriptionSourceLineage})
	}
	if len(reads) > 0 {
		if s.sourceLineageGuard == nil {
			return nil, access.ErrNextcloudPublicationUnavailable
		}
		if err := s.sourceLineageGuard.CheckBatch(ctx, reads); err != nil {
			return nil, err
		}
	}
	return &copied, nil
}

func (s *sessionService) authorizeSessionTexts(
	ctx context.Context,
	sessions []*types.Session,
) ([]*types.Session, error) {
	out := make([]*types.Session, 0, len(sessions))
	union, _ := types.NewCompleteSourceLineage()
	for _, row := range sessions {
		visible, err := s.authorizeSessionText(ctx, row)
		if err != nil {
			return nil, err
		}
		out = append(out, visible)
		if visible.TitleAuthorized {
			union, err = types.UnionSourceLineage(union, visible.TitleSourceLineage)
			if err != nil {
				return nil, err
			}
		}
		if visible.DescriptionAuthorized {
			union, err = types.UnionSourceLineage(union, visible.DescriptionSourceLineage)
			if err != nil {
				return nil, err
			}
		}
	}
	// One final coherent dependency check after all per-field remote calls.
	if len(union.Sources) > 0 {
		if s.sourceLineageGuard == nil {
			return nil, access.ErrNextcloudPublicationUnavailable
		}
		if err := s.sourceLineageGuard.Check(ctx, union, access.LineageReadScope{}); err != nil {
			return nil, err
		}
	}
	return out, nil
}

type sessionTitleCandidates interface {
	ListSessionTitleCandidates(context.Context, *types.SessionListQuery) ([]*types.SessionListItem, error)
}
type sessionTitleWriter interface {
	SaveGeneratedSessionTitle(
		context.Context,
		uint64,
		string,
		string,
		types.SessionTitleSnapshot,
		string,
		*types.SourceLineage,
	) (int64, error)
}

func (s *sessionService) authorizedTitleMatches(
	ctx context.Context,
	query *types.SessionListQuery,
) ([]*types.SessionListItem, error) {
	reader, ok := s.sessionRepo.(sessionTitleCandidates)
	if !ok {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	rows, err := reader.ListSessionTitleCandidates(ctx, query)
	if err != nil {
		return nil, err
	}
	matched := make([]*types.SessionListItem, 0, len(rows))
	for _, row := range rows {
		visible, err := s.authorizeSessionText(ctx, &row.Session)
		if err != nil {
			return nil, err
		}
		if !visible.TitleAuthorized {
			continue
		}
		copied2 := *row
		copied2.Session = *visible
		matched = append(matched, &copied2)
	}
	visibleSessions := make([]*types.Session, 0, len(matched))
	for _, row := range matched {
		visibleSessions = append(visibleSessions, &row.Session)
	}
	final, err := s.authorizeSessionTexts(ctx, visibleSessions)
	if err != nil {
		return nil, err
	}
	kept := matched[:0]
	for i, row := range matched {
		if final[i].TitleAuthorized {
			row.Session = *final[i]
			kept = append(kept, row)
		}
	}
	matched = kept
	return matched, nil
}
