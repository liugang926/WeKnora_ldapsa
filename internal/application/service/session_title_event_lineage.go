package service

import (
	"context"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
)

// The title returned by GenerateTitle can be a previously committed title.
// Bind the emitted text to the committed original proof; the event sink cannot
// infer that it was generated from today's raw user query.
func (s *sessionService) committedSessionTitleEventLineage(ctx context.Context, session *types.Session, title string) (*types.SourceLineage, error) {
	stored, err := loadSessionForRead(ctx, s.sessionRepo, session.TenantID, sessionUserIDFromContext(ctx), session.ID)
	if err != nil {
		return nil, err
	}
	if stored.Title != title || stored.TitleSourceLineage.RequireComplete() != nil {
		return nil, access.ErrNextcloudPublicationDenied
	}
	visible, err := s.authorizeSessionText(ctx, stored)
	if err != nil {
		return nil, err
	}
	if !visible.TitleAuthorized || visible.Title != title {
		return nil, access.ErrNextcloudPublicationDenied
	}
	return types.UnionSourceLineage(stored.TitleSourceLineage)
}
