package service

import (
	"context"
	"fmt"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
)

// RejectNextcloudDerivedKB is also used by the Wiki service, whose agent tools
// call it without going through the HTTP Wiki handler.
func (s *knowledgeBaseService) RejectNextcloudDerivedKB(ctx context.Context, kbID string) error {
	if s == nil || s.repo == nil {
		return apperrors.NewProtocolError(fmt.Errorf(
			"%w: knowledge-base lookup unavailable",
			ErrNextcloudDerivedContent,
		), fmt.Sprintf("%s: knowledge-base lookup unavailable", apperrors.PublicMessage(
			ErrNextcloudDerivedContent,
		)))
	}
	kb, err := s.repo.GetKnowledgeBaseByID(ctx, kbID)
	if err != nil {
		return err
	}
	if kb == nil || kb.ID != kbID {
		return apperrors.NewProtocolError(fmt.Errorf(
			"%w: invalid knowledge-base binding",
			ErrNextcloudDerivedContent,
		), fmt.Sprintf("%s: invalid knowledge-base binding", apperrors.PublicMessage(
			ErrNextcloudDerivedContent,
		)))
	}
	return RejectNextcloudDerivedKB(ctx, kb, s.kgRepo, s.dsRepo)
}
