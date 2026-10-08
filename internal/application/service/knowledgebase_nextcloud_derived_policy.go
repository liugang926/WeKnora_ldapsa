package service

import (
	"context"
	"fmt"
)

// RejectNextcloudDerivedKB is also used by the Wiki service, whose agent tools
// call it without going through the HTTP Wiki handler.
func (s *knowledgeBaseService) RejectNextcloudDerivedKB(ctx context.Context, kbID string) error {
	if s == nil || s.repo == nil {
		return fmt.Errorf("%w: knowledge-base lookup unavailable", ErrNextcloudDerivedContent)
	}
	kb, err := s.repo.GetKnowledgeBaseByID(ctx, kbID)
	if err != nil {
		return err
	}
	if kb == nil || kb.ID != kbID {
		return fmt.Errorf("%w: invalid knowledge-base binding", ErrNextcloudDerivedContent)
	}
	return RejectNextcloudDerivedKB(ctx, kb, s.kgRepo, s.dsRepo)
}
