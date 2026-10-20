package service

import (
	"context"
)

// RejectNextcloudDerivedKB is also used by the Wiki service, whose agent tools
// call it without going through the HTTP Wiki handler.
func (s *knowledgeBaseService) RejectNextcloudDerivedKB(ctx context.Context, kbID string) error {
	if s == nil || s.repo == nil {
		return nextcloudDerivedContentContext("knowledge-base lookup unavailable")
	}
	kb, err := s.repo.GetKnowledgeBaseByID(ctx, kbID)
	if err != nil {
		return err
	}
	if kb == nil || kb.ID != kbID {
		return nextcloudDerivedContentContext("invalid knowledge-base binding")
	}
	return RejectNextcloudDerivedKB(ctx, kb, s.kgRepo, s.dsRepo)
}
