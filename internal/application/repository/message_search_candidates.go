package repository

import (
	"context"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// These queries deliberately select only control metadata. Neither candidate
// discovery nor partner lookup evaluates a predicate on saved message text.
func (r *messageRepository) historySearchCandidates(ctx context.Context, tenant uint64, owner string, sessionIDs []string) *gorm.DB {
	q := r.db.WithContext(ctx).Table("messages m").
		Select("m.id,m.session_id,m.request_id,m.role,m.created_at,m.knowledge_id").
		Joins("JOIN sessions s ON s.id=m.session_id AND s.deleted_at IS NULL").
		Where("s.tenant_id=? AND s.user_id=? AND m.deleted_at IS NULL AND m.is_completed=?", tenant, owner, true).
		Where("m.role IN ?", []string{"user", "assistant"})
	if len(sessionIDs) != 0 {
		q = q.Where("m.session_id IN ?", sessionIDs)
	}
	return q
}

func (r *messageRepository) ListOwnedHistorySearchCandidates(ctx context.Context, tenant uint64, owner string, sessionIDs []string, before time.Time, beforeID string, limit int) ([]*types.MessageSearchCandidate, error) {
	q := r.historySearchCandidates(ctx, tenant, owner, sessionIDs)
	if !before.IsZero() {
		q = q.Where("m.created_at < ? OR (m.created_at = ? AND m.id < ?)", before, before, beforeID)
	}
	var rows []*types.MessageSearchCandidate
	err := q.Order("m.created_at DESC,m.id DESC").Limit(limit).Scan(&rows).Error
	return rows, err
}

func (r *messageRepository) HistorySearchPartnerCandidates(ctx context.Context, tenant uint64, owner string, candidate *types.MessageSearchCandidate) ([]*types.MessageSearchCandidate, error) {
	q := r.historySearchCandidates(ctx, tenant, owner, []string{candidate.SessionID})
	if candidate.RequestID == "" {
		q = q.Where("m.id=?", candidate.ID)
	} else {
		q = q.Where("m.request_id=?", candidate.RequestID)
	}
	var rows []*types.MessageSearchCandidate
	// Injected steer inputs deliberately share the running request ID. Fetch
	// their control metadata with one overflow row, never an unbounded body set.
	err := q.Order("m.created_at DESC,m.id DESC").Limit(types.MessageSearchMaximumExchangeMessages + 1).Scan(&rows).Error
	return rows, err
}

func (r *messageRepository) OwnedHistorySearchCandidatesByKnowledgeIDs(ctx context.Context, tenant uint64, owner string, sessionIDs, knowledgeIDs []string, limit int) ([]*types.MessageSearchCandidate, error) {
	var rows []*types.MessageSearchCandidate
	err := r.historySearchCandidates(ctx, tenant, owner, sessionIDs).
		Where("m.knowledge_id IN ?", knowledgeIDs).
		Order("m.created_at DESC,m.id DESC").Limit(limit).Scan(&rows).Error
	return rows, err
}

func (r *messageRepository) OwnedHistorySearchCandidatesByIDs(ctx context.Context, tenant uint64, owner string, sessionIDs, ids []string) ([]*types.MessageSearchCandidate, error) {
	var rows []*types.MessageSearchCandidate
	err := r.historySearchCandidates(ctx, tenant, owner, sessionIDs).Where("m.id IN ?", ids).
		Order("m.created_at DESC,m.id DESC").Limit(200).Scan(&rows).Error
	return rows, err
}
