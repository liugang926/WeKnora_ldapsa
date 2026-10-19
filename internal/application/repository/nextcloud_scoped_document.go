package repository

import (
	"context"
	"errors"
	"github.com/Tencent/WeKnora/internal/types"
)

// ScopedDocument resolves metadata only. QA still performs its live source,
// original-input and final output guards before using document content.
func (s *NextcloudOnboardingStore) ScopedDocument(ctx context.Context, tenant uint64, id string) (string, error) {
	var k types.Knowledge
	if err := s.db.WithContext(ctx).Select("id", "tenant_id", "knowledge_base_id", "channel").
		Where("id=? AND tenant_id=? AND channel=?", id, tenant, types.ConnectorTypeNextcloud).Take(&k).Error; err != nil {
		return "", err
	}
	var count int64
	if err := s.db.WithContext(ctx).Model(&nextcloudSourceVersion{}).
		Where("tenant_id=? AND knowledge_base_id=? AND candidate_knowledge_id=? AND state='published'", tenant, k.KnowledgeBaseID, id).
		Count(&count).Error; err != nil {
		return "", err
	}
	if count != 1 {
		return "", errors.New("current source document unavailable")
	}
	return k.KnowledgeBaseID, nil
}
