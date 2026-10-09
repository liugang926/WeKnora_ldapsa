package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type liveGrantKnowledgeStub struct {
	interfaces.KnowledgeService
	revoked bool
	seen    types.SearchTargets
}

func (s *liveGrantKnowledgeStub) GetKnowledgeByIDOnly(_ context.Context, id string) (*types.Knowledge, error) {
	if id != "source-doc" {
		return nil, errors.New("missing document")
	}
	return &types.Knowledge{ID: id, TenantID: 7, KnowledgeBaseID: "source-kb"}, nil
}

func (s *liveGrantKnowledgeStub) CheckNextcloudReadTargets(_ context.Context, targets types.SearchTargets) error {
	s.seen = targets
	if s.revoked {
		return errors.New("directory group withdrawn")
	}
	return nil
}

type liveGrantKBStub struct {
	interfaces.KnowledgeBaseService
}

func (*liveGrantKBStub) GetKnowledgeBaseByIDOnly(_ context.Context, id string) (*types.KnowledgeBase, error) {
	if id != "source-kb" {
		return nil, errors.New("missing knowledge base")
	}
	return &types.KnowledgeBase{ID: id, TenantID: 7}, nil
}

func TestLiveAnswerRechecksCurrentKBGrantForDocumentOnlyToolReference(t *testing.T) {
	knowledge := &liveGrantKnowledgeStub{}
	svc := &messageService{knowService: knowledge, kbService: &liveGrantKBStub{}}
	message := &types.Message{Role: "assistant", KnowledgeReferences: types.References{
		&types.SearchResult{KnowledgeID: "source-doc"},
	}}
	require.NoError(t, svc.checkLiveKnowledgeGrants(context.Background(), message))
	require.Len(t, knowledge.seen, 1)
	require.Equal(t, uint64(7), knowledge.seen[0].TenantID)
	knowledge.revoked = true
	require.ErrorIs(t, svc.checkLiveKnowledgeGrants(context.Background(), message),
		access.ErrNextcloudPublicationDenied)
}
