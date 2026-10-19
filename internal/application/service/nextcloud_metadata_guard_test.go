package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type nextcloudMetadataKBService struct {
	interfaces.KnowledgeBaseService
	kb *types.KnowledgeBase
}

func (s *nextcloudMetadataKBService) GetKnowledgeBaseByID(context.Context, string) (*types.KnowledgeBase, error) {
	return s.kb, nil
}

type nextcloudMetadataKnowledgeService struct {
	interfaces.KnowledgeService
	listed bool
}

func (s *nextcloudMetadataKnowledgeService) ListKnowledgeByKnowledgeBaseID(
	context.Context,
	string,
) ([]*types.Knowledge, error) {
	s.listed = true
	return []*types.Knowledge{{Title: "private-title", Description: "private-summary", EnableStatus: "enabled"}}, nil
}

func TestNextcloudKnowledgeBaseProfileAndFallbackCannotAggregateFiles(t *testing.T) {
	kb := &types.KnowledgeBase{
		ID: "nextcloud-kb", Type: types.KnowledgeBaseTypeDocument,
		EverHadNextcloudSource: true,
	}
	require.False(t, knowledgeBaseProfileEligible(kb))
	knowledge := &nextcloudMetadataKnowledgeService{}
	session := &sessionService{
		knowledgeBaseService: &nextcloudMetadataKBService{kb: kb},
		knowledgeService:     knowledge,
	}
	listing := session.buildKBDocumentListing(context.Background(),
		&types.ChatManage{PipelineRequest: types.PipelineRequest{KnowledgeBaseIDs: []string{kb.ID}}})
	require.Empty(t, listing)
	require.False(t, knowledge.listed, "fallback must not read raw Nextcloud titles")
}
