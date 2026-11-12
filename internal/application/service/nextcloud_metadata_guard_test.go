package service

import (
	"context"
	"slices"
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
	listed    bool
	documents []*types.Knowledge
}

func (s *nextcloudMetadataKnowledgeService) ListKnowledgeByKnowledgeBaseID(
	context.Context,
	string,
) ([]*types.Knowledge, error) {
	s.listed = true
	return s.documents, nil
}

func (s *nextcloudMetadataKnowledgeService) GetKnowledgeBatch(
	_ context.Context, tenant uint64, ids []string,
) ([]*types.Knowledge, error) {
	var rows []*types.Knowledge
	for _, doc := range s.documents {
		if doc.TenantID == tenant && slices.Contains(ids, doc.ID) {
			rows = append(rows, doc)
		}
	}
	return rows, nil
}

func (s *nextcloudMetadataKnowledgeService) ListKnowledgeIDsByTagIDs(
	_ context.Context, _ uint64, _ string, tags []string,
) ([]string, error) {
	if slices.Contains(tags, "tag-one") {
		return []string{"one"}, nil
	}
	return nil, nil
}

func TestNextcloudKnowledgeBaseProfileAndFallbackCannotAggregateFiles(t *testing.T) {
	kb := &types.KnowledgeBase{
		ID: "nextcloud-kb", TenantID: 1, Type: types.KnowledgeBaseTypeDocument,
		EverHadNextcloudSource: true,
	}
	require.False(t, knowledgeBaseProfileEligible(kb))
	knowledge := &nextcloudMetadataKnowledgeService{documents: []*types.Knowledge{
		{ID: "one", TenantID: 1, KnowledgeBaseID: kb.ID, Title: "first", EnableStatus: "enabled"},
		{ID: "two", TenantID: 1, KnowledgeBaseID: kb.ID, Title: "second", EnableStatus: "enabled"},
	}}
	session := &sessionService{
		knowledgeBaseService: &nextcloudMetadataKBService{kb: kb},
		knowledgeService:     knowledge,
	}
	request := &types.ChatManage{PipelineRequest: types.PipelineRequest{KnowledgeBaseIDs: []string{kb.ID}}}
	ctx := newSharedAccessContext()
	require.Empty(t, session.buildKBDocumentListing(ctx, request))
	require.False(t, knowledge.listed, "a requested KB without an admitted target must not be read")

	request.SearchTargets = types.SearchTargets{{
		TenantID: kb.TenantID, KnowledgeBaseID: kb.ID,
		Type: types.SearchTargetTypeKnowledge, KnowledgeIDs: []string{"two"},
	}}
	require.Equal(t, "- second\n", session.buildKBDocumentListing(ctx, request))
	require.False(t, knowledge.listed, "a file mention must not enumerate its entire KB")

	request.SearchTargets[0].ScopeTagIDs = []string{"tag-one"}
	require.Empty(t, session.buildKBDocumentListing(ctx, request), "a tag cannot widen an explicit file mention")
	request.SearchTargets[0].ScopeTagIDs = nil
	request.SearchTargets[0].Type = types.SearchTargetTypeKnowledgeBase
	request.SearchTargets[0].KnowledgeIDs = nil
	deniedCtx := types.WithCaller(ctx, types.Caller{TenantID: 2, UserID: "other"})
	require.Empty(t, session.buildKBDocumentListing(deniedCtx, request))
	require.False(t, knowledge.listed)
	require.Equal(t, "- first\n- second\n", session.buildKBDocumentListing(ctx, request))
	require.True(t, knowledge.listed, "the historical source marker does not hide an authorized KB")
}
