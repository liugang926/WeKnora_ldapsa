package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestSearchPublicationRevokedAfterInitialLookupOmitsHydratedSource(t *testing.T) {
	knowledgeService, db := newKnowledgeSharedAccessService(t, nil)
	source := &types.Knowledge{
		ID: "source-doc", TenantID: 1, KnowledgeBaseID: "kb-1", Type: "file",
		Channel: types.ConnectorTypeNextcloud, Title: "source title",
		Metadata: types.JSON(("{\"datasource_id\":\"source-1\",\"nextcloud_instance_id\":\"in" +
			"stance-1\",\"external_id\":\"nextcloud:instance-1:42\"}")),
	}
	ordinary := &types.Knowledge{
		ID: "ordinary-doc", TenantID: 1,
		KnowledgeBaseID: "kb-1", Type: "file", Channel: "web", Title: "ordinary title",
	}
	seedKnowledge(t, db, source)
	seedKnowledge(t, db, ordinary)

	revoked := false
	checks := 0
	svc := &knowledgeBaseService{
		kgRepo: knowledgeService.repo,
		searchPublicationCheck: func(_ context.Context, knowledge *types.Knowledge) error {
			if knowledge.Channel != types.ConnectorTypeNextcloud {
				return nil
			}
			checks++
			if revoked {
				return errors.New("source ACL revoked")
			}
			return nil
		},
	}
	ctx := newSharedAccessContext()
	initial, err := svc.fetchKnowledgeDataWithShared(ctx, 1, []string{source.ID, ordinary.ID})
	require.NoError(t, err)
	require.Len(t, initial, 2)
	require.Equal(t, 1, checks)

	// Simulate authorization changing while chunks are hydrated from the DB.
	revoked = true
	results := []*types.SearchResult{
		{
			KnowledgeID: source.ID, KnowledgeBaseID: source.KnowledgeBaseID,
			KnowledgeChannel: source.Channel, Metadata: source.GetMetadata(), Content: "source secret",
		},
		{
			KnowledgeID: ordinary.ID, KnowledgeBaseID: ordinary.KnowledgeBaseID,
			KnowledgeChannel: ordinary.Channel, Metadata: ordinary.GetMetadata(), Content: "ordinary text",
		},
	}
	filtered, err := svc.filterSearchResultsAtPublicationBoundary(ctx, results)
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	require.Equal(t, "ordinary text", filtered[0].Content)
	require.Equal(t, 2, checks)
}

func TestSearchPublicationVersionChangeAfterHydrationOmitsOldResult(t *testing.T) {
	knowledgeService, db := newKnowledgeSharedAccessService(t, nil)
	source := &types.Knowledge{
		ID: "source-doc", TenantID: 1,
		KnowledgeBaseID: "kb-1", Type: "file", Channel: types.ConnectorTypeNextcloud,
		Metadata: types.JSON(`{"datasource_id":"source-1","nextcloud_etag":"old"}`),
	}
	seedKnowledge(t, db, source)
	svc := &knowledgeBaseService{
		kgRepo:                 knowledgeService.repo,
		searchPublicationCheck: func(context.Context, *types.Knowledge) error { return nil },
	}
	result := &types.SearchResult{
		KnowledgeID: source.ID, KnowledgeBaseID: source.KnowledgeBaseID,
		KnowledgeChannel: source.Channel, Metadata: source.GetMetadata(), Content: "old source content",
	}
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", source.ID).
		Update("metadata", types.JSON(`{"datasource_id":"source-1","nextcloud_etag":"new"}`)).Error)
	filtered, err := svc.filterSearchResultsAtPublicationBoundary(newSharedAccessContext(), []*types.SearchResult{
		result,
	})
	require.NoError(t, err)
	require.Empty(t, filtered)
}
