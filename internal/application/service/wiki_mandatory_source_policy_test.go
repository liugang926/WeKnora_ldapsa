package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// Existing ordinary Wiki fixtures explicitly provide their scope policy.
type wikiOrdinaryTestPolicy struct {
	interfaces.KnowledgeBaseService
}

func (wikiOrdinaryTestPolicy) RejectNextcloudDerivedKB(context.Context, string) error { return nil }

type wikiSourceRejectTestPolicy struct {
	interfaces.KnowledgeBaseService
}

func (wikiSourceRejectTestPolicy) RejectNextcloudDerivedKB(context.Context, string) error {
	return ErrNextcloudDerivedContent
}

func TestWikiMandatorySourcePolicy(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name   string
		policy interfaces.KnowledgeBaseService
	}{
		{"missing", nil}, {"unsupported", &wikiGuardKBService{}}, {"source", wikiSourceRejectTestPolicy{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			// A nil repository would panic if any denied entry touched body/storage.
			svc := NewWikiPageService(nil, nil, test.policy, nil, nil)
			_, err := svc.CreatePage(ctx, &types.WikiPage{KnowledgeBaseID: "kb", Slug: "s", Content: "body"})
			require.ErrorIs(t, err, ErrNextcloudDerivedContent)
			_, err = svc.ListAllPages(ctx, "kb")
			require.ErrorIs(t, err, ErrNextcloudDerivedContent)
			_, err = svc.ListSummariesByKnowledgeIDs(ctx, "kb", []string{"k"})
			require.ErrorIs(t, err, ErrNextcloudDerivedContent)
			_, err = svc.GetRevision(ctx, "kb", "s", 1)
			require.ErrorIs(t, err, ErrNextcloudDerivedContent)
			_, _, err = svc.ListPagesCursor(ctx, "kb", "", 1)
			require.ErrorIs(t, err, ErrNextcloudDerivedContent)
			err = svc.UpdateAutoLinkedContent(ctx, &types.WikiPage{KnowledgeBaseID: "kb", Content: "body"})
			require.ErrorIs(t, err, ErrNextcloudDerivedContent)
			err = svc.DeletePage(ctx, "kb", "s")
			require.ErrorIs(t, err, ErrNextcloudDerivedContent)
			_, err = svc.CreateFolder(ctx, "kb", 1, "", "folder")
			require.ErrorIs(t, err, ErrNextcloudDerivedContent)
			err = svc.RebuildLinks(ctx, "kb")
			require.ErrorIs(t, err, ErrNextcloudDerivedContent)
			svc.InjectCrossLinks(ctx, "kb", []string{"s"})
		})
	}
}

func TestWikiIDScopeBeforeBodyAndOrdinaryUsable(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.WikiPage{}, &types.WikiPageRevision{}, &types.WikiFolder{}))
	repo := repository.NewWikiPageRepository(db)
	ctx := context.Background()
	ordinary := NewWikiPageService(repo, nil, wikiOrdinaryTestPolicy{}, nil, nil)
	page, err := ordinary.CreatePage(ctx, &types.WikiPage{ID: "owned", TenantID: 1, KnowledgeBaseID: "kb", Slug: "s", Content: "secret"})
	require.NoError(t, err)
	var queries []string
	require.NoError(t, db.Callback().Query().After("gorm:query").Register("wiki_query_inventory", func(tx *gorm.DB) { queries = append(queries, tx.Statement.SQL.String()) }))
	denied := NewWikiPageService(repo, nil, wikiSourceRejectTestPolicy{}, nil, nil)
	_, err = denied.GetPageByID(ctx, page.ID)
	require.ErrorIs(t, err, ErrNextcloudDerivedContent)
	require.Len(t, queries, 1)
	require.Contains(t, queries[0], "SELECT `knowledge_base_id`")
	require.NotContains(t, queries[0], "SELECT *")
	got, err := ordinary.GetPageByID(ctx, page.ID)
	require.NoError(t, err)
	require.Equal(t, "secret", got.Content)
	_, err = denied.UpdatePage(ctx, &types.WikiPage{KnowledgeBaseID: "kb", Slug: "s", Content: "replacement"})
	require.ErrorIs(t, err, ErrNextcloudDerivedContent)
	got, err = repo.GetByID(ctx, page.ID)
	require.NoError(t, err)
	require.Equal(t, "secret", got.Content)
}

func TestWikiMetadataCannotRelabelProtectedOwner(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.WikiPage{}))
	repo := repository.NewWikiPageRepository(db)
	ctx := context.Background()
	require.NoError(t, repo.Create(ctx, &types.WikiPage{ID: "owned", TenantID: 1, KnowledgeBaseID: "protected", Slug: "s", Status: "published", Content: "secret"}))
	svc := NewWikiPageService(repo, nil, wikiOrdinaryTestPolicy{}, nil, nil)
	err = svc.UpdatePageMeta(ctx, &types.WikiPage{ID: "owned", KnowledgeBaseID: "ordinary", Status: "archived"})
	require.ErrorIs(t, err, ErrNextcloudDerivedContent)
	require.Error(t, repo.UpdateMeta(ctx, &types.WikiPage{ID: "owned", KnowledgeBaseID: "ordinary", Status: "archived"}))
	got, err := repo.GetByID(ctx, "owned")
	require.NoError(t, err)
	require.Equal(t, "published", got.Status)
	require.Equal(t, "protected", got.KnowledgeBaseID)
}
