package handler

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type wikiDerivedKnowledgeRepo struct {
	interfaces.KnowledgeRepository
}

func (wikiDerivedKnowledgeRepo) ListKnowledgeByKnowledgeBaseID(
	context.Context, uint64, string,
) ([]*types.Knowledge, error) {
	return nil, nil
}

type wikiDerivedSourceRepo struct {
	interfaces.DataSourceRepository
}

func (wikiDerivedSourceRepo) FindByKnowledgeBase(context.Context, string) ([]*types.DataSource, error) {
	return []*types.DataSource{{
		TenantID: 7, KnowledgeBaseID: "kb", Type: types.ConnectorTypeNextcloud,
	}}, nil
}

func TestWikiRouteRejectsNextcloudSourceBeforeReadingPage(t *testing.T) {
	h := &WikiPageHandler{
		kbService: &stubKBService{get: func(_ context.Context, _ string) (*types.KnowledgeBase, error) {
			return &types.KnowledgeBase{
				ID: "kb", TenantID: 7,
				IndexingStrategy: types.IndexingStrategy{WikiEnabled: true},
			}, nil
		}},
		knowledgeRepo:  wikiDerivedKnowledgeRepo{},
		dataSourceRepo: wikiDerivedSourceRepo{},
	}
	c := newKBLookupCtx(t, 7, "kb")
	c.Params = gin.Params{{Key: "kb_id", Value: "kb"}}
	_, _, err := h.validateWikiKB(c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Wiki is unavailable")
}
