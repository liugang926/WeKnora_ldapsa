package handler

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type groupAccessResourceCatalog struct {
	interfaces.ResourceCatalog
	revocations int
}

func (r *groupAccessResourceCatalog) RevokeAccessGrantsByKnowledgeBase(
	context.Context, uint64, string,
) (int64, error) {
	r.revocations++
	return 0, nil
}

func TestPreparingInheritedResourceGroupsPreservesCapabilities(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, mode := range []types.ResourceAccessMode{types.ResourceAccessInherit, types.ResourceAccessRestricted} {
		t.Run(string(mode), func(t *testing.T) {
			catalog := &groupAccessResourceCatalog{}
			h := NewGroupAccessHandler(
				&groupAccessDirectoryStub{}, &groupAccessRepoStub{}, &groupAccessServiceStub{},
				&groupAccessKBRepoStub{kb: &types.KnowledgeBase{ID: "kb", TenantID: 7}}, nil, nil,
			)
			ConfigureGroupAccessResourceCatalog(h, catalog)
			ctx, recorder := groupAccessTestContext(
				"PUT", "/group-access/knowledge_base/kb", `{"mode":"`+string(mode)+`","grants":[]}`, 7,
			)
			ctx.Params = gin.Params{
				{Key: "resource_type", Value: "knowledge_base"}, {Key: "resource_id", Value: "kb"},
			}
			h.UpdateResourceGroupAccess(ctx)
			require.Empty(t, ctx.Errors)
			require.Equal(t, 200, recorder.Code)
			if mode == types.ResourceAccessInherit {
				require.Zero(t, catalog.revocations, "preparing groups in inherit mode preserves current sharing")
			} else {
				require.Equal(t, 1, catalog.revocations, "restricting the KB revokes its previous capabilities")
			}
		})
	}
}
