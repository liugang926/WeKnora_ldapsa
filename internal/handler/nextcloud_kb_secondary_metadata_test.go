package handler

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type secondaryMetadataSources struct {
	interfaces.DataSourceRepository
}

func (secondaryMetadataSources) FindByKnowledgeBase(_ context.Context, id string) ([]*types.DataSource, error) {
	switch id {
	case "source-inventory":
		return []*types.DataSource{{Type: types.ConnectorTypeNextcloud}}, nil
	case "inventory-offline":
		return nil, errors.New("inventory unavailable")
	default:
		return nil, nil
	}
}

type secondaryMetadataKBs struct {
	interfaces.KnowledgeBaseService
	kbs []*types.KnowledgeBase
}

func (s secondaryMetadataKBs) GetKnowledgeBaseByID(_ context.Context, id string) (*types.KnowledgeBase, error) {
	return s.GetKnowledgeBaseByIDOnly(context.Background(), id)
}

func (s secondaryMetadataKBs) GetKnowledgeBaseByIDOnly(_ context.Context, id string) (*types.KnowledgeBase, error) {
	for _, kb := range s.kbs {
		if kb.ID == id {
			return kb, nil
		}
	}
	return nil, errors.New("missing KB")
}

func (s secondaryMetadataKBs) ListKnowledgeBases(context.Context) ([]*types.KnowledgeBase, error) {
	return s.kbs, nil
}

func secondaryMetadataFixture() ([]*types.KnowledgeBase, *KnowledgeBaseHandler) {
	kbs := []*types.KnowledgeBase{
		{ID: "ordinary", TenantID: 7, Name: "Ordinary target", Type: types.KnowledgeBaseTypeDocument},
		{
			ID:                     "source-marker",
			TenantID:               7,
			Name:                   "Retired source marker",
			Type:                   types.KnowledgeBaseTypeDocument,
			EverHadNextcloudSource: true,
		},
		{
			ID:          "source-inventory",
			TenantID:    7,
			Name:        "Source root private",
			Type:        types.KnowledgeBaseTypeDocument,
			Description: "retired body",
		},
		{ID: "inventory-offline", TenantID: 7, Name: "Unknown source private", Type: types.KnowledgeBaseTypeDocument},
		{ID: "ordinary-target", TenantID: 7, Name: "Allowed ordinary target", Type: types.KnowledgeBaseTypeDocument},
	}
	return kbs, &KnowledgeBaseHandler{
		service:        secondaryMetadataKBs{kbs: kbs},
		dataSourceRepo: secondaryMetadataSources{},
	}
}

func TestNextcloudSecondaryMetadataMoveTargetsExcludeManagedSources(t *testing.T) {
	_, h := secondaryMetadataFixture()
	for _, sourceID := range []string{"ordinary", "source-inventory", "source-marker"} {
		c, w := newOrganizationGroupContext(t)
		c.Params = gin.Params{{Key: "id", Value: sourceID}}
		h.ListMoveTargets(c)
		if sourceID != "ordinary" {
			require.NotEmpty(t, c.Errors)
			require.Empty(t, w.Body.String())
			continue
		}
		require.Empty(t, c.Errors)
		require.Contains(t, w.Body.String(), "Allowed ordinary target")
		for _, secret := range []string{"Retired source marker", "Source root private", "Unknown source private", "retired body"} {
			require.NotContains(t, w.Body.String(), secret)
		}
	}
}

type secondaryMetadataShares struct{ organizationGroupShareStub }

func (secondaryMetadataShares) ListSharedKnowledgeBaseIDsByOrganizations(
	context.Context,
	[]string,
	uint64,
) (map[string][]string, error) {
	return map[string][]string{"org-1": {"ordinary", "source-marker", "source-inventory", "inventory-offline"}}, nil
}

type secondaryMetadataAgents struct {
	organizationGroupAgentShareStub
}

func (secondaryMetadataAgents) CountByOrganizations(context.Context, []string) (map[string]int64, error) {
	return map[string]int64{}, nil
}

func (secondaryMetadataAgents) ListSharedAgentsInOrganizations(
	context.Context,
	[]string,
	uint64,
	types.TenantRole,
) (map[string][]*types.OrganizationSharedAgentItem, error) {
	return map[string][]*types.OrganizationSharedAgentItem{}, nil
}

func TestNextcloudSecondaryMetadataSharedListsAndCountsExcludeManagedSources(t *testing.T) {
	kbs, kbHandler := secondaryMetadataFixture()
	shares := secondaryMetadataShares{}
	for _, kb := range kbs {
		info := &types.SharedKnowledgeBaseInfo{KnowledgeBase: kb, SourceTenantID: 7}
		shares.shared = append(shares.shared, info)
		shares.inOrg = append(shares.inOrg, &types.OrganizationSharedKnowledgeBaseItem{SharedKnowledgeBaseInfo: *info})
	}
	h := &OrganizationHandler{
		orgService:        organizationGroupOrgStub{},
		shareService:      shares,
		agentShareService: secondaryMetadataAgents{},
		kbService:         secondaryMetadataKBs{kbs: kbs},
	}
	ConfigureOrganizationSourceMetadataGuard(h, kbHandler)
	for _, invoke := range []func(*gin.Context){h.ListSharedKnowledgeBases, h.ListOrganizationSharedKnowledgeBases} {
		c, w := newOrganizationGroupContext(t)
		invoke(c)
		require.Empty(t, c.Errors)
		require.Contains(t, w.Body.String(), "Allowed ordinary target")
		for _, secret := range []string{"Retired source marker", "Source root private", "Unknown source private", "retired body"} {
			require.NotContains(t, w.Body.String(), secret)
		}
	}
	c, _ := newOrganizationGroupContext(t)
	counts := h.buildResourceCountsByOrg(c.Request.Context(), []*types.Organization{{ID: "org-1"}}, "user-7", 7)
	require.NotNil(t, counts)
	require.Equal(t, 1, counts.KnowledgeBases.ByOrganization["org-1"])
	// An unconfigured optional overlay must still reject known source lineage.
	h.sourceMetadata = nil
	require.False(t, h.mayListSharedKnowledgeBase(context.Background(), 7, kbs[1], kbs[1].ID))
}
