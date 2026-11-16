package im

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type failingIMResourceCatalog struct{ interfaces.ResourceCatalog }

func (s failingIMResourceCatalog) ResolvePath(context.Context, string) (string, *types.StoredResource, error) {
	return "", nil, errors.New("catalog unavailable")
}

type failingIMShareService struct{ interfaces.KBShareService }

func (s failingIMShareService) CheckTenantKBPermission(
	context.Context, string, uint64, types.TenantRole,
) (types.OrgMemberRole, bool, error) {
	return "", false, errors.New("sharing unavailable")
}

func TestIMResourceAccessFailsClosed(t *testing.T) {
	cases := []string{
		"unshared", "binding missing", "knowledge deleted", "borrower removed", "owner removed",
		"organization deleted", "catalog failure", "sharing failure", "missing identity",
		"other selected KB", "no KBs", "explicit KB restriction",
	}
	for _, externalURL := range []string{"", "https://weknora.example"} {
		for _, scenario := range cases {
			t.Run(scenario+externalURL, func(t *testing.T) {
				f := newIMResourceStorageFixture(t)
				t.Setenv("APP_EXTERNAL_URL", externalURL)
				s, _, _, _ := newFullOutputHarness("")
				f.wire(s)
				ctx := f.context()
				var agent *types.CustomAgent
				var kbIDs []string
				switch scenario {
				case "unshared":
					require.NoError(t, f.db.Where("id = ?", "share").Delete(&types.KnowledgeBaseShare{}).Error)
				case "binding missing":
					require.NoError(t, f.db.Where("owner_id = ?", "shared-doc").Delete(&types.ResourceBinding{}).Error)
				case "knowledge deleted":
					require.NoError(t, f.db.Where("id = ?", "shared-doc").Delete(&types.Knowledge{}).Error)
				case "borrower removed", "owner removed":
					id := uint64(7)
					if scenario == "owner removed" {
						id = 9
					}
					require.NoError(t, f.db.Where("tenant_id = ?", id).Delete(&types.OrganizationTenantMember{}).Error)
				case "organization deleted":
					require.NoError(t, f.db.Where("id = ?", "org").Delete(&types.Organization{}).Error)
				case "catalog failure":
					s.resourceCatalog = failingIMResourceCatalog{ResourceCatalog: f.catalog}
				case "sharing failure":
					s.kbShareService = failingIMShareService{}
				case "missing identity":
					ctx = context.Background()
				case "other selected KB":
					agent = &types.CustomAgent{Config: types.CustomAgentConfig{
						KBSelectionMode: "selected", KnowledgeBases: []string{"other-kb"},
					}}
				case "no KBs":
					agent = &types.CustomAgent{Config: types.CustomAgentConfig{KBSelectionMode: "none"}}
				case "explicit KB restriction":
					kbIDs = []string{"other-kb"}
				}
				resource, err := f.catalog.Resolve(ctx, f.refs[2])
				require.NoError(t, err)
				for _, reference := range []string{f.refs[2], resource.PhysicalPath} {
					content := "![private](" + reference + ")"
					out := cleanIMContent(ctx, content, s.newOutboundFileServiceResolver(f.tenant, agent, kbIDs))
					require.Equal(t, content, out, "an answer mentioning a resource cannot authorize access")
				}
				var grants int64
				require.NoError(t, f.db.Model(&types.ResourceAccessGrant{}).Count(&grants).Error)
				require.Zero(t, grants, "denied resources must not mint short-link capabilities")
			})
		}
	}
}

func TestIMResourceAccessRechecksSharingWithCachedStorage(t *testing.T) {
	f := newIMResourceStorageFixture(t)
	s, _, _, _ := newFullOutputHarness("")
	f.wire(s)
	agent := &types.CustomAgent{Config: types.CustomAgentConfig{
		KBSelectionMode: "selected", KnowledgeBases: []string{"shared-kb"},
	}}
	resolver := s.newOutboundFileServiceResolver(f.tenant, agent, nil)
	content := "![shared](" + f.refs[2] + ")"
	first := cleanIMContent(f.context(), content, resolver)
	require.Contains(t, first, "x-oss-signature=")
	require.NoError(t, f.db.Where("id = ?", "share").Delete(&types.KnowledgeBaseShare{}).Error)
	require.Equal(t, content, cleanIMContent(f.context(), content, resolver),
		"a cached storage client must not bypass a revoked share on the next stream update")
}

func TestIMChannelResourceRequiresLiveKBBindingOrCurrentArtifact(t *testing.T) {
	f := newIMResourceStorageFixture(t)
	s, _, _, _ := newFullOutputHarness("")
	f.wire(s)
	require.NoError(t, f.db.Create(&types.KnowledgeBase{ID: "own-kb", TenantID: 7}).Error)
	require.NoError(t, f.db.Create(&types.Knowledge{ID: "own-doc", KnowledgeBaseID: "own-kb", TenantID: 7}).Error)
	require.NoError(t, f.catalog.Bind(context.Background(), f.refs[0],
		types.ResourceOwnerKnowledge, "own-doc", types.ResourceRelationExtractedImage))
	require.NoError(t, f.catalog.Bind(context.Background(), f.refs[1],
		types.ResourceOwnerMessage, "this-answer", types.ResourceRelationArtifact))
	scope := types.IMChannelScope{
		TenantID: 7, AgentID: "agent", KnowledgeBases: map[string]uint64{"own-kb": 7, "shared-kb": 9},
		Verify: func(context.Context, uint64, types.ResourceType, string, types.ResourceAction) error { return nil },
	}
	check := func(scope types.IMChannelScope, ref, messageID string) error {
		ctx := types.WithIMChannelScope(f.context(), scope)
		ctx = context.WithValue(ctx, inflightOwnerKey{}, &inflightEntry{assistantMessageID: messageID})
		return s.authorizeOutboundResource(ctx, f.tenant, nil, nil, ref)
	}
	require.NoError(t, check(scope, f.refs[0], "this-answer"))
	require.NoError(t, check(scope, f.refs[1], "this-answer"))
	require.NoError(t, check(scope, f.refs[2], "this-answer"))
	require.Error(t, check(scope, f.refs[1], "other-answer"))
	delete(scope.KnowledgeBases, "own-kb")
	require.Error(t, check(scope, f.refs[0], "this-answer"))
	scope.Verify = func(context.Context, uint64, types.ResourceType, string, types.ResourceAction) error {
		return errors.New("revoked")
	}
	require.Error(t, check(scope, f.refs[1], "this-answer"))
	require.Error(t, check(scope, f.refs[2], "this-answer"))
}
