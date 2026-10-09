package access

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type lineageSnapshotStub struct {
	knowledge *types.Knowledge
	identity  types.SourceIdentity
	err       error
	batchErr  error
	calls     int
	onRead    func(int)
}

func (s *lineageSnapshotStub) RecheckNextcloudSourceLineage(ctx context.Context, lineage *types.SourceLineage) (
	bool,
	error,
) {
	if s.batchErr != nil {
		return false, s.batchErr
	}
	for _, saved := range lineage.Sources {
		_, current, err := s.ResolveNextcloudSourceLineage(
			ctx,
			saved.TenantID,
			saved.KnowledgeBaseID,
			saved.KnowledgeID,
		)
		if err != nil {
			return false, err
		}
		if current != saved {
			return false, nil
		}
	}
	return true, nil
}

func (
	s *lineageSnapshotStub,
) ResolveNextcloudSourceLineage(_ context.Context, tenant uint64, kb, id string) (
	*types.Knowledge,
	types.SourceIdentity,
	error,
) {
	s.calls++
	if s.onRead != nil {
		s.onRead(s.calls)
	}
	if s.err != nil {
		return nil, types.SourceIdentity{}, s.err
	}
	if tenant != s.identity.TenantID || kb != s.identity.KnowledgeBaseID || id != s.identity.KnowledgeID {
		return nil, types.SourceIdentity{}, errors.New("not found")
	}
	return s.knowledge, s.identity, nil
}

type lineageKBStub struct {
	kb  *types.KnowledgeBase
	err error
}

func (s *lineageKBStub) GetKnowledgeBaseByIDOnly(context.Context, string) (*types.KnowledgeBase, error) {
	return s.kb, s.err
}

type lineageSharesStub struct {
	allowed bool
	err     error
	calls   int
}

func (
	s *lineageSharesStub,
) CheckTenantKBPermission(context.Context, string, uint64, types.TenantRole) (types.OrgMemberRole, bool, error) {
	s.calls++
	return types.OrgRoleViewer, s.allowed, s.err
}

type lineageGroupsStub struct {
	allowed bool
	err     error
	callers []types.Caller
}

func (
	s *lineageGroupsStub,
) EffectivePermission(
	ctx context.Context,
	_ uint64,
	_ types.ResourceType,
	_ string,
	_ types.ResourceAction,
	_ time.Time,
) (
	types.EffectiveResourcePermission,
	error,
) {
	s.callers = append(s.callers, types.CallerFromContext(ctx))
	return types.EffectiveResourcePermission{Allowed: s.allowed}, s.err
}

type lineageAgentsStub struct {
	agent   *types.CustomAgent
	allowed bool
	err     error
}

func (
	s *lineageAgentsStub,
) GetSharedAgentForTenant(context.Context, uint64, types.TenantRole, string, ...uint64) (*types.CustomAgent, error) {
	if !s.allowed {
		return nil, s.err
	}
	return s.agent, s.err
}

func (
	s *lineageAgentsStub,
) TenantCanAccessKBViaSomeSharedAgent(context.Context, uint64, types.TenantRole, *types.KnowledgeBase) (
	bool,
	error,
) {
	return s.allowed, s.err
}

func lineagePolicyFixture(
	t *testing.T,
) (*NextcloudSourceLineageGuard, context.Context, *types.SourceLineage, *lineageSnapshotStub, *lineageGroupsStub) {
	t.Helper()
	publication, ctx, knowledge := publicationFixture(t)
	knowledge.ID = "document-1"
	knowledge.ParseStatus = types.ParseStatusCompleted
	knowledge.EnableStatus = "enabled"
	identity := types.SourceIdentity{
		Provider:        types.ConnectorTypeNextcloud,
		TenantID:        12,
		KnowledgeBaseID: "kb-1",
		DataSourceID:    "source-1",
		PairOperationID: "pair-1",
		InstanceID:      "instance-1",
		BindingID:       "binding-1",
		FileID:          "42",
		ExternalID:      "nextcloud:instance-1:42",
		KnowledgeID:     "document-1",
		Revision:        1,
		ETag:            "etag-42",
	}
	snapshots := &lineageSnapshotStub{knowledge: knowledge, identity: identity}
	groups := &lineageGroupsStub{allowed: true}
	guard := NewNextcloudSourceLineageGuard(snapshots, &lineageKBStub{kb: &types.KnowledgeBase{
		ID:                     "kb-1",
		TenantID:               12,
		EverHadNextcloudSource: true,
	}}, nil, nil, groups, publication)
	lineage, err := types.NewCompleteSourceLineage(identity)
	require.NoError(t, err)
	return guard, ctx, lineage, snapshots, groups
}

func TestNextcloudSourceLineageChecksOriginalIdentityAndVersion(t *testing.T) {
	guard, ctx, saved, snapshots, _ := lineagePolicyFixture(t)
	require.NoError(t, guard.Check(ctx, saved, LineageReadScope{}))
	for name, change := range map[string]func(*types.SourceIdentity){
		"same knowledge reindexed etag":  func(s *types.SourceIdentity) { s.ETag = "new-etag" },
		"same etag new build generation": func(s *types.SourceIdentity) { s.Revision++ },
		"pair recreated":                 func(s *types.SourceIdentity) { s.PairOperationID = "other-pair" },
		"binding moved":                  func(s *types.SourceIdentity) { s.BindingID = "other-binding" },
		"current candidate changed":      func(s *types.SourceIdentity) { s.KnowledgeID = "new-candidate" },
	} {
		t.Run(name, func(t *testing.T) {
			g, c, original, current, _ := lineagePolicyFixture(t)
			change(&current.identity)
			require.Error(t, g.Check(c, original, LineageReadScope{}))
		})
	}
	// Name/path/observation bookkeeping is not part of the saved content identity.
	snapshots.knowledge.Title = "renamed"
	snapshots.knowledge.FileName = "renamed.md"
	require.NoError(t, guard.Check(ctx, saved, LineageReadScope{}))
	resolved, err := guard.Resolve(ctx, 12, "kb-1", "document-1", LineageReadScope{})
	require.NoError(t, err)
	require.Equal(t, saved, resolved)
}

func TestNextcloudSourceLineageRechecksFreshGrantsAndLocalVersionAfterRemoteAuth(t *testing.T) {
	for _, changed := range []string{"KB directory grant", "org share", "shared agent", "local generation"} {
		t.Run(changed, func(t *testing.T) {
			guard, ctx, saved, snapshots, groups := lineagePolicyFixture(t)
			shares := &lineageSharesStub{allowed: true}
			guard.shares = shares
			scope := LineageReadScope{}
			if changed == "org share" || changed == "shared agent" {
				ctx = types.WithCaller(ctx, types.Caller{
					TenantID: 99,
					UserID:   "person-1",
					Role:     types.TenantRoleViewer,
				})
				ctx = types.WithExecutionTenant(ctx, 12)
				// An old exact upstream context grant must not bypass fresh checks.
				ctx = (&KBAccess{KnowledgeBase: &types.KnowledgeBase{
					ID:       "kb-1",
					TenantID: 12,
				}, Caller: types.CallerFromContext(
					ctx,
				), EffectiveTenantID: 12, Permission: types.OrgRoleViewer}).WithGrant(ctx)
			}
			agent := &types.CustomAgent{ID: "agent-1", TenantID: 12}
			agent.Config.KBSelectionMode = "selected"
			agent.Config.KnowledgeBases = []string{"kb-1"}
			agents := &lineageAgentsStub{allowed: true, agent: agent}
			if changed == "shared agent" {
				guard.agents = agents
				scope.AgentID = "agent-1"
				scope.AgentTenantID = 12
			}
			guard.publication.authorize = func(
				context.Context,
				*types.DataSourceConfig,
				string,
				string,
				string,
				string,
				int64,
			) (
				nextcloud.AuthorizationDecision,
				error,
			) {
				switch changed {
				case "KB directory grant":
					groups.allowed = false
				case "org share":
					shares.allowed = false
				case "shared agent":
					agents.allowed = false
				case "local generation":
					snapshots.identity.Revision++
				}
				return nextcloud.AuthorizationDecision{Allow: true, SourceETag: "etag-42"}, nil
			}
			require.ErrorIs(t, guard.Check(ctx, saved, scope), ErrNextcloudPublicationDenied)
		})
	}
}

func TestNextcloudSourceLineageSharedEffectiveTenantRequiresCurrentCallerGrant(t *testing.T) {
	guard, ctx, saved, _, groups := lineagePolicyFixture(t)
	ctx = types.WithCaller(ctx, types.Caller{TenantID: 99, UserID: "person-1", Role: types.TenantRoleViewer})
	ctx = types.WithExecutionTenant(ctx, 12)
	shares := &lineageSharesStub{allowed: true}
	guard.shares = shares
	require.NoError(t, guard.Check(ctx, saved, LineageReadScope{}))
	for _, caller := range groups.callers {
		require.Equal(t, uint64(99), caller.TenantID)
		require.Equal(t, "person-1", caller.UserID)
	}
	shares.allowed = false
	require.ErrorIs(t, guard.Check(ctx, saved, LineageReadScope{}), ErrNextcloudPublicationDenied)
	// Merely changing execution scope never creates a caller grant.
	require.Equal(t, uint64(99), types.CallerFromContext(ctx).TenantID)
}

func TestNextcloudSourceLineageUnknownUnsupportedAndUnavailableDeny(t *testing.T) {
	for _, name := range []string{
		"legacy NULL",
		"unknown",
		"overflow",
		"snapshot error",
		"KB error",
		"missing groups",
		"uncaptured caller",
		"machine",
		"web API key",
		"principal mismatch",
		"unknown principal",
		"source denied",
		"source error",
		"source etag changed",
	} {
		t.Run(name, func(t *testing.T) {
			guard, ctx, lineage, snapshots, _ := lineagePolicyFixture(t)
			switch name {
			case "legacy NULL":
				lineage = nil
			case "unknown":
				lineage = types.UnknownSourceLineage()
			case "overflow":
				lineage.Sources = make([]types.SourceIdentity, types.SourceLineageMaxSources+1)
			case "snapshot error":
				snapshots.err = errors.New("database offline")
			case "KB error":
				guard.kbs.(*lineageKBStub).err = errors.New("KB offline")
			case "missing groups":
				guard.groups = nil
			case "uncaptured caller":
				ctx = context.Background()
			case "machine":
				ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalMCPEndpoint, ID: "endpoint"})
			case "web API key":
				ctx = types.WithTenantAPIKeyScope(ctx, types.TenantAPIKeyScope{FullAccess: true})
			case "principal mismatch":
				ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "other"})
			case "unknown principal":
				ctx = types.WithPrincipal(ctx, types.Principal{Type: "unsupported", ID: "person-1"})
			case "source denied":
				guard.publication.authorize = func(
					context.Context,
					*types.DataSourceConfig,
					string,
					string,
					string,
					string,
					int64,
				) (
					nextcloud.AuthorizationDecision,
					error,
				) {
					return nextcloud.AuthorizationDecision{}, nil
				}
			case "source error":
				guard.publication.authorize = func(
					context.Context,
					*types.DataSourceConfig,
					string,
					string,
					string,
					string,
					int64,
				) (
					nextcloud.AuthorizationDecision,
					error,
				) {
					return nextcloud.AuthorizationDecision{}, errors.New("source offline")
				}
			case "source etag changed":
				guard.publication.authorize = func(
					context.Context,
					*types.DataSourceConfig,
					string,
					string,
					string,
					string,
					int64,
				) (
					nextcloud.AuthorizationDecision,
					error,
				) {
					return nextcloud.AuthorizationDecision{Allow: true, SourceETag: "new-etag"}, nil
				}
			}
			require.Error(t, guard.Check(ctx, lineage, LineageReadScope{}))
		})
	}
}

func TestNextcloudSourceLineageExplicitEmptyStillChecksOrdinaryKBAndAPIKeyScope(t *testing.T) {
	guard, ctx, _, _, _ := lineagePolicyFixture(t)
	guard.snapshots = nil
	guard.publication = nil
	guard.groups = nil
	guard.kbs.(*lineageKBStub).kb.EverHadNextcloudSource = false
	lineage, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	scope := LineageReadScope{Targets: types.SearchTargets{{
		Type:            types.SearchTargetTypeKnowledgeBase,
		TenantID:        12,
		KnowledgeBaseID: "kb-1",
	}}}
	require.NoError(t, guard.Check(ctx, lineage, scope))
	ctx = types.WithCaller(ctx, types.Caller{TenantID: 99, UserID: "person-1"})
	require.ErrorIs(t, guard.Check(ctx, lineage, scope), ErrNextcloudPublicationDenied)
	guard.shares = &lineageSharesStub{allowed: true}
	require.NoError(t, guard.Check(ctx, lineage, scope))
	apiCtx := types.WithCaller(ctx, types.Caller{TenantID: 12, Role: types.TenantRoleViewer})
	apiCtx = types.WithPrincipal(apiCtx, types.Principal{Type: types.PrincipalAPITenant, ID: "api-key"})
	apiCtx = types.WithTenantAPIKeyScope(apiCtx, types.TenantAPIKeyScope{KnowledgeBaseIDs: types.StringArray{
		"other-kb",
	}})
	require.ErrorIs(t, guard.Check(apiCtx, lineage, scope), ErrNextcloudPublicationDenied)
	apiCtx = types.WithTenantAPIKeyScope(apiCtx, types.TenantAPIKeyScope{KnowledgeBaseIDs: types.StringArray{"kb-1"}})
	require.NoError(t, guard.Check(apiCtx, lineage, scope), ("explicit ordinary lineage preserves valid machine KB sc" +
		"ope"))
	guard.kbs.(*lineageKBStub).kb.TenantID = 99 // Wrong persisted owner for requested scope.
	require.ErrorIs(t, guard.Check(ctx, lineage, scope), ErrNextcloudPublicationDenied)
}

func TestNextcloudSourceLineageFinalSnapshotFailureIsUnavailable(t *testing.T) {
	guard, ctx, saved, snapshots, _ := lineagePolicyFixture(t)
	snapshots.batchErr = errors.New("snapshot store offline")
	err := guard.Check(ctx, saved, LineageReadScope{})
	require.ErrorIs(t, err, ErrNextcloudPublicationUnavailable)
	require.NotErrorIs(t, err, ErrNextcloudPublicationDenied)
}
