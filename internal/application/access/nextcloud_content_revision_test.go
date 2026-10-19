package access

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestNextcloudPublicationSameETagHigherContentRevisionClosesRead(t *testing.T) {
	guard, ctx, k := publicationFixture(t)
	metadata := k.GetMetadata()
	metadata["nextcloud_content_revision"] = "1"
	raw, err := json.Marshal(metadata)
	require.NoError(t, err)
	k.Metadata = raw
	original := guard.authorize
	revision := "1"
	guard.authorize = func(
		c context.Context,
		config *types.DataSourceConfig,
		instance,
		binding,
		directory,
		guid string,
		file int64,
	) (nextcloud.AuthorizationDecision, error) {
		decision, err := original(c, config, instance, binding, directory, guid, file)
		decision.SourceContentRevision = revision
		return decision, err
	}
	require.NoError(t, guard.CheckKnowledge(ctx, k))
	revision = "2"
	require.ErrorIs(
		t,
		guard.CheckKnowledge(ctx, k),
		ErrNextcloudPublicationDenied,
		"same raw ETag must not extend old byte-version access",
	)
}

type recoveryClosedSourceLookup struct {
	NextcloudDataSourceLookup
	closed bool
}

func (s *recoveryClosedSourceLookup) InstanceControlsClosed(context.Context, string) (bool, error) {
	return s.closed, nil
}

func TestNextcloudActualOwnerReadControlsClosedBeforeAndAfterSourceProof(t *testing.T) {
	guard, ctx, k := publicationFixture(t)
	lookup := &recoveryClosedSourceLookup{NextcloudDataSourceLookup: guard.dataSources, closed: true}
	guard.dataSources = lookup
	caller := ctx.Value(types.CallerContextKey).(types.Caller)
	caller.Role = types.TenantRoleOwner
	ctx = types.WithCaller(ctx, caller)
	require.ErrorIs(
		t,
		guard.CheckKnowledge(ctx, k),
		ErrNextcloudPublicationDenied,
		"a normal Owner cannot expose a new source candidate while final recovery controls are closed",
	)
	lookup.closed = false
	require.NoError(t, guard.CheckKnowledge(ctx, k))
	original := guard.authorize
	guard.authorize = func(
		c context.Context,
		cfg *types.DataSourceConfig,
		i,
		b,
		d,
		g string,
		f int64,
	) (nextcloud.AuthorizationDecision, error) {
		decision, err := original(c, cfg, i, b, d, g, f)
		lookup.closed = true
		return decision, err
	}
	require.ErrorIs(
		t,
		guard.CheckKnowledge(ctx, k),
		ErrNextcloudPublicationDenied,
		"a close concurrent with live source authorization invalidates the allow",
	)
}
