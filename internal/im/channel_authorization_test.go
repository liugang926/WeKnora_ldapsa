package im

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type imScopeResolverStub struct{ interfaces.SessionService }

func (imScopeResolverStub) ResolveIMChannelKnowledgeBases(
	context.Context,
	*types.CustomAgent) (map[string]uint64,
	error,
) {
	return map[string]uint64{"kb": 42}, nil
}

func (imScopeResolverStub) CheckIMChannelKnowledgeBase(context.Context, uint64, string) error {
	return nil
}

func TestIMAuthorizationReloadsChannelAndAgent(t *testing.T) {
	db := newLifecycleTestDB(t)
	channel := &IMChannel{
		ID:          "channel",
		TenantID:    42,
		AgentID:     "agent",
		Platform:    "dingtalk",
		Enabled:     true,
		Mode:        "websocket",
		OutputMode:  "stream",
		SessionMode: "user",
	}
	require.NoError(t, db.Create(channel).Error)
	agent := &types.CustomAgent{
		ID:       "agent",
		TenantID: 42,
		Config: types.CustomAgentConfig{
			KBSelectionMode: "selected",
			KnowledgeBases: []string{
				"kb",
			},
		},
	}
	svc := &Service{db: db, agentService: &stubIMAgentService{agent: agent}, sessionService: imScopeResolverStub{}}
	msg := &IncomingMessage{Platform: PlatformDingtalk, UserID: "sender"}
	ctx := withIMIdentity(context.Background(), 42, "channel", msg)
	authorized, _, err := svc.authorizeChannelContext(ctx, channel, msg)
	require.NoError(t, err)
	scope, ok := types.IMChannelScopeFromContext(authorized)
	require.True(t, ok)
	check := func() error {
		return scope.Check(authorized, 42, types.GroupResourceTypeKnowledgeBase, "kb", types.ResourceActionRead)
	}
	require.NoError(t, check())
	require.NoError(t, db.Model(&IMChannel{}).Where("id = ?", channel.ID).Update("enabled", false).Error)
	require.Error(t, check())
	require.NoError(t, db.Model(&IMChannel{}).Where("id = ?", channel.ID).Update("enabled", true).Error)
	agent.Config.KnowledgeBases = nil
	require.Error(t, check())
	msg.Platform = PlatformFeishu
	_, _, err = svc.authorizeChannelContext(ctx, channel, msg)
	require.Error(t, err)
	require.NoError(t, db.Delete(channel).Error)
	require.Error(t, check())
}

func TestIMChannelSessionOwnerIsolation(t *testing.T) {
	ch := &IMChannel{ID: "a", TenantID: 42, Platform: "dingtalk", SessionMode: "user"}
	first := &IncomingMessage{UserID: "one", ChatID: "chat", ThreadID: "thread"}
	other := *first
	other.UserID = "two"
	require.NotEqual(t, imChannelSessionOwner(ch, first), imChannelSessionOwner(ch, &other))
	ch.SessionMode = "thread"
	require.Equal(t, imChannelSessionOwner(ch, first), imChannelSessionOwner(ch, &other))
	original := imChannelSessionOwner(ch, first)
	other.ChatID = "other-chat"
	require.NotEqual(t, original, imChannelSessionOwner(ch, &other))
	ch.ID = "b"
	require.NotEqual(t, original, imChannelSessionOwner(ch, first))
	ch.ID = "a"
	ch.TenantID = 43
	require.NotEqual(t, original, imChannelSessionOwner(ch, first))
	ch.TenantID = 42
	first.ThreadID = ""
	other = *first
	other.UserID = "two"
	require.NotEqual(t, imChannelSessionOwner(ch, first), imChannelSessionOwner(ch, &other))
}

type imSearchCapture struct {
	interfaces.SessionService
	ids []string
}

func (c *imSearchCapture) SearchKnowledge(
	_ context.Context,
	ids,
	_ []string,
	_ []types.TagScope,
	_ string) ([]*types.SearchResult,
	error,
) {
	c.ids = ids
	return nil, nil
}

func TestIMSearchUsesExactChannelScopeIncludingShares(t *testing.T) {
	capture := &imSearchCapture{}
	command := newSearchCommand(capture, nil)
	base := withIMIdentity(context.Background(),
		42,
		"channel",
		&IncomingMessage{
			Platform: PlatformDingtalk,
			UserID:   "sender",
		})
	for _, ids := range []map[string]uint64{{"own": 42, "shared": 43}, {}} {
		ctx := types.WithIMChannelScope(base,
			types.IMChannelScope{
				TenantID:       42,
				AgentID:        "agent",
				KnowledgeBases: ids,
				Verify: func(context.Context,
					uint64,
					types.ResourceType,
					string,
					types.ResourceAction,
				) error {
					return nil
				},
			})
		_, err := command.Execute(ctx, &CommandContext{}, []string{"synthetic"})
		require.NoError(t, err)
		require.Len(t, capture.ids, len(ids))
	}
}
