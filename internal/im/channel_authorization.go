package im

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/Tencent/WeKnora/internal/types"
)

type imKnowledgeScopeResolver interface {
	ResolveIMChannelKnowledgeBases(context.Context, *types.CustomAgent) (map[string]uint64, error)
	CheckIMChannelKnowledgeBase(context.Context, uint64, string) error
}

func (s *Service) authorizeChannelContext(
	ctx context.Context,
	channel *IMChannel,
	msg *IncomingMessage) (context.Context,
	*types.CustomAgent,
	error,
) {
	if channel == nil ||
		!channel.Enabled ||
		channel.TenantID == 0 ||
		channel.ID == "" ||
		channel.AgentID == "" ||
		msg == nil ||
		msg.UserID == "" ||
		string(msg.Platform) != channel.Platform {

		return ctx, nil, fmt.Errorf("invalid IM channel identity")
	}
	agent, err := s.agentService.GetAgentByID(ctx, channel.AgentID)
	if err != nil || agent == nil || agent.ID != channel.AgentID || agent.TenantID != channel.TenantID {
		return ctx, nil, fmt.Errorf("IM agent is unavailable")
	}
	resolver, ok := s.sessionService.(imKnowledgeScopeResolver)
	if !ok {
		return ctx, nil, fmt.Errorf("IM knowledge scope resolver is unavailable")
	}
	kbIDs, err := resolver.ResolveIMChannelKnowledgeBases(ctx, agent)
	if err != nil {
		return ctx, nil, err
	}
	config, _ := json.Marshal(agent.Config)
	scope := types.IMChannelScope{
		TenantID: channel.TenantID,
		AgentID:  channel.AgentID,
		OwnerID: imChannelSessionOwner(channel,
			msg),
		ChannelID:      channel.ID,
		KnowledgeBases: kbIDs,
	}
	scope.Verify = func(checkCtx context.Context,
		tenant uint64,
		kind types.ResourceType,
		id string,
		_ types.ResourceAction,
	) error {
		fresh, err := s.GetChannelByID(channel.ID)
		if err != nil || !sameChannelRuntimeConfig(channel, fresh) || !fresh.Enabled {
			return fmt.Errorf("IM channel authorization changed")
		}
		// Configuration is revalidated in queued and detached work as well.
		base := context.WithValue(checkCtx, types.IMChannelScopeContextKey, nil)
		base = types.WithExecutionTenant(base, channel.TenantID)
		current, err := s.agentService.GetAgentByID(base, channel.AgentID)
		if err != nil || current == nil || current.TenantID != channel.TenantID {
			return fmt.Errorf("IM agent is unavailable")
		}
		currentConfig, _ := json.Marshal(current.Config)
		if !bytes.Equal(config, currentConfig) {
			return fmt.Errorf("IM agent authorization changed")
		}
		if kind == types.GroupResourceTypeKnowledgeBase {
			return resolver.CheckIMChannelKnowledgeBase(base, tenant, id)
		}
		return nil
	}
	return types.WithIMChannelScope(ctx, scope), agent, nil
}

// Session ownership follows the existing user/thread mapping, while the IM
// principal remains sender-specific for OAuth and audit. Hash structured fields
// so platform identifiers cannot collide through delimiter injection.
func imChannelSessionOwner(channel *IMChannel, msg *IncomingMessage) string {
	subject, mode := msg.UserID, "user"
	if channel.SessionMode == string(SessionModeThread) && msg.ThreadID != "" {
		subject = msg.ThreadID
		mode = "thread"
	}
	raw, _ := json.Marshal([]any{channel.TenantID, channel.ID, channel.Platform, mode, msg.ChatID, subject})
	return fmt.Sprintf("im_session:%x", sha256.Sum256(raw))
}
