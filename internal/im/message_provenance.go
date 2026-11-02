package im

import (
	"context"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/types"
	"sort"
	"sync"
)

// The channel grant and trusted producer event envelope are separate facts:
// channel access never certifies an answer's source lineage.
type imMessageProvenance struct {
	mu      sync.Mutex
	lineage *types.SourceLineage
}

func (p *imMessageProvenance) observe(evt event.Event) {
	lineage, ok := evt.SourceLineage.(*types.SourceLineage)
	if !ok {
		switch data := evt.Data.(type) {
		case event.AgentFinalAnswerData:
			if data.Content == "" {
				return
			}
		case event.AgentCompleteData:
			if data.FinalAnswer == "" {
				return
			}
		default:
			return
		}
		lineage = types.UnknownSourceLineage()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lineage == nil {
		p.lineage, _ = types.UnionSourceLineage(lineage)
	} else {
		p.lineage, _ = types.UnionSourceLineage(p.lineage, lineage)
	}
}
func (p *imMessageProvenance) apply(ctx context.Context, message *types.Message) error {
	if _, ok := types.IMChannelScopeFromContext(ctx); !ok {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.lineage.RequireComplete(); err != nil {
		return err
	}
	message.SourceLineage, _ = types.UnionSourceLineage(p.lineage)
	return nil
}
func imAssistantMessage(ctx context.Context, sessionID, requestID string, agent *types.CustomAgent) *types.Message {
	message := createIMAssistantMessagePayload(sessionID, requestID)
	if agent != nil {
		message.AgentID = agent.ID
		message.AgentTenantID = agent.TenantID
		message.ModelID = agent.Config.ModelID
		message.ExecutionContext.AgentKBSelectionMode = agent.Config.KBSelectionMode
		message.ExecutionContext.AgentKnowledgeBaseIDs = append([]string(nil), agent.Config.KnowledgeBases...)
		message.ExecutionContext.WebSearchEnabled = agent.Config.WebSearchEnabled
	}
	if scope, ok := types.IMChannelScopeFromContext(ctx); ok {
		for id := range scope.KnowledgeBases {
			message.ExecutionContext.KnowledgeBaseIDs = append(message.ExecutionContext.KnowledgeBaseIDs, id)
		}
		sort.Strings(message.ExecutionContext.KnowledgeBaseIDs)
	}
	return message
}

func (p *imMessageProvenance) check(ctx context.Context) error {
	if _, ok := types.IMChannelScopeFromContext(ctx); !ok {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lineage.RequireComplete()
}

func (s *Service) checkIMMessagePublication(ctx context.Context, p *imMessageProvenance, message *types.Message) error {
	scope, ok := types.IMChannelScopeFromContext(ctx)
	if !ok {
		return nil
	}
	if err := scope.Check(ctx, scope.TenantID, types.GroupResourceTypeAgent, scope.AgentID, types.ResourceActionUse); err != nil {
		return err
	}
	if err := p.apply(ctx, message); err != nil {
		return err
	}
	checker, ok := s.messageService.(interface {
		CheckLiveMessagePublication(context.Context, *types.Message) error
	})
	if !ok {
		return types.ErrSourceLineageUnknown
	}
	return checker.CheckLiveMessagePublication(ctx, message)
}
