package service

import (
	"context"

	"github.com/Tencent/WeKnora/internal/application/access"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

func NewSourceLineageGuard(knowledge interfaces.KnowledgeRepository, kbs interfaces.KnowledgeBaseService,
	shares interfaces.KBShareService, agents interfaces.AgentShareService, groups interfaces.GroupAccessService,
	publication *access.NextcloudPublicationGuard,
) *access.NextcloudSourceLineageGuard {
	snapshots, _ := knowledge.(access.NextcloudLineageSnapshotLookup)
	return access.NewNextcloudSourceLineageGuard(snapshots, kbs, shares, agents, groups, publication)
}

func ConfigureMessageSourceLineage(messages interfaces.MessageService, sessions interfaces.SessionService, forks *SessionForkService, manager *chatpipeline.EventManager, guard *access.NextcloudSourceLineageGuard) {
	if impl, ok := messages.(*messageService); ok {
		impl.sourceLineageGuard = guard
	}
	verifier, _ := messages.(interfaces.MessageSourceLineageVerifier)
	if impl, ok := sessions.(*sessionService); ok {
		impl.lineageVerifier = verifier
		impl.sourceLineageGuard = guard
	}
	forks.lineageVerifier = verifier
	chatpipeline.ConfigureSourceLineage(manager, guard)
}

func ConfigureAgentRAGChunkHydration(agents interfaces.AgentService, reader interfaces.RAGChunkSnapshotReader) {
	if impl, ok := agents.(*agentService); ok {
		impl.ragChunkSnapshotReader = reader
	}
}

type messageLineageHistoryAdmission struct {
	verifier interfaces.MessageSourceLineageVerifier
}

func (a messageLineageHistoryAdmission) CheckAgentHistoryReplay(context.Context) error {
	if a.verifier == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	return nil
}
func (a messageLineageHistoryAdmission) CheckMessageSourceLineage(ctx context.Context, message *types.Message) error {
	if a.verifier == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	return a.verifier.CheckMessagePublication(ctx, message)
}

// Saved execution selectors are reloaded solely for current grants. Source
// evidence remains the original saved envelope and is never reconstructed.
func (s *messageService) messageSourceLineageScope(ctx context.Context, message *types.Message) (access.LineageReadScope, error) {
	scope := access.LineageReadScope{AgentID: message.AgentID, AgentTenantID: message.AgentTenantID}
	seen := map[string]bool{}
	addKB := func(id string) error {
		if id == "" || seen[id] {
			return nil
		}
		seen[id] = true
		if s.kbService == nil {
			return access.ErrNextcloudPublicationUnavailable
		}
		kb, err := s.kbService.GetKnowledgeBaseByIDOnly(ctx, id)
		if err != nil || kb == nil {
			return access.ErrNextcloudPublicationUnavailable
		}
		if kb.ID != id || kb.TenantID == 0 {
			return access.ErrNextcloudPublicationDenied
		}
		scope.Targets = append(scope.Targets, &types.SearchTarget{Type: types.SearchTargetTypeKnowledgeBase, KnowledgeBaseID: id, TenantID: kb.TenantID})
		return nil
	}
	for _, id := range append(append([]string{}, message.ExecutionContext.KnowledgeBaseIDs...), message.ExecutionContext.AgentKnowledgeBaseIDs...) {
		if err := addKB(id); err != nil {
			return scope, err
		}
	}
	for _, tag := range message.ExecutionContext.TagScopes {
		if err := addKB(tag.KnowledgeBaseID); err != nil {
			return scope, err
		}
	}
	for _, id := range message.ExecutionContext.KnowledgeIDs {
		if s.knowService == nil {
			return scope, access.ErrNextcloudPublicationUnavailable
		}
		knowledge, err := s.knowService.GetKnowledgeByIDOnly(ctx, id)
		if err != nil || knowledge == nil {
			return scope, access.ErrNextcloudPublicationUnavailable
		}
		if knowledge.ID != id {
			return scope, access.ErrNextcloudPublicationDenied
		}
		if err := addKB(knowledge.KnowledgeBaseID); err != nil {
			return scope, err
		}
	}
	return scope, nil
}

func (s *messageService) checkTypedSourceLineage(ctx context.Context, message *types.Message) error {
	if message == nil || message.SourceLineage.RequireComplete() != nil {
		return access.ErrNextcloudPublicationDenied
	}
	if s.sourceLineageGuard == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	scope, err := s.messageSourceLineageScope(ctx, message)
	if err != nil {
		return err
	}
	lineage := message.SourceLineage
	if message.ContextCheckpoint != nil {
		lineage, err = types.UnionSourceLineage(lineage, message.ContextCheckpoint.SourceLineage)
		if err != nil || lineage.RequireComplete() != nil {
			return access.ErrNextcloudPublicationDenied
		}
	}
	if len(message.Artifacts) > 0 || len(message.Attachments) > 0 || len(message.Images) > 0 || message.RenderedContent != "" || message.SandboxCheckpoint != nil {
		return access.ErrNextcloudPublicationDenied // These generated fields lack producer contracts.
	}
	return s.sourceLineageGuard.Check(ctx, lineage, scope)
}

func (s *messageService) checkMessageSourceLineageBatch(ctx context.Context, messages []*types.Message, titleReads []access.SourceLineageRead) error {
	return s.checkMessageSourceLineageReadBatch(ctx, messages, titleReads, false)
}

// CheckMessagePublications is the mandatory multi-parent derivative boundary.
// Raw user inputs require their original complete envelope here; public HTTP
// history adapters separately allow only the author's raw Content.
func (s *messageService) CheckMessagePublications(ctx context.Context, messages []*types.Message) error {
	if len(messages) == 0 {
		return access.ErrNextcloudPublicationDenied
	}
	for _, message := range messages {
		if message == nil || message.SourceLineage.RequireComplete() != nil {
			return access.ErrNextcloudPublicationDenied
		}
	}
	return s.checkMessageSourceLineageReadBatch(ctx, messages, nil, true)
}

func (s *messageService) checkMessageSourceLineageReadBatch(ctx context.Context, messages []*types.Message, titleReads []access.SourceLineageRead, includeRawUser bool) error {
	reads := append([]access.SourceLineageRead{}, titleReads...)
	for _, message := range messages {
		if message == nil || message.Role == "user" && !includeRawUser {
			continue
		}
		scope, err := s.messageSourceLineageScope(ctx, message)
		if err != nil {
			return err
		}
		lineage := message.SourceLineage
		if message.ContextCheckpoint != nil {
			lineage, err = types.UnionSourceLineage(lineage, message.ContextCheckpoint.SourceLineage)
			if err != nil {
				return access.ErrNextcloudPublicationDenied
			}
		}
		reads = append(reads, access.SourceLineageRead{Lineage: lineage, Scope: scope})
	}
	if len(reads) == 0 {
		return nil
	}
	if s.sourceLineageGuard == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	return s.sourceLineageGuard.CheckBatch(ctx, reads)
}

func rawUserContentOnly(messages []*types.Message) []*types.Message {
	out := make([]*types.Message, 0, len(messages))
	for _, message := range messages {
		if message == nil || message.Role != "user" {
			out = append(out, message)
			continue
		}
		// Preserve exactly the user's authored Content. Captions, rendered
		// contexts, attachments, references and generated artifacts have no
		// covered user-field producer in this phase.
		copy := types.Message{ID: message.ID, SessionID: message.SessionID, RequestID: message.RequestID,
			Role: message.Role, Content: message.Content, CreatedAt: message.CreatedAt, UpdatedAt: message.UpdatedAt,
			IsCompleted: message.IsCompleted}
		out = append(out, &copy)
	}
	return out
}
