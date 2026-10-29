package service

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/Tencent/WeKnora/internal/agent"
	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/readlease"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
)

// This guard owns the actual retained producers, independently of the model's
// emitted citations. It is transferred to the shared HTTP owner before any
// model call and must not reenter that owner's boundary while being checked.
type agentPromptInputGuard struct {
	mu           sync.Mutex
	ctx          context.Context
	knowledge    *knowledgeService
	store        *repository.KnowledgeInputProvenanceStore
	checker      knowledgeOutputLineageChecker
	targets      types.SearchTargets
	physical     *readlease.NextcloudReadGuard
	bases        []*repository.KnowledgeBasePromptInputSnapshot
	documents    []*types.KnowledgeBuildInputSnapshot
	kbInfos      []*agent.KnowledgeBaseInfo
	selectedDocs []*agent.SelectedDocumentInfo
	origin       *agent.QuestionOriginInfo
	body         string
	builtinCheck func(context.Context) error
	closed       bool
	held         bool
}

func agentPromptProjectionBody(g *agentPromptInputGuard) (string, error) {
	var baseLineages, documentLineages []*types.SourceLineage
	for _, base := range g.kbInfos {
		if base != nil {
			baseLineages = append(baseLineages, base.SourceLineage)
		}
	}
	for _, document := range g.selectedDocs {
		if document != nil {
			documentLineages = append(documentLineages, document.SourceLineage)
		}
	}
	var originLineage, originDocumentLineage *types.SourceLineage
	if g.origin != nil {
		originLineage = g.origin.SourceLineage
		if g.origin.Document != nil {
			originDocumentLineage = g.origin.Document.SourceLineage
		}
	}
	body, err := json.Marshal(struct {
		Bases                                []*agent.KnowledgeBaseInfo
		Documents                            []*agent.SelectedDocumentInfo
		Origin                               *agent.QuestionOriginInfo
		BaseLineages, DocumentLineages       []*types.SourceLineage
		OriginLineage, OriginDocumentLineage *types.SourceLineage
	}{g.kbInfos, g.selectedDocs, g.origin, baseLineages, documentLineages, originLineage, originDocumentLineage})
	return string(body), err
}

// CheckHeld revalidates private producers without reentering the HTTP owner.
func (g *agentPromptInputGuard) CheckHeld() error {
	if g == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || g.ctx == nil || g.ctx.Err() != nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	checkBody := func() error {
		actual, err := agentPromptProjectionBody(g)
		if err != nil || actual != g.body {
			return access.ErrNextcloudPublicationDenied
		}
		return nil
	}
	if err := checkBody(); err != nil {
		return err
	}
	if g.builtinCheck != nil {
		if err := g.builtinCheck(g.ctx); err != nil {
			return err
		}
	}
	if g.physical == nil {
		return checkBody()
	}
	if g.store == nil || g.knowledge == nil || g.checker == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	if err := g.physical.Verify(); err != nil {
		return err
	}
	if err := g.knowledge.CheckNextcloudReadTargets(g.ctx, g.targets); err != nil {
		return err
	}
	scope := access.LineageReadScope{Targets: g.targets}
	var proofs []*types.KnowledgeBuildInputProof
	// Recheck local original bodies after every current remote authorization.
	// Never acquire a newer original admission to certify retained prompt text.
	for round := 0; round < 2; round++ {
		for _, base := range g.bases {
			metadata := base.Metadata()
			if err := g.checker.Check(g.ctx, metadata.SourceLineage, scope); err != nil {
				return err
			}
			if err := g.store.RecheckKnowledgeBasePromptInputs(g.ctx, base); err != nil {
				return err
			}
		}
		proofs = nil
		for _, document := range g.documents {
			if document == nil || document.Proof == nil {
				return types.ErrSourceLineageUnknown
			}
			if err := g.checker.Check(g.ctx, document.Proof.SourceLineage, scope); err != nil {
				return err
			}
			proofs = append(proofs, document.Proof)
		}
		if len(proofs) > 0 {
			if err := g.store.RecheckKnowledgeOutputInputs(g.ctx, proofs); err != nil {
				return err
			}
		}
		if err := g.physical.Verify(); err != nil {
			return err
		}
	}
	if err := g.knowledge.CheckNextcloudReadTargets(g.ctx, g.targets); err != nil {
		return err
	}
	// The last current-grant call may execute a remote callback. Recheck the
	// retained local producer versions after it, without another remote pass.
	for _, base := range g.bases {
		if err := g.store.RecheckKnowledgeBasePromptInputs(g.ctx, base); err != nil {
			return err
		}
	}
	if len(proofs) > 0 {
		if err := g.store.RecheckKnowledgeOutputInputs(g.ctx, proofs); err != nil {
			return err
		}
	}
	if err := g.physical.Verify(); err != nil {
		return err
	}
	return checkBody()
}

func (g *agentPromptInputGuard) Close() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	bases := append([]*repository.KnowledgeBasePromptInputSnapshot{}, g.bases...)
	physical := g.physical
	g.mu.Unlock()
	var failures []error
	for _, base := range bases {
		failures = append(failures, base.Close())
	}
	if physical != nil {
		failures = append(failures, physical.Close())
	}
	return errors.Join(failures...)
}

func holdAgentPromptInputs(ctx context.Context, g *agentPromptInputGuard) (bool, error) {
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	if !ok {
		return false, nil
	}
	if err := g.CheckHeld(); err != nil {
		return false, err
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.Closed || slot.Failed != nil {
		return false, access.ErrNextcloudPublicationUnavailable
	}
	slot.AgentPromptReads = append(slot.AgentPromptReads, g)
	g.held = true
	return true, nil
}

func (s *agentService) prepareAgentPromptInputs(
	ctx context.Context,
	config *types.AgentConfig,
) (*agentPromptInputGuard, error) {
	g := &agentPromptInputGuard{ctx: ragPhysicalReadLifetimeContext(ctx)}
	emptyLineage, err := types.NewCompleteSourceLineage()
	if err != nil {
		return nil, err
	}

	// WeKnora owns the read boundary for WeKnora knowledge. Nextcloud source
	// receipts describe lifecycle and provenance; they must not turn ordinary
	// space browsing, @mentioned documents or question_origin hints into a
	// Nextcloud permission check.
	kbIDs, kbTenantMap := knowledgeBaseScopesForPrompt(config)
	g.kbInfos, err = s.getKnowledgeBaseInfos(ctx, kbIDs, kbTenantMap)
	if err != nil {
		return nil, err
	}
	for _, info := range g.kbInfos {
		if info != nil {
			info.SourceLineage = emptyLineage
		}
	}

	knowledgeIDs := dedupStrings(append([]string{}, config.KnowledgeIDs...))
	g.selectedDocs, err = s.getSelectedDocumentInfos(ctx, knowledgeIDs)
	if err != nil {
		return nil, err
	}
	for _, doc := range g.selectedDocs {
		if doc == nil || !config.SearchTargets.ContainsKB(doc.KnowledgeBaseID) {
			return nil, access.ErrNextcloudPublicationDenied
		}
		doc.SourceLineage = emptyLineage
	}

	g.origin = s.resolveQuestionOriginInfo(ctx, config.QuestionOrigin, config.SearchTargets, g.kbInfos)
	if g.origin != nil {
		g.origin.SourceLineage = emptyLineage
		if g.origin.Document != nil {
			g.origin.Document.SourceLineage = emptyLineage
		}
	}
	g.body, err = agentPromptProjectionBody(g)
	if err != nil {
		return nil, err
	}
	if err := g.CheckHeld(); err != nil {
		return nil, err
	}
	return g, nil
}

type agentPromptInputEngine struct {
	*agent.AgentEngine
	inputs *agentPromptInputGuard
}

// SetSourceLineageVerifier preserves prompt producer checks around session authority.
func (e *agentPromptInputEngine) SetSourceLineageVerifier(check func(context.Context, *types.SourceLineage) error) {
	e.AgentEngine.SetSourceLineageVerifier(func(ctx context.Context, lineage *types.SourceLineage) error {
		if err := e.inputs.CheckHeld(); err != nil {
			return err
		}
		if check == nil {
			return access.ErrNextcloudPublicationUnavailable
		}
		if err := check(ctx, lineage); err != nil {
			return err
		}
		return e.inputs.CheckHeld()
	})
}

// Execute keeps retained prompt inputs alive through the last HTTP consumer.
func (e *agentPromptInputEngine) Execute(
	ctx context.Context,
	session, message, query string,
	history []chat.Message,
	images ...[]string,
) (state *types.AgentState, err error) {
	if !e.inputs.held {
		defer func() { err = errors.Join(err, e.inputs.Close()) }()
	}
	if err := e.inputs.CheckHeld(); err != nil {
		return nil, err
	}
	return e.AgentEngine.Execute(ctx, session, message, query, history, images...)
}

// builtinAgentPromptInput binds the exact server-resolved prompt to the
// current WeKnora caller, session owner, Agent identity and live generation.
// Nextcloud publication receipts are intentionally not part of this check:
// a system prompt is WeKnora-owned input, while explicit user materials keep
// their separate native-material path.
func (s *agentService) builtinAgentPromptInput(
	ctx context.Context,
	config *types.AgentConfig,
	sessionID, assistantID, template string,
) (*types.SourceLineage, func(context.Context) error, error) {
	if template == "" {
		return nil, nil, nil
	}
	messages, ok := s.messageService.(*messageService)
	if !ok || messages == nil || messages.materialStore == nil || messages.sessionRepo == nil ||
		s.db == nil || config == nil {
		return nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	control, generation, err := messages.materialStore.MessageLiveControl(ctx, sessionID, assistantID)
	if err != nil || control == nil || generation == "" || control.Role != "assistant" || control.IsCompleted ||
		control.AgentID == "" || control.AgentTenantID == 0 {
		return nil, nil, types.ErrSourceLineageUnknown
	}
	caller, callerOK := ctx.Value(types.CallerContextKey).(types.Caller)
	principal, principalOK := ctx.Value(types.PrincipalContextKey).(types.Principal)
	owner := types.SessionOwnerIDFromContext(ctx)
	if !callerOK || !principalOK || !principal.Valid() || owner == "" {
		return nil, nil, types.ErrSourceLineageUnknown
	}

	check := func(current context.Context) error {
		if current == nil || current.Err() != nil {
			return access.ErrNextcloudPublicationUnavailable
		}
		actualCaller, callerOK := current.Value(types.CallerContextKey).(types.Caller)
		actualPrincipal, principalOK := current.Value(types.PrincipalContextKey).(types.Principal)
		if !callerOK || !principalOK || actualCaller != caller || actualPrincipal != principal ||
			types.SessionOwnerIDFromContext(current) != owner {
			return access.ErrNextcloudPublicationDenied
		}
		tenant, tenantOK := types.TenantIDFromContext(current)
		if !tenantOK || tenant == 0 {
			return access.ErrNextcloudPublicationDenied
		}
		owned, err := messages.sessionRepo.Get(current, tenant, owner, sessionID)
		if err != nil || owned == nil || owned.ID != sessionID || owned.TenantID != tenant || owned.UserID != owner {
			return access.ErrNextcloudPublicationDenied
		}
		live, liveGeneration, err := messages.materialStore.MessageLiveControl(current, sessionID, assistantID)
		if err != nil || live == nil || liveGeneration != generation || live.Role != "assistant" ||
			live.RequestID != control.RequestID ||
			live.AgentID != control.AgentID ||
			live.AgentTenantID != control.AgentTenantID {
			return access.ErrNextcloudPublicationDenied
		}

		var actual *types.CustomAgent
		if types.IsBuiltinAgentID(control.AgentID) {
			factory := types.GetBuiltinAgentWithContext(current, control.AgentID, control.AgentTenantID)
			if factory == nil {
				return types.ErrSourceLineageUnknown
			}
			actual, err = repository.NewCustomAgentRepository(s.db).
				GetAgentByID(current, control.AgentID, control.AgentTenantID)
			if errors.Is(err, repository.ErrCustomAgentNotFound) {
				actual = factory
			} else if err != nil {
				return access.ErrNextcloudPublicationUnavailable
			}
		} else {
			actual, err = repository.NewCustomAgentRepository(s.db).
				GetAgentByID(current, control.AgentID, control.AgentTenantID)
			if err != nil {
				return access.ErrNextcloudPublicationUnavailable
			}
		}
		if actual == nil || actual.ID != control.AgentID || actual.TenantID != control.AgentTenantID {
			return access.ErrNextcloudPublicationDenied
		}
		actual.EnsureDefaults()
		resolved, _ := s.cfg.ResolveCustomAgentPrompts(actual)
		if resolved != template || config.ResolveSystemPrompt(config.WebSearchEnabled) != template {
			return types.ErrSourceLineageUnknown
		}
		return nil
	}
	if err := check(ctx); err != nil {
		return nil, nil, err
	}
	lineage, err := types.NewCompleteSourceLineage()
	if err != nil {
		return nil, nil, err
	}
	return lineage, check, nil
}
