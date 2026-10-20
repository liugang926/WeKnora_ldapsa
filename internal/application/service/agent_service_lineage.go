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
	kbIDs, _ := knowledgeBaseScopesForPrompt(config)
	if len(kbIDs) == 0 && len(config.KnowledgeIDs) == 0 && config.QuestionOrigin == nil {
		var err error
		g.body, err = agentPromptProjectionBody(g)
		return g, err
	}
	knowledge, ok := s.knowledgeService.(*knowledgeService)
	if !ok || knowledge == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	store, ok := knowledge.knowledgeInputs.(*repository.KnowledgeInputProvenanceStore)
	messages, messageOK := s.messageService.(*messageService)
	if !ok || store == nil || !messageOK || messages == nil || messages.sourceLineageGuard == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	targets, err := cloneCollection(config.SearchTargets)
	if err != nil {
		return nil, err
	}
	g.knowledge, g.store, g.checker, g.targets = knowledge, store, messages.sourceLineageGuard, targets
	g.physical, err = knowledge.BeginNextcloudRead(g.ctx, targets)
	if err != nil {
		return nil, err
	}
	g.ctx = g.physical.ContextFor(g.ctx)
	fail := func(err error) (*agentPromptInputGuard, error) { _ = g.Close(); return nil, err }
	byKB := map[string]*agent.KnowledgeBaseInfo{}
	addBase := func(id string) (*agent.KnowledgeBaseInfo, error) {
		if existing := byKB[id]; existing != nil {
			return existing, nil
		}
		tenant := targets.GetTenantIDForKB(id)
		if id == "" || tenant == 0 || !targets.ContainsKB(id) {
			return nil, access.ErrNextcloudPublicationDenied
		}
		original, err := store.CaptureKnowledgeBasePromptInputs(g.ctx, tenant, id)
		if err != nil {
			return nil, err
		}
		g.bases = append(g.bases, original)
		metadata := original.Metadata()
		if metadata.IsTemporary {
			return nil, nil
		}
		info := &agent.KnowledgeBaseInfo{
			ID:                   metadata.KnowledgeBaseID,
			Name:                 metadata.Name,
			Type:                 metadata.Type,
			Capabilities:         metadata.Capabilities,
			SourceLineage:        metadata.SourceLineage,
			OmitDocumentMetadata: true,
		}
		byKB[id] = info
		return info, nil
	}
	for _, id := range kbIDs {
		info, err := addBase(id)
		if err != nil {
			return fail(err)
		}
		if info != nil {
			g.kbInfos = append(g.kbInfos, info)
		}
	}
	ids := append([]string{}, config.KnowledgeIDs...)
	if config.QuestionOrigin != nil && config.QuestionOrigin.KnowledgeID != "" {
		ids = append(ids, config.QuestionOrigin.KnowledgeID)
	}
	ids = dedupStrings(ids)
	byDoc := map[string]*agent.SelectedDocumentInfo{}
	if len(ids) > 0 {
		tenant, _ := types.TenantIDFromContext(g.ctx)
		candidates, err := knowledge.GetKnowledgeBatchWithSharedAccess(g.ctx, tenant, ids)
		if err != nil {
			return fail(err)
		}
		candidatesByID := map[string]*types.Knowledge{}
		for _, candidate := range candidates {
			if candidate != nil {
				candidatesByID[candidate.ID] = candidate
			}
		}
		for _, id := range ids {
			candidate := candidatesByID[id]
			if candidate == nil || candidate.ID != id || !targets.ContainsKB(candidate.KnowledgeBaseID) ||
				targets.GetTenantIDForKB(candidate.KnowledgeBaseID) != candidate.TenantID {
				return fail(access.ErrNextcloudPublicationDenied)
			}
			if err := g.physical.PinKnowledge(candidate); err != nil {
				return fail(err)
			}
			original, err := store.CaptureKnowledgeOutputInputs(
				g.ctx,
				types.KnowledgeBuildInputCaptureRequest{
					TenantID:        candidate.TenantID,
					KnowledgeBaseID: candidate.KnowledgeBaseID,
					KnowledgeID:     id,
				},
			)
			if err != nil {
				return fail(err)
			}
			if original == nil || original.Knowledge == nil || original.Proof == nil ||
				original.Proof.SourceLineage.RequireComplete() != nil {
				return fail(types.ErrSourceLineageUnknown)
			}
			actual := original.Knowledge
			if actual.ID != id || actual.TenantID != candidate.TenantID ||
				actual.KnowledgeBaseID != candidate.KnowledgeBaseID {
				return fail(repository.ErrKnowledgeInputChanged)
			}
			if err := g.physical.PinKnowledge(actual); err != nil {
				return fail(err)
			}
			g.documents = append(g.documents, original)
			byDoc[id] = &agent.SelectedDocumentInfo{
				KnowledgeID:     actual.ID,
				KnowledgeBaseID: actual.KnowledgeBaseID,
				Title:           actual.Title,
				FileName:        actual.FileName,
				FileType:        actual.FileType,
				SourceLineage:   original.Proof.SourceLineage,
			}
		}
	}
	for _, id := range config.KnowledgeIDs {
		if doc := byDoc[id]; doc != nil {
			g.selectedDocs = append(g.selectedDocs, doc)
		}
	}
	if source := config.QuestionOrigin; source != nil && source.KnowledgeBaseID != "" &&
		targets.ContainsKB(source.KnowledgeBaseID) {
		base, err := addBase(source.KnowledgeBaseID)
		if err != nil {
			return fail(err)
		}
		if base != nil {
			g.origin = &agent.QuestionOriginInfo{
				KnowledgeBaseID:   base.ID,
				KnowledgeBaseName: base.Name,
				SourceLineage:     base.SourceLineage,
			}
			if doc := byDoc[source.KnowledgeID]; doc != nil && doc.KnowledgeBaseID == base.ID {
				g.origin.Document = doc
			}
		}
	}
	g.body, err = agentPromptProjectionBody(g)
	if err != nil {
		return fail(err)
	}
	if err := g.CheckHeld(); err != nil {
		return fail(err)
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

// A current DB row is an authorization/value check, not original provenance.
// Only the exact image-owned builtin factory and YAML template bytes certify
// the rendered template. A custom override or retained copy remains unknown.
func (s *agentService) builtinAgentPromptInput(
	ctx context.Context,
	config *types.AgentConfig,
	sessionID, assistantID, template string,
) (*types.SourceLineage, func(context.Context) error, error) {
	if template == "" {
		return nil, nil, nil
	}
	messages, ok := s.messageService.(*messageService)
	if !ok || messages == nil || messages.materialStore == nil || messages.sessionRepo == nil || s.db == nil ||
		s.cfg == nil {
		return nil, nil, types.ErrSourceLineageUnknown
	}
	control, generation, err := messages.materialStore.MessageLiveControl(ctx, sessionID, assistantID)
	if err != nil || control == nil || generation == "" || control.Role != "assistant" || control.IsCompleted ||
		control.AgentID == "" ||
		control.AgentTenantID == 0 ||
		!types.IsBuiltinAgentID(control.AgentID) {
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
		actualCaller, ok := current.Value(types.CallerContextKey).(types.Caller)
		actualPrincipal, pOK := current.Value(types.PrincipalContextKey).(types.Principal)
		if !ok || !pOK || actualCaller != caller || actualPrincipal != principal ||
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
		factory := types.GetBuiltinAgentWithContext(current, control.AgentID, control.AgentTenantID)
		if factory == nil {
			return types.ErrSourceLineageUnknown
		}
		expected, _ := s.cfg.ResolveCustomAgentPrompts(factory)
		if expected == "" || expected != template {
			return types.ErrSourceLineageUnknown
		}
		actual, err := repository.NewCustomAgentRepository(s.db).
			GetAgentByID(current, control.AgentID, control.AgentTenantID)
		if errors.Is(err, repository.ErrCustomAgentNotFound) {
			actual = factory
		} else if err != nil {
			return access.ErrNextcloudPublicationUnavailable
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
	return lineage, check, err
}
