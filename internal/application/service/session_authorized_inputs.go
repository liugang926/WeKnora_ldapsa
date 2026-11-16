package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"slices"
	"strings"

	"github.com/Tencent/WeKnora/internal/agent"
	"github.com/Tencent/WeKnora/internal/agent/collaboration"
	"github.com/Tencent/WeKnora/internal/types"
)

// AgentImageUploadReceipt binds the inline image bytes saved by the authenticated
// HTTP request to its owner and session. It never travels in JSON or snapshots.
type AgentImageUploadReceipt struct {
	sessionID, owner string
	tenantID         uint64
	digests          [][sha256.Size]byte
}
type agentImageUploadKey struct{}

func NewAgentImageUploadReceipt(ctx context.Context, session *types.Session, uploaded []string) *AgentImageUploadReceipt {
	owner := types.SessionOwnerIDFromContext(ctx)
	if session == nil || owner == "" || session.UserID != owner || session.TenantID == 0 || session.ID == "" || len(uploaded) == 0 {
		return nil
	}
	receipt := &AgentImageUploadReceipt{sessionID: session.ID, tenantID: session.TenantID, owner: owner}
	for _, value := range uploaded {
		if !strings.HasPrefix(value, "data:image/") {
			return nil
		}
		receipt.digests = append(receipt.digests, sha256.Sum256([]byte(value)))
	}
	return receipt
}

func WithAgentImageUploadReceipt(ctx context.Context, receipt *AgentImageUploadReceipt) context.Context {
	return context.WithValue(ctx, agentImageUploadKey{}, receipt)
}

func (p *AgentImageUploadReceipt) Matches(ctx context.Context, session *types.Session, urls []string) bool {
	if p == nil {
		return false
	}
	actual := NewAgentImageUploadReceipt(ctx, session, urls)
	return actual != nil && actual.sessionID == p.sessionID && actual.owner == p.owner && actual.tenantID == p.tenantID && slices.Equal(actual.digests, p.digests)
}

func hasAgentRawInputMaterials(req *types.QARequest) bool {
	return req != nil && (len(req.ImageURLs) > 0 || len(req.Attachments) > 0)
}

// loadResume reads the durable request and resolved answer from their server-backed stores.
func (s *sessionService) loadResume(ctx context.Context, req *types.QARequest) (*types.AgentResume, bool, error) {
	if req == nil || req.Session == nil {
		return nil, false, ErrResourceAccessDenied
	}
	if req.Resume == nil {
		return nil, false, nil
	}
	caller := types.CallerFromContext(ctx)
	principal, ok := types.PrincipalFromContext(ctx)
	owner := types.SessionOwnerIDFromContext(ctx)
	if !ok || owner == "" || caller.TenantID == 0 || caller.TenantID != req.Session.TenantID ||
		caller.UserID == "" || req.Session.UserID != owner ||
		(principal.Type == types.PrincipalWebUser && (principal.ID != caller.UserID || owner != principal.ID)) {
		return nil, false, ErrResourceAccessDenied
	}
	if s.sessionRepo == nil {
		return nil, false, types.ErrSourceLineageUnknown
	}
	session, err := s.sessionRepo.Get(ctx, req.Session.TenantID, owner, req.Session.ID)
	if err != nil {
		return nil, false, err
	}
	if session == nil || session.UserID != owner || session.ID != req.Session.ID || session.TenantID != caller.TenantID {
		return nil, false, ErrResourceAccessDenied
	}
	resume, err := s.storedAgentResume(ctx, session, principal, req)
	if err != nil {
		return nil, false, err
	}
	snap, err := agent.DecodeRunSnapshot(resume.Snapshot)
	if err != nil {
		return nil, false, err
	}
	var original types.ResumableAgentRequest
	if err := json.Unmarshal(snap.Request, &original); err != nil {
		return nil, false, err
	}
	if req.CustomAgent == nil || original.AgentID != req.CustomAgent.ID || original.AgentTenantID != req.CustomAgent.TenantID ||
		original.ActingVia != principal.ActingVia || original.UserMessageID != req.UserMessageID || (original.SharedAgentReadOnly && !req.SharedAgentReadOnly) ||
		original.AssistantAccess != req.AssistantAccess ||
		!slices.Equal(original.KnowledgeBaseIDs, req.KnowledgeBaseIDs) || !slices.Equal(original.KnowledgeIDs, req.KnowledgeIDs) ||
		!slices.EqualFunc(original.TagScopes, req.TagScopes, func(a, b types.TagScope) bool {
			return a.KnowledgeBaseID == b.KnowledgeBaseID && slices.Equal(a.TagIDs, b.TagIDs)
		}) {
		return nil, false, ErrResourceAccessDenied
	}
	acl := snap.WeKnoraACL && snap.SourceLineage.RequireComplete() == nil && len(snap.SourceLineage.Sources) == 0 && !hasAgentRawInputMaterials(req)
	hydrated, err := s.restoreResumeSteering(ctx, session, req.AssistantMessageID, snap)
	if err != nil {
		return nil, false, err
	}
	if hydrated {
		resume.Snapshot, err = json.Marshal(snap)
		return resume, false, err
	}
	return resume, acl, nil
}

func (s *sessionService) storedAgentResume(ctx context.Context, session *types.Session, principal types.Principal, req *types.QARequest) (*types.AgentResume, error) {
	if req.Resume.Prompt == nil {
		// Only the collaboration scheduler has a live parent lease and a durable wait checkpoint.
		b := collaboration.FromContext(ctx)
		if b == nil || b.Scope.SessionID != session.ID || b.Scope.TenantID != session.TenantID ||
			b.Scope.UserID != session.UserID || principal.Type != types.PrincipalWebUser || principal.ActingVia != "" {
			return nil, ErrResourceAccessDenied
		}
		group, err := readParentWait(ctx, b, req.Resume.Snapshot)
		if err != nil {
			return nil, err
		}
		if group.ParentMessageID != req.AssistantMessageID {
			return nil, ErrResourceAccessDenied
		}
		return &types.AgentResume{Snapshot: group.ParentWait.Snapshot}, nil
	}

	if s.promptBroker == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	saved, err := s.promptBroker.Suspensions().Get(ctx, req.AssistantMessageID)
	if err != nil {
		return nil, err
	}
	if saved == nil || saved.SessionID != session.ID || saved.TenantID != session.TenantID ||
		saved.AssistantMessageID != req.AssistantMessageID || saved.PromptID != req.Resume.Prompt.ID ||
		!bytes.Equal(saved.Snapshot, req.Resume.Snapshot) {
		return nil, types.ErrSourceLineageUnknown
	}
	prompt, err := s.promptBroker.Store().Get(ctx, saved.PromptID)
	if err != nil {
		return nil, err
	}
	if prompt == nil || prompt.ID != saved.PromptID || prompt.UserID != principal.StorageID() ||
		prompt.TenantID != saved.TenantID || prompt.SessionID != saved.SessionID ||
		prompt.AssistantMessageID != saved.AssistantMessageID || prompt.Status != types.AgentPromptAnswered {
		return nil, ErrResourceAccessDenied
	}
	return &types.AgentResume{Snapshot: saved.Snapshot, Prompt: prompt}, nil
}

// Older checkpoints kept the steering message IDs but omitted their text.
// Read only those original user fields under the ongoing turn's material guards.
func (s *sessionService) restoreResumeSteering(ctx context.Context, session *types.Session, assistant string, snap *agent.RunSnapshot) (bool, error) {
	missing := make(map[string]bool)
	for _, step := range append(slices.Clone(snap.CompletedSteps), snap.PendingRound.Step) {
		for _, id := range step.UserMessagesBefore {
			if _, ok := snap.UserMessages[id]; !ok {
				missing[id] = true
			}
		}
	}
	if len(missing) == 0 {
		return false, nil
	}
	native, ok := s.lineageVerifier.(*messageService)
	if !ok || native.materialStore == nil {
		return false, types.ErrSourceLineageUnknown
	}
	if err := CheckMessageMaterialOutputBoundary(ctx); err != nil {
		return false, err
	}
	requestID, err := native.materialStore.MessageRequestControl(ctx, session.ID, assistant)
	if err != nil {
		return false, err
	}
	ids := make([]string, 0, len(missing))
	for id := range missing {
		ids = append(ids, id)
	}
	rows, err := native.admittedMessages(ctx, types.MessageMaterialReadRequest{
		TenantID: session.TenantID, SessionID: session.ID, MessageIDs: ids, IncludeOriginalUserFields: true,
	})
	if err != nil {
		return false, err
	}
	if snap.UserMessages == nil {
		snap.UserMessages = make(map[string]string)
	}
	for _, row := range rows {
		if row == nil || !missing[row.ID] || row.SessionID != session.ID || row.RequestID != requestID || row.Role != "user" {
			return false, ErrResourceAccessDenied
		}
		snap.SourceLineage, err = types.UnionSourceLineage(snap.SourceLineage, row.SourceLineage)
		if err != nil {
			return false, err
		}
		snap.UserMessages[row.ID] = row.Content
		delete(missing, row.ID)
	}
	if len(missing) != 0 {
		return false, types.ErrSourceLineageUnknown
	}
	snap.WeKnoraACL = false
	return true, nil
}
