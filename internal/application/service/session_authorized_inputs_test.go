package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/agent"
	"github.com/Tencent/WeKnora/internal/agent/collaboration"
	"github.com/Tencent/WeKnora/internal/agent/hitl"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type resumeOwnerStore struct {
	interfaces.SessionRepository
	row *types.Session
}

func (r *resumeOwnerStore) Get(context.Context, uint64, string, string) (*types.Session, error) {
	return r.row, nil
}

func TestAgentResumeUsesOwnedStoredSnapshotAndResolvedPrompt(t *testing.T) {
	ctx := context.WithValue(t.Context(), types.UserIDContextKey, "alice")
	ctx = context.WithValue(ctx, types.TenantIDContextKey, uint64(4))
	broker := hitl.NewBroker(nil, nil)
	session := &types.Session{ID: "s", UserID: "alice", TenantID: 4}
	repo := &resumeOwnerStore{row: session}
	svc := &sessionService{sessionRepo: repo, promptBroker: broker}
	principal := types.Principal{Type: types.PrincipalWebUser, ID: session.UserID}
	broker.RegisterKind("question", func(_, _ json.RawMessage) error { return nil })
	prompt := &types.AgentPrompt{
		ID: "p", SessionID: "s", UserID: principal.StorageID(),
		TenantID: 4, AssistantMessageID: "answer",
		Kind: "question", Status: types.AgentPromptSuspended,
	}
	require.NoError(t, broker.Store().Create(ctx, prompt))
	resolved, err := broker.Resolve(ctx, hitl.ResolveRequest{
		SessionID: "s", TenantID: 4,
		UserID: principal.StorageID(), PromptID: prompt.ID,
		Response: json.RawMessage(`{"decision":"denied"}`),
	})
	require.NoError(t, err)
	require.True(t, resolved.Resume)
	prompt = resolved.Prompt
	original, err := json.Marshal(types.ResumableAgentRequest{
		AgentID: "agent", AgentTenantID: 4,
		KnowledgeBaseIDs: []string{"kb"},
	})
	require.NoError(t, err)
	lineage, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	snap := agent.RunSnapshot{
		Version: agent.RunSnapshotVersion, WeKnoraACL: true,
		SourceLineage: lineage, Request: original,
	}
	state := &types.AgentRunSuspension{AssistantMessageID: "answer", SessionID: "s", TenantID: 4, PromptID: "p"}
	candidate := *prompt
	candidate.Response = types.JSON(`{"decision":"allowed"}`)
	req := &types.QARequest{
		Session: session, AssistantMessageID: "answer",
		CustomAgent: &types.CustomAgent{ID: "agent", TenantID: 4}, KnowledgeBaseIDs: []string{"kb"},
		Resume: &types.AgentResume{Prompt: &candidate},
	}
	save := func() {
		state.Snapshot, err = json.Marshal(snap)
		require.NoError(t, err)
		require.NoError(t, broker.Suspensions().Save(ctx, state))
		req.Resume.Snapshot = append(types.JSON(nil), state.Snapshot...)
	}
	save()
	got, ordinary, err := svc.loadResume(ctx, req)
	require.NoError(t, err)
	require.True(t, ordinary)
	require.JSONEq(t, string(prompt.Response), string(got.Prompt.Response))
	for _, unauthorized := range []context.Context{
		context.WithValue(ctx, types.TenantIDContextKey, uint64(5)),
		context.WithValue(ctx, types.UserIDContextKey, "bob"),
		types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalIMUser, ID: "alice"}),
		types.WithCaller(ctx, types.Caller{TenantID: 4, UserID: "bob"}),
	} {
		_, _, err = svc.loadResume(unauthorized, req)
		require.ErrorIs(t, err, ErrResourceAccessDenied)
	}
	req.KnowledgeBaseIDs = nil
	_, _, err = svc.loadResume(ctx, req)
	require.Error(t, err)
	req.KnowledgeBaseIDs = []string{"kb"}
	req.Resume.Snapshot = append(req.Resume.Snapshot, ' ')
	_, _, err = svc.loadResume(ctx, req)
	require.Error(t, err)
	snap.WeKnoraACL = false
	save()
	_, ordinary, err = svc.loadResume(ctx, req)
	require.NoError(t, err)
	require.False(t, ordinary)
	snap.WeKnoraACL = true
	snap.SourceLineage = types.UnknownSourceLineage()
	save()
	_, ordinary, err = svc.loadResume(ctx, req)
	require.NoError(t, err)
	require.False(t, ordinary)
	var scheduled types.ResumableAgentRequest
	require.NoError(t, json.Unmarshal(snap.Request, &scheduled))
	scheduled.ActingVia = types.AutomationActingVia("schedule")
	snap.Request, err = json.Marshal(scheduled)
	require.NoError(t, err)
	save()
	principal.ActingVia = scheduled.ActingVia
	_, _, err = svc.loadResume(types.WithPrincipal(ctx, principal), req)
	require.NoError(t, err)
	_, _, err = svc.loadResume(ctx, req)
	require.ErrorIs(t, err, ErrResourceAccessDenied)
	snap.Request = original
	save()
	// The answer belongs to the caller, but their active agent grant has been revoked.
	access := &tagTargetGroupAccessService{}
	svc.groupAccess = access
	require.ErrorIs(t, svc.AgentQA(ctx, req, event.NewEventBus()), ErrResourceAccessDenied)
	require.Equal(t, []string{"agent"}, access.calls)
	repo.row = &types.Session{ID: "s", UserID: "bob", TenantID: 4}
	_, _, err = svc.loadResume(ctx, req)
	require.Error(t, err)
}

func TestAgentImageReceiptRequiresExactOwnerSessionAndBytes(t *testing.T) {
	ctx := context.WithValue(t.Context(), types.UserIDContextKey, "alice")
	s := &types.Session{ID: "s", TenantID: 7, UserID: "alice"}
	urls := []string{"data:image/png;base64,aGVsbG8="}
	proof := NewAgentImageUploadReceipt(ctx, s, urls)
	require.True(t, proof.Matches(ctx, s, urls))
	require.False(t, proof.Matches(ctx, s, []string{"data:image/png;base64,b3RoZXI="}))
	require.False(t, proof.Matches(context.WithValue(ctx, types.UserIDContextKey, "bob"), s, urls))
	s.ID = "other"
	require.False(t, proof.Matches(ctx, s, urls))
	require.Nil(t, NewAgentImageUploadReceipt(ctx, s, []string{"https://example.test/stored.png"}))
}

func TestAgentResumeRequiresCurrentParentLeaseAndStoredWait(t *testing.T) {
	coord, agents, _, ctx := collaborationService(t)
	scope := collaboration.Scope{TenantID: 7, UserID: "u", SessionID: "parent", ID: "answer"}
	b, err := coord.Runtime.AcquireParent(ctx, scope)
	require.NoError(t, err)
	session := &types.Session{ID: scope.SessionID, TenantID: scope.TenantID, UserID: scope.UserID}
	original, err := json.Marshal(types.ResumableAgentRequest{
		AgentID:       agents.agent.ID,
		AgentTenantID: agents.agent.TenantID,
	})
	require.NoError(t, err)
	snapshot, err := json.Marshal(agent.RunSnapshot{
		Version: agent.RunSnapshotVersion, CollaborationWait: true,
		BetweenRounds: true, Request: original,
	})
	require.NoError(t, err)
	require.NoError(t, b.SaveWait(ctx, snapshot, time.Now()))
	req := &types.QARequest{
		Session: session, AssistantMessageID: "answer",
		CustomAgent: agents.agent,
		Resume:      &types.AgentResume{Snapshot: snapshot},
	}
	svc := &sessionService{sessionRepo: &resumeOwnerStore{row: session}}
	_, _, err = svc.loadResume(ctx, req)
	require.Error(t, err)
	ctx = collaboration.WithBinding(ctx, b)
	resume, _, err := svc.loadResume(ctx, req)
	require.NoError(t, err)
	require.Equal(t, snapshot, resume.Snapshot)
	require.Nil(t, resume.Prompt)
	req.Resume.Snapshot = append(append([]byte(nil), snapshot...), ' ')
	_, _, err = svc.loadResume(ctx, req)
	require.Error(t, err)
	req.Resume.Snapshot = snapshot
	require.NoError(t, b.ClearWait(ctx))
	_, _, err = svc.loadResume(ctx, req)
	require.Error(t, err)
}

func TestAgentResumeSteeringReadsOriginalMessagesFromSameTurn(t *testing.T) {
	messages, _, _, ctx, session, answer := steerMaterialActualFixture(t, false)
	steer, err := messages.CreateMessage(ctx, &types.Message{
		SessionID: session.ID, RequestID: answer.RequestID,
		Role:    "user",
		Content: "follow the staging version", IsCompleted: true,
	})
	require.NoError(t, err)
	other, err := messages.CreateMessage(ctx, &types.Message{
		SessionID: session.ID, RequestID: "another-turn",
		Role:    "user",
		Content: "another question", IsCompleted: true,
	})
	require.NoError(t, err)
	ctx = WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(ctx)
	lineage, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	snap := &agent.RunSnapshot{
		SourceLineage: lineage,
		PendingRound:  agent.PendingRound{Step: types.AgentStep{UserMessagesBefore: []string{steer.ID}}},
	}
	svc := &sessionService{lineageVerifier: messages}
	hydrated, err := svc.restoreResumeSteering(ctx, session, answer.ID, snap)
	require.NoError(t, err)
	require.True(t, hydrated)
	require.Equal(t, steer.Content, snap.UserMessages[steer.ID])
	require.NoError(t, snap.SourceLineage.RequireComplete())
	require.NoError(t, CheckMessageMaterialOutputBoundary(ctx))

	snap.PendingRound.Step.UserMessagesBefore = []string{other.ID}
	_, err = svc.restoreResumeSteering(ctx, session, answer.ID, snap)
	require.ErrorIs(t, err, ErrResourceAccessDenied)
	require.NotContains(t, snap.UserMessages, other.ID)
}
