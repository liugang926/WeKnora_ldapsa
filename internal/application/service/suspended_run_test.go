package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/agent"
	"github.com/Tencent/WeKnora/internal/agent/hitl"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func newSuspendedRunFixture(t *testing.T) (*SuspendedRunService, *hitl.Broker, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, db.AutoMigrate(&types.Session{}, &types.Message{}, &types.MessageArtifactRecord{}, &types.AgentPrompt{}, &types.AgentRunSuspension{}))
	broker := hitl.NewBroker(repository.NewAgentPromptRepository(db), nil, hitl.WithSuspensionStore(repository.NewAgentRunSuspensionRepository(db)))
	runs := NewSuspendedRunService(broker, repository.NewMessageRepository(db))
	runs.db = db
	runs.durableBroker = true
	runs.authorize = func(ctx context.Context, _ *types.Session, _ *types.AgentPrompt, _ *types.Message, _ *agent.RunSnapshot) (context.Context, error) {
		return ctx, nil
	}
	return runs, broker, db
}

// seedSuspendedRun stores a run of sessionID suspended on promptID and
// returns its assistant message ID.
func seedSuspendedRun(
	t *testing.T, broker *hitl.Broker, db *gorm.DB, sessionID, promptID string, expires *time.Time,
) string {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, db.Session(&gorm.Session{SkipHooks: true}).FirstOrCreate(&types.Session{ID: sessionID, TenantID: 1, UserID: "u"}, "id = ?", sessionID).Error)
	snap := agent.RunSnapshot{
		WeKnoraACL: true, SourceLineage: sourceIndependentPromptLineage(),
		Version:        agent.RunSnapshotVersion,
		Query:          "q",
		CompletedSteps: []types.AgentStep{{Iteration: 0, Thought: "earlier round"}},
		PendingRound: agent.PendingRound{
			Step: types.AgentStep{Iteration: 1, Thought: "checking", ToolCalls: []types.ToolCall{
				{ID: "call-a", Name: "first", Result: &types.ToolResult{Success: true, Output: "ok"}},
			}},
			Calls: []agent.PendingCall{
				{ID: "call-a", Name: "first", Arguments: `{}`},
				{ID: "call-b", Name: "danger", Arguments: `{"x":1}`},
			},
			BlockedIndex: 1,
		},
	}
	data, err := json.Marshal(snap)
	require.NoError(t, err)
	msg := &types.Message{
		SessionID: sessionID, Role: "assistant",
		RunState: types.MessageRunStateAwaitingInput, CreatedAt: time.Now(),
	}
	require.NoError(t, db.Create(msg).Error)
	messageID := msg.ID
	require.NoError(t, broker.Store().Create(ctx, &types.AgentPrompt{
		ID: promptID, UserID: automationGrantUser("u"), TenantID: 1, SessionID: sessionID, AssistantMessageID: messageID,
		Kind: types.AgentPromptKindToolApproval, ToolCallID: "call-b",
		Status: types.AgentPromptSuspended, Suspendable: true, ExpiresAt: expires,
	}))
	require.NoError(t, broker.Suspensions().Save(ctx, &types.AgentRunSuspension{
		AssistantMessageID: messageID, TenantID: 1, SessionID: sessionID, PromptID: promptID,
		SnapshotVersion: agent.RunSnapshotVersion, Snapshot: types.JSON(data),
	}))
	return messageID
}

func TestSuspendedRunSupersedeCompletesTheMessage(t *testing.T) {
	ctx := context.Background()
	runs, broker, db := newSuspendedRunFixture(t)
	m1 := seedSuspendedRun(t, broker, db, "s1", "p1", nil)

	n, err := runs.SupersedeSession(ctx, "s1")
	require.NoError(t, err)
	require.Equal(t, 1, n)

	var msg types.Message
	require.NoError(t, db.First(&msg, "id = ?", m1).Error)
	require.True(t, msg.IsCompleted)
	require.Empty(t, msg.RunState, "run_state is cleared, not left stale")
	require.Len(t, msg.AgentSteps, 2)
	calls := msg.AgentSteps[1].ToolCalls
	require.Len(t, calls, 2, "every call the model issued keeps a result")
	require.True(t, calls[0].Result.Success)
	require.False(t, calls[1].Result.Success)
	require.Equal(t, hitl.ReasonPromptSupersede, calls[1].Result.Error)

	saved, err := broker.Suspensions().Get(ctx, m1)
	require.NoError(t, err)
	require.Nil(t, saved)
}

func TestSuspendedRunReapExpiresAndCloses(t *testing.T) {
	ctx := context.Background()
	runs, broker, db := newSuspendedRunFixture(t)
	m1 := seedSuspendedRun(t, broker, db, "s1", "p1", nil)
	past := time.Now().Add(-time.Minute)
	m2 := seedSuspendedRun(t, broker, db, "s2", "p-old", &past)

	n, err := runs.Reap(ctx, time.Now())
	require.NoError(t, err)
	require.Equal(t, 1, n)

	var msg types.Message
	require.NoError(t, db.First(&msg, "id = ?", m2).Error)
	require.True(t, msg.IsCompleted)
	require.Equal(t, hitl.ReasonPromptExpired, msg.AgentSteps[1].ToolCalls[1].Result.Error)

	var open types.Message
	require.NoError(t, db.First(&open, "id = ?", m1).Error)
	require.False(t, open.IsCompleted)
	require.Equal(t, types.MessageRunStateAwaitingInput, open.RunState)
}

func TestSuspendedRunSupersedeAlsoStopsDelegation(t *testing.T) {
	runs, broker, db := newSuspendedRunFixture(t)
	messageID := seedSuspendedRun(t, broker, db, "parent", "prompt", nil)
	var stopped *types.AgentPrompt
	runs.StopDelegation = func(_ context.Context, p *types.AgentPrompt) error { stopped = p; return nil }
	n, err := runs.SupersedeSession(context.Background(), "parent")
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NotNil(t, stopped)
	require.Equal(t, messageID, stopped.AssistantMessageID)
	require.Equal(t, "parent", stopped.SessionID)
}

func TestSuspendedRunDoesNotPublishMaterialAfterOwnerChange(t *testing.T) {
	ctx := context.Background()
	runs, broker, db := newSuspendedRunFixture(t)
	id := seedSuspendedRun(t, broker, db, "s", "p", nil)
	require.NoError(t, db.Model(&types.Session{}).Where("id = ?", "s").Update("user_id", "new-owner").Error)
	_, err := runs.SupersedeSession(ctx, "s")
	require.NoError(t, err)
	var msg types.Message
	require.NoError(t, db.First(&msg, "id = ?", id).Error)
	require.True(t, msg.IsCompleted)
	require.Empty(t, msg.AgentSteps)
	require.Empty(t, msg.KnowledgeReferences)
}
func TestSuspendedRunRejectsSubstitutedSnapshot(t *testing.T) {
	ctx := context.Background()
	runs, broker, db := newSuspendedRunFixture(t)
	id := seedSuspendedRun(t, broker, db, "s", "p", nil)
	saved, err := broker.Suspensions().Get(ctx, id)
	require.NoError(t, err)
	p, err := broker.Store().Get(ctx, "p")
	require.NoError(t, err)
	altered := *saved
	altered.Snapshot = []byte(`{"version":1,"query":"other"}`)
	require.Error(t, runs.MarkAwaitingInput(ctx, p, &altered))
	var msg types.Message
	require.NoError(t, db.First(&msg, "id = ?", id).Error)
	require.Empty(t, msg.AgentSteps)
}
