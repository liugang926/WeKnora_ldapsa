package service

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestAgentFailureMarksOnlyTheOriginalPlaceholder(t *testing.T) {
	for _, tc := range []struct {
		name     string
		postgres bool
	}{{"SQLite", false}, {"PostgreSQL", true}} {
		t.Run(tc.name, func(t *testing.T) { testAgentFailureMark(t, tc.postgres) })
	}
}

func testAgentFailureMark(t *testing.T, postgres bool) {
	messages, _, store, db, ctx, sess, _ := messageBodyIntegrationFixture(t, postgres)
	m, err := messages.CreateMessage(ctx, &types.Message{
		SessionID: sess.ID, RequestID: "failed-agent-turn", Role: "assistant",
	})
	require.NoError(t, err)
	require.Error(t, messages.MarkAgentReplyFailed(ctx, sess.ID, m.ID, "another-turn"))
	require.Error(t, messages.MarkAgentReplyFailed(types.WithExecutionTenant(ctx, sess.TenantID+1),
		sess.ID, m.ID, m.RequestID))

	// This caller holds refused/unfinished output. None of it may be stored.
	m.Content = "unpublished content"
	ctx = WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(ctx)
	RefuseMessageMaterialOutputBoundary(ctx)
	require.NoError(t, messages.MarkAgentReplyFailed(ctx, sess.ID, m.ID, m.RequestID))
	control, origin, err := store.MessageLiveControl(ctx, sess.ID, m.ID)
	require.NoError(t, err)
	require.NotEmpty(t, origin)
	require.True(t, control.AgentGenerationFailed())
	replay, check, release, err := messages.BeginCompletedMessageMaterialReplay(ctx, sess.ID, m.ID)
	require.NoError(t, err)
	require.True(t, replay.AgentGenerationFailed())
	require.Nil(t, check)
	require.Nil(t, release)
	require.Empty(t, replay.Content)
	rows, err := messages.GetRecentMessagesBySession(ctx, sess.ID, 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.True(t, rows[0].AgentGenerationFailed())
	require.Empty(t, rows[0].Content)
	require.Empty(t, rows[0].AgentSteps)
	require.False(t, rows[0].IsCompleted)

	var priorCount, afterCount int64
	count := db.Table("message_material_admissions").Where("message_id=?", m.ID)
	require.NoError(t, count.Count(&priorCount).Error)
	require.NoError(t, messages.MarkAgentReplyFailed(ctx, sess.ID, m.ID, m.RequestID))
	require.NoError(t, count.Count(&afterCount).Error)
	require.Equal(t, priorCount, afterCount)

	// A late error must not change successful, cancelled/completed or suspended turns.
	for _, state := range []struct {
		completed bool
		runState  string
	}{
		{true, ""}, {false, types.MessageRunStateAwaitingInput},
	} {
		require.NoError(t, db.Model(&types.Message{}).Where("id=?", m.ID).
			Updates(map[string]any{"run_state": state.runState, "is_completed": state.completed}).Error)
		require.NoError(t, messages.MarkAgentReplyFailed(ctx, sess.ID, m.ID, m.RequestID))
		var stored types.Message
		require.NoError(t, db.Where("id=?", m.ID).Take(&stored).Error)
		require.Equal(t, state.completed, stored.IsCompleted)
		require.Equal(t, state.runState, stored.RunState)
	}
}
