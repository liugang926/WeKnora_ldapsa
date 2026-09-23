package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestRoundGuardStopsBeforeModelOrToolExecution(t *testing.T) {
	model := &mockChat{}
	engine := newTestEngine(t, model, withMaxIterations(3))
	revoked := errors.New("permission revoked")
	calls := 0
	engine.SetRoundGuard(func(context.Context) error {
		calls++
		return revoked
	})

	_, err := engine.executeLoop(
		context.Background(),
		&types.AgentState{},
		"test query",
		emptyMessages(),
		emptyTools(),
		"session-1",
		"message-1",
	)

	require.ErrorIs(t, err, revoked)
	require.Equal(t, 1, calls)
	require.Equal(t, 0, model.callCount)
}
