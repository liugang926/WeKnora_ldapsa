package session

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAgentTurnFailureDoesNotPersistCompletedMessage(t *testing.T) {
	require.False(t, shouldCompleteAgentTurn(true, nil))
	require.True(t, shouldCompleteAgentTurn(false, nil))
	require.True(t, shouldCompleteAgentTurn(true, context.Canceled),
		"a user-requested stop preserves the existing partial-turn completion behavior")
}
