package session

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/stream"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

// These synthetic stores exercise the legacy queue algorithms only. Actual
// production repositories/services cannot implement this package-private port.
func (*steerMessageLookupStub) steerAlgorithmFixture()     {}
func (*steerPersistingMessageStub) steerAlgorithmFixture() {}
func (*steerFailingCreateStub) steerAlgorithmFixture()     {}

func TestSteerProductionMissingOriginalServiceRejectsCachedInput(t *testing.T) {
	ctx := context.Background()
	manager := stream.NewMemoryStreamManager()
	require.NoError(
		t,
		manager.AppendSteerEvents(
			ctx,
			"session",
			"assistant",
			[]interfaces.StreamEvent{steerEvent("unaccounted", "unaccounted cached bytes", nil, "web")},
		),
	)
	sink := newSteerSink(ctx, "session", "request", &types.Message{ID: "assistant"}, nil, manager)
	items, _, err := sink.PollSteer(ctx, "session", "assistant", 0)
	require.Error(t, err)
	require.Empty(t, items)
}
