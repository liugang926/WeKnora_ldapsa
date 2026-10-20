package session

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/stream"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

// generationStopPolls observes the actual setupSSEStream watcher. Its second
// read stays in flight until generation cancellation, like a blocked Redis read.
// The cleanup-only terminal frame ensures a failing regression cannot leak its
// old detached watcher for stopWatcherMaxDuration.
type generationStopPolls struct {
	interfaces.StreamManager
	polls    chan context.Context
	release  chan struct{}
	readDone chan struct{}
	calls    atomic.Int32
}

func (s *generationStopPolls) GetEvents(
	ctx context.Context, sessionID, messageID string, offset int,
) ([]interfaces.StreamEvent, int, error) {
	events, next, err := s.StreamManager.GetEvents(ctx, sessionID, messageID, offset)
	if err != nil {
		return nil, offset, err
	}
	call := s.calls.Add(1)
	s.polls <- ctx
	if call == 1 {
		return events, next, nil
	}
	select {
	case <-ctx.Done():
		if call == 2 {
			close(s.readDone)
		}
		return nil, next, ctx.Err()
	case <-s.release:
		return []interfaces.StreamEvent{{Type: types.ResponseTypeComplete, Done: true}}, next + 1, nil
	}
}

// Cancellation is a control operation even when no generated body has been
// admitted. Refuse persistence rather than inventing a body receipt for it.
type generationStopMessages struct {
	interfaces.MessageService
	updates   int
	updateCtx context.Context
}

func (s *generationStopMessages) UpdateMessage(ctx context.Context, _ *types.Message) error {
	s.updates++
	s.updateCtx = ctx
	return access.ErrNextcloudPublicationUnavailable
}

func awaitGenerationStopPoll(t *testing.T, polls <-chan context.Context) context.Context {
	t.Helper()
	select {
	case ctx := <-polls:
		return ctx
	case <-time.After(2 * time.Second):
		t.Fatal("setupSSEStream watcher did not poll")
		return nil
	}
}

func awaitGenerationStopContext(ctx context.Context, t *testing.T) {
	t.Helper()
	select {
	case <-ctx.Done():
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("generation stop did not cancel the initialized watcher")
	}
}

func TestSetupSSEStreamWatcherUsesGenerationLifecycle(t *testing.T) {
	for _, mode := range []string{"publication_refusal", "original_material_refusal"} {
		t.Run(mode, func(t *testing.T) {
			memory := stream.NewMemoryStreamManager()
			streams := &generationStopPolls{
				StreamManager: memory,
				polls:         make(chan context.Context, 8),
				release:       make(chan struct{}),
				readDone:      make(chan struct{}),
			}
			t.Cleanup(func() { close(streams.release) })
			messages := &generationStopMessages{}
			h := &Handler{streamManager: streams, messageService: messages}
			httpCtx, disconnect := context.WithCancel(t.Context())
			t.Cleanup(disconnect)
			c := newSteerWiringGinContext()
			c.Request = c.Request.WithContext(httpCtx)
			reqCtx := &qaRequestContext{
				c: c, ctx: httpCtx, sessionID: "session", requestID: "request",
				session: &types.Session{ID: "session", TenantID: 7},
				assistantMessage: &types.Message{
					ID: "assistant", SessionID: "session", RequestID: "request", Role: "assistant",
				},
			}
			initialized := h.setupSSEStream(reqCtx, false, qaModeNormal)
			require.NotNil(t, initialized)
			t.Cleanup(initialized.cancel)
			firstPoll := awaitGenerationStopPoll(t, streams.polls)
			require.NoError(t, firstPoll.Err())

			// The initialized generation and its watcher survive HTTP disconnect.
			// setupSSEStream has also returned: quick-answer startup must not stop it.
			disconnect()
			inFlight := awaitGenerationStopPoll(t, streams.polls)
			require.NoError(t, inFlight.Err())
			require.NoError(t, initialized.asyncCtx.Err())

			// Use the production refusal entry points, not a manually canceled
			// watcher context or a fabricated terminal event in the stream store.
			if mode == "publication_refusal" {
				stopLiveStreamForPublication(
					initialized.asyncCtx, c, initialized.eventBus,
					"session", "assistant", "request", access.ErrNextcloudPublicationDenied,
				)
			} else {
				stopLiveStreamForOriginalMaterial(
					initialized.asyncCtx, c, initialized.eventBus, "session", "assistant", "request",
				)
			}
			awaitGenerationStopContext(initialized.asyncCtx, t)
			awaitGenerationStopContext(inFlight, t)
			select {
			case <-streams.readDone:
			case <-time.After(2 * time.Second):
				t.Fatal("the in-flight watcher poll did not finish after generation stop")
			}
			// A ready ticker may race with cancellation after the in-flight
			// read. It must use the already-canceled context and then go quiet.
			select {
			case late := <-streams.polls:
				require.ErrorIs(t, late.Err(), context.Canceled)
			case <-time.After(650 * time.Millisecond):
			}
			select {
			case <-streams.polls:
				t.Fatal("the canceled generation continued watcher polling")
			case <-time.After(650 * time.Millisecond):
			}
			require.LessOrEqual(t, streams.calls.Load(), int32(3))
			require.Equal(t, 1, messages.updates)
			require.NoError(t, messages.updateCtx.Err(), "stop persistence retains its uncanceled cleanup context")
			tenant, ok := types.TenantIDFromContext(messages.updateCtx)
			require.True(t, ok)
			require.EqualValues(t, 7, tenant)
			frames, _, err := memory.GetEvents(t.Context(), "session", "assistant", 0)
			require.NoError(t, err)
			for _, frame := range frames {
				require.NotEqual(t, types.ResponseTypeComplete, frame.Type)
				require.NotEqual(t, types.ResponseType("stop"), frame.Type)
			}
		})
	}
}
