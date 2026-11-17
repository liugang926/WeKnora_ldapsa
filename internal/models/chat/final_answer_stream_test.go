package chat

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestFinalAnswerMissingUsageTrailer(t *testing.T) {
	source := make(chan types.StreamResponse, 1)
	source <- types.StreamResponse{
		ResponseType: types.ResponseTypeAnswer, Content: "done", Done: true, UsagePending: true, FinishReason: "length",
	}
	out := make(chan types.StreamResponse, 3)
	forwardFinalAnswer(context.Background(), source, out, time.Millisecond)
	close(out)
	partial := <-out
	require.Equal(t, "done", partial.Content)
	require.False(t, partial.Done)
	done := <-out
	require.True(t, done.Done)
	require.Empty(t, done.Content)
	require.Equal(t, "length", done.FinishReason)
	require.Nil(t, done.Usage)
	_, more := <-out
	require.False(t, more)
}
