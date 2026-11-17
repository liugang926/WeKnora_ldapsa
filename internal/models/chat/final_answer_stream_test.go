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

func TestFinalAnswerForwardsErrorAfterFinishReason(t *testing.T) {
	source := make(chan types.StreamResponse, 3)
	source <- types.StreamResponse{ResponseType: types.ResponseTypeAnswer, Done: true, UsagePending: true, FinishReason: "stop"}
	source <- types.StreamResponse{ResponseType: types.ResponseTypeThinking, Done: true}
	source <- types.StreamResponse{ResponseType: types.ResponseTypeError, Done: true, Content: "bad trailer"}
	close(source)

	out := make(chan types.StreamResponse, 3)
	forwardFinalAnswer(context.Background(), source, out, time.Second)
	close(out)
	result := <-out
	require.Equal(t, types.ResponseTypeError, result.ResponseType)
	require.Equal(t, "bad trailer", result.Content)
	_, more := <-out
	require.False(t, more)
}
