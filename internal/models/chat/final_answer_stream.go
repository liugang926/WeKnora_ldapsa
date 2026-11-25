package chat

import (
	"context"
	"time"

	"github.com/Tencent/WeKnora/internal/models/api"
	"github.com/Tencent/WeKnora/internal/types"
)

// StreamFinalAnswer delivers one answer completion with provider usage. Some
// protocols send a finish reason before the separate usage-only trailer. Text
// is forwarded immediately, and a provider that never closes after finishing
// cannot retain the conversation or its source permissions indefinitely.
func StreamFinalAnswer(
	ctx context.Context, model Chat, messages []Message, opts *ChatOptions,
) (<-chan types.StreamResponse, error) {
	callCtx, cancel := context.WithCancel(ctx)
	source, err := model.ChatStream(api.WithFinalAnswerUsage(callCtx), messages, opts)
	if err != nil || source == nil {
		cancel()
		return nil, err
	}
	out := make(chan types.StreamResponse)
	go func() {
		defer cancel()
		defer close(out)
		forwardFinalAnswer(callCtx, source, out, time.Second)
	}()
	return out, nil
}

func forwardFinalAnswer(
	ctx context.Context, source <-chan types.StreamResponse, out chan<- types.StreamResponse, grace time.Duration,
) {
	var usage *types.TokenUsage
	var pending *types.StreamResponse
	var timeout <-chan time.Time
	send := func(chunk types.StreamResponse) bool {
		select {
		case out <- chunk:
			return true
		case <-ctx.Done():
			return false
		}
	}
	finish := func() {
		if pending != nil {
			pending.Usage = usage
			send(*pending)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-timeout:
			finish()
			return
		case chunk, ok := <-source:
			if !ok {
				finish()
				return
			}
			if chunk.Usage != nil {
				usage = chunk.Usage // provider chunks are cumulative snapshots
			}
			if pending != nil {
				if chunk.ResponseType == types.ResponseTypeError {
					send(chunk)
					return
				}
				if chunk.ResponseType == types.ResponseTypeAnswer && !chunk.UsagePending && chunk.Done {
					if chunk.FinishReason == "" {
						chunk.FinishReason = pending.FinishReason
					}
					chunk.Content = ""
					chunk.Usage = usage
					send(chunk)
					return
				}
				continue
			}
			if chunk.ResponseType == types.ResponseTypeAnswer && chunk.Done {
				if chunk.UsagePending {
					terminal := chunk
					terminal.Content = ""
					terminal.UsagePending = false
					pending = &terminal
					if chunk.Content != "" {
						chunk.Done = false
						if !send(chunk) {
							return
						}
					}
					timeout = time.After(grace)
					continue
				}
				// Adapters that don't flag UsagePending may still queue an EOF
				// usage trailer. Consume an available terminal snapshot without
				// delaying a provider that has already finished.
				if usage == nil {
					select {
					case trailer, ok := <-source:
						if ok && trailer.ResponseType == types.ResponseTypeError {
							if chunk.Content != "" {
								chunk.Done = false
								if !send(chunk) {
									return
								}
							}
							send(trailer)
							return
						}
						if ok && trailer.ResponseType == types.ResponseTypeAnswer {
							usage = trailer.Usage
						}
					default:
					}
				}
				chunk.Usage = usage
				send(chunk)
				return
			}
			if !send(chunk) {
				return
			}
		}
	}
}
