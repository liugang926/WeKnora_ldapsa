// Package logprivacy carries a request-local restriction on content telemetry.
// It has no application or logger dependencies so source contexts can mark it.
package logprivacy

import "context"

type contentSuppressedKey struct{}

// SuppressContent prevents source text, model inputs and outputs from being
// recorded in content logs or telemetry. Descendant contexts retain the flag.
func SuppressContent(ctx context.Context) context.Context {
	return context.WithValue(ctx, contentSuppressedKey{}, true)
}

func ContentSuppressed(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	suppressed, _ := ctx.Value(contentSuppressedKey{}).(bool)
	return suppressed
}
