package chatpipeline

import "context"

type strictRetrievalKey struct{}

// WithStrictRetrieval makes model-service failures fatal for an evaluation run.
// Normal chat requests keep their best-effort keyword and rerank fallbacks.
func WithStrictRetrieval(ctx context.Context) context.Context {
	return context.WithValue(ctx, strictRetrievalKey{}, true)
}

func strictRetrieval(ctx context.Context) bool {
	strict, _ := ctx.Value(strictRetrievalKey{}).(bool)
	return strict
}
