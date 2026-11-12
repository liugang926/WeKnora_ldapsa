package datasource

import "context"

// FetchProgress carries live enumeration/processing counts out of a
// connector's fetch phase so long syncs can surface a progress percentage
// before the first item is imported (DingTalk wiki downloads routinely take
// over an hour for a few hundred files).
type FetchProgress struct {
	// Enumerated is the number of remote documents discovered so far.
	Enumerated int
	// Processed is the number of enumerated documents whose fetch finished
	// (downloaded, rendered from blocks, or failed).
	Processed int
}

type fetchProgressKey struct{}

// WithFetchProgress installs a progress callback for the connector to report
// through. The callback must be cheap and must not block: implementations are
// expected to throttle persistence themselves.
func WithFetchProgress(ctx context.Context, fn func(FetchProgress)) context.Context {
	if fn == nil {
		return ctx
	}
	return context.WithValue(ctx, fetchProgressKey{}, fn)
}

// ReportFetchProgress invokes the installed callback, if any.
func ReportFetchProgress(ctx context.Context, progress FetchProgress) {
	if fn, ok := ctx.Value(fetchProgressKey{}).(func(FetchProgress)); ok && fn != nil {
		fn(progress)
	}
}
