package types

import (
	"context"
	"errors"
	"strconv"
)

// NormalizeNextcloudContentRevision returns the canonical nonnegative revision, including the legacy zero.
func NormalizeNextcloudContentRevision(value string) (string, error) {
	if value == "" {
		return "0", nil
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n < 0 || strconv.FormatInt(n, 10) != value {
		return "", errors.New("invalid Nextcloud content revision")
	}
	return value, nil
}

// NextcloudLineageContentRevision encodes a content revision for immutable source lineage.
//
// Zero is omitted from immutable v1 lineage JSON to preserve legacy digests.
func NextcloudLineageContentRevision(value string) string {
	if value == "0" || value == "" {
		return ""
	}
	return value
}

type nextcloudContentRevisionKey struct{}

// WithNextcloudContentRevision attaches a server-resolved source revision to a request.
func WithNextcloudContentRevision(ctx context.Context, revision string) context.Context {
	return context.WithValue(ctx, nextcloudContentRevisionKey{}, revision)
}

// NextcloudContentRevisionFromContext returns the source revision attached to a request.
func NextcloudContentRevisionFromContext(ctx context.Context) string {
	revision, _ := ctx.Value(nextcloudContentRevisionKey{}).(string)
	return revision
}
