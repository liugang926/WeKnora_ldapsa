package handler

import (
	"context"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

// Hold the selected KB through capture, predicate evaluation and actual HTTP
// output. The original-input/current-source guards still authorize all data;
// a lease prevents physical GC and never supplies a read permission.
func beginCollectionHTTPRead(c *gin.Context, ctx context.Context, store *repository.NextcloudContentLeaseStore, kbID string) (context.Context, func(), error) {
	if store == nil {
		return ctx, nil, errors.NewServiceUnavailableError("Collection content lease store unavailable")
	}
	tenant, ok := types.TenantIDFromContext(ctx)
	if !ok || tenant == 0 || kbID == "" {
		return ctx, nil, errors.NewForbiddenError("Current collection scope is required")
	}
	fence, err := acquireNextcloudHTTPKBReadLease(ctx, store, tenant, kbID, nextcloudHTTPReadLeaseHeartbeat)
	if err != nil {
		return ctx, nil, err
	}
	ctx = fence.Context(ctx)
	restore := guardNextcloudResponseWrites(c, fence, nil)
	return ctx, func() { restore(); fence.Close() }, nil
}
