package repository

import (
	"context"
	"time"

	"gorm.io/gorm"
)

// NextcloudBuildLeaseValidator is deliberately small so a worker can carry its
// exact generation and fencing token to every SQL repository write.
type NextcloudBuildLeaseValidator interface {
	ValidateBuildLeaseInTx(context.Context, *gorm.DB, NextcloudContentLease, NextcloudContentScope) error
}

type nextcloudBuildContextKey struct{}

type nextcloudBuildContext struct {
	validator NextcloudBuildLeaseValidator
	lease     NextcloudContentLease
	scope     NextcloudContentScope
	// Retain the original cancelable worker context. Detached finalization
	// contexts preserve values; they must not revive a canceled worker.
	admissionCtx context.Context
}

// CheckNextcloudBuildLease is a checkpoint before a non-SQL side effect. It
// narrows the retirement race but cannot fence an external provider commit;
// those providers need their own token-aware adapters before GC is enabled.
func CheckNextcloudBuildLease(ctx context.Context) error {
	guard, ok := ctx.Value(nextcloudBuildContextKey{}).(nextcloudBuildContext)
	if !ok {
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := guard.admissionCtx.Err(); err != nil {
		return err
	}
	renewer, ok := guard.validator.(interface {
		RenewLease(context.Context, string, time.Duration) (NextcloudContentLease, error)
	})
	if !ok {
		return ErrNextcloudContentLeaseInvalid
	}
	_, err := renewer.RenewLease(ctx, guard.lease.ID, 2*time.Minute)
	return err
}

// WithNextcloudBuildLease annotates the worker context after it acquires the
// durable lease. It does not extend the lease or authorize a source write.
func WithNextcloudBuildLease(ctx context.Context, validator NextcloudBuildLeaseValidator,
	lease NextcloudContentLease, scope NextcloudContentScope,
) context.Context {
	return context.WithValue(ctx, nextcloudBuildContextKey{}, nextcloudBuildContext{
		validator: validator, lease: lease, scope: scope, admissionCtx: ctx,
	})
}

// ValidateNextcloudBuildWrite must run before each SQL mutation, inside the
// same transaction. It serializes that write with source retirement/GC. An
// unrelated mutation cannot reuse this worker's token for another document.
// Ordinary writes without a build context retain their existing behavior;
// worker admission is enforced by the task dispatch wrapper.
func ValidateNextcloudBuildWrite(ctx context.Context, tx *gorm.DB,
	tenantID uint64, kbID, knowledgeID string,
) error {
	guard, ok := ctx.Value(nextcloudBuildContextKey{}).(nextcloudBuildContext)
	if !ok {
		return nil
	}
	if err := CheckNextcloudBuildIdentity(ctx, tenantID, kbID, knowledgeID); err != nil {
		return err
	}
	if guard.validator == nil {
		return ErrNextcloudContentLeaseInvalid
	}
	return guard.validator.ValidateBuildLeaseInTx(ctx, tx, guard.lease, guard.scope)
}

// CheckNextcloudBuildIdentity is a cheap batch preflight; the caller still
// must validate the token once in the transaction before the first SQL write.
func CheckNextcloudBuildIdentity(ctx context.Context, tenantID uint64, kbID, knowledgeID string) error {
	guard, ok := ctx.Value(nextcloudBuildContextKey{}).(nextcloudBuildContext)
	if !ok {
		return nil
	}
	if guard.validator == nil || guard.scope.TenantID != tenantID ||
		guard.scope.KnowledgeBaseID != kbID || guard.scope.KnowledgeID != knowledgeID {
		return ErrNextcloudContentLeaseInvalid
	}
	if err := guard.admissionCtx.Err(); err != nil {
		return err
	}
	return nil
}

// NextcloudBuildLeasePresent allows a direct processing path to reject source
// writes that bypassed task admission. The repository transaction still has
// the final authority at commit time.
func NextcloudBuildLeasePresent(ctx context.Context, tenantID uint64, kbID, knowledgeID string) bool {
	guard, ok := ctx.Value(nextcloudBuildContextKey{}).(nextcloudBuildContext)
	return ok && guard.validator != nil && guard.scope.TenantID == tenantID &&
		guard.scope.KnowledgeBaseID == kbID && guard.scope.KnowledgeID == knowledgeID
}
