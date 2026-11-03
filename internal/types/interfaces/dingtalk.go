package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

// DingTalkRepository persists DingTalk SSO identity bindings and the
// append-only audit trail. The unique constraints on dingtalk_identities
// (natural key (corp_id, dingtalk_user_id) plus the partial unique index on
// user_id) are the authoritative concurrency guard: CreateBinding does
// check-then-insert inside one transaction and surfaces constraint
// violations as ErrDingTalkBindingConflict so callers can distinguish a
// lost race from a transient database failure.
type DingTalkRepository interface {
	// FindActiveByDingTalkID returns the active binding for the stable
	// (corpID, dingTalkUserID) pair, or nil when none exists.
	FindActiveByDingTalkID(ctx context.Context, corpID, dingTalkUserID string) (*types.DingTalkIdentity, error)
	// FindActiveByUserID returns the active binding linked to the WeKnora
	// user, or nil when none exists.
	FindActiveByUserID(ctx context.Context, userID string) (*types.DingTalkIdentity, error)
	// CreateBinding inserts a new active binding in one transaction. A lost
	// race against the unique constraints returns
	// ErrDingTalkBindingConflict.
	CreateBinding(ctx context.Context, binding *types.DingTalkIdentity) error
	// UpdateSnapshot refreshes the job-number/name/unionID snapshot of an
	// existing binding without touching the linked user.
	UpdateSnapshot(ctx context.Context, id uint64, jobNumber, displayName, unionID string) error
	// MarkUnlinked clears the user link and flags the binding unlinked.
	MarkUnlinked(ctx context.Context, id uint64) error
	// TouchLastLogin records a successful login on the binding.
	TouchLastLogin(ctx context.Context, id uint64) error
	// InsertEvent appends an audit event row.
	InsertEvent(ctx context.Context, event *types.DingTalkAuthEvent) error
	// FindADIdentitiesBySAMAccountName looks up linked directory identities
	// by exact case-sensitive sam_account_name match. The job number string
	// is passed through verbatim (no trimming or normalization) so leading
	// zeros survive the comparison.
	FindADIdentitiesBySAMAccountName(ctx context.Context, samAccountName string) ([]*types.DirectoryIdentity, error)
	// GetSettings loads the single-row database-managed provider
	// configuration, or nil when no administrator has saved one.
	GetSettings(ctx context.Context) (*types.DingTalkAuthSettings, error)
	// SaveSettings upserts the single-row database-managed provider
	// configuration.
	SaveSettings(ctx context.Context, settings *types.DingTalkAuthSettings) error
}
