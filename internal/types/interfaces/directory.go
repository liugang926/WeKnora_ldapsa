package interfaces

import (
	"context"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
)

// DirectoryRepository is the transactional persistence boundary for directory
// configuration, stable object identities and complete synchronization
// snapshots. Implementations must never replace membership data from an
// incomplete snapshot.
type DirectoryRepository interface {
	Create(ctx context.Context, directory *types.Directory) error
	Update(ctx context.Context, directory *types.Directory) error
	Get(ctx context.Context, id string) (*types.Directory, error)
	List(ctx context.Context) ([]*types.Directory, error)
	Delete(ctx context.Context, id string) error

	GetIdentity(ctx context.Context, id string) (*types.DirectoryIdentity, error)
	GetIdentityByObjectGUID(ctx context.Context, directoryID, objectGUID string) (*types.DirectoryIdentity, error)
	GetLoginSnapshot(ctx context.Context, directoryID, objectGUID string) (*types.DirectoryLoginSnapshot, error)
	GetIdentityByUserID(ctx context.Context, userID string) ([]*types.DirectoryIdentity, error)
	LinkIdentity(ctx context.Context, identityID, userID string) error
	UnlinkIdentity(ctx context.Context, identityID string) error
	ListIdentities(ctx context.Context, directoryID string, offset, limit int) ([]*types.DirectoryIdentity, error)
	ListGroups(ctx context.Context, directoryID, query string, offset, limit int) ([]*types.DirectoryGroup, error)
	ListGroupEdges(ctx context.Context, directoryID string) ([]*types.DirectoryGroupEdge, error)
	ListGroupMemberships(ctx context.Context, groupID string) ([]*types.DirectoryGroupMembership, error)
	// Sync leases serialize network collection across application processes.
	// Leases expire automatically after ttl so a crashed worker cannot block
	// synchronization forever. Renew/Release succeed only for the same owner.
	TryAcquireSyncLease(ctx context.Context, directoryID, owner string, ttl time.Duration) (bool, error)
	RenewSyncLease(ctx context.Context, directoryID, owner string, ttl time.Duration) (bool, error)
	ReleaseSyncLease(ctx context.Context, directoryID, owner string) error

	// ApplySnapshot atomically replaces edges/effective memberships and updates
	// object state. It also bumps every affected tenant permission version.
	ApplySnapshot(ctx context.Context, snapshot *types.DirectorySnapshot) (*types.DirectorySnapshotResult, error)
	RecordSyncFailure(ctx context.Context, directoryID, code, message string, startedAt time.Time, expectedSnapshotVersion, expectedConfigVersion uint64, trigger types.DirectorySyncTrigger) (*types.DirectorySyncRun, error)
	ListSyncRuns(ctx context.Context, directoryID string, offset, limit int) ([]*types.DirectorySyncRun, error)
}

type DirectoryService interface {
	Create(ctx context.Context, directory *types.Directory) (*types.Directory, error)
	Update(ctx context.Context, directory *types.Directory) (*types.Directory, error)
	Get(ctx context.Context, id string) (*types.Directory, error)
	List(ctx context.Context) ([]*types.Directory, error)
	Delete(ctx context.Context, id string) error
	LinkIdentity(ctx context.Context, identityID, userID string) error
	UnlinkIdentity(ctx context.Context, identityID string) error
	// RunSync serializes the complete fetch + validation + atomic apply cycle
	// for one directory. The callback runs only after the directory lock is held.
	RunSync(ctx context.Context, directoryID string, fetch func(context.Context) (*types.DirectorySnapshot, error)) (*types.DirectorySnapshotResult, error)
	ApplySnapshot(ctx context.Context, snapshot *types.DirectorySnapshot) (*types.DirectorySnapshotResult, error)
	RecordSyncFailure(ctx context.Context, directoryID, code, message string, startedAt time.Time, expectedSnapshotVersion, expectedConfigVersion uint64, trigger types.DirectorySyncTrigger) (*types.DirectorySyncRun, error)
	ListSyncRuns(ctx context.Context, directoryID string, offset, limit int) ([]*types.DirectorySyncRun, error)
}
