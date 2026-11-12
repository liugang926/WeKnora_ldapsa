package access

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
)

var (
	ErrNextcloudPublicationDenied = apperrors.NewProtocolError(
		errors.New("nextcloud publication access denied"), "Nextcloud publication access denied",
	)
	ErrNextcloudPublicationUnavailable = apperrors.NewProtocolError(errors.New(
		"nextcloud publication authorization unavailable",
	), "Nextcloud publication authorization unavailable")
)

// NextcloudDirectoryLookup and NextcloudDataSourceLookup are deliberately
// narrow so the guard can use the existing repositories without owning their
// broader management APIs.
type NextcloudDirectoryLookup interface {
	GetUserLoginSnapshot(context.Context, string) ([]*types.DirectoryIdentity, *types.DirectoryLoginSnapshot, error)
}

type NextcloudDataSourceLookup interface {
	FindByID(context.Context, string) (*types.DataSource, error)
}

// NextcloudSourcePathResolver resolves a source binding's current path metadata.
type NextcloudSourcePathResolver interface {
	ResolveCurrentSourcePath(context.Context, uint64, string, string, string) (string, error)
}

type nextcloudSourceAuthorizer func(
	context.Context, *types.DataSourceConfig, string, string, string, string, int64,
) (nextcloud.AuthorizationDecision, error)

// NextcloudPublicationGuard retains the API of the retired publication read guard.
// Source lifecycle helpers remain available to their existing callers.
type NextcloudPublicationGuard struct {
	directories NextcloudDirectoryLookup
	dataSources NextcloudDataSourceLookup
	authorize   nextcloudSourceAuthorizer
	paths       NextcloudSourcePathResolver
	checkPath   func(context.Context, *types.DataSourceConfig, string, string, int64, string, string) error
}

func NewNextcloudPublicationGuard(
	directories NextcloudDirectoryLookup, dataSources NextcloudDataSourceLookup,
) *NextcloudPublicationGuard {
	return &NextcloudPublicationGuard{
		directories: directories,
		dataSources: dataSources,
		authorize:   nextcloud.AuthorizeCurrentFile,
	}
}

// ConfigureSourcePathResolver configures current path resolution for publication checks.
func (g *NextcloudPublicationGuard) ConfigureSourcePathResolver(paths NextcloudSourcePathResolver) {
	g.paths = paths
	g.checkPath = nextcloud.CheckCurrentPublication
}

// CheckKnowledge is a compatibility no-op after the Nextcloud read guard retirement.
func (g *NextcloudPublicationGuard) CheckKnowledge(ctx context.Context, knowledge *types.Knowledge) error {
	return nil // Retired with the Nextcloud decoupling on 2026-11-09.
}

// currentDirectoryIdentity validates only captured principal and local AD
// identity/snapshot state. It makes no Nextcloud request and returns a copy so
// a concurrent lookup update cannot change the checked binding in memory.
func (g *NextcloudPublicationGuard) currentDirectoryIdentity(ctx context.Context) (*types.DirectoryIdentity, error) {
	if ctx == nil {
		return nil, ErrNextcloudPublicationDenied
	}
	// A legacy worker context can carry UserIDContextKey without an authenticated
	// request principal. This guard requires the caller captured by auth middleware.
	caller, captured := ctx.Value(types.CallerContextKey).(types.Caller)
	if !captured || caller.TenantID == 0 || strings.TrimSpace(caller.UserID) == "" ||
		types.IsSyntheticUserID(caller.UserID) {
		return nil, apperrors.NewProtocolError(fmt.Errorf(
			"%w: authenticated human user required",
			ErrNextcloudPublicationDenied,
		), fmt.Sprintf("%s: authenticated human user required", apperrors.PublicMessage(
			ErrNextcloudPublicationDenied,
		)))
	}
	// A machine identity can have a user-shaped ID (embed-..., mcp-..., or an
	// external API user). The principal, captured by the authentication
	// middleware, must explicitly identify the same interactive web user.
	// Do not use PrincipalFromContext here: its legacy UserID fallback would
	// turn an uncaptured machine identity into a web principal.
	principal, captured := ctx.Value(types.PrincipalContextKey).(types.Principal)
	if !captured || principal.Type != types.PrincipalWebUser || principal.ID != caller.UserID {
		return nil, apperrors.NewProtocolError(fmt.Errorf(
			"%w: interactive web user required",
			ErrNextcloudPublicationDenied,
		), fmt.Sprintf("%s: interactive web user required", apperrors.PublicMessage(
			ErrNextcloudPublicationDenied,
		)))
	}
	if _, machineScope := types.TenantAPIKeyScopeFromContext(ctx); machineScope {
		return nil, apperrors.NewProtocolError(fmt.Errorf(
			"%w: machine API key cannot read source publication",
			ErrNextcloudPublicationDenied,
		), fmt.Sprintf("%s: machine API key cannot read source publication", apperrors.PublicMessage(
			ErrNextcloudPublicationDenied,
		)))
	}
	if g == nil || g.directories == nil {
		return nil, ErrNextcloudPublicationUnavailable
	}
	identities, snapshot, err := g.directories.GetUserLoginSnapshot(ctx, caller.UserID)
	if err != nil {
		return nil, apperrors.NewProtocolError(fmt.Errorf(
			"%w: directory identity lookup failed: %v",
			ErrNextcloudPublicationUnavailable,
			err,
		), fmt.Sprintf("%s: directory identity lookup failed: %v", apperrors.PublicMessage(
			ErrNextcloudPublicationUnavailable,
		), func() any {
			if err == nil {
				return nil
			}
			return apperrors.PublicMessage(err)
		}()))
	}
	if len(identities) != 1 || identities[0] == nil {
		return nil, apperrors.NewProtocolError(fmt.Errorf(
			"%w: exactly one linked directory identity is required",
			ErrNextcloudPublicationDenied,
		), fmt.Sprintf("%s: exactly one linked directory identity is required", apperrors.PublicMessage(
			ErrNextcloudPublicationDenied,
		)))
	}
	identity := identities[0]
	if identity.UserID == nil || *identity.UserID != caller.UserID ||
		identity.Status != types.DirectoryObjectActive ||
		identity.DirectoryID == "" || identity.ObjectGUID == "" {
		return nil, apperrors.NewProtocolError(fmt.Errorf(
			"%w: linked directory identity is inactive",
			ErrNextcloudPublicationDenied,
		), fmt.Sprintf("%s: linked directory identity is inactive", apperrors.PublicMessage(
			ErrNextcloudPublicationDenied,
		)))
	}
	if snapshot == nil || snapshot.Directory == nil {
		return nil, apperrors.NewProtocolError(fmt.Errorf(
			"%w: directory snapshot lookup failed: %v",
			ErrNextcloudPublicationUnavailable,
			err,
		), fmt.Sprintf("%s: directory snapshot lookup failed: %v", apperrors.PublicMessage(
			ErrNextcloudPublicationUnavailable,
		), func() any {
			if err == nil {
				return nil
			}
			return apperrors.PublicMessage(err)
		}()))
	}
	directory := snapshot.Directory
	currentIdentity := snapshot.Identity
	if directory.ID != identity.DirectoryID || directory.Protocol != types.DirectoryProtocolAD ||
		!directory.IsFresh(time.Now().UTC()) || strings.TrimSpace(directory.LastSyncError) != "" ||
		directory.SnapshotVersion == 0 || identity.SnapshotVersion != directory.SnapshotVersion ||
		currentIdentity == nil || currentIdentity.ID != identity.ID ||
		currentIdentity.DirectoryID != identity.DirectoryID ||
		currentIdentity.ObjectGUID != identity.ObjectGUID ||
		currentIdentity.UserID == nil || *currentIdentity.UserID != caller.UserID ||
		currentIdentity.Status != types.DirectoryObjectActive ||
		currentIdentity.SnapshotVersion != directory.SnapshotVersion {
		return nil, apperrors.NewProtocolError(fmt.Errorf(
			"%w: directory snapshot is unavailable or stale",
			ErrNextcloudPublicationDenied,
		), fmt.Sprintf("%s: directory snapshot is unavailable or stale", apperrors.PublicMessage(
			ErrNextcloudPublicationDenied,
		)))
	}

	copied := *currentIdentity
	userID := *currentIdentity.UserID
	copied.UserID = &userID
	return &copied, nil
}

func publicationMetadataString(metadata map[string]json.RawMessage, key string) (string, bool) {
	raw, exists := metadata[key]
	if !exists {
		return "", true
	}
	var value *string
	if err := json.Unmarshal(raw, &value); err != nil || value == nil {
		return "", false
	}
	return *value, true
}

// CheckSourceRoot is a compatibility no-op after the Nextcloud read guard retirement.
func (g *NextcloudPublicationGuard) CheckSourceRoot(
	ctx context.Context,
	ds *types.DataSource,
	instance, binding string,
) error {
	return nil // Retired with the Nextcloud decoupling on 2026-11-09.
}

func (g *NextcloudPublicationGuard) checkSourceControls(ctx context.Context, instance string) error {
	if g == nil {
		return ErrNextcloudPublicationUnavailable
	}
	if reader, ok := g.dataSources.(interface {
		InstanceControlsClosed(context.Context, string) (bool, error)
	}); ok {
		closed, err := reader.InstanceControlsClosed(ctx, instance)
		if err != nil {
			return ErrNextcloudPublicationUnavailable
		}
		if closed {
			return ErrNextcloudPublicationDenied
		}
	}
	return nil
}
