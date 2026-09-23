package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

// DirectoryRuntimeService orchestrates configuration resolution, live LDAP
// operations and atomic persistence. It is deliberately distinct from the
// directory CRUD/snapshot service so protocol details do not leak into the
// persistence layer.
type DirectoryRuntimeService interface {
	GetConfig(ctx context.Context) (*types.DirectoryAdminConfig, error)
	UpdateConfig(
		ctx context.Context,
		update *types.DirectoryAdminConfigUpdate,
	) (*types.DirectoryAdminConfig, error)
	GetStatus(ctx context.Context) (*types.DirectoryHealth, error)
	TestConnection(
		ctx context.Context,
		candidate *types.DirectoryAdminConfigUpdate,
	) (*types.DirectoryTestResult, error)
	QueryUsers(
		ctx context.Context,
		query string,
		limit, offset int,
	) (*types.DirectoryObjectSearchResult, error)
	QueryGroups(
		ctx context.Context,
		query string,
		limit, offset int,
	) (*types.DirectoryGroupSearchResult, error)
	QueryGroupMembers(
		ctx context.Context,
		groupGUID, query string,
		limit, offset int,
	) (*types.DirectoryGroupMembersResult, error)
	Catalog(
		ctx context.Context,
		kind, query string,
		limit, offset int,
	) (*types.DirectoryCatalogResult, error)
	CatalogGroupMembers(
		ctx context.Context,
		groupGUID, query string,
		limit, offset int,
	) (*types.DirectoryGroupMembersResult, error)
	AddTenantDirectoryMember(
		ctx context.Context,
		tenantID uint64,
		objectGUID string,
		role types.TenantRole,
	) (*types.TenantMember, error)
	PreviewSync(ctx context.Context) (*types.DirectorySyncPreview, error)
	ManualSync(ctx context.Context) (*types.DirectorySyncRunView, error)
	ListSyncRuns(ctx context.Context, limit int) (*types.DirectorySyncRunsResponse, error)
	LinkIdentity(ctx context.Context, objectGUID, userID string) error
	UnlinkIdentity(ctx context.Context, objectGUID string) error
	Login(ctx context.Context, identifier, password string) (*types.LoginResponse, error)
	Start(ctx context.Context)
	Stop()
}
