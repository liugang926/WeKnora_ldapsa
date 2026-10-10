package access

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
)

var (
	ErrNextcloudPublicationDenied      = errors.New("Nextcloud publication access denied")
	ErrNextcloudPublicationUnavailable = errors.New("Nextcloud publication authorization unavailable")
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

type nextcloudSourceAuthorizer func(
	context.Context, *types.DataSourceConfig, string, string, string, string, int64,
) (nextcloud.AuthorizationDecision, error)

// NextcloudPublicationGuard checks the original source for a single knowledge
// document. Call CheckKnowledge immediately before exposing source-derived
// content; passing a document through a KB grant alone is insufficient.
type NextcloudPublicationGuard struct {
	directories NextcloudDirectoryLookup
	dataSources NextcloudDataSourceLookup
	authorize   nextcloudSourceAuthorizer
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

// CheckKnowledge returns nil for unrelated knowledge and for a currently
// authorized Nextcloud file. For Nextcloud knowledge every other outcome is a
// denial, including an untrusted principal, stale directory snapshot, missing
// source configuration, failed source call, or malformed source response.
func (g *NextcloudPublicationGuard) CheckKnowledge(ctx context.Context, knowledge *types.Knowledge) error {
	if knowledge == nil {
		return ErrNextcloudPublicationDenied
	}
	metadata := map[string]json.RawMessage{}
	if len(knowledge.Metadata) != 0 {
		if err := json.Unmarshal(knowledge.Metadata, &metadata); err != nil || metadata == nil {
			return fmt.Errorf("%w: invalid knowledge metadata", ErrNextcloudPublicationDenied)
		}
	}
	marked := knowledge.Channel == types.ConnectorTypeNextcloud ||
		metadata["nextcloud_instance_id"] != nil || metadata["nextcloud_binding_id"] != nil ||
		metadata["nextcloud_file_id"] != nil
	dsID, valid := publicationMetadataString(metadata, "datasource_id")
	if !valid {
		return fmt.Errorf("%w: invalid data source identity", ErrNextcloudPublicationDenied)
	}
	if dsID == "" {
		if marked {
			return fmt.Errorf("%w: missing data source identity", ErrNextcloudPublicationDenied)
		}
		return nil
	}
	if g == nil || g.dataSources == nil {
		return ErrNextcloudPublicationUnavailable
	}
	ds, err := g.dataSources.FindByID(ctx, dsID)
	if err != nil || ds == nil {
		return fmt.Errorf("%w: data source lookup failed: %v", ErrNextcloudPublicationUnavailable, err)
	}
	if ds.ID != dsID || ds.TenantID != knowledge.TenantID ||
		ds.KnowledgeBaseID != knowledge.KnowledgeBaseID {
		return fmt.Errorf("%w: data source ownership mismatch", ErrNextcloudPublicationDenied)
	}
	if ds.Type != types.ConnectorTypeNextcloud {
		if marked {
			return fmt.Errorf("%w: source type mismatch", ErrNextcloudPublicationDenied)
		}
		return nil
	}
	if knowledge.Channel != types.ConnectorTypeNextcloud ||
		ds.Status != types.DataSourceStatusActive {
		return fmt.Errorf("%w: source channel or state mismatch", ErrNextcloudPublicationDenied)
	}
	instanceID, instanceValid := publicationMetadataString(metadata, "nextcloud_instance_id")
	bindingID, bindingValid := publicationMetadataString(metadata, "nextcloud_binding_id")
	fileIDRaw, fileIDValid := publicationMetadataString(metadata, "nextcloud_file_id")
	importedETag, etagValid := publicationMetadataString(metadata, "nextcloud_etag")
	sourceResourceID, sourceResourceValid := publicationMetadataString(metadata, "source_resource_id")
	externalID, externalIDValid := publicationMetadataString(metadata, "external_id")
	fileID, err := strconv.ParseInt(fileIDRaw, 10, 64)
	if !instanceValid || !bindingValid || !fileIDValid || !etagValid || !sourceResourceValid || !externalIDValid ||
		err != nil || fileID < 1 || strconv.FormatInt(fileID, 10) != fileIDRaw ||
		instanceID == "" || bindingID == "" || strings.TrimSpace(importedETag) == "" ||
		sourceResourceID != bindingID ||
		externalID != "nextcloud:"+instanceID+":"+fileIDRaw {
		return fmt.Errorf("%w: incomplete source identity", ErrNextcloudPublicationDenied)
	}
	config, err := ds.ParseConfig()
	if err != nil || config == nil {
		return fmt.Errorf("%w: data source configuration failed: %v", ErrNextcloudPublicationUnavailable, err)
	}
	// The V1 rollout binds one dedicated knowledge base to one source folder.
	// Old multi-binding configurations must not remain readable after a policy
	// change, even if this particular file still belongs to one of the roots.
	if len(config.ResourceIDs) != 1 || config.ResourceIDs[0] != bindingID {
		return fmt.Errorf("%w: source requires exactly one selected binding", ErrNextcloudPublicationDenied)
	}
	config.Type = ds.Type

	currentIdentity, err := g.currentDirectoryIdentity(ctx)
	if err != nil {
		return err
	}
	if g.authorize == nil {
		return ErrNextcloudPublicationUnavailable
	}
	decision, err := g.authorize(ctx, config, instanceID, bindingID,
		currentIdentity.DirectoryID, currentIdentity.ObjectGUID, fileID)
	if err != nil {
		return fmt.Errorf("%w: source check failed: %v", ErrNextcloudPublicationUnavailable, err)
	}
	if !decision.Allow {
		return fmt.Errorf("%w: source denied file access", ErrNextcloudPublicationDenied)
	}
	// Publication and ACL may still be valid while the source bytes have
	// changed. Never expose an imported older version as the current file.
	if decision.SourceETag == "" || decision.SourceETag != importedETag {
		return fmt.Errorf("%w: source version changed", ErrNextcloudPublicationDenied)
	}
	return nil
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
		return nil, fmt.Errorf("%w: authenticated human user required", ErrNextcloudPublicationDenied)
	}
	// A machine identity can have a user-shaped ID (embed-..., mcp-..., or an
	// external API user). The principal, captured by the authentication
	// middleware, must explicitly identify the same interactive web user.
	// Do not use PrincipalFromContext here: its legacy UserID fallback would
	// turn an uncaptured machine identity into a web principal.
	principal, captured := ctx.Value(types.PrincipalContextKey).(types.Principal)
	if !captured || principal.Type != types.PrincipalWebUser || principal.ID != caller.UserID {
		return nil, fmt.Errorf("%w: interactive web user required", ErrNextcloudPublicationDenied)
	}
	if _, machineScope := types.TenantAPIKeyScopeFromContext(ctx); machineScope {
		return nil, fmt.Errorf("%w: machine API key cannot read source publication", ErrNextcloudPublicationDenied)
	}
	if g == nil || g.directories == nil {
		return nil, ErrNextcloudPublicationUnavailable
	}
	identities, snapshot, err := g.directories.GetUserLoginSnapshot(ctx, caller.UserID)
	if err != nil {
		return nil, fmt.Errorf("%w: directory identity lookup failed: %v", ErrNextcloudPublicationUnavailable, err)
	}
	if len(identities) != 1 || identities[0] == nil {
		return nil, fmt.Errorf("%w: exactly one linked directory identity is required", ErrNextcloudPublicationDenied)
	}
	identity := identities[0]
	if identity.UserID == nil || *identity.UserID != caller.UserID ||
		identity.Status != types.DirectoryObjectActive ||
		identity.DirectoryID == "" || identity.ObjectGUID == "" {
		return nil, fmt.Errorf("%w: linked directory identity is inactive", ErrNextcloudPublicationDenied)
	}
	if snapshot == nil || snapshot.Directory == nil {
		return nil, fmt.Errorf("%w: directory snapshot lookup failed: %v", ErrNextcloudPublicationUnavailable, err)
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
		return nil, fmt.Errorf("%w: directory snapshot is unavailable or stale", ErrNextcloudPublicationDenied)
	}

	copy := *currentIdentity
	userID := *currentIdentity.UserID
	copy.UserID = &userID
	return &copy, nil
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
