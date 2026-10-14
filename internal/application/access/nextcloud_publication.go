package access

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
)

var (
	// ErrNextcloudPublicationDenied reports a denied live source-publication check.
	ErrNextcloudPublicationDenied = apperrors.NewProtocolError(
		errors.New("nextcloud publication access denied"), "Nextcloud publication access denied",
	)
	// ErrNextcloudPublicationUnavailable reports an unavailable live source-publication check.
	ErrNextcloudPublicationUnavailable = apperrors.NewProtocolError(errors.New(
		"nextcloud publication authorization unavailable",
	), "Nextcloud publication authorization unavailable")
)

// NextcloudDirectoryLookup and NextcloudDataSourceLookup are deliberately
// narrow so the guard can use the existing repositories without owning their
// broader management APIs.
type NextcloudDirectoryLookup interface {
	GetIdentityByUserID(context.Context, string) ([]*types.DirectoryIdentity, error)
	GetLoginSnapshot(context.Context, string, string) (*types.DirectoryLoginSnapshot, error)
}

// NextcloudDataSourceLookup loads the authoritative data-source row for a publication check.
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

// NewNextcloudPublicationGuard returns the live source-publication authorization guard.
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
			return apperrors.NewProtocolError(fmt.Errorf(
				"%w: invalid knowledge metadata",
				ErrNextcloudPublicationDenied,
			), fmt.Sprintf("%s: invalid knowledge metadata", apperrors.PublicMessage(
				ErrNextcloudPublicationDenied,
			)))
		}
	}
	marked := knowledge.Channel == types.ConnectorTypeNextcloud ||
		metadata["nextcloud_instance_id"] != nil || metadata["nextcloud_binding_id"] != nil ||
		metadata["nextcloud_file_id"] != nil
	dsID, valid := publicationMetadataString(metadata, "datasource_id")
	if !valid {
		return apperrors.NewProtocolError(fmt.Errorf(
			"%w: invalid data source identity",
			ErrNextcloudPublicationDenied,
		), fmt.Sprintf("%s: invalid data source identity", apperrors.PublicMessage(
			ErrNextcloudPublicationDenied,
		)))
	}
	if dsID == "" {
		if marked {
			return apperrors.NewProtocolError(fmt.Errorf(
				"%w: missing data source identity",
				ErrNextcloudPublicationDenied,
			), fmt.Sprintf("%s: missing data source identity", apperrors.PublicMessage(
				ErrNextcloudPublicationDenied,
			)))
		}
		return nil
	}
	if g == nil || g.dataSources == nil {
		return ErrNextcloudPublicationUnavailable
	}
	ds, err := g.dataSources.FindByID(ctx, dsID)
	if err != nil || ds == nil {
		return apperrors.NewProtocolError(fmt.Errorf(
			"%w: data source lookup failed: %v",
			ErrNextcloudPublicationUnavailable,
			err,
		), fmt.Sprintf("%s: data source lookup failed: %v", apperrors.PublicMessage(
			ErrNextcloudPublicationUnavailable,
		), func() any {
			if err == nil {
				return nil
			}
			return apperrors.PublicMessage(err)
		}()))
	}
	if ds.ID != dsID || ds.TenantID != knowledge.TenantID ||
		ds.KnowledgeBaseID != knowledge.KnowledgeBaseID {
		return apperrors.NewProtocolError(fmt.Errorf(
			"%w: data source ownership mismatch",
			ErrNextcloudPublicationDenied,
		), fmt.Sprintf("%s: data source ownership mismatch", apperrors.PublicMessage(
			ErrNextcloudPublicationDenied,
		)))
	}
	if ds.Type != types.ConnectorTypeNextcloud {
		if marked {
			return apperrors.NewProtocolError(fmt.Errorf(
				"%w: source type mismatch",
				ErrNextcloudPublicationDenied,
			), fmt.Sprintf("%s: source type mismatch", apperrors.PublicMessage(
				ErrNextcloudPublicationDenied,
			)))
		}
		return nil
	}
	if knowledge.Channel != types.ConnectorTypeNextcloud ||
		ds.Status != types.DataSourceStatusActive {
		return apperrors.NewProtocolError(fmt.Errorf(
			"%w: source channel or state mismatch",
			ErrNextcloudPublicationDenied,
		), fmt.Sprintf("%s: source channel or state mismatch", apperrors.PublicMessage(
			ErrNextcloudPublicationDenied,
		)))
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
		return apperrors.NewProtocolError(fmt.Errorf(
			"%w: incomplete source identity",
			ErrNextcloudPublicationDenied,
		), fmt.Sprintf("%s: incomplete source identity", apperrors.PublicMessage(
			ErrNextcloudPublicationDenied,
		)))
	}
	config, err := ds.ParseConfig()
	if err != nil || config == nil {
		return apperrors.NewProtocolError(fmt.Errorf(
			"%w: data source configuration failed: %v",
			ErrNextcloudPublicationUnavailable,
			err,
		), fmt.Sprintf("%s: data source configuration failed: %v", apperrors.PublicMessage(
			ErrNextcloudPublicationUnavailable,
		), func() any {
			if err == nil {
				return nil
			}
			return apperrors.PublicMessage(err)
		}()))
	}
	// The V1 rollout binds one dedicated knowledge base to one source folder.
	// Old multi-binding configurations must not remain readable after a policy
	// change, even if this particular file still belongs to one of the roots.
	if len(config.ResourceIDs) != 1 || config.ResourceIDs[0] != bindingID {
		return apperrors.NewProtocolError(fmt.Errorf(
			"%w: source requires exactly one selected binding",
			ErrNextcloudPublicationDenied,
		), fmt.Sprintf("%s: source requires exactly one selected binding", apperrors.PublicMessage(
			ErrNextcloudPublicationDenied,
		)))
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
		return apperrors.NewProtocolError(fmt.Errorf(
			"%w: source check failed: %v",
			ErrNextcloudPublicationUnavailable,
			err,
		), fmt.Sprintf("%s: source check failed: %v", apperrors.PublicMessage(
			ErrNextcloudPublicationUnavailable,
		), func() any {
			if err == nil {
				return nil
			}
			return apperrors.PublicMessage(err)
		}()))
	}
	if !decision.Allow {
		return apperrors.NewProtocolError(fmt.Errorf(
			"%w: source denied file access",
			ErrNextcloudPublicationDenied,
		), fmt.Sprintf("%s: source denied file access", apperrors.PublicMessage(
			ErrNextcloudPublicationDenied,
		)))
	}
	// Publication and ACL may still be valid while the source bytes have
	// changed. Never expose an imported older version as the current file.
	if decision.SourceETag == "" || decision.SourceETag != importedETag {
		return apperrors.NewProtocolError(fmt.Errorf(
			"%w: source version changed",
			ErrNextcloudPublicationDenied,
		), fmt.Sprintf("%s: source version changed", apperrors.PublicMessage(
			ErrNextcloudPublicationDenied,
		)))
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
	identities, err := g.directories.GetIdentityByUserID(ctx, caller.UserID)
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
	snapshot, err := g.directories.GetLoginSnapshot(ctx, identity.DirectoryID, identity.ObjectGUID)
	if err != nil || snapshot == nil || snapshot.Directory == nil {
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

	cloned := *currentIdentity
	userID := *currentIdentity.UserID
	cloned.UserID = &userID
	return &cloned, nil
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
