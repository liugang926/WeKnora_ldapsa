package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

const (
	groupLookupPageSize = 1000
	groupListLimit      = 100
)

// GroupAccessHandler manages the administrator-facing directory-group
// overlays. Authentication and Owner/Admin gates are deliberately mounted by
// the router so this handler can share the project's standard RBAC policy.
// It still verifies tenant/resource ownership as a defence against a path or
// active-tenant mismatch.
type GroupAccessHandler struct {
	directories interfaces.DirectoryRepository
	groups      interfaces.GroupAccessRepository
	access      interfaces.GroupAccessService
	kbs         interfaces.KnowledgeBaseRepository
	agents      interfaces.CustomAgentRepository
	audit       interfaces.AuditLogService
	resources   interfaces.ResourceCatalog
}

// ConfigureGroupAccessResourceCatalog enables revocation of outstanding file
// capabilities when a knowledge-base policy changes. It is kept separate from
// the constructor so focused handler tests can remain lightweight.
func ConfigureGroupAccessResourceCatalog(h *GroupAccessHandler, resources interfaces.ResourceCatalog) {
	if h != nil {
		h.resources = resources
	}
}

func NewGroupAccessHandler(
	directories interfaces.DirectoryRepository,
	groups interfaces.GroupAccessRepository,
	access interfaces.GroupAccessService,
	kbs interfaces.KnowledgeBaseRepository,
	agents interfaces.CustomAgentRepository,
	audit interfaces.AuditLogService,
) *GroupAccessHandler {
	return &GroupAccessHandler{
		directories: directories,
		groups:      groups,
		access:      access,
		kbs:         kbs,
		agents:      agents,
		audit:       audit,
	}
}

// RegisterTenantGroupRoutes registers routes on a group rooted at
// /tenants/:id/directory-groups. The caller must attach the existing tenant
// path-match and Admin middleware before calling this method.
func (h *GroupAccessHandler) RegisterTenantGroupRoutes(group *gin.RouterGroup) {
	group.GET("", h.ListTenantDirectoryGroups)
	group.POST("", h.AddTenantDirectoryGroup)
	group.PUT("/:grant_id", h.UpdateTenantDirectoryGroup)
	group.DELETE("/:grant_id", h.DeleteTenantDirectoryGroup)
}

// RegisterResourceGroupRoutes registers routes on a group rooted at
// /group-access. The caller must attach the existing authenticated Admin
// middleware before calling this method.
func (h *GroupAccessHandler) RegisterResourceGroupRoutes(group *gin.RouterGroup) {
	group.GET("/:resource_type/:resource_id", h.GetResourceGroupAccess)
	group.PUT("/:resource_type/:resource_id", h.UpdateResourceGroupAccess)
	group.POST("/:resource_type/:resource_id/preview", h.PreviewResourceGroupAccess)
}

type tenantDirectoryGroupView struct {
	ID                   string            `json:"id"`
	TenantID             uint64            `json:"tenant_id"`
	DirectoryID          string            `json:"directory_id"`
	DirectoryGroupID     string            `json:"directory_group_id"`
	ObjectGUID           string            `json:"object_guid,omitempty"`
	SID                  string            `json:"sid,omitempty"`
	DN                   string            `json:"dn"`
	DisplayName          string            `json:"display_name"`
	Role                 types.TenantRole  `json:"role"`
	Origin               types.GrantOrigin `json:"origin"`
	DirectMemberCount    int               `json:"direct_member_count"`
	EffectiveMemberCount int               `json:"effective_member_count"`
	NestedGroupCount     int               `json:"nested_group_count"`
	UpdatedAt            time.Time         `json:"updated_at"`
}

type tenantDirectoryGroupCandidate struct {
	DirectoryID          string            `json:"directory_id"`
	DirectoryGroupID     string            `json:"directory_group_id"`
	ObjectGUID           string            `json:"object_guid,omitempty"`
	SID                  string            `json:"sid,omitempty"`
	DN                   string            `json:"dn"`
	DisplayName          string            `json:"display_name"`
	DirectMemberCount    int               `json:"direct_member_count"`
	EffectiveMemberCount int               `json:"effective_member_count"`
	Linked               bool              `json:"linked"`
	CurrentRole          *types.TenantRole `json:"current_role,omitempty"`
}

type tenantGroupMutationRequest struct {
	DirectoryID      string           `json:"directory_id"`
	DirectoryGroupID string           `json:"directory_group_id"`
	Role             types.TenantRole `json:"role" binding:"required"`
}

type tenantGroupRoleRequest struct {
	Role types.TenantRole `json:"role" binding:"required"`
}

func (h *GroupAccessHandler) ListTenantDirectoryGroups(c *gin.Context) {
	tenantID, ok := h.pathTenant(c)
	if !ok {
		return
	}
	ctx := c.Request.Context()
	if parseBoolQuery(c.Query("available")) {
		h.listAvailableTenantGroups(c, ctx, tenantID)
		return
	}

	grants, err := h.groups.ListTenantGroupRoleGrants(ctx, tenantID)
	if err != nil {
		h.internalError(c, "list workspace directory groups", err)
		return
	}
	views := make([]tenantDirectoryGroupView, 0, len(grants))
	for _, grant := range grants {
		group, directory, err := h.findGroup(ctx, "", grant.DirectoryGroupID, false)
		if err != nil && !errors.Is(err, errGroupNotFound) {
			h.internalError(c, "look up workspace directory group", err)
			return
		}
		if err != nil || group == nil || directory == nil {
			// Preserve visibility of stale grants so an administrator can remove
			// them even after the source group disappears.
			views = append(views, tenantDirectoryGroupView{
				ID: grant.ID, TenantID: tenantID, DirectoryGroupID: grant.DirectoryGroupID,
				DisplayName: grant.DirectoryGroupID, Role: grant.Role, Origin: grant.Origin,
				UpdatedAt: grant.UpdatedAt,
			})
			continue
		}
		views = append(views, h.tenantGroupView(ctx, grant, directory, group))
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": gin.H{"groups": views, "total": len(views)}})
}

func (h *GroupAccessHandler) listAvailableTenantGroups(c *gin.Context, ctx context.Context, tenantID uint64) {
	limit := boundedQueryLimit(c, 30)
	query := strings.TrimSpace(c.Query("q"))
	grants, err := h.groups.ListTenantGroupRoleGrants(ctx, tenantID)
	if err != nil {
		h.internalError(c, "list workspace directory group grants", err)
		return
	}
	linked := make(map[string]*types.TenantGroupRoleGrant, len(grants))
	for _, grant := range grants {
		linked[grant.DirectoryGroupID] = grant
	}

	directories, err := h.directories.List(ctx)
	if err != nil {
		h.internalError(c, "list directories", err)
		return
	}
	candidates := make([]tenantDirectoryGroupCandidate, 0, limit)
	truncated := false

searchDirectories:
	for _, directory := range directories {
		if directory == nil || !directory.Enabled {
			continue
		}
		for offset := 0; ; offset += groupLookupPageSize {
			rows, err := h.directories.ListGroups(ctx, directory.ID, query, offset, groupLookupPageSize)
			if err != nil {
				h.internalError(c, "search directory groups", err)
				return
			}
			for _, group := range rows {
				if group == nil || group.Status != types.DirectoryObjectActive {
					continue
				}
				// available=true means groups which can be newly associated. The
				// linked/current fields remain in the response type for forwards
				// compatibility, but linked groups are intentionally omitted here.
				if _, exists := linked[group.ID]; exists {
					continue
				}
				if len(candidates) == limit {
					truncated = true
					break searchDirectories
				}
				candidates = append(candidates, h.tenantGroupCandidate(ctx, directory, group, nil))
			}
			if len(rows) < groupLookupPageSize {
				break
			}
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].DisplayName == candidates[j].DisplayName {
			return candidates[i].DirectoryGroupID < candidates[j].DirectoryGroupID
		}
		return strings.ToLower(candidates[i].DisplayName) < strings.ToLower(candidates[j].DisplayName)
	})
	c.JSON(http.StatusOK, gin.H{"success": true, "data": gin.H{
		"groups": candidates, "total": len(candidates), "truncated": truncated,
	}})
}

func (h *GroupAccessHandler) AddTenantDirectoryGroup(c *gin.Context) {
	tenantID, ok := h.pathTenant(c)
	if !ok {
		return
	}
	var request tenantGroupMutationRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.Error(apperrors.NewValidationError("directory_id, directory_group_id and role are required").WithDetails(err.Error()))
		return
	}
	if !validDirectoryRole(request.Role) {
		c.Error(apperrors.NewValidationError("role must be viewer, contributor or admin"))
		return
	}
	if strings.TrimSpace(request.DirectoryID) == "" || strings.TrimSpace(request.DirectoryGroupID) == "" {
		c.Error(apperrors.NewValidationError("directory_id and directory_group_id are required"))
		return
	}
	ctx := c.Request.Context()
	group, directory, err := h.findGroup(ctx, strings.TrimSpace(request.DirectoryID), strings.TrimSpace(request.DirectoryGroupID), true)
	if err != nil {
		h.groupLookupError(c, err)
		return
	}
	actorID, _ := types.UserIDFromContext(ctx)
	grant := &types.TenantGroupRoleGrant{
		TenantID: tenantID, DirectoryGroupID: group.ID, Role: request.Role,
		Origin: types.GrantOriginManual, CreatedBy: actorID,
	}
	if err := h.access.UpsertTenantGroupRoleGrant(ctx, grant); err != nil {
		h.internalError(c, "add workspace directory group", err)
		return
	}
	h.emitAudit(ctx, tenantID, types.AuditAction("directory.tenant_group_grant_changed"), "tenant_group_role_grant", grant.ID, map[string]any{
		"operation": "created", "directory_id": directory.ID, "directory_group_id": group.ID, "role": request.Role,
	})
	c.JSON(http.StatusCreated, gin.H{"success": true, "data": h.tenantGroupView(ctx, grant, directory, group)})
}

func (h *GroupAccessHandler) UpdateTenantDirectoryGroup(c *gin.Context) {
	tenantID, ok := h.pathTenant(c)
	if !ok {
		return
	}
	var request tenantGroupRoleRequest
	if err := c.ShouldBindJSON(&request); err != nil || !validDirectoryRole(request.Role) {
		c.Error(apperrors.NewValidationError("role must be viewer, contributor or admin"))
		return
	}
	ctx := c.Request.Context()
	grant, err := h.manualTenantGrantByID(ctx, tenantID, c.Param("grant_id"))
	if err != nil {
		h.grantLookupError(c, err)
		return
	}
	group, directory, err := h.findGroup(ctx, "", grant.DirectoryGroupID, true)
	if err != nil {
		h.groupLookupError(c, err)
		return
	}
	oldRole := grant.Role
	grant.Role = request.Role
	if actorID, exists := types.UserIDFromContext(ctx); exists {
		grant.CreatedBy = actorID
	}
	if err := h.access.UpsertTenantGroupRoleGrant(ctx, grant); err != nil {
		h.internalError(c, "update workspace directory group", err)
		return
	}
	h.emitAudit(ctx, tenantID, types.AuditAction("directory.tenant_group_grant_changed"), "tenant_group_role_grant", grant.ID, map[string]any{
		"operation": "updated", "directory_id": directory.ID, "directory_group_id": group.ID,
		"old_role": oldRole, "new_role": request.Role,
	})
	c.JSON(http.StatusOK, gin.H{"success": true, "data": h.tenantGroupView(ctx, grant, directory, group)})
}

func (h *GroupAccessHandler) DeleteTenantDirectoryGroup(c *gin.Context) {
	tenantID, ok := h.pathTenant(c)
	if !ok {
		return
	}
	ctx := c.Request.Context()
	grant, err := h.manualTenantGrantByID(ctx, tenantID, c.Param("grant_id"))
	if err != nil {
		h.grantLookupError(c, err)
		return
	}
	if err := h.access.DeleteTenantGroupRoleGrant(ctx, tenantID, grant.DirectoryGroupID, types.GrantOriginManual); err != nil {
		h.internalError(c, "delete workspace directory group", err)
		return
	}
	h.emitAudit(ctx, tenantID, types.AuditAction("directory.tenant_group_grant_changed"), "tenant_group_role_grant", grant.ID, map[string]any{
		"operation": "deleted", "directory_group_id": grant.DirectoryGroupID, "role": grant.Role,
	})
	c.Status(http.StatusNoContent)
}

type resourceGrantRequest struct {
	DirectoryID      string                   `json:"directory_id"`
	DirectoryGroupID string                   `json:"directory_group_id" binding:"required"`
	Permission       types.ResourcePermission `json:"permission" binding:"required"`
}

type resourceAccessUpdateRequest struct {
	Mode   types.ResourceAccessMode `json:"mode" binding:"required"`
	Grants []resourceGrantRequest   `json:"grants"`
}

type resourceGroupView struct {
	ID                   string                   `json:"id,omitempty"`
	DirectoryID          string                   `json:"directory_id"`
	DirectoryGroupID     string                   `json:"directory_group_id"`
	DisplayName          string                   `json:"display_name"`
	DN                   string                   `json:"dn,omitempty"`
	Permission           types.ResourcePermission `json:"permission,omitempty"`
	Origin               types.GrantOrigin        `json:"origin,omitempty"`
	WorkspaceRole        *types.TenantRole        `json:"workspace_role"`
	DirectMemberCount    int                      `json:"direct_member_count"`
	EffectiveMemberCount int                      `json:"effective_member_count"`
	MembershipSources    []string                 `json:"membership_sources,omitempty"`
}

type resourceAccessView struct {
	ResourceType    types.ResourceType       `json:"resource_type"`
	ResourceID      string                   `json:"resource_id"`
	TenantID        uint64                   `json:"tenant_id"`
	Mode            types.ResourceAccessMode `json:"mode"`
	Grants          []resourceGroupView      `json:"grants"`
	AvailableGroups []resourceGroupView      `json:"available_groups"`
	UpdatedAt       *time.Time               `json:"updated_at,omitempty"`
}

type resourceAccessImpactView struct {
	CurrentlyAllowed   int      `json:"currently_allowed"`
	AllowedAfter       int      `json:"allowed_after"`
	LosingAccess       int      `json:"losing_access"`
	GainingAccess      int      `json:"gaining_access"`
	UnaffectedManagers int      `json:"unaffected_managers"`
	Warnings           []string `json:"warnings,omitempty"`
}

func (h *GroupAccessHandler) GetResourceGroupAccess(c *gin.Context) {
	resourceType, resourceID, tenantID, ok := h.resolveResource(c)
	if !ok {
		return
	}
	view, err := h.buildResourceAccessView(c.Request.Context(), tenantID, resourceType, resourceID)
	if err != nil {
		h.internalError(c, "get resource group access", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": view})
}

func (h *GroupAccessHandler) UpdateResourceGroupAccess(c *gin.Context) {
	resourceType, resourceID, tenantID, ok := h.resolveResource(c)
	if !ok {
		return
	}
	var request resourceAccessUpdateRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.Error(apperrors.NewValidationError("mode and grants are required").WithDetails(err.Error()))
		return
	}
	ctx := c.Request.Context()
	validated, err := h.validateResourceGrantRequest(ctx, resourceType, request)
	if err != nil {
		h.resourceRequestError(c, err)
		return
	}

	existing, err := h.groups.ListResourceGroupGrants(ctx, tenantID, resourceType, resourceID)
	if err != nil {
		h.internalError(c, "list resource group grants", err)
		return
	}
	desired := make(map[string]validatedResourceGrant, len(validated))
	for _, grant := range validated {
		desired[resourceGrantKey(grant.group.ID, grant.request.Permission)] = grant
	}
	for _, grant := range existing {
		if grant.Origin != types.GrantOriginManual {
			continue
		}
		if _, keep := desired[resourceGrantKey(grant.DirectoryGroupID, grant.Permission)]; keep {
			continue
		}
		if err := h.access.DeleteResourceGroupGrant(ctx, tenantID, resourceType, resourceID, grant.DirectoryGroupID, grant.Permission, types.GrantOriginManual); err != nil {
			h.internalError(c, "delete resource group grant", err)
			return
		}
	}
	actorID, _ := types.UserIDFromContext(ctx)
	for _, desiredGrant := range validated {
		grant := &types.ResourceGroupGrant{
			TenantID: tenantID, ResourceType: resourceType, ResourceID: resourceID,
			DirectoryGroupID: desiredGrant.group.ID, Permission: desiredGrant.request.Permission,
			Origin: types.GrantOriginManual, CreatedBy: actorID,
		}
		if err := h.access.UpsertResourceGroupGrant(ctx, grant); err != nil {
			h.internalError(c, "upsert resource group grant", err)
			return
		}
	}
	// Commit the visibility switch last, after the complete desired manual
	// grant set has been validated and applied.
	policy := &types.ResourceAccessPolicy{
		TenantID: tenantID, ResourceType: resourceType, ResourceID: resourceID,
		Mode: request.Mode, UpdatedBy: actorID,
	}
	if err := h.access.SetResourceAccessPolicy(ctx, policy); err != nil {
		h.internalError(c, "set resource access policy", err)
		return
	}
	if resourceType == types.GroupResourceTypeKnowledgeBase && h.resources != nil {
		if _, err := h.resources.RevokeAccessGrantsByKnowledgeBase(ctx, tenantID, resourceID); err != nil {
			// Runtime capability checks still fail closed against the new policy;
			// log the durable cleanup failure so an operator can retry the update.
			logger.Errorf(ctx, "revoke knowledge base access grants after policy update: %v", err)
		}
	}
	h.emitAudit(ctx, tenantID, types.AuditAction("directory.resource_group_access_changed"), string(resourceType), resourceID, map[string]any{
		"mode": request.Mode, "manual_grant_count": len(validated),
	})
	view, err := h.buildResourceAccessView(ctx, tenantID, resourceType, resourceID)
	if err != nil {
		h.internalError(c, "read updated resource group access", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": view})
}

func (h *GroupAccessHandler) PreviewResourceGroupAccess(c *gin.Context) {
	resourceType, resourceID, tenantID, ok := h.resolveResource(c)
	if !ok {
		return
	}
	var request resourceAccessUpdateRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.Error(apperrors.NewValidationError("mode and grants are required").WithDetails(err.Error()))
		return
	}
	ctx := c.Request.Context()
	validated, err := h.validateResourceGrantRequest(ctx, resourceType, request)
	if err != nil {
		h.resourceRequestError(c, err)
		return
	}
	impact, err := h.previewResourceAccess(ctx, tenantID, resourceType, resourceID, request.Mode, validated)
	if err != nil {
		h.internalError(c, "preview resource group access", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": impact})
}

type validatedResourceGrant struct {
	request   resourceGrantRequest
	directory *types.Directory
	group     *types.DirectoryGroup
}

var (
	errGroupNotFound        = errors.New("directory group not found")
	errGroupInactive        = errors.New("directory group is inactive or its directory is disabled")
	errGrantNotFound        = errors.New("group grant not found")
	errDirectoryGrantLocked = errors.New("directory-derived grant cannot be changed manually")
	errDuplicateGrant       = errors.New("duplicate resource group grant")
)

func (h *GroupAccessHandler) validateResourceGrantRequest(
	ctx context.Context,
	resourceType types.ResourceType,
	request resourceAccessUpdateRequest,
) ([]validatedResourceGrant, error) {
	if !request.Mode.IsValid() {
		return nil, errors.New("mode must be inherit or restricted")
	}
	validated := make([]validatedResourceGrant, 0, len(request.Grants))
	seenGroups := make(map[string]struct{}, len(request.Grants))
	for _, item := range request.Grants {
		item.DirectoryID = strings.TrimSpace(item.DirectoryID)
		item.DirectoryGroupID = strings.TrimSpace(item.DirectoryGroupID)
		if item.DirectoryID == "" || item.DirectoryGroupID == "" || !item.Permission.ValidFor(resourceType) {
			return nil, errors.New("each group must identify a directory and have a valid permission for the resource type")
		}
		if _, duplicate := seenGroups[item.DirectoryGroupID]; duplicate {
			return nil, errDuplicateGrant
		}
		seenGroups[item.DirectoryGroupID] = struct{}{}
		group, directory, err := h.findGroup(ctx, item.DirectoryID, item.DirectoryGroupID, true)
		if err != nil {
			return nil, err
		}
		validated = append(validated, validatedResourceGrant{request: item, directory: directory, group: group})
	}
	return validated, nil
}

func (h *GroupAccessHandler) buildResourceAccessView(
	ctx context.Context,
	tenantID uint64,
	resourceType types.ResourceType,
	resourceID string,
) (*resourceAccessView, error) {
	policy, err := h.groups.GetResourceAccessPolicy(ctx, tenantID, resourceType, resourceID)
	if err != nil {
		return nil, err
	}
	mode := types.ResourceAccessInherit
	var updatedAt *time.Time
	if policy != nil {
		mode = policy.Mode
		updatedAt = &policy.UpdatedAt
	}
	tenantGrants, err := h.groups.ListTenantGroupRoleGrants(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	workspaceRoles := effectiveWorkspaceGrantRoles(tenantGrants)
	resourceGrants, err := h.groups.ListResourceGroupGrants(ctx, tenantID, resourceType, resourceID)
	if err != nil {
		return nil, err
	}
	grantViews := make([]resourceGroupView, 0, len(resourceGrants))
	selectedGroups := make(map[string]struct{}, len(resourceGrants))
	for _, grant := range resourceGrants {
		group, directory, lookupErr := h.findGroup(ctx, "", grant.DirectoryGroupID, false)
		if lookupErr != nil && !errors.Is(lookupErr, errGroupNotFound) {
			return nil, lookupErr
		}
		if lookupErr != nil || group == nil || directory == nil {
			grantViews = append(grantViews, resourceGroupView{
				ID: grant.ID, DirectoryGroupID: grant.DirectoryGroupID, DisplayName: grant.DirectoryGroupID,
				Permission: grant.Permission, Origin: grant.Origin, WorkspaceRole: workspaceRoles[grant.DirectoryGroupID],
			})
			continue
		}
		selectedGroups[group.ID] = struct{}{}
		grantViews = append(grantViews, h.resourceGroupView(ctx, directory, group, grant, workspaceRoles[group.ID]))
	}

	available := make([]resourceGroupView, 0)
	directories, err := h.directories.List(ctx)
	if err != nil {
		return nil, err
	}
	for _, directory := range directories {
		if directory == nil || !directory.Enabled || len(available) >= groupListLimit {
			continue
		}
		for offset := 0; len(available) < groupListLimit; offset += groupLookupPageSize {
			groups, err := h.directories.ListGroups(ctx, directory.ID, "", offset, groupLookupPageSize)
			if err != nil {
				return nil, err
			}
			for _, group := range groups {
				if group == nil || group.Status != types.DirectoryObjectActive {
					continue
				}
				if _, selected := selectedGroups[group.ID]; selected {
					continue
				}
				available = append(available, h.resourceGroupView(ctx, directory, group, nil, workspaceRoles[group.ID]))
				if len(available) == groupListLimit {
					break
				}
			}
			if len(groups) < groupLookupPageSize {
				break
			}
		}
	}
	return &resourceAccessView{
		ResourceType: resourceType, ResourceID: resourceID, TenantID: tenantID,
		Mode: mode, Grants: grantViews, AvailableGroups: available, UpdatedAt: updatedAt,
	}, nil
}

func (h *GroupAccessHandler) previewResourceAccess(
	ctx context.Context,
	tenantID uint64,
	resourceType types.ResourceType,
	resourceID string,
	proposedMode types.ResourceAccessMode,
	requested []validatedResourceGrant,
) (*resourceAccessImpactView, error) {
	userIDs, err := h.groups.ListTenantUserIDs(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	existing, err := h.groups.ListResourceGroupGrants(ctx, tenantID, resourceType, resourceID)
	if err != nil {
		return nil, err
	}
	// Directory-derived grants remain present after PUT, so include them in
	// the proposed set even when the client only submits manual grants.
	proposed := append([]validatedResourceGrant(nil), requested...)
	requestedKeys := make(map[string]struct{}, len(requested))
	for _, item := range requested {
		requestedKeys[resourceGrantKey(item.group.ID, item.request.Permission)] = struct{}{}
	}
	for _, grant := range existing {
		if grant.Origin != types.GrantOriginDirectory {
			continue
		}
		if _, exists := requestedKeys[resourceGrantKey(grant.DirectoryGroupID, grant.Permission)]; exists {
			continue
		}
		group, directory, lookupErr := h.findGroup(ctx, "", grant.DirectoryGroupID, true)
		if lookupErr == nil {
			proposed = append(proposed, validatedResourceGrant{
				request:   resourceGrantRequest{DirectoryID: directory.ID, DirectoryGroupID: group.ID, Permission: grant.Permission},
				directory: directory, group: group,
			})
		}
	}
	now := time.Now().UTC()
	allowedUsers, err := h.proposedAllowedUsers(ctx, proposed, now)
	if err != nil {
		return nil, err
	}
	action := types.ResourceActionRead
	if resourceType == types.GroupResourceTypeAgent {
		action = types.ResourceActionUse
	}
	impact := &resourceAccessImpactView{}
	for _, userID := range userIDs {
		role, err := h.access.EffectiveTenantRole(ctx, userID, tenantID, now)
		if err != nil {
			return nil, err
		}
		if !role.Member {
			continue
		}
		userCtx := types.WithCaller(context.Background(), types.Caller{TenantID: tenantID, UserID: userID, Role: role.Role})
		userCtx = types.WithPrincipal(userCtx, types.Principal{Type: types.PrincipalWebUser, ID: userID})
		current, err := h.access.EffectivePermission(userCtx, tenantID, resourceType, resourceID, action, now)
		if err != nil {
			return nil, err
		}
		manager := role.Role == types.TenantRoleOwner || role.Role == types.TenantRoleAdmin
		proposedAllowed := proposedMode == types.ResourceAccessInherit || manager
		if proposedMode == types.ResourceAccessRestricted && !manager {
			_, proposedAllowed = allowedUsers[userID]
		}
		if current.Allowed {
			impact.CurrentlyAllowed++
		}
		if proposedAllowed {
			impact.AllowedAfter++
		}
		if current.Allowed && !proposedAllowed {
			impact.LosingAccess++
		}
		if !current.Allowed && proposedAllowed {
			impact.GainingAccess++
		}
		if manager && current.Allowed && proposedAllowed {
			impact.UnaffectedManagers++
		}
	}
	if proposedMode == types.ResourceAccessRestricted && len(proposed) == 0 {
		impact.Warnings = append(impact.Warnings, "No directory groups are selected; only workspace owners and administrators will retain access.")
	}
	tenantGrants, err := h.groups.ListTenantGroupRoleGrants(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	workspaceRoles := effectiveWorkspaceGrantRoles(tenantGrants)
	for _, grant := range proposed {
		if workspaceRoles[grant.group.ID] == nil {
			impact.Warnings = append(impact.Warnings, "Directory group "+grant.group.DisplayName+" is not associated with this workspace; only users who are already workspace members through another path can benefit from this resource grant.")
		}
	}
	return impact, nil
}

func (h *GroupAccessHandler) proposedAllowedUsers(
	ctx context.Context,
	grants []validatedResourceGrant,
	now time.Time,
) (map[string]struct{}, error) {
	allowed := make(map[string]struct{})
	for _, grant := range grants {
		// A stale directory pauses directory-derived access exactly like the
		// request-time authorizer. The preview must not promise access that the
		// committed policy will immediately deny.
		if grant.directory == nil || !grant.directory.IsFresh(now) {
			continue
		}
		memberships, err := h.directories.ListGroupMemberships(ctx, grant.group.ID)
		if err != nil {
			return nil, err
		}
		for _, membership := range memberships {
			if membership == nil {
				continue
			}
			identity, err := h.directories.GetIdentity(ctx, membership.IdentityID)
			if err != nil {
				return nil, err
			}
			if identity != nil && identity.Status == types.DirectoryObjectActive && identity.UserID != nil && *identity.UserID != "" {
				allowed[*identity.UserID] = struct{}{}
			}
		}
	}
	return allowed, nil
}

func (h *GroupAccessHandler) resolveResource(c *gin.Context) (types.ResourceType, string, uint64, bool) {
	resourceType := types.ResourceType(strings.TrimSpace(c.Param("resource_type")))
	resourceID := strings.TrimSpace(c.Param("resource_id"))
	if !resourceType.IsValid() || resourceID == "" {
		c.Error(apperrors.NewValidationError("resource_type must be knowledge_base or agent and resource_id is required"))
		return "", "", 0, false
	}
	ctx := c.Request.Context()
	tenantID, exists := types.TenantIDFromContext(ctx)
	if !exists || tenantID == 0 {
		c.Error(apperrors.NewForbiddenError("an active workspace is required"))
		return "", "", 0, false
	}
	var err error
	switch resourceType {
	case types.GroupResourceTypeKnowledgeBase:
		_, err = h.kbs.GetKnowledgeBaseByIDAndTenant(ctx, resourceID, tenantID)
	case types.GroupResourceTypeAgent:
		_, err = h.agents.GetAgentByID(ctx, resourceID, tenantID)
	}
	if err != nil {
		// Scope mismatches are intentionally indistinguishable from absence.
		c.Error(apperrors.NewNotFoundError("resource not found in the active workspace"))
		return "", "", 0, false
	}
	return resourceType, resourceID, tenantID, true
}

func (h *GroupAccessHandler) pathTenant(c *gin.Context) (uint64, bool) {
	tenantID, ok := parseTenantIDFromPath(c)
	if !ok {
		return 0, false
	}
	ctx := c.Request.Context()
	activeTenantID, exists := types.TenantIDFromContext(ctx)
	if (!exists || activeTenantID != tenantID) && !types.IsSystemAdminFromContext(ctx) {
		c.Error(apperrors.NewForbiddenError("workspace path does not match the active workspace"))
		return 0, false
	}
	return tenantID, true
}

func (h *GroupAccessHandler) manualTenantGrantByID(ctx context.Context, tenantID uint64, grantID string) (*types.TenantGroupRoleGrant, error) {
	grantID = strings.TrimSpace(grantID)
	if grantID == "" {
		return nil, errGrantNotFound
	}
	grants, err := h.groups.ListTenantGroupRoleGrants(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	for _, grant := range grants {
		if grant.ID != grantID {
			continue
		}
		if grant.Origin != types.GrantOriginManual {
			return nil, errDirectoryGrantLocked
		}
		return grant, nil
	}
	return nil, errGrantNotFound
}

func (h *GroupAccessHandler) findGroup(
	ctx context.Context,
	directoryID string,
	groupID string,
	requireActive bool,
) (*types.DirectoryGroup, *types.Directory, error) {
	groupID = strings.TrimSpace(groupID)
	if groupID == "" {
		return nil, nil, errGroupNotFound
	}
	var directories []*types.Directory
	if directoryID = strings.TrimSpace(directoryID); directoryID != "" {
		directory, err := h.directories.Get(ctx, directoryID)
		if err != nil {
			return nil, nil, err
		}
		if directory == nil {
			return nil, nil, errGroupNotFound
		}
		directories = []*types.Directory{directory}
	} else {
		var err error
		directories, err = h.directories.List(ctx)
		if err != nil {
			return nil, nil, err
		}
	}
	for _, directory := range directories {
		if directory == nil {
			continue
		}
		for offset := 0; ; offset += groupLookupPageSize {
			groups, err := h.directories.ListGroups(ctx, directory.ID, "", offset, groupLookupPageSize)
			if err != nil {
				return nil, nil, err
			}
			for _, group := range groups {
				if group != nil && group.ID == groupID {
					if requireActive && (!directory.Enabled || group.Status != types.DirectoryObjectActive) {
						return nil, nil, errGroupInactive
					}
					return group, directory, nil
				}
			}
			if len(groups) < groupLookupPageSize {
				break
			}
		}
	}
	return nil, nil, errGroupNotFound
}

func (h *GroupAccessHandler) tenantGroupView(
	ctx context.Context,
	grant *types.TenantGroupRoleGrant,
	directory *types.Directory,
	group *types.DirectoryGroup,
) tenantDirectoryGroupView {
	direct, effective, _ := h.membershipCounts(ctx, group.ID)
	nested := h.nestedGroupCount(ctx, directory.ID, group.ID)
	return tenantDirectoryGroupView{
		ID: grant.ID, TenantID: grant.TenantID, DirectoryID: directory.ID, DirectoryGroupID: group.ID,
		ObjectGUID: group.ObjectGUID, SID: group.ObjectSID, DN: group.DN, DisplayName: group.DisplayName,
		Role: grant.Role, Origin: grant.Origin, DirectMemberCount: direct, EffectiveMemberCount: effective,
		NestedGroupCount: nested, UpdatedAt: grant.UpdatedAt,
	}
}

func (h *GroupAccessHandler) tenantGroupCandidate(
	ctx context.Context,
	directory *types.Directory,
	group *types.DirectoryGroup,
	grant *types.TenantGroupRoleGrant,
) tenantDirectoryGroupCandidate {
	direct, effective, _ := h.membershipCounts(ctx, group.ID)
	view := tenantDirectoryGroupCandidate{
		DirectoryID: directory.ID, DirectoryGroupID: group.ID, ObjectGUID: group.ObjectGUID,
		SID: group.ObjectSID, DN: group.DN, DisplayName: group.DisplayName,
		DirectMemberCount: direct, EffectiveMemberCount: effective,
	}
	if grant != nil {
		view.Linked = true
		role := grant.Role
		view.CurrentRole = &role
	}
	return view
}

func (h *GroupAccessHandler) resourceGroupView(
	ctx context.Context,
	directory *types.Directory,
	group *types.DirectoryGroup,
	grant *types.ResourceGroupGrant,
	workspaceRole *types.TenantRole,
) resourceGroupView {
	direct, effective, sources := h.membershipCounts(ctx, group.ID)
	view := resourceGroupView{
		DirectoryID: directory.ID, DirectoryGroupID: group.ID, DisplayName: group.DisplayName, DN: group.DN,
		WorkspaceRole: workspaceRole, DirectMemberCount: direct, EffectiveMemberCount: effective,
		MembershipSources: sources,
	}
	if grant != nil {
		view.ID = grant.ID
		view.Permission = grant.Permission
		view.Origin = grant.Origin
	}
	return view
}

func (h *GroupAccessHandler) membershipCounts(ctx context.Context, groupID string) (int, int, []string) {
	memberships, err := h.directories.ListGroupMemberships(ctx, groupID)
	if err != nil {
		return 0, 0, nil
	}
	direct := 0
	sourcesSet := make(map[string]struct{})
	for _, membership := range memberships {
		if membership == nil {
			continue
		}
		if membership.Direct || membership.Source == types.DirectoryMembershipDirect {
			direct++
		}
		source := string(membership.Source)
		if source == string(types.DirectoryMembershipPrimary) {
			source = "primary"
		}
		if source != "" {
			sourcesSet[source] = struct{}{}
		}
	}
	sources := make([]string, 0, len(sourcesSet))
	for source := range sourcesSet {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	return direct, len(memberships), sources
}

func (h *GroupAccessHandler) nestedGroupCount(ctx context.Context, directoryID, root string) int {
	edges, err := h.directories.ListGroupEdges(ctx, directoryID)
	if err != nil {
		return 0
	}
	children := make(map[string][]string)
	for _, edge := range edges {
		if edge != nil {
			children[edge.ParentGroupID] = append(children[edge.ParentGroupID], edge.ChildGroupID)
		}
	}
	seen := map[string]struct{}{root: {}}
	queue := append([]string(nil), children[root]...)
	count := 0
	for len(queue) > 0 {
		child := queue[0]
		queue = queue[1:]
		if _, exists := seen[child]; exists {
			continue
		}
		seen[child] = struct{}{}
		count++
		queue = append(queue, children[child]...)
	}
	return count
}

func effectiveWorkspaceGrantRoles(grants []*types.TenantGroupRoleGrant) map[string]*types.TenantRole {
	roles := make(map[string]*types.TenantRole)
	for _, grant := range grants {
		if grant == nil || !validDirectoryRole(grant.Role) {
			continue
		}
		current := roles[grant.DirectoryGroupID]
		if current == nil || grant.Role.Level() > current.Level() {
			role := grant.Role
			roles[grant.DirectoryGroupID] = &role
		}
	}
	return roles
}

func validDirectoryRole(role types.TenantRole) bool {
	return role.IsValid() && role != types.TenantRoleOwner
}

func resourceGrantKey(groupID string, permission types.ResourcePermission) string {
	return groupID + "\x00" + string(permission)
}

func boundedQueryLimit(c *gin.Context, fallback int) int {
	limit, err := strconv.Atoi(c.Query("limit"))
	if err != nil || limit <= 0 {
		return fallback
	}
	if limit > groupListLimit {
		return groupListLimit
	}
	return limit
}

func parseBoolQuery(value string) bool {
	parsed, _ := strconv.ParseBool(strings.TrimSpace(value))
	return parsed
}

func (h *GroupAccessHandler) emitAudit(
	ctx context.Context,
	tenantID uint64,
	action types.AuditAction,
	targetType string,
	targetID string,
	details map[string]any,
) {
	if h.audit == nil {
		return
	}
	actor := types.CallerFromContext(ctx)
	if actor.UserID == "" {
		actor.UserID, _ = types.UserIDFromContext(ctx)
	}
	encoded, _ := json.Marshal(details)
	_ = h.audit.Log(ctx, &types.AuditLog{
		TenantID: tenantID, ActorUserID: actor.UserID, ActorRole: string(actor.Role), Action: action,
		TargetType: targetType, TargetID: targetID, Outcome: types.AuditOutcomeSuccess, Details: types.JSON(encoded),
	})
}

func (h *GroupAccessHandler) groupLookupError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, errGroupNotFound):
		c.Error(apperrors.NewNotFoundError("directory group not found"))
	case errors.Is(err, errGroupInactive):
		c.Error(apperrors.NewConflictError("directory group is inactive or its directory is disabled"))
	default:
		h.internalError(c, "look up directory group", err)
	}
}

func (h *GroupAccessHandler) grantLookupError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, errGrantNotFound):
		c.Error(apperrors.NewNotFoundError("group grant not found"))
	case errors.Is(err, errDirectoryGrantLocked):
		c.Error(apperrors.NewConflictError("directory-derived grants are managed by synchronization"))
	default:
		h.internalError(c, "look up group grant", err)
	}
}

func (h *GroupAccessHandler) resourceRequestError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, errGroupNotFound):
		c.Error(apperrors.NewNotFoundError("directory group not found"))
	case errors.Is(err, errGroupInactive):
		c.Error(apperrors.NewConflictError("directory group is inactive or its directory is disabled"))
	case errors.Is(err, errDuplicateGrant):
		c.Error(apperrors.NewValidationError("a directory group may appear only once"))
	default:
		c.Error(apperrors.NewValidationError(err.Error()))
	}
}

func (h *GroupAccessHandler) internalError(c *gin.Context, operation string, err error) {
	logger.ErrorWithFields(c.Request.Context(), err, map[string]interface{}{"operation": operation})
	c.Error(apperrors.NewInternalServerError(operation + " failed").WithDetails(err.Error()))
}
