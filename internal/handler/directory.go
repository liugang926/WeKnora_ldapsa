package handler

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/Tencent/WeKnora/internal/application/service"
	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/handler/dto"
	ldapdirectory "github.com/Tencent/WeKnora/internal/infrastructure/directory"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// DirectoryHandler exposes the single-domain LDAP/AD administration surface
// and the public LDAP login endpoint. System-admin authorization belongs to
// the router group, matching the existing system handler convention.
type DirectoryHandler struct {
	runtime interfaces.DirectoryRuntimeService
}

// NewDirectoryHandler exposes directory administration and login routes.
func NewDirectoryHandler(runtime interfaces.DirectoryRuntimeService) *DirectoryHandler {
	return &DirectoryHandler{runtime: runtime}
}

// RegisterAdminRoutes is optional wiring sugar for the system-admin group.
// The caller must attach the existing authentication and SystemAdmin guards
// before invoking it.
func (h *DirectoryHandler) RegisterAdminRoutes(group *gin.RouterGroup) {
	group.GET("/directory/config", h.GetConfig)
	group.PUT("/directory/config", h.UpdateConfig)
	group.GET("/directory/status", h.GetStatus)
	group.POST("/directory/test", h.TestConnection)
	group.GET("/directory/users", h.QueryUsers)
	group.GET("/directory/groups", h.QueryGroups)
	group.GET("/directory/groups/:object_guid/members", h.QueryGroupMembers)
	group.POST("/directory/sync/preview", h.PreviewSync)
	group.POST("/directory/sync", h.ManualSync)
	group.GET("/directory/sync/runs", h.ListSyncRuns)
	group.POST("/directory/identities/:object_guid/link", h.LinkIdentity)
	group.DELETE("/directory/identities/:object_guid/link", h.UnlinkIdentity)
}

// GetConfig returns the redacted directory configuration.
func (h *DirectoryHandler) GetConfig(c *gin.Context) {
	result, err := h.runtime.GetConfig(c.Request.Context())
	if err != nil {
		directoryHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// UpdateConfig saves UI-managed directory settings.
func (h *DirectoryHandler) UpdateConfig(c *gin.Context) {
	var request types.DirectoryAdminConfigUpdate
	if err := c.ShouldBindJSON(&request); err != nil {
		_ = c.Error(
			apperrors.NewValidationError("Invalid directory configuration").
				WithDetails(err.Error()),
		)
		return
	}
	result, err := h.runtime.UpdateConfig(c.Request.Context(), &request)
	if err != nil {
		directoryHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// GetStatus returns the latest directory synchronization state.
func (h *DirectoryHandler) GetStatus(c *gin.Context) {
	result, err := h.runtime.GetStatus(c.Request.Context())
	if err != nil {
		directoryHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// TestConnection verifies connectivity and a paged directory search.
func (h *DirectoryHandler) TestConnection(c *gin.Context) {
	var request types.DirectoryAdminConfigUpdate
	if err := c.ShouldBindJSON(&request); err != nil {
		_ = c.Error(
			apperrors.NewValidationError("Invalid directory test configuration").
				WithDetails(err.Error()),
		)
		return
	}
	result, err := h.runtime.TestConnection(c.Request.Context(), &request)
	if err != nil {
		directoryHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// QueryUsers searches the committed directory user catalog.
func (h *DirectoryHandler) QueryUsers(c *gin.Context) {
	result, err := h.runtime.QueryUsers(
		c.Request.Context(),
		c.Query("q"),
		directoryQueryLimit(c),
		directoryQueryOffset(c),
	)
	if err != nil {
		directoryHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// QueryGroups searches the committed directory group catalog.
func (h *DirectoryHandler) QueryGroups(c *gin.Context) {
	result, err := h.runtime.QueryGroups(
		c.Request.Context(),
		c.Query("q"),
		directoryQueryLimit(c),
		directoryQueryOffset(c),
	)
	if err != nil {
		directoryHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// QueryGroupMembers returns direct and inherited members of a group.
func (h *DirectoryHandler) QueryGroupMembers(c *gin.Context) {
	result, err := h.runtime.QueryGroupMembers(
		c.Request.Context(),
		c.Param("object_guid"),
		c.Query("q"),
		directoryQueryLimit(c),
		directoryQueryOffset(c),
	)
	if err != nil {
		directoryHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

func directoryQueryOffset(c *gin.Context) int {
	offset, err := strconv.Atoi(c.DefaultQuery("offset", "0"))
	if err != nil || offset < 0 {
		return 0
	}
	return offset
}

// TenantCatalog exposes selectable directory objects to workspace managers.
func (h *DirectoryHandler) TenantCatalog(c *gin.Context) {
	result, err := h.runtime.Catalog(
		c.Request.Context(),
		c.Param("kind"),
		c.Query("q"),
		directoryQueryLimit(c),
		directoryQueryOffset(c),
	)
	if err != nil {
		directoryHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// TenantCatalogGroupMembers previews a selectable group's effective members.
func (h *DirectoryHandler) TenantCatalogGroupMembers(c *gin.Context) {
	result, err := h.runtime.CatalogGroupMembers(
		c.Request.Context(),
		c.Param("object_guid"),
		c.Query("q"),
		directoryQueryLimit(c),
		directoryQueryOffset(c),
	)
	if err != nil {
		directoryHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// AddTenantDirectoryMember links an AD account to a workspace member.
func (h *DirectoryHandler) AddTenantDirectoryMember(c *gin.Context) {
	tenantID, ok := parseTenantIDFromPath(c)
	if !ok {
		return
	}
	var request struct {
		ObjectGUID string           `json:"object_guid" binding:"required"`
		Role       types.TenantRole `json:"role" binding:"required"`
	}
	if err := c.ShouldBindJSON(&request); err != nil {
		_ = c.Error(apperrors.NewValidationError("object_guid and role are required"))
		return
	}
	member, err := h.runtime.AddTenantDirectoryMember(
		c.Request.Context(),
		tenantID,
		request.ObjectGUID,
		request.Role,
	)
	if errors.Is(err, service.ErrMembershipAlreadyExists) ||
		errors.Is(err, service.ErrDirectoryIdentityLinkRequired) {
		_ = c.Error(apperrors.NewConflictError(err.Error()))
		return
	}
	if err != nil {
		directoryHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": member})
}

// PreviewSync reports pending directory changes without applying them.
func (h *DirectoryHandler) PreviewSync(c *gin.Context) {
	result, err := h.runtime.PreviewSync(c.Request.Context())
	if err != nil {
		directoryHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// ManualSync runs a complete directory synchronization.
func (h *DirectoryHandler) ManualSync(c *gin.Context) {
	result, err := h.runtime.ManualSync(c.Request.Context())
	if err != nil {
		directoryHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// ListSyncRuns returns directory synchronization history.
func (h *DirectoryHandler) ListSyncRuns(c *gin.Context) {
	result, err := h.runtime.ListSyncRuns(c.Request.Context(), directoryQueryLimit(c))
	if err != nil {
		directoryHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// LinkIdentity explicitly associates a directory identity with a local user.
func (h *DirectoryHandler) LinkIdentity(c *gin.Context) {
	var request types.DirectoryIdentityLinkRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		_ = c.Error(apperrors.NewValidationError("user_id is required"))
		return
	}
	if err := h.runtime.LinkIdentity(c.Request.Context(), c.Param("object_guid"), request.UserID); err != nil {
		directoryHTTPError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

// UnlinkIdentity removes an explicit directory-to-user association.
func (h *DirectoryHandler) UnlinkIdentity(c *gin.Context) {
	if err := h.runtime.UnlinkIdentity(c.Request.Context(), c.Param("object_guid")); err != nil {
		directoryHTTPError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func directoryQueryLimit(c *gin.Context) int {
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if err != nil || limit <= 0 {
		return 20
	}
	if limit > 100 {
		return 100
	}
	return limit
}

// LDAPLogin returns exactly the same HTTP-safe response shape used by local
// password login and OIDC callbacks. Authentication errors are intentionally
// generic except for the explicit administrator-link conflict.
func (h *DirectoryHandler) LDAPLogin(c *gin.Context) {
	var request types.DirectoryLoginRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		_ = c.Error(apperrors.NewValidationError("Identifier and password are required"))
		return
	}
	if strings.TrimSpace(request.Identifier) == "" || request.Password == "" {
		_ = c.Error(apperrors.NewValidationError("Identifier and password are required"))
		return
	}
	result, err := h.runtime.Login(c.Request.Context(), request.Identifier, request.Password)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrDirectoryIdentityLinkRequired):
			_ = c.Error(
				apperrors.NewConflictError(
					"Directory identity conflicts with an existing account; ask an administrator to link it",
				),
			)
		case errors.Is(err, service.ErrDirectoryDisabled),
			errors.Is(err, service.ErrDirectoryUnavailable),
			errors.Is(err, service.ErrDirectoryMembershipMismatch),
			errors.Is(err, service.ErrDirectoryNotConfigured),
			errors.Is(err, ldapdirectory.ErrInvalidServiceCredentials),
			errors.Is(err, ldapdirectory.ErrIncompleteResults),
			errors.Is(err, ldapdirectory.ErrMembershipCycle),
			errors.Is(err, ldapdirectory.ErrInvalidMembershipGraph),
			errors.Is(err, ldapdirectory.ErrInvalidDirectoryObject),
			errors.Is(err, ldapdirectory.ErrDuplicateDirectoryObject),
			isDirectoryFailoverError(err):
			_ = c.Error(
				apperrors.NewServiceUnavailableError(
					"Directory authentication is temporarily unavailable",
				),
			)
		case errors.Is(err, service.ErrDirectoryIdentityUnavailable):
			_ = c.Error(
				apperrors.NewForbiddenError(
					"Directory account is disabled or outside the allowed login scope",
				),
			)
		case errors.Is(err, ldapdirectory.ErrInvalidCredentials),
			errors.Is(err, ldapdirectory.ErrUserNotFound),
			errors.Is(err, ldapdirectory.ErrAmbiguousUser),
			errors.Is(err, ldapdirectory.ErrUserDisabled):
			_ = c.Error(apperrors.NewUnauthorizedError("Directory login failed"))
		default:
			_ = c.Error(
				apperrors.NewServiceUnavailableError(
					"Directory authentication is temporarily unavailable",
				),
			)
		}
		return
	}
	c.JSON(http.StatusOK, dto.NewAuthLoginResponse(result))
}

func isDirectoryFailoverError(err error) bool {
	var failover *ldapdirectory.FailoverError
	return errors.As(err, &failover)
}

func directoryHTTPError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, service.ErrDirectoryFileManaged):
		_ = c.Error(
			apperrors.NewConflictError(
				"Directory configuration is managed by deployment files and is read-only",
			),
		)
	case errors.Is(err, service.ErrDirectoryIdentityAlreadyLinked):
		_ = c.Error(
			apperrors.NewConflictError("Directory identity is already linked to a different user"),
		)
	case errors.Is(err, service.ErrDirectoryIdentityUnavailable):
		_ = c.Error(
			apperrors.NewNotFoundError("Directory identity was not found in the active snapshot"),
		)
	case errors.Is(err, service.ErrDirectorySyncInProgress):
		_ = c.Error(apperrors.NewConflictError("A directory synchronization is already running"))
	case errors.Is(err, service.ErrInvalidDirectoryConfig),
		errors.Is(err, service.ErrDirectoryEncryptionKey):
		_ = c.Error(apperrors.NewValidationError(err.Error()))
	case errors.Is(err, service.ErrDirectoryDisabled),
		errors.Is(err, service.ErrDirectoryUnavailable),
		errors.Is(err, service.ErrDirectoryNotConfigured):
		_ = c.Error(apperrors.NewServiceUnavailableError(err.Error()))
	case errors.Is(err, ldapdirectory.ErrInvalidServiceCredentials),
		errors.Is(err, ldapdirectory.ErrIncompleteResults),
		errors.Is(err, ldapdirectory.ErrMembershipCycle):
		_ = c.Error(
			apperrors.NewServiceUnavailableError("Directory operation failed").
				WithDetails(err.Error()),
		)
	default:
		_ = c.Error(
			apperrors.NewInternalServerError("Directory operation failed").WithDetails(err.Error()),
		)
	}
}
