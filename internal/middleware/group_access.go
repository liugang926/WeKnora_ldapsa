package middleware

import (
	"net/http"
	"strings"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
)

// GroupResourceIDResolver resolves the durable resource identifier used by a
// resource_access_policies row. It may walk from a document/chunk to its KB.
type GroupResourceIDResolver = func(*gin.Context) (string, error)

// RequireGroupResourceAccess is the uniform resource-policy overlay. An
// absent service (module disabled) and inherit-mode policy preserve existing
// behaviour; restricted mode requires a verifiable web user and a matching
// fresh directory-group grant (workspace Owner/Admin remain managers).
func RequireGroupResourceAccess(
	resourceType types.ResourceType,
	action types.ResourceAction,
	resolveID GroupResourceIDResolver,
	groupAccess interfaces.GroupAccessService,
) gin.HandlerFunc {
	return func(c *gin.Context) {
		if groupAccess == nil {
			c.Next()
			return
		}
		resourceID, err := resolveID(c)
		if err != nil {
			_ = c.Error(err)
			c.Abort()
			return
		}
		resourceID = strings.TrimSpace(resourceID)
		tenantID, ok := types.TenantIDFromContext(c.Request.Context())
		if !ok || tenantID == 0 || resourceID == "" {
			_ = c.Error(apperrors.NewUnauthorizedError("resource authorization context missing"))
			c.Abort()
			return
		}
		permission, err := groupAccess.EffectivePermission(
			c.Request.Context(), tenantID, resourceType, resourceID, action, time.Now().UTC(),
		)
		if err != nil {
			logger.Errorf(c.Request.Context(), "resource group authorization failed: %v", err)
			_ = c.Error(apperrors.NewServiceUnavailableError("cannot verify resource access right now"))
			c.Abort()
			return
		}
		if !permission.Allowed {
			logger.Warnf(c.Request.Context(), "resource group access denied type=%s id=%s action=%s reason=%s",
				resourceType, resourceID, action, permission.Reason)
			c.JSON(http.StatusForbidden, gin.H{"error": "Forbidden: directory group grant required"})
			c.Abort()
			return
		}
		c.Next()
	}
}
