package handler

import (
	"context"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// Project the narrow resource grant separately from legacy ownership/share
// metadata so an edit grant cannot accidentally enable management controls.
func projectedGroupPermission(
	ctx context.Context, groups interfaces.GroupAccessService,
	tenantID uint64, resourceType types.ResourceType, resourceID string,
) types.ResourcePermission {
	if groups == nil {
		return ""
	}
	action := types.ResourceActionRead
	permission := types.ResourcePermissionRead
	if resourceType == types.GroupResourceTypeAgent {
		action = types.ResourceActionUse
		permission = types.ResourcePermissionUse
	}
	read, err := groups.EffectivePermission(ctx, tenantID, resourceType, resourceID, action, time.Now().UTC())
	if err != nil || read.Mode != types.ResourceAccessRestricted || !read.Allowed {
		return ""
	}
	edit, err := groups.EffectivePermission(
		ctx, tenantID, resourceType, resourceID, types.ResourceActionEdit, time.Now().UTC(),
	)
	if err == nil && edit.Allowed {
		return types.ResourcePermissionEdit
	}
	return permission
}
