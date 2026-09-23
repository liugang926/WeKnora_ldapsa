package types

import "time"

// GrantOrigin distinguishes manual grants from directory-managed grants.
type GrantOrigin string

const (
	// GrantOriginManual is managed by a workspace administrator.
	GrantOriginManual GrantOrigin = "manual"
	// GrantOriginDirectory is managed by directory synchronization.
	GrantOriginDirectory GrantOrigin = "directory"
)

// ResourceType identifies a resource that supports group-level access.
type ResourceType string

const (
	// GroupResourceTypeKnowledgeBase identifies a knowledge base.
	GroupResourceTypeKnowledgeBase ResourceType = "knowledge_base"
	// GroupResourceTypeAgent identifies a custom agent.
	GroupResourceTypeAgent ResourceType = "agent"
)

// IsValid reports whether group access supports this resource type.
func (t ResourceType) IsValid() bool {
	return t == GroupResourceTypeKnowledgeBase || t == GroupResourceTypeAgent
}

// ResourceAccessMode selects inherited or group-restricted access.
type ResourceAccessMode string

const (
	// ResourceAccessInherit uses the workspace access rules.
	ResourceAccessInherit ResourceAccessMode = "inherit"
	// ResourceAccessRestricted requires an explicit resource group grant.
	ResourceAccessRestricted ResourceAccessMode = "restricted"
)

// IsValid reports whether the resource access mode is supported.
func (m ResourceAccessMode) IsValid() bool {
	return m == ResourceAccessInherit || m == ResourceAccessRestricted
}

// ResourcePermission is the level granted to a directory group.
type ResourcePermission string

const (
	// ResourcePermissionRead permits knowledge-base reads.
	ResourcePermissionRead ResourcePermission = "read"
	// ResourcePermissionEdit permits resource editing.
	ResourcePermissionEdit ResourcePermission = "edit"
	// ResourcePermissionUse permits agent execution.
	ResourcePermissionUse ResourcePermission = "use"
)

// ValidFor reports whether the permission applies to the resource type.
func (p ResourcePermission) ValidFor(resourceType ResourceType) bool {
	switch resourceType {
	case GroupResourceTypeKnowledgeBase:
		return p == ResourcePermissionRead || p == ResourcePermissionEdit
	case GroupResourceTypeAgent:
		return p == ResourcePermissionUse || p == ResourcePermissionEdit
	default:
		return false
	}
}

// ResourceAction is the operation checked at the authorization boundary.
type ResourceAction string

const (
	// ResourceActionRead reads knowledge-base content.
	ResourceActionRead ResourceAction = "read"
	// ResourceActionUse executes an agent.
	ResourceActionUse ResourceAction = "use"
	// ResourceActionEdit edits a resource.
	ResourceActionEdit ResourceAction = "edit"
	// ResourceActionManage changes grants or ownership.
	ResourceActionManage ResourceAction = "manage"
)

// ValidFor reports whether the action applies to the resource type.
func (a ResourceAction) ValidFor(resourceType ResourceType) bool {
	switch resourceType {
	case GroupResourceTypeKnowledgeBase:
		return a == ResourceActionRead || a == ResourceActionEdit || a == ResourceActionManage
	case GroupResourceTypeAgent:
		return a == ResourceActionUse || a == ResourceActionEdit || a == ResourceActionManage
	default:
		return false
	}
}

// TenantGroupRoleGrant maps one directory group to a workspace role.
type TenantGroupRoleGrant struct {
	ID               string      `json:"id" gorm:"type:varchar(36);primaryKey"`
	TenantID         uint64      `json:"tenant_id" gorm:"not null;index"`
	DirectoryGroupID string      `json:"directory_group_id" gorm:"type:varchar(36);not null;index"`
	Role             TenantRole  `json:"role" gorm:"type:varchar(20);not null"`
	Origin           GrantOrigin `json:"origin" gorm:"type:varchar(16);not null;default:'manual'"`
	CreatedBy        string      `json:"created_by,omitempty" gorm:"type:varchar(36)"`
	CreatedAt        time.Time   `json:"created_at"`
	UpdatedAt        time.Time   `json:"updated_at"`
}

// TableName names the workspace group grant table.
func (TenantGroupRoleGrant) TableName() string { return "tenant_group_role_grants" }

// ResourceAccessPolicy records a resource's inherited or restricted mode.
type ResourceAccessPolicy struct {
	ID           string             `json:"id" gorm:"type:varchar(36);primaryKey"`
	TenantID     uint64             `json:"tenant_id" gorm:"not null;index"`
	ResourceType ResourceType       `json:"resource_type" gorm:"type:varchar(32);not null"`
	ResourceID   string             `json:"resource_id" gorm:"type:varchar(64);not null"`
	Mode         ResourceAccessMode `json:"mode" gorm:"type:varchar(16);not null;default:'inherit'"`
	UpdatedBy    string             `json:"updated_by,omitempty" gorm:"type:varchar(36)"`
	CreatedAt    time.Time          `json:"created_at"`
	UpdatedAt    time.Time          `json:"updated_at"`
}

// TableName names the resource access policy table.
func (ResourceAccessPolicy) TableName() string { return "resource_access_policies" }

// ResourceGroupGrant maps a directory group to a resource permission.
type ResourceGroupGrant struct {
	ID               string             `json:"id" gorm:"type:varchar(36);primaryKey"`
	TenantID         uint64             `json:"tenant_id" gorm:"not null;index"`
	ResourceType     ResourceType       `json:"resource_type" gorm:"type:varchar(32);not null"`
	ResourceID       string             `json:"resource_id" gorm:"type:varchar(64);not null"`
	DirectoryGroupID string             `json:"directory_group_id" gorm:"type:varchar(36);not null;index"`
	Permission       ResourcePermission `json:"permission" gorm:"type:varchar(16);not null"`
	Origin           GrantOrigin        `json:"origin" gorm:"type:varchar(16);not null;default:'manual'"`
	CreatedBy        string             `json:"created_by,omitempty" gorm:"type:varchar(36)"`
	CreatedAt        time.Time          `json:"created_at"`
	UpdatedAt        time.Time          `json:"updated_at"`
}

// TableName names the resource group grant table.
func (ResourceGroupGrant) TableName() string { return "resource_group_grants" }

// PermissionVersion is a tenant-scoped monotonic invalidation token. Every
// snapshot/grant/policy mutation increments it transactionally; cache layers
// may include it in their key without coupling this package to Redis.
type PermissionVersion struct {
	TenantID  uint64    `json:"tenant_id" gorm:"primaryKey"`
	Version   uint64    `json:"version" gorm:"not null;default:0"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TableName names the tenant permission version table.
func (PermissionVersion) TableName() string { return "directory_permission_versions" }

// GroupRoleMatch explains a workspace role derived from an AD group.
type GroupRoleMatch struct {
	DirectoryID          string                    `json:"directory_id"`
	DirectoryGroupID     string                    `json:"directory_group_id"`
	GroupDisplayName     string                    `json:"group_display_name"`
	Role                 TenantRole                `json:"role"`
	MembershipSource     DirectoryMembershipSource `json:"membership_source"`
	MembershipDepth      int                       `json:"membership_depth"`
	DirectoryEnabled     bool                      `json:"-"`
	LastSuccessfulSyncAt *time.Time                `json:"-"`
	StaleAfterSeconds    int                       `json:"-"`
}

// Fresh reports whether this group's directory snapshot remains valid.
func (m GroupRoleMatch) Fresh(now time.Time) bool {
	if !m.DirectoryEnabled || m.LastSuccessfulSyncAt == nil {
		return false
	}
	seconds := m.StaleAfterSeconds
	if seconds <= 0 {
		seconds = 900
	}
	return !now.After(m.LastSuccessfulSyncAt.Add(time.Duration(seconds) * time.Second))
}

// ResourceGroupMatch explains a resource grant derived from an AD group.
type ResourceGroupMatch struct {
	DirectoryID          string                    `json:"directory_id"`
	DirectoryGroupID     string                    `json:"directory_group_id"`
	GroupDisplayName     string                    `json:"group_display_name"`
	Permission           ResourcePermission        `json:"permission"`
	MembershipSource     DirectoryMembershipSource `json:"membership_source"`
	MembershipDepth      int                       `json:"membership_depth"`
	DirectoryEnabled     bool                      `json:"-"`
	LastSuccessfulSyncAt *time.Time                `json:"-"`
	StaleAfterSeconds    int                       `json:"-"`
}

// Fresh reports whether this group's directory snapshot remains valid.
func (m ResourceGroupMatch) Fresh(now time.Time) bool {
	return GroupRoleMatch{
		DirectoryEnabled: m.DirectoryEnabled, LastSuccessfulSyncAt: m.LastSuccessfulSyncAt,
		StaleAfterSeconds: m.StaleAfterSeconds,
	}.Fresh(now)
}

// EffectiveTenantRole combines direct and group-derived workspace membership.
type EffectiveTenantRole struct {
	TenantID     uint64           `json:"tenant_id"`
	Member       bool             `json:"member"`
	Role         TenantRole       `json:"role,omitempty"`
	DirectRole   *TenantRole      `json:"direct_role,omitempty"`
	GroupMatches []GroupRoleMatch `json:"group_matches,omitempty"`
}

// EffectiveResourcePermission reports a resource authorization decision.
type EffectiveResourcePermission struct {
	Allowed       bool                 `json:"allowed"`
	Mode          ResourceAccessMode   `json:"mode"`
	Action        ResourceAction       `json:"action"`
	EffectiveRole EffectiveTenantRole  `json:"effective_role"`
	GroupMatches  []ResourceGroupMatch `json:"group_matches,omitempty"`
	Reason        string               `json:"reason"`
}

// ResourceAccessImpactPreview estimates access changes before a policy update.
type ResourceAccessImpactPreview struct {
	TenantID              uint64             `json:"tenant_id"`
	ResourceType          ResourceType       `json:"resource_type"`
	ResourceID            string             `json:"resource_id"`
	CurrentMode           ResourceAccessMode `json:"current_mode"`
	ProposedMode          ResourceAccessMode `json:"proposed_mode"`
	WorkspaceUserCount    int                `json:"workspace_user_count"`
	AllowedUserCount      int                `json:"allowed_user_count"`
	AffectedUserCount     int                `json:"affected_user_count"`
	AffectedUserIDs       []string           `json:"affected_user_ids,omitempty"`
	MissingTenantGroupIDs []string           `json:"missing_tenant_group_ids,omitempty"`
}
