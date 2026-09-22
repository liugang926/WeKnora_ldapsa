package types

import "time"

type GrantOrigin string

const (
	GrantOriginManual    GrantOrigin = "manual"
	GrantOriginDirectory GrantOrigin = "directory"
)

type ResourceType string

const (
	GroupResourceTypeKnowledgeBase ResourceType = "knowledge_base"
	GroupResourceTypeAgent         ResourceType = "agent"
)

func (t ResourceType) IsValid() bool {
	return t == GroupResourceTypeKnowledgeBase || t == GroupResourceTypeAgent
}

type ResourceAccessMode string

const (
	ResourceAccessInherit    ResourceAccessMode = "inherit"
	ResourceAccessRestricted ResourceAccessMode = "restricted"
)

func (m ResourceAccessMode) IsValid() bool {
	return m == ResourceAccessInherit || m == ResourceAccessRestricted
}

type ResourcePermission string

const (
	ResourcePermissionRead ResourcePermission = "read"
	ResourcePermissionEdit ResourcePermission = "edit"
	ResourcePermissionUse  ResourcePermission = "use"
)

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

type ResourceAction string

const (
	ResourceActionRead   ResourceAction = "read"
	ResourceActionUse    ResourceAction = "use"
	ResourceActionEdit   ResourceAction = "edit"
	ResourceActionManage ResourceAction = "manage"
)

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

func (TenantGroupRoleGrant) TableName() string { return "tenant_group_role_grants" }

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

func (ResourceAccessPolicy) TableName() string { return "resource_access_policies" }

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

func (ResourceGroupGrant) TableName() string { return "resource_group_grants" }

// PermissionVersion is a tenant-scoped monotonic invalidation token. Every
// snapshot/grant/policy mutation increments it transactionally; cache layers
// may include it in their key without coupling this package to Redis.
type PermissionVersion struct {
	TenantID  uint64    `json:"tenant_id" gorm:"primaryKey"`
	Version   uint64    `json:"version" gorm:"not null;default:0"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (PermissionVersion) TableName() string { return "directory_permission_versions" }

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

func (m ResourceGroupMatch) Fresh(now time.Time) bool {
	return GroupRoleMatch{
		DirectoryEnabled: m.DirectoryEnabled, LastSuccessfulSyncAt: m.LastSuccessfulSyncAt,
		StaleAfterSeconds: m.StaleAfterSeconds,
	}.Fresh(now)
}

type EffectiveTenantRole struct {
	TenantID     uint64           `json:"tenant_id"`
	Member       bool             `json:"member"`
	Role         TenantRole       `json:"role,omitempty"`
	DirectRole   *TenantRole      `json:"direct_role,omitempty"`
	GroupMatches []GroupRoleMatch `json:"group_matches,omitempty"`
}

type EffectiveResourcePermission struct {
	Allowed       bool                 `json:"allowed"`
	Mode          ResourceAccessMode   `json:"mode"`
	Action        ResourceAction       `json:"action"`
	EffectiveRole EffectiveTenantRole  `json:"effective_role"`
	GroupMatches  []ResourceGroupMatch `json:"group_matches,omitempty"`
	Reason        string               `json:"reason"`
}

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
