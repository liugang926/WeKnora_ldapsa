package types

import "time"

// NextcloudOnboardingCredential is installed by an operator, never issued to a
// browser. Its hash only authorizes one instance/tenant/template combination.
type NextcloudOnboardingCredential struct {
	ID                string `json:"id" gorm:"primaryKey"`
	TokenSHA256       string `json:"-"`
	InstanceID        string `json:"instance_id"`
	BaseURL           string `json:"base_url"`
	TenantID          uint64 `json:"tenant_id"`
	DirectoryID       string `json:"directory_id"`
	TemplateKBID      string `json:"processing_template_id"`
	Enabled           bool   `json:"enabled"`
	MaxKnowledgeBases int    `json:"max_knowledge_bases"`
}

func (NextcloudOnboardingCredential) TableName() string { return "nextcloud_onboarding_credentials" }

// Delegation does not create workspace membership or give source writers admin.
type NextcloudOnboardingDelegation struct {
	TenantID    uint64 `gorm:"primaryKey"`
	DirectoryID string `gorm:"primaryKey"`
	ObjectGUID  string `gorm:"primaryKey"`
}

func (NextcloudOnboardingDelegation) TableName() string { return "nextcloud_onboarding_delegations" }

type NextcloudOnboardingOperation struct {
	OperationID          string     `json:"operation_id" gorm:"primaryKey"`
	CredentialID         string     `json:"-"`
	TenantID             uint64     `json:"tenant_id"`
	InstanceID           string     `json:"instance_id"`
	BindingID            string     `json:"binding_id"`
	KnowledgeBaseID      string     `json:"knowledge_base_id"`
	ActorDirectoryID     string     `json:"actor_directory_id"`
	ActorObjectGUID      string     `json:"actor_object_guid"`
	ActorUserID          string     `json:"-"`
	Mode                 string     `json:"mode"`
	Name                 string     `json:"name"`
	ProcessingTemplateID string     `json:"processing_template_id"`
	RequestSHA256        string     `json:"-"`
	State                string     `json:"state"`
	Revision             uint64     `json:"revision"`
	PermissionVersion    uint64     `json:"permission_version"`
	PermissionSHA256     string     `json:"-"`
	PermissionSyncedAt   *time.Time `json:"permission_synced_at,omitempty"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
}

func (NextcloudOnboardingOperation) TableName() string { return "nextcloud_onboarding_operations" }

type NextcloudOnboardingRequest struct {
	OperationID          string `json:"operation_id"`
	TenantID             uint64 `json:"tenant_id"`
	InstanceID           string `json:"instance_id"`
	BindingID            string `json:"binding_id"`
	ActorDirectoryID     string `json:"actor_directory_id"`
	ActorObjectGUID      string `json:"actor_object_guid"`
	Mode                 string `json:"mode"`
	Name                 string `json:"name"`
	KnowledgeBaseID      string `json:"knowledge_base_id"`
	ProcessingTemplateID string `json:"processing_template_id"`
}

type NextcloudGrantSubject struct {
	Kind             string `json:"kind"`
	DirectoryID      string `json:"directory_id"`
	ObjectGUID       string `json:"object_guid"`
	SourceRelationID string `json:"source_relation_id"`
}
type NextcloudGrantSnapshot struct {
	PermissionVersion uint64                  `json:"permission_version"`
	PublicationEpoch  int64                   `json:"publication_epoch"`
	Complete          bool                    `json:"complete"`
	Subjects          []NextcloudGrantSubject `json:"subjects"`
}

// Each relation is retained separately, even when two relations grant the same
// principal. No row is projected to tenant_members or tenant_group_role_grants.
type NextcloudSourceGrant struct {
	OperationID       string `gorm:"primaryKey"`
	SourceRelationID  string `gorm:"primaryKey"`
	Kind              string `gorm:"primaryKey"`
	DirectoryID       string `gorm:"primaryKey"`
	ObjectGUID        string `gorm:"primaryKey"`
	TenantID          uint64
	KnowledgeBaseID   string
	PrincipalID       string
	PermissionVersion uint64
	PublicationEpoch  int64
	Origin            string
}

func (NextcloudSourceGrant) TableName() string { return "nextcloud_source_grants" }

type NextcloudScopedGrantMatch struct {
	TenantID        uint64
	KnowledgeBaseID string
	DirectoryID     string
}
