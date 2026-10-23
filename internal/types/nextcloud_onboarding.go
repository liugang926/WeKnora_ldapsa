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

// TableName returns the persistent table for this source control record.
func (NextcloudOnboardingCredential) TableName() string { return "nextcloud_onboarding_credentials" }

// NextcloudOnboardingDelegation authorizes one directory actor without adding workspace membership.
// Delegation does not create workspace membership or give source writers admin.
type NextcloudOnboardingDelegation struct {
	TenantID    uint64 `gorm:"primaryKey"`
	DirectoryID string `gorm:"primaryKey"`
	ObjectGUID  string `gorm:"primaryKey"`
}

// TableName returns the persistent table for this source control record.
func (NextcloudOnboardingDelegation) TableName() string { return "nextcloud_onboarding_delegations" }

// NextcloudOnboardingOperation records the exact recoverable source onboarding request.
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

// TableName returns the persistent table for this source control record.
func (NextcloudOnboardingOperation) TableName() string { return "nextcloud_onboarding_operations" }

// NextcloudOnboardingRequest selects one instance, actor, target and processing template.
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
	CandidateRevision    string `json:"candidate_revision,omitempty"`
}

// NextcloudKnowledgeBaseCandidate is a server-filtered selectable target or a
// redacted explanation for why a visible target cannot be selected.
type NextcloudKnowledgeBaseCandidate struct {
	ID                     string `json:"id"`
	Name                   string `json:"name"`
	ContentCount           int64  `json:"content_count"`
	Eligible               bool   `json:"eligible"`
	ReasonCode             string `json:"reason_code,omitempty"`
	Reason                 string `json:"reason,omitempty"`
	NextAction             string `json:"next_action,omitempty"`
	CanTakeOver            bool   `json:"can_take_over"`
	ProcessingTemplateID   string `json:"processing_template_id"`
	ProcessingTemplateName string `json:"processing_template_name,omitempty"`
	ConfigurationSummary   string `json:"configuration_summary,omitempty"`
}

// NextcloudCandidatePage is a bounded, stable candidate observation. The
// revision covers all candidates in the scoped workspace, not only this page.
type NextcloudCandidatePage struct {
	ContractVersion        int                               `json:"contract_version"`
	RequestID              string                            `json:"request_id"`
	CandidateRevision      string                            `json:"candidate_revision"`
	TenantID               uint64                            `json:"tenant_id"`
	ProcessingTemplateID   string                            `json:"processing_template_id"`
	ProcessingTemplateName string                            `json:"processing_template_name,omitempty"`
	GraphMode              string                            `json:"graph_mode"`
	Candidates             []NextcloudKnowledgeBaseCandidate `json:"candidates"`
	Ineligible             []NextcloudKnowledgeBaseCandidate `json:"ineligible"`
	NextCursor             string                            `json:"next_cursor,omitempty"`
	Total                  int                               `json:"total"`
}

// NextcloudGrantSubject names one original Nextcloud sharing relation.
type NextcloudGrantSubject struct {
	Kind             string `json:"kind"`
	DirectoryID      string `json:"directory_id"`
	ObjectGUID       string `json:"object_guid"`
	SourceRelationID string `json:"source_relation_id"`
}

// NextcloudGrantSnapshot carries a complete versioned source sharing snapshot.
type NextcloudGrantSnapshot struct {
	PermissionVersion uint64                  `json:"permission_version"`
	PublicationEpoch  int64                   `json:"publication_epoch"`
	Complete          bool                    `json:"complete"`
	Subjects          []NextcloudGrantSubject `json:"subjects"`
}

// NextcloudSourceGrant retains one sharing relation without creating workspace membership.
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

// TableName returns the persistent table for this source control record.
func (NextcloudSourceGrant) TableName() string { return "nextcloud_source_grants" }

// NextcloudScopedGrantMatch identifies a matching scoped knowledge-base grant.
type NextcloudScopedGrantMatch struct {
	TenantID        uint64
	KnowledgeBaseID string
	DirectoryID     string
}
