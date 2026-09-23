package types

import "time"

// DirectoryAdminServer is the secret-free controller representation exposed
// by the system-administration API. Controllers are always tried in order.
type DirectoryAdminServer struct {
	Address    string `json:"address"`
	ServerName string `json:"server_name,omitempty"`
}

// DirectoryAdminConfig is intentionally separate from Directory: it mirrors
// the public management contract and cannot serialize the stored bind secret
// or an enterprise CA PEM value.
type DirectoryAdminConfig struct {
	Enabled               bool                   `json:"enabled"`
	DisplayName           string                 `json:"display_name"`
	Servers               []DirectoryAdminServer `json:"servers"`
	Transport             DirectoryTLSMode       `json:"transport"`
	CAFile                string                 `json:"ca_file,omitempty"`
	BaseDN                string                 `json:"base_dn"`
	UserBaseDN            string                 `json:"user_base_dn,omitempty"`
	GroupBaseDN           string                 `json:"group_base_dn,omitempty"`
	BindDN                string                 `json:"bind_dn"`
	UserFilter            string                 `json:"user_filter"`
	GroupFilter           string                 `json:"group_filter"`
	AllowedLoginFilter    string                 `json:"allowed_login_filter,omitempty"`
	LoginAttributes       []string               `json:"login_attributes"`
	ConnectTimeoutSeconds int                    `json:"connect_timeout_seconds"`
	QueryTimeoutSeconds   int                    `json:"query_timeout_seconds"`
	ResultLimit           int                    `json:"result_limit"`
	PageSize              int                    `json:"page_size"`
	SyncIntervalSeconds   int                    `json:"sync_interval_seconds"`
	StaleAfterSeconds     int                    `json:"stale_after_seconds"`
	HasBindPassword       bool                   `json:"has_bind_password"`
	Source                string                 `json:"source"`
	ReadOnlyFields        []string               `json:"read_only_fields"`
	BindPasswordSource    string                 `json:"bind_password_source,omitempty"`
	CASource              string                 `json:"ca_source,omitempty"`
}

// DirectoryAdminConfigUpdate accepts a replacement bind secret write-only.
// Omission preserves the current database-backed secret.
type DirectoryAdminConfigUpdate struct {
	Enabled               bool                   `json:"enabled"`
	DisplayName           string                 `json:"display_name"`
	Servers               []DirectoryAdminServer `json:"servers"`
	Transport             DirectoryTLSMode       `json:"transport"`
	CAFile                string                 `json:"ca_file,omitempty"`
	BaseDN                string                 `json:"base_dn"`
	UserBaseDN            string                 `json:"user_base_dn,omitempty"`
	GroupBaseDN           string                 `json:"group_base_dn,omitempty"`
	BindDN                string                 `json:"bind_dn"`
	BindPassword          *string                `json:"bind_password,omitempty"`
	UserFilter            string                 `json:"user_filter"`
	GroupFilter           string                 `json:"group_filter"`
	AllowedLoginFilter    string                 `json:"allowed_login_filter,omitempty"`
	LoginAttributes       []string               `json:"login_attributes"`
	ConnectTimeoutSeconds int                    `json:"connect_timeout_seconds"`
	QueryTimeoutSeconds   int                    `json:"query_timeout_seconds"`
	ResultLimit           int                    `json:"result_limit"`
	PageSize              int                    `json:"page_size"`
	SyncIntervalSeconds   int                    `json:"sync_interval_seconds"`
	StaleAfterSeconds     int                    `json:"stale_after_seconds"`
}

// DirectoryHealth reports availability and freshness of the directory module.
type DirectoryHealth struct {
	Enabled             bool       `json:"enabled"`
	Available           bool       `json:"available"`
	AccessPaused        bool       `json:"access_paused"`
	Syncing             bool       `json:"syncing"`
	ActiveServer        string     `json:"active_server,omitempty"`
	LastAttemptAt       *time.Time `json:"last_attempt_at,omitempty"`
	LastSuccessAt       *time.Time `json:"last_success_at,omitempty"`
	LastError           string     `json:"last_error,omitempty"`
	ConsecutiveFailures int        `json:"consecutive_failures,omitempty"`
	NextSyncAt          *time.Time `json:"next_sync_at,omitempty"`
}

// DirectoryTestResult reports a redacted connection-test outcome.
type DirectoryTestResult struct {
	OK        bool   `json:"ok"`
	Server    string `json:"server,omitempty"`
	LatencyMS int64  `json:"latency_ms,omitempty"`
	Message   string `json:"message,omitempty"`
}

// DirectoryObjectSummary identifies a selectable directory user or group.
type DirectoryObjectSummary struct {
	DirectoryID       string `json:"directory_id"`
	IdentityID        string `json:"identity_id,omitempty"`
	LinkedUserID      string `json:"linked_user_id,omitempty"`
	ObjectGUID        string `json:"object_guid"`
	SID               string `json:"sid,omitempty"`
	DN                string `json:"dn"`
	DisplayName       string `json:"display_name"`
	Email             string `json:"email,omitempty"`
	AccountName       string `json:"account_name,omitempty"`
	UserPrincipalName string `json:"user_principal_name,omitempty"`
	Disabled          bool   `json:"disabled,omitempty"`
	Status            string `json:"status,omitempty"`
}

// DirectoryGroupSummary includes group hierarchy and member counts.
type DirectoryGroupSummary struct {
	DirectoryObjectSummary
	DirectMemberCount    int `json:"direct_member_count,omitempty"`
	EffectiveMemberCount int `json:"effective_member_count,omitempty"`
	ParentGroupCount     int `json:"parent_group_count,omitempty"`
}

// DirectoryObjectSearchResult pages directory user search results.
type DirectoryObjectSearchResult struct {
	Items     []DirectoryObjectSummary `json:"items"`
	Total     int                      `json:"total"`
	Truncated bool                     `json:"truncated,omitempty"`
	Warning   string                   `json:"warning,omitempty"`
}

// DirectoryGroupSearchResult pages directory group search results.
type DirectoryGroupSearchResult struct {
	Items     []DirectoryGroupSummary `json:"items"`
	Total     int                     `json:"total"`
	Truncated bool                    `json:"truncated,omitempty"`
	Warning   string                  `json:"warning,omitempty"`
}

// DirectoryChangeCounts summarizes changes in a synchronization preview.
type DirectoryChangeCounts struct {
	Create    int `json:"create"`
	Update    int `json:"update"`
	Disable   int `json:"disable"`
	Remove    int `json:"remove"`
	Add       int `json:"add"`
	Unchanged int `json:"unchanged"`
}

// DirectoryMembershipOrigin explains a direct, primary, or nested group path.
type DirectoryMembershipOrigin struct {
	Source       string `json:"source"`
	OriginSource string `json:"origin_source"`
	Depth        int    `json:"depth"`
	// One shortest path per origin, in member-group to parent-group order.
	Path []DirectoryObjectSummary `json:"path"`
}

// DirectoryGroupMember includes an effective member and its provenance.
type DirectoryGroupMember struct {
	DirectoryObjectSummary
	Origins []DirectoryMembershipOrigin `json:"origins"`
}

// DirectoryGroupMembersResult includes effective members and adjacent groups.
type DirectoryGroupMembersResult struct {
	Group                 DirectoryObjectSummary   `json:"group"`
	Items                 []DirectoryGroupMember   `json:"items"`
	Total                 int                      `json:"total"`
	ParentGroups          []DirectoryObjectSummary `json:"parent_groups"`
	ChildGroups           []DirectoryObjectSummary `json:"child_groups"`
	UnresolvedMemberCount int                      `json:"unresolved_member_count"`
}

// DirectoryCatalogItem is a selectable user or group from the committed snapshot.
type DirectoryCatalogItem struct {
	DirectoryObjectSummary
	DirectoryGroupID string `json:"directory_group_id,omitempty"`
}

// DirectoryCatalogResult pages objects visible to workspace managers.
type DirectoryCatalogResult struct {
	Items               []DirectoryCatalogItem `json:"items"`
	Total               int                    `json:"total"`
	Enabled             bool                   `json:"enabled"`
	Fresh               bool                   `json:"fresh"`
	LastSuccessAt       *time.Time             `json:"last_success_at,omitempty"`
	SyncIntervalSeconds int                    `json:"sync_interval_seconds"`
}

// DirectorySyncPreview describes a proposed snapshot without applying it.
type DirectorySyncPreview struct {
	Users       DirectoryChangeCounts `json:"users"`
	Groups      DirectoryChangeCounts `json:"groups"`
	Memberships DirectoryChangeCounts `json:"memberships"`
	Warnings    []string              `json:"warnings,omitempty"`
	Complete    bool                  `json:"complete"`
}

// DirectorySyncRunView adapts the durable synchronization row to the public
// admin API without leaking implementation-specific error codes.
type DirectorySyncRunView struct {
	ID              string     `json:"id"`
	Trigger         string     `json:"trigger"`
	Status          string     `json:"status"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
	UsersSeen       int        `json:"users_seen,omitempty"`
	GroupsSeen      int        `json:"groups_seen,omitempty"`
	MembershipsSeen int        `json:"memberships_seen,omitempty"`
	Error           string     `json:"error,omitempty"`
	Warnings        []string   `json:"warnings,omitempty"`
}

// DirectorySyncRunsResponse pages the administrator synchronization history.
type DirectorySyncRunsResponse struct {
	Runs  []DirectorySyncRunView `json:"runs"`
	Total int                    `json:"total"`
}

// DirectoryLoginRequest carries a username and password for LDAP bind.
type DirectoryLoginRequest struct {
	Identifier string `json:"identifier" binding:"required"`
	Password   string `json:"password" binding:"required"`
}

// DirectoryIdentityLinkRequest identifies an explicit local-account link.
type DirectoryIdentityLinkRequest struct {
	UserID string `json:"user_id" binding:"required"`
}

const (
	// AuditActionDirectoryConfigChanged records a connection configuration change.
	AuditActionDirectoryConfigChanged AuditAction = "directory.config_changed"
	// AuditActionDirectorySyncCompleted records a successful synchronization.
	AuditActionDirectorySyncCompleted AuditAction = "directory.sync_completed"
	// AuditActionDirectorySyncFailed records a failed synchronization.
	AuditActionDirectorySyncFailed AuditAction = "directory.sync_failed"
	// AuditActionDirectoryIdentityLinked records an explicit account link.
	AuditActionDirectoryIdentityLinked AuditAction = "directory.identity_linked"
	// AuditActionDirectoryIdentityUnlinked records a removed account link.
	AuditActionDirectoryIdentityUnlinked AuditAction = "directory.identity_unlinked"
)
