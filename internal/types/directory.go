package types

import (
	"time"
)

// DirectoryProtocol identifies the server-side directory dialect. AD keeps
// the LDAP wire protocol but needs AD-specific attributes such as objectGUID,
// objectSid and primaryGroupID.
type DirectoryProtocol string

const (
	// DirectoryProtocolLDAP identifies a generic LDAP directory.
	DirectoryProtocolLDAP DirectoryProtocol = "ldap"
	// DirectoryProtocolAD enables Active Directory-specific semantics.
	DirectoryProtocolAD DirectoryProtocol = "active_directory"
)

// IsValid reports whether the directory protocol is supported.
func (p DirectoryProtocol) IsValid() bool {
	return p == DirectoryProtocolLDAP || p == DirectoryProtocolAD
}

// DirectoryTLSMode deliberately has no plaintext option. A deployment must
// use either implicit TLS (LDAPS) or upgrade the connection with StartTLS.
type DirectoryTLSMode string

const (
	// DirectoryTLSLDAPS uses implicit TLS.
	DirectoryTLSLDAPS DirectoryTLSMode = "ldaps"
	// DirectoryTLSStartTLS upgrades an LDAP connection to TLS.
	DirectoryTLSStartTLS DirectoryTLSMode = "starttls"
)

// IsValid reports whether the encrypted directory transport is supported.
func (m DirectoryTLSMode) IsValid() bool {
	return m == DirectoryTLSLDAPS || m == DirectoryTLSStartTLS
}

// DirectoryConfigSource identifies the authority that manages connection settings.
type DirectoryConfigSource string

const (
	// DirectoryConfigSourceDatabase stores UI-managed configuration in the database.
	DirectoryConfigSourceDatabase DirectoryConfigSource = "database"
	// DirectoryConfigSourceUI is a source-compatible name for callers that
	// describe the same database-managed mode from the management UI.
	DirectoryConfigSourceUI DirectoryConfigSource = DirectoryConfigSourceDatabase
	// DirectoryConfigSourceFile makes deployment-file settings read-only in the UI.
	DirectoryConfigSourceFile DirectoryConfigSource = "file"
)

// DirectoryObjectStatus tracks whether an identity or group belongs to the active snapshot.
type DirectoryObjectStatus string

const (
	// DirectoryObjectActive is available for login and group authorization.
	DirectoryObjectActive DirectoryObjectStatus = "active"
	// DirectoryObjectDisabled cannot authenticate.
	DirectoryObjectDisabled DirectoryObjectStatus = "disabled"
	// DirectoryObjectOutOfScope was absent from the latest complete snapshot.
	DirectoryObjectOutOfScope DirectoryObjectStatus = "out_of_scope"
	// DirectoryObjectConflict requires administrator resolution.
	DirectoryObjectConflict DirectoryObjectStatus = "conflict"
)

// DirectorySyncStatus records the state of a synchronization attempt.
type DirectorySyncStatus string

const (
	// DirectorySyncRunning indicates an active synchronization.
	DirectorySyncRunning DirectorySyncStatus = "running"
	// DirectorySyncSuccess indicates a complete applied snapshot.
	DirectorySyncSuccess DirectorySyncStatus = "success"
	// DirectorySyncFailed indicates a failed snapshot attempt.
	DirectorySyncFailed DirectorySyncStatus = "failed"
)

// DirectorySyncTrigger identifies why synchronization started.
type DirectorySyncTrigger string

const (
	// DirectorySyncTriggerManual starts from an administrator action.
	DirectorySyncTriggerManual DirectorySyncTrigger = "manual"
	// DirectorySyncTriggerScheduled starts from the background timer.
	DirectorySyncTriggerScheduled DirectorySyncTrigger = "scheduled"
	// DirectorySyncTriggerLogin starts from an authentication-time refresh.
	DirectorySyncTriggerLogin DirectorySyncTrigger = "login"
)

// Directory stores non-secret connection settings alongside an encrypted
// service-account credential. PasswordCiphertext and EnterpriseCAPEM are
// intentionally omitted from JSON so list/get APIs cannot accidentally expose
// either the credential or private PKI material.
type Directory struct {
	ID                        string                `json:"id" gorm:"type:varchar(36);primaryKey"`
	Name                      string                `json:"name" gorm:"type:varchar(128);not null"`
	Protocol                  DirectoryProtocol     `json:"protocol" gorm:"type:varchar(32);not null"`
	Enabled                   bool                  `json:"enabled" gorm:"not null;default:false;index"`
	ConfigSource              DirectoryConfigSource `json:"config_source" gorm:"type:varchar(16);not null;default:'database'"` //nolint:lll // struct tag
	TLSMode                   DirectoryTLSMode      `json:"tls_mode" gorm:"type:varchar(16);not null"`
	ServerURLs                StringArray           `json:"server_urls" gorm:"type:jsonb;not null;default:'[]'"`
	ServerNames               StringArray           `json:"server_names,omitempty" gorm:"type:jsonb;not null;default:'[]'"` //nolint:lll // struct tag
	BaseDN                    string                `json:"base_dn" gorm:"type:text;not null"`
	UserBaseDN                string                `json:"user_base_dn" gorm:"type:text;not null"`
	GroupBaseDN               string                `json:"group_base_dn" gorm:"type:text;not null"`
	UserFilter                string                `json:"user_filter" gorm:"type:text;not null"`
	GroupFilter               string                `json:"group_filter" gorm:"type:text;not null"`
	AllowedLoginFilter        string                `json:"allowed_login_filter,omitempty" gorm:"type:text;not null;default:''"` //nolint:lll // struct tag
	ServiceAccountDN          string                `json:"service_account_dn" gorm:"type:text;not null"`
	PasswordCiphertext        string                `json:"-" gorm:"column:password_ciphertext;type:text;not null"`
	EnterpriseCAPEM           string                `json:"-" gorm:"column:enterprise_ca_pem;type:text"`
	SecurityConfigFingerprint string                `json:"-" gorm:"column:security_config_fingerprint;type:varchar(64);not null;default:''"` //nolint:lll // struct tag
	ConnectTimeoutSeconds     int                   `json:"connect_timeout_seconds" gorm:"not null;default:5"`
	QueryTimeoutSeconds       int                   `json:"query_timeout_seconds" gorm:"not null;default:10"`
	PageSize                  int                   `json:"page_size" gorm:"not null;default:500"`
	ResultLimit               int                   `json:"result_limit" gorm:"not null;default:10000"`
	SyncIntervalSeconds       int                   `json:"sync_interval_seconds" gorm:"not null;default:300"`
	StaleAfterSeconds         int                   `json:"stale_after_seconds" gorm:"not null;default:900"`
	ConfigVersion             uint64                `json:"config_version" gorm:"not null;default:1"`
	SnapshotVersion           uint64                `json:"snapshot_version" gorm:"not null;default:0"`
	LastSuccessfulSyncAt      *time.Time            `json:"last_successful_sync_at,omitempty"`
	LastSyncAttemptAt         *time.Time            `json:"last_sync_attempt_at,omitempty"`
	LastSyncError             string                `json:"last_sync_error,omitempty" gorm:"type:text"`
	SyncLeaseOwner            string                `json:"-" gorm:"type:varchar(64);not null;default:''"`
	SyncLeaseExpiresAt        *time.Time            `json:"-"`
	CreatedAt                 time.Time             `json:"created_at"`
	UpdatedAt                 time.Time             `json:"updated_at"`
}

// TableName names the directory configuration table.
func (Directory) TableName() string { return "directories" }

// IsFileManaged reports whether deployment files own this configuration.
func (d Directory) IsFileManaged() bool { return d.ConfigSource == DirectoryConfigSourceFile }

// IsFresh reports whether the last complete snapshot remains within the stale window.
func (d Directory) IsFresh(now time.Time) bool {
	if !d.Enabled || d.LastSuccessfulSyncAt == nil {
		return false
	}
	staleAfter := d.StaleAfterSeconds
	if staleAfter <= 0 {
		staleAfter = 900
	}
	return !now.After(d.LastSuccessfulSyncAt.Add(time.Duration(staleAfter) * time.Second))
}

// DirectoryIdentity is the durable link between one directory object and a
// WeKnora user. DirectoryID + ObjectGUID, not DN/email/name, is the identity
// key so OU moves and renames do not create duplicate accounts.
type DirectoryIdentity struct {
	ID              string                `json:"id" gorm:"type:varchar(36);primaryKey"`
	DirectoryID     string                `json:"directory_id" gorm:"type:varchar(36);not null;index"`
	ObjectGUID      string                `json:"object_guid" gorm:"type:varchar(128);not null"`
	ObjectSID       string                `json:"object_sid,omitempty" gorm:"column:object_sid;type:varchar(256)"`
	DN              string                `json:"dn" gorm:"type:text;not null"`
	SAMAccountName  string                `json:"sam_account_name,omitempty" gorm:"type:varchar(256);index"`
	UPN             string                `json:"upn,omitempty" gorm:"type:varchar(320);index"`
	DisplayName     string                `json:"display_name,omitempty" gorm:"type:varchar(512)"`
	Email           string                `json:"email,omitempty" gorm:"type:varchar(320)"`
	PrimaryGroupSID string                `json:"primary_group_sid,omitempty" gorm:"column:primary_group_sid;type:varchar(256)"` //nolint:lll // struct tag
	UserID          *string               `json:"user_id,omitempty" gorm:"type:varchar(36);index"`
	Status          DirectoryObjectStatus `json:"status" gorm:"type:varchar(24);not null;default:'active';index"`
	DisabledReason  string                `json:"disabled_reason,omitempty" gorm:"type:text"`
	SnapshotVersion uint64                `json:"snapshot_version" gorm:"not null;default:0"`
	LastSeenAt      time.Time             `json:"last_seen_at"`
	CreatedAt       time.Time             `json:"created_at"`
	UpdatedAt       time.Time             `json:"updated_at"`
}

// TableName names the directory identities table.
func (DirectoryIdentity) TableName() string { return "directory_identities" }

// DirectoryGroup stores a stable AD group identity and its latest attributes.
type DirectoryGroup struct {
	ID              string                `json:"id" gorm:"type:varchar(36);primaryKey"`
	DirectoryID     string                `json:"directory_id" gorm:"type:varchar(36);not null;index"`
	ObjectGUID      string                `json:"object_guid" gorm:"type:varchar(128);not null"`
	ObjectSID       string                `json:"object_sid,omitempty" gorm:"column:object_sid;type:varchar(256);index"`
	DN              string                `json:"dn" gorm:"type:text;not null"`
	SAMAccountName  string                `json:"sam_account_name,omitempty" gorm:"type:varchar(256);index"`
	DisplayName     string                `json:"display_name" gorm:"type:varchar(512);not null"`
	Email           string                `json:"email,omitempty" gorm:"type:varchar(320)"`
	Status          DirectoryObjectStatus `json:"status" gorm:"type:varchar(24);not null;default:'active';index"`
	SnapshotVersion uint64                `json:"snapshot_version" gorm:"not null;default:0"`
	LastSeenAt      time.Time             `json:"last_seen_at"`
	CreatedAt       time.Time             `json:"created_at"`
	UpdatedAt       time.Time             `json:"updated_at"`
}

// TableName names the directory groups table.
func (DirectoryGroup) TableName() string { return "directory_groups" }

// DirectoryGroupEdge preserves the directory hierarchy. ParentGroupID is the
// containing group and ChildGroupID is its direct nested group.
type DirectoryGroupEdge struct {
	ID              uint64    `json:"id" gorm:"primaryKey;autoIncrement"`
	DirectoryID     string    `json:"directory_id" gorm:"type:varchar(36);not null;index"`
	ParentGroupID   string    `json:"parent_group_id" gorm:"type:varchar(36);not null;index"`
	ChildGroupID    string    `json:"child_group_id" gorm:"type:varchar(36);not null;index"`
	SnapshotVersion uint64    `json:"snapshot_version" gorm:"not null"`
	CreatedAt       time.Time `json:"created_at"`
}

// TableName names the group hierarchy table.
func (DirectoryGroupEdge) TableName() string { return "directory_group_edges" }

// DirectoryMembershipSource records the provenance of a group membership.
type DirectoryMembershipSource string

const (
	// DirectoryMembershipDirect is a direct member edge.
	DirectoryMembershipDirect DirectoryMembershipSource = "direct"
	// DirectoryMembershipNested is inherited from a child group.
	DirectoryMembershipNested DirectoryMembershipSource = "nested"
	// DirectoryMembershipPrimary is an AD primary-group edge.
	DirectoryMembershipPrimary DirectoryMembershipSource = "primary_group"
)

// DirectoryGroupMembership stores the flattened effective membership used on
// the request hot path. Direct/Primary/Depth preserve provenance for the UI.
type DirectoryGroupMembership struct {
	ID              uint64                    `json:"id" gorm:"primaryKey;autoIncrement"`
	DirectoryID     string                    `json:"directory_id" gorm:"type:varchar(36);not null;index"`
	GroupID         string                    `json:"group_id" gorm:"type:varchar(36);not null;index"`
	IdentityID      string                    `json:"identity_id" gorm:"type:varchar(36);not null;index"`
	Direct          bool                      `json:"direct" gorm:"not null;default:false"`
	Primary         bool                      `json:"primary" gorm:"not null;default:false"`
	Depth           int                       `json:"depth" gorm:"not null;default:0"`
	Source          DirectoryMembershipSource `json:"source" gorm:"type:varchar(24);not null"`
	SnapshotVersion uint64                    `json:"snapshot_version" gorm:"not null"`
	CreatedAt       time.Time                 `json:"created_at"`
}

// TableName names the effective membership table.
func (DirectoryGroupMembership) TableName() string { return "directory_group_memberships" }

// DirectoryLoginSnapshot is a transactionally consistent view of the latest
// complete directory snapshot used immediately after a live LDAP bind. Only
// groups and memberships from Directory.SnapshotVersion are included.
type DirectoryLoginSnapshot struct {
	Directory                 *Directory
	Identity                  *DirectoryIdentity
	EffectiveGroupObjectGUIDs []string
}

// DirectorySyncRun records one synchronization attempt and its outcome.
type DirectorySyncRun struct {
	ID              string               `json:"id" gorm:"type:varchar(36);primaryKey"`
	DirectoryID     string               `json:"directory_id" gorm:"type:varchar(36);not null;index"`
	Status          DirectorySyncStatus  `json:"status" gorm:"type:varchar(16);not null;index"`
	Trigger         DirectorySyncTrigger `json:"trigger" gorm:"type:varchar(16);not null;default:'manual'"`
	SnapshotVersion uint64               `json:"snapshot_version" gorm:"not null;default:0"`
	UserCount       int                  `json:"user_count" gorm:"not null;default:0"`
	GroupCount      int                  `json:"group_count" gorm:"not null;default:0"`
	MembershipCount int                  `json:"membership_count" gorm:"not null;default:0"`
	ErrorCode       string               `json:"error_code,omitempty" gorm:"type:varchar(64)"`
	ErrorMessage    string               `json:"error_message,omitempty" gorm:"type:text"`
	StartedAt       time.Time            `json:"started_at"`
	CompletedAt     *time.Time           `json:"completed_at,omitempty"`
	CreatedAt       time.Time            `json:"created_at"`
}

// TableName names the directory synchronization runs table.
func (DirectorySyncRun) TableName() string { return "directory_sync_runs" }

// DirectoryIdentitySnapshot is adapter-neutral input to atomic persistence.
// The snapshot types are adapter-neutral input to the atomic persistence
// boundary. LDAP/AD adapters may collect pages/fail over independently, but
// ApplySnapshot accepts only a complete, validated result.
type DirectoryIdentitySnapshot struct {
	ObjectGUID      string
	ObjectSID       string
	DN              string
	SAMAccountName  string
	UPN             string
	DisplayName     string
	Email           string
	PrimaryGroupSID string
	Enabled         bool
}

// DirectoryGroupSnapshot carries one group in a complete directory snapshot.
type DirectoryGroupSnapshot struct {
	ObjectGUID     string
	ObjectSID      string
	DN             string
	SAMAccountName string
	DisplayName    string
	Email          string
}

// DirectoryGroupEdgeSnapshot carries one nested-group relationship.
type DirectoryGroupEdgeSnapshot struct {
	ParentGroupObjectGUID string
	ChildGroupObjectGUID  string
}

// DirectoryMembershipSnapshot carries a user-to-group membership.
type DirectoryMembershipSnapshot struct {
	GroupObjectGUID string
	UserObjectGUID  string
	Direct          bool
	Primary         bool
	Depth           int
	Source          DirectoryMembershipSource
}

// DirectorySnapshot contains a complete, validated synchronization result.
type DirectorySnapshot struct {
	DirectoryID           string
	ExpectedConfigVersion uint64
	Complete              bool
	PaginationComplete    bool
	Identities            []DirectoryIdentitySnapshot
	Groups                []DirectoryGroupSnapshot
	GroupEdges            []DirectoryGroupEdgeSnapshot
	// Memberships supplied by an adapter are direct/primary seed edges. The
	// directory service validates the hierarchy and replaces this slice with
	// the flattened direct+nested set before it reaches the repository.
	Memberships []DirectoryMembershipSnapshot
	Anomalies   []string
	StartedAt   time.Time
	Trigger     DirectorySyncTrigger
}

// DirectorySnapshotResult reports the applied run and affected workspaces.
type DirectorySnapshotResult struct {
	SyncRun         *DirectorySyncRun `json:"sync_run"`
	AffectedTenants map[uint64]uint64 `json:"affected_tenant_versions"`
}
