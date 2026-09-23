package directory

import (
	"crypto/tls"
	"time"
)

// TLSMode selects the encrypted LDAP transport. Plaintext LDAP is
// deliberately unsupported: callers must choose either implicit TLS (LDAPS)
// or an LDAP connection upgraded with StartTLS.
type TLSMode string

const (
	// TLSModeLDAPS establishes TLS before sending LDAP requests.
	TLSModeLDAPS TLSMode = "ldaps"
	// TLSModeStartTLS upgrades an LDAP connection before authentication.
	TLSModeStartTLS TLSMode = "starttls"
)

// Controller is one domain controller in failover order.
type Controller struct {
	URL        string
	TLSMode    TLSMode
	ServerName string
}

// TLSOptions controls server certificate validation. InsecureSkipVerify is
// intentionally not exposed. CAPEM and CAFile extend the host trust store so
// private enterprise CAs can be used without weakening hostname validation.
type TLSOptions struct {
	CAPEM      []byte
	CAFile     string
	MinVersion uint16
}

// Config is the transport and search configuration consumed by Adapter. It is
// independent of WeKnora's deployment configuration so file-, environment-,
// and database-backed config resolvers can all construct the same adapter.
type Config struct {
	DirectoryID string
	Controllers []Controller

	BindDN       string
	BindPassword string
	UserBaseDN   string
	GroupBaseDN  string
	UserFilter   string
	GroupFilter  string
	// LoginFilter is an LDAP filter template whose {login} placeholders are
	// replaced with an RFC 4515-escaped identifier. It is always ANDed with
	// UserFilter so a custom login selector cannot escape the configured user
	// and allowed-login scope.
	LoginFilter string

	ConnectTimeout time.Duration
	QueryTimeout   time.Duration
	PageSize       uint32
	MaxPages       int
	ResultLimit    int
	TLS            TLSOptions
}

const (
	defaultConnectTimeout = 5 * time.Second
	defaultQueryTimeout   = 15 * time.Second
	defaultPageSize       = uint32(500)
	defaultMaxPages       = 1000
	defaultResultLimit    = 100_000
	defaultUserFilter     = "(&(objectCategory=person)(objectClass=user))"
	defaultGroupFilter    = "(objectClass=group)"
	defaultLoginFilter    = "(|(sAMAccountName={login})(userPrincipalName={login}))"
)

func (c Config) withDefaults() Config {
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = defaultConnectTimeout
	}
	if c.QueryTimeout <= 0 {
		c.QueryTimeout = defaultQueryTimeout
	}
	if c.PageSize == 0 {
		c.PageSize = defaultPageSize
	}
	if c.MaxPages <= 0 {
		c.MaxPages = defaultMaxPages
	}
	if c.ResultLimit <= 0 {
		c.ResultLimit = defaultResultLimit
	}
	if c.UserFilter == "" {
		c.UserFilter = defaultUserFilter
	}
	if c.GroupFilter == "" {
		c.GroupFilter = defaultGroupFilter
	}
	if c.LoginFilter == "" {
		c.LoginFilter = defaultLoginFilter
	}
	if c.TLS.MinVersion == 0 {
		c.TLS.MinVersion = tls.VersionTLS12
	}
	return c
}

// User is the stable directory projection used by identity reconciliation.
// ObjectGUID, not DN/email/name, is the identity key.
type User struct {
	ObjectGUID        string
	SID               string
	DN                string
	SAMAccountName    string
	UserPrincipalName string
	DisplayName       string
	Email             string
	Enabled           bool
	PrimaryGroupRID   uint32
}

// Group is the stable directory group projection.
type Group struct {
	ObjectGUID     string
	SID            string
	DN             string
	SAMAccountName string
	DisplayName    string
	Email          string
}

// MembershipSource distinguishes directory-native direct membership from an
// AD primary-group edge. Nested is produced only by the closure algorithm.
type MembershipSource string

const (
	// MembershipDirect denotes an explicit user-to-group edge.
	MembershipDirect MembershipSource = "direct"
	// MembershipPrimary denotes an AD primary-group edge.
	MembershipPrimary MembershipSource = "primary"
	// MembershipNested denotes membership inherited through a child group.
	MembershipNested MembershipSource = "nested"
)

// UserGroupMembership is a seed edge obtained directly from AD.
type UserGroupMembership struct {
	UserGUID  string
	GroupGUID string
	Source    MembershipSource
}

// GroupMembership means MemberGroupGUID is nested in ParentGroupGUID.
type GroupMembership struct {
	MemberGroupGUID string
	ParentGroupGUID string
}

// EffectiveMembership is one explainable path from a direct/primary seed to
// an effective group. Multiple origins may legitimately yield multiple rows;
// exact duplicate paths are collapsed to their shortest depth.
type EffectiveMembership struct {
	UserGUID        string
	GroupGUID       string
	Source          MembershipSource
	OriginSource    MembershipSource
	OriginGroupGUID string
	Depth           int
}

// UnresolvedMember records a group member DN outside the selected user/group
// search scopes (for example a computer or foreign security principal). It is
// preserved for preview/diagnostics instead of silently becoming an edge.
type UnresolvedMember struct {
	ParentGroupGUID string
	MemberDN        string
}

// Snapshot is complete and safe to apply atomically. Adapter.Sync never
// returns a partial Snapshot alongside an error.
type Snapshot struct {
	DirectoryID          string
	ControllerURL        string
	CompletedAt          time.Time
	Users                []User
	Groups               []Group
	DirectMemberships    []UserGroupMembership
	GroupMemberships     []GroupMembership
	EffectiveMemberships []EffectiveMembership
	UnresolvedMembers    []UnresolvedMember
}

// AuthenticationResult contains the resolved immutable directory identity.
// Passwords are never retained.
type AuthenticationResult struct {
	User                      User
	EffectiveGroupObjectGUIDs []string
	ControllerURL             string
}
