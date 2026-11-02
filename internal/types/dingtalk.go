package types

import "time"

// DingTalk identity binding status. Bindings are keyed by the stable
// (corp_id, dingtalk_user_id) pair, never by job number: a legitimate job
// number change updates the snapshot but keeps the same linked user.
const (
	// DingTalkIdentityActive marks a binding eligible for login.
	DingTalkIdentityActive = "active"
	// DingTalkIdentityUnlinked marks a binding whose user link was removed.
	DingTalkIdentityUnlinked = "unlinked"
)

// DingTalk audit event types (dingtalk_auth_events.event_type).
const (
	DingTalkEventBind           = "bind"
	DingTalkEventUnbind         = "unbind"
	DingTalkEventLoginSuccess   = "login_success"
	DingTalkEventLoginRejected  = "login_rejected"
	DingTalkEventSnapshotUpdate = "snapshot_update"
)

// Machine-readable rejection reasons shared by the audit trail and the
// #dingtalk_error= redirect fragment.
const (
	DingTalkRejectDisabled           = "dingtalk_disabled"
	DingTalkRejectWrongCorp          = "wrong_corp"
	DingTalkRejectNotEnterpriseMember = "not_enterprise_member"
	DingTalkRejectUserInactive       = "user_inactive"
	DingTalkRejectEmptyJobNumber     = "empty_job_number"
	DingTalkRejectNoADMatch          = "no_ad_match"
	DingTalkRejectDuplicateJobNumber = "duplicate_job_number"
	DingTalkRejectBindingConflict    = "binding_conflict"
	DingTalkRejectADOrUserDisabled   = "ad_or_user_disabled"
	DingTalkRejectUnavailable        = "dingtalk_unavailable"
)

// DingTalkIdentity is the durable link between one DingTalk enterprise user
// (corp_id + DingTalk user id) and a WeKnora user. Job number is stored as a
// binding-time snapshot only — the string is preserved verbatim (including
// leading zeros) because it is matched exactly against
// directory_identities.sam_account_name.
type DingTalkIdentity struct {
	// Surrogate primary key; the natural identity key is
	// (corp_id, dingtalk_user_id).
	ID uint64 `json:"id" gorm:"primaryKey;autoIncrement"`
	// CorpID is the DingTalk enterprise identifier the binding belongs to.
	CorpID string `json:"corp_id" gorm:"type:varchar(64);not null;uniqueIndex:uq_dingtalk_identities_dingtalk,priority:1"`
	// DingTalkUserID is the stable per-corp user identifier from DingTalk.
	DingTalkUserID string `json:"dingtalk_user_id" gorm:"column:dingtalk_user_id;type:varchar(64);not null;uniqueIndex:uq_dingtalk_identities_dingtalk,priority:2"`
	// UnionID is the cross-app DingTalk user identifier snapshot.
	UnionID string `json:"union_id,omitempty" gorm:"type:varchar(64);not null;default:''"`
	// JobNumber is the AD-linked employee number snapshot taken at bind time.
	JobNumber string `json:"job_number,omitempty" gorm:"type:varchar(128);not null;default:''"`
	// DisplayName is the DingTalk profile name snapshot.
	DisplayName string `json:"display_name,omitempty" gorm:"type:varchar(256);not null;default:''"`
	// UserID links to users.id; NULL means the binding is not linked.
	UserID *string `json:"user_id,omitempty" gorm:"type:varchar(36)"`
	// Status is active or unlinked. The partial unique index
	// uq_dingtalk_identities_user enforces at most one active binding per user.
	Status string `json:"status" gorm:"type:varchar(16);not null;default:'active'"`
	// BoundAt records when the user link was established.
	BoundAt time.Time `json:"bound_at"`
	// UnboundAt records when the user link was removed.
	UnboundAt *time.Time `json:"unbound_at,omitempty"`
	// LastLoginAt records the most recent successful DingTalk login.
	LastLoginAt *time.Time `json:"last_login_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// TableName names the DingTalk identity binding table.
func (DingTalkIdentity) TableName() string { return "dingtalk_identities" }

// DingTalkAuthEvent is the append-only audit trail for the DingTalk SSO
// module (bind / unbind / login_success / login_rejected / snapshot_update).
type DingTalkAuthEvent struct {
	ID             uint64    `json:"id" gorm:"primaryKey;autoIncrement"`
	EventType      string    `json:"event_type" gorm:"type:varchar(32);not null"`
	CorpID         string    `json:"corp_id" gorm:"type:varchar(64);not null;default:''"`
	DingTalkUserID string    `json:"dingtalk_user_id" gorm:"column:dingtalk_user_id;type:varchar(64);not null;default:''"`
	JobNumber      string    `json:"job_number,omitempty" gorm:"type:varchar(128);not null;default:''"`
	UserID         string    `json:"user_id,omitempty" gorm:"type:varchar(36);not null;default:''"`
	Reason         string    `json:"reason,omitempty" gorm:"type:varchar(64);not null;default:''"`
	Detail         string    `json:"detail,omitempty" gorm:"type:varchar(512);not null;default:''"`
	CreatedAt      time.Time `json:"created_at"`
}

// TableName names the DingTalk audit event table.
func (DingTalkAuthEvent) TableName() string { return "dingtalk_auth_events" }

// DingTalkConfigResponse is the public provider discovery payload for
// /auth/dingtalk/config. CorpID is omitted when the module is disabled.
type DingTalkConfigResponse struct {
	Success             bool   `json:"success"`
	Enabled             bool   `json:"enabled"`
	ProviderDisplayName string `json:"provider_display_name,omitempty"`
	CorpID              string `json:"corp_id,omitempty"`
}

// DingTalkAuthURLResponse is the payload for /auth/dingtalk/url. State and
// the nonce stay out of the JSON body: the nonce is bound to an HttpOnly
// cookie and the signed state travels in the authorization URL only.
type DingTalkAuthURLResponse struct {
	Success             bool   `json:"success"`
	ProviderDisplayName string `json:"provider_display_name,omitempty"`
	AuthorizationURL    string `json:"authorization_url,omitempty"`
	State               string `json:"state,omitempty"`
	// Nonce is bound to an HttpOnly cookie on /auth/dingtalk/url and verified
	// on callback; omitted from JSON so clients cannot replay it alone.
	Nonce string `json:"-"`
}
