package types

// NextcloudOnboardingContractVersion is the versioned machine contract used by
// the Nextcloud integration. Additive fields remain readable by older clients.
const NextcloudOnboardingContractVersion = 3

// NextcloudConnectionWorkspace is one workspace that a Nextcloud administrator
// may authorize for directory bindings. The processing template is selected by
// WeKnora and is intentionally opaque to ordinary directory users.
type NextcloudConnectionWorkspace struct {
	TenantID             uint64 `json:"tenant_id"`
	TenantName           string `json:"tenant_name"`
	ProcessingTemplateID string `json:"processing_template_id,omitempty"`
	ProcessingTemplate   string `json:"processing_template_name,omitempty"`
	Eligible             bool   `json:"eligible"`
	ReasonCode           string `json:"reason_code,omitempty"`
	Reason               string `json:"reason,omitempty"`
	NextAction           string `json:"next_action,omitempty"`
	Visibility           string `json:"visibility,omitempty"`
	Available            bool   `json:"available"`
}

// NextcloudConnectionAuthorizeRequest creates one scoped onboarding credential
// per selected workspace. It never creates a tenant or a tenant membership.
type NextcloudConnectionAuthorizeRequest struct {
	InstanceID       string   `json:"instance_id"`
	BaseURL          string   `json:"base_url"`
	DirectoryID      string   `json:"directory_id"`
	ActorDirectoryID string   `json:"actor_directory_id"`
	ActorObjectGUID  string   `json:"actor_object_guid"`
	TenantIDs        []uint64 `json:"tenant_ids"`
}

// NextcloudConnectionCredential is returned exactly once when a connection is
// authorized. Only its SHA-256 hash is retained by WeKnora.
type NextcloudConnectionCredential struct {
	TenantID             uint64 `json:"tenant_id"`
	TenantName           string `json:"tenant_name"`
	CredentialID         string `json:"credential_id"`
	Token                string `json:"token"`
	ProcessingTemplateID string `json:"processing_template_id"`
	ProcessingTemplate   string `json:"processing_template_name,omitempty"`
}

// NextcloudConnectionAuthorizeResponse is the one-time connection result.
type NextcloudConnectionAuthorizeResponse struct {
	ConnectionID string                          `json:"connection_id"`
	Credentials  []NextcloudConnectionCredential `json:"credentials"`
}

// NextcloudConnectionRevokeRequest disconnects the exact credentials issued for
// one Nextcloud connection. It does not delete workspaces or knowledge bases.
type NextcloudConnectionRevokeRequest struct {
	CredentialIDs []string `json:"credential_ids"`
}

// NextcloudConnectionHealth is deliberately non-secret and fail-closed. Unknown
// component state is reported as unverified instead of being treated as ready.
type NextcloudConnectionHealth struct {
	Status                    string `json:"status"`
	ContractVersion           int    `json:"contract_version"`
	WorkerStatus              string `json:"worker_status"`
	WorkerHeartbeatAt         *int64 `json:"worker_heartbeat_at,omitempty"`
	WorkerLastSuccessAt       *int64 `json:"worker_last_success_at,omitempty"`
	DispatcherStatus          string `json:"dispatcher_status"`
	DispatcherLastSuccessAt   *int64 `json:"dispatcher_last_success_at,omitempty"`
	BindingAllowed            bool   `json:"binding_allowed"`
	ReasonCode                string `json:"reason_code,omitempty"`
	CheckedAt                 int64  `json:"checked_at"`
	ProcessingQueueUnverified bool   `json:"processing_queue_unverified"`
}
