package types

// Target observations are returned only by the existing authenticated server
// operation status. Decimal strings preserve exact integers in PHP/JSON.
type NextcloudTargetObservation struct {
	Schema            string `json:"schema"`
	State             string `json:"state"`
	Reason            string `json:"reason"`
	OperationID       string `json:"operation_id"`
	InstanceID        string `json:"instance_id"`
	BindingID         string `json:"binding_id"`
	TenantID          string `json:"tenant_id"`
	KnowledgeBaseID   string `json:"knowledge_base_id"`
	OperationRevision string `json:"operation_revision"`
	ClosureID         string `json:"closure_id"`
	DeletionTimeMS    string `json:"deletion_time_ms"`
	ClosedAt          string `json:"closed_at"`
}

// A tenant deletion is independent of any prior target closure. The anchor
// operation supplies the authenticated observer scope; it does not mutate the
// old target receipt or claim that a migrated root belongs to the deleted owner.
type NextcloudTenantDeletionObservation struct {
	Schema         string `json:"schema"`
	State          string `json:"state"`
	OperationID    string `json:"operation_id"`
	InstanceID     string `json:"instance_id"`
	BindingID      string `json:"binding_id"`
	TenantID       string `json:"tenant_id"`
	DeletionID     string `json:"deletion_id"`
	DeletionTimeMS string `json:"deletion_time_ms"`
	ClosedAt       string `json:"closed_at"`
}
