package types

// MessageMaterialProof is a private immutable producer receipt for original
// text/execution material. It is never reconstructed from current row metadata.
type MessageMaterialProof struct {
	AdmissionID, GenerationID, MessageID, SessionID, OwnerID, BodyDigest string
	TenantID, Revision, OwnerRevision                                    uint64
	SourceLineage                                                        *SourceLineage
	Execution                                                            MessageExecutionContext
	AgentID                                                              string
	AgentTenantID                                                        uint64
}
type MessageMaterialReadRequest struct {
	TenantID   uint64
	SessionID  string
	MessageIDs []string
	AllSession bool
}
type MessageMaterialReadPlan struct {
	Request        MessageMaterialReadRequest
	MetadataDigest string
	Scopes         []KnowledgeSearchScope
	SourceLineage  *SourceLineage
}
type MessageMaterialSnapshot struct {
	Request                                  MessageMaterialReadRequest
	MetadataDigest                           string
	SessionOwnerID                           string
	SessionOwnerRevision, MembershipRevision uint64
	Messages                                 []*Message
	Proofs                                   []*MessageMaterialProof
}
