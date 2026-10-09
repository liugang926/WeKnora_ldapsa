package types

import "time"

// MessageMaterialProof is a private immutable producer receipt for original
// text/execution material. It is never reconstructed from current row metadata.
type MessageMaterialProof struct {
	GeneratedFields                                                      []*MessageGeneratedFieldProof
	AdmissionID, GenerationID, MessageID, SessionID, OwnerID, BodyDigest string
	TenantID, Revision, OwnerRevision                                    uint64
	SourceLineage                                                        *SourceLineage
	Execution                                                            MessageExecutionContext
	AgentID                                                              string
	AgentTenantID                                                        uint64
}
type MessageMaterialReadRequest struct {
	TenantID                  uint64
	SessionID                 string
	MessageIDs                []string
	AllSession                bool
	CompletedOnly             bool
	IncludeOriginalUserFields bool
	ExcludeRequestID          string
}
type MessageMaterialReadPlan struct {
	ParentPlans    []*MessageMaterialReadPlan
	Request        MessageMaterialReadRequest
	MetadataDigest string
	Controls       []*MessageMaterialCandidateControl
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

type MessageMaterialCandidateControl struct {
	ID, RequestID, Role, AdmissionID, GenerationID string
	Complete                                       bool
	CreatedAt                                      time.Time
	Revision, CurrentRevision                      uint64
}

type MessageGeneratedFieldProof struct {
	AdmissionID, Field, BodyDigest string
	Revision                       uint64
	SourceLineage                  *SourceLineage
}
