package types

import (
	"context"
	"encoding/json"
	"strings"
	"time"
)

// SteerDelivery identifies whether authored input follows or interrupts the current turn.
type SteerDelivery string

const (
	SteerDeliveryAfter  SteerDelivery = "after"
	SteerDeliveryInject SteerDelivery = "inject"
)

// SteerAuthoredRequest carries exact authenticated input from the HTTP enqueue producer.
//
// Only the authenticated HTTP enqueue producer supplies these exact authored
// fields. Actor/source contexts are server capabilities, never JSON evidence;
// repository writers still refresh authority and check the actual run origin.
type SteerAuthoredRequest struct {
	RawInput                                               json.RawMessage
	TenantID                                               uint64
	SessionID, RunMessageID, RunGenerationID, RunRequestID string
	ClientSteerID                                          string
	Query                                                  string
	MentionedItems                                         MentionedItems
	Channel                                                string
	Delivery                                               SteerDelivery
	ActorContext                                           context.Context `json:"-"`
	SourceContext                                          context.Context `json:"-"`
}

// SteerCanonicalQuery is the fixed authored-text transfer used at enqueue,
// persistence and model input. It does not certify cached queue text.
func SteerCanonicalQuery(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	return strings.TrimSpace(value)
}

// SteerOriginalProof identifies immutable original authored material without embedding its body.
//
// The proof describes an immutable original; it never embeds input_body.
// A body descriptor, digest, or source envelope alone is not read authority.
type SteerOriginalProof struct {
	AdmissionID, OriginID, SteerID                                string
	SessionID, OwnerID                                            string
	OriginRunMessageID, OriginRunGenerationID, OriginRunRequestID string
	TenantID, OwnerRevision                                       uint64
	BodyDigest                                                    string
	SourceLineage                                                 *SourceLineage          `json:"-"`
	ExecutionContext                                              MessageExecutionContext `json:"-"`
	AgentID                                                       string
	AgentTenantID                                                 uint64 `json:"-"`
	CreatedAt                                                     time.Time
}

// SteerControlOperation identifies a control-only mutation of an original steer head.
type SteerControlOperation string

const (
	SteerControlPromote     SteerControlOperation = "promote"
	SteerControlConsume     SteerControlOperation = "consume"
	SteerControlDiscard     SteerControlOperation = "discard"
	SteerControlClaimFailed SteerControlOperation = "claim_failed"
	SteerControlDelete      SteerControlOperation = "delete"
	SteerControlRebind      SteerControlOperation = "rebind"
)

// SteerControlRequest names the exact original head consumed by a control-only mutation.
//
// Every mutation names its exact previous immutable head. Body/mention/channel
// fields cannot be changed by this control-only producer.
type SteerControlRequest struct {
	TenantID                                                      uint64
	SessionID, SteerID                                            string
	OriginalAdmissionID                                           string
	ExpectedHeadID                                                string
	ExpectedRevision                                              uint64
	Operation                                                     SteerControlOperation
	TargetRunMessageID, TargetRunGenerationID, TargetRunRequestID string
	ConsumedUserMessageID                                         string
}

// SteerCandidateControl contains retained original selection and delivery controls.
type SteerCandidateControl struct {
	SteerID, OriginalAdmissionID, BodyDigest    string
	HeadID                                      string
	Revision                                    uint64
	RunMessageID, RunGenerationID, RunRequestID string
	Delivery                                    SteerDelivery
	Consumed, Discarded, Deleted                bool
	UserMessageID                               string
	CreatedAt                                   time.Time
}

// SteerMaterialUse identifies the consumer of retained original steer material.
type SteerMaterialUse string

const (
	SteerMaterialPending  SteerMaterialUse = "pending"
	SteerMaterialInject   SteerMaterialUse = "inject"
	SteerMaterialFollowUp SteerMaterialUse = "follow_up"
)

// SteerMaterialRequest selects original steer material for a specific server-owned run.
type SteerMaterialRequest struct {
	TenantID                                 uint64
	SessionID, RunMessageID, RunGenerationID string
	SteerIDs                                 []string
	Use                                      SteerMaterialUse
}

// SteerMaterialPlan retains all original controls influencing material selection.
//
// Candidates include every original control that influenced selection,
// including skipped/consumed candidates. Selection keeps their existing order;
// no new queue ordinal or cached body is introduced by this plan.
type SteerMaterialPlan struct {
	Request           SteerMaterialRequest
	MetadataDigest    string
	OwnerID           string
	OwnerRevision     uint64
	CandidateControls []*SteerCandidateControl
	SelectedSteerIDs  []string
	OriginalProofs    []*SteerOriginalProof
	Scopes            []KnowledgeSearchScope
	SourceLineage     *SourceLineage `json:"-"`
}

// SteerMaterialInput holds an original body after its plan, authority and lease checks.
//
// Typed original body becomes available only after plan/authority/lease checks.
// HTTP/model adapters consume this value, never a Redis StreamEvent body.
type SteerMaterialInput struct {
	Proof          *SteerOriginalProof
	Control        *SteerCandidateControl
	Query          string
	MentionedItems MentionedItems
	Channel        string
}

// SteerMaterialSnapshot pairs the original selection plan with authenticated input bodies.
type SteerMaterialSnapshot struct {
	Plan   *SteerMaterialPlan
	Inputs []*SteerMaterialInput
}

// SteerConsumptionProof seals one atomic original-to-message and control transition.
//
// A consume writer seals this exact original→new-message/control transition
// atomically. Own-insertion transfer cannot refresh unrelated parent controls.
type SteerConsumptionProof struct {
	OriginalAdmissionID, PreviousHeadID, ResultHeadID                string
	PreviousRevision, ResultRevision                                 uint64
	UserMessageID, UserGenerationID, UserAdmissionID, UserBodyDigest string
	SessionID, RunMessageID, RunGenerationID, RunRequestID           string
	PreviousMembershipRevision, ResultMembershipRevision             uint64
}

// OriginalSteerSink persists original material while retaining its lineage and read checks.
//
// The actual production sink supplies original lineage and a retained read
// checker after its atomic consume and own-insertion transfer. Model input is
// built only after this succeeds; Redis maps are not proof capabilities.
type OriginalSteerSink interface {
	SteerSink
	PersistSteerMaterial(
		ctx context.Context,
		sessionID, messageID, steerID, content string,
		mentionedItems MentionedItems,
		channel string,
	) (userMessageID string, lineage *SourceLineage, check func() error, closeMaterial func(), err error)
}
