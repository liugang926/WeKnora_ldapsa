package types

import (
	"context"
	"encoding/json"
	"strings"
	"time"
)

type SteerDelivery string

const (
	SteerDeliveryAfter  SteerDelivery = "after"
	SteerDeliveryInject SteerDelivery = "inject"
)

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

type SteerControlOperation string

const (
	SteerControlPromote     SteerControlOperation = "promote"
	SteerControlConsume     SteerControlOperation = "consume"
	SteerControlDiscard     SteerControlOperation = "discard"
	SteerControlClaimFailed SteerControlOperation = "claim_failed"
	SteerControlDelete      SteerControlOperation = "delete"
	SteerControlRebind      SteerControlOperation = "rebind"
)

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

type SteerMaterialUse string

const (
	SteerMaterialPending  SteerMaterialUse = "pending"
	SteerMaterialInject   SteerMaterialUse = "inject"
	SteerMaterialFollowUp SteerMaterialUse = "follow_up"
)

type SteerMaterialRequest struct {
	TenantID                                 uint64
	SessionID, RunMessageID, RunGenerationID string
	SteerIDs                                 []string
	Use                                      SteerMaterialUse
}

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

// Typed original body becomes available only after plan/authority/lease checks.
// HTTP/model adapters consume this value, never a Redis StreamEvent body.
type SteerMaterialInput struct {
	Proof          *SteerOriginalProof
	Control        *SteerCandidateControl
	Query          string
	MentionedItems MentionedItems
	Channel        string
}

type SteerMaterialSnapshot struct {
	Plan   *SteerMaterialPlan
	Inputs []*SteerMaterialInput
}

// A consume writer seals this exact original→new-message/control transition
// atomically. Own-insertion transfer cannot refresh unrelated parent controls.
type SteerConsumptionProof struct {
	OriginalAdmissionID, PreviousHeadID, ResultHeadID                string
	PreviousRevision, ResultRevision                                 uint64
	UserMessageID, UserGenerationID, UserAdmissionID, UserBodyDigest string
	SessionID, RunMessageID, RunGenerationID, RunRequestID           string
	PreviousMembershipRevision, ResultMembershipRevision             uint64
}

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
	) (userMessageID string, lineage *SourceLineage, check func() error, close func(), err error)
}
