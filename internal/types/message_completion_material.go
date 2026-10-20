package types

// MessageCompletionSteerParent contains metadata for one actual consumed completion parent.
// Metadata-only actual completion parents. No message body is duplicated.
type MessageCompletionSteerParent struct {
	Consumption         *SteerConsumptionProof
	Original            *SteerOriginalProof
	OriginalSource      *SourceLineage
	OriginalExecution   MessageExecutionContext
	OriginalAgentTenant uint64
	User                *MessageMaterialProof
	SelectionDigest     string
}

// MessageCompletionInputManifest binds a completion to its admitted original parents.
type MessageCompletionInputManifest struct {
	Version                                                                                        int
	Recipe                                                                                         string
	AssistantMessageID, AssistantGenerationID, AssistantAdmissionID, SessionID, RequestID, OwnerID string

	TenantID, OwnerRevision uint64
	Initial                 *MessageMaterialProof
	Steers                  []*MessageCompletionSteerParent
	Inputs                  []*MessageMaterialSnapshot
	SourceLineage           *SourceLineage
}
