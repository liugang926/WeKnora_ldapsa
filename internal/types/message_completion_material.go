package types

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
type MessageCompletionInputManifest struct {
	Version                                                                                        int
	Recipe                                                                                         string
	AssistantMessageID, AssistantGenerationID, AssistantAdmissionID, SessionID, RequestID, OwnerID string
	TenantID, OwnerRevision                                                                        uint64
	Initial                                                                                        *MessageMaterialProof
	Steers                                                                                         []*MessageCompletionSteerParent
	Inputs                                                                                         []*MessageMaterialSnapshot
	SourceLineage                                                                                  *SourceLineage
}
