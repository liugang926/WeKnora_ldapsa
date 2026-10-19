package types

// SpanAdditionalOriginalInput is private producer evidence written at Begin.
// It is not decoded from an HTTP request or reconstructed at End.
type SpanAdditionalOriginalInput struct {
	Kind        string
	ID          string
	InputDigest string
	AutoTag     *KnowledgeAutoTagInputSnapshot
}
