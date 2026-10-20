package types

// SpanOutputReadRequest is authenticated server scope, never client proof.
type SpanOutputReadRequest struct {
	TenantID        uint64
	KnowledgeBaseID string
	KnowledgeID     string
	Attempt         int
}

// SpanOutputInputSnapshot binds complete attempt membership before tree
// construction. Unproved rows contain only a generic status projection.
type SpanOutputInputSnapshot struct {
	TenantID           uint64                    `json:"tenant_id"`
	KnowledgeBaseID    string                    `json:"knowledge_base_id"`
	KnowledgeID        string                    `json:"knowledge_id"`
	Attempt            int                       `json:"attempt"`
	LatestAttempt      int                       `json:"latest_attempt"`
	CurrentParseStatus string                    `json:"current_parse_status"`
	Rows               []KnowledgeProcessingSpan `json:"rows"`
	SourceLineage      *SourceLineage            `json:"source_lineage"`
	Receipt            string                    `json:"-"`
}
