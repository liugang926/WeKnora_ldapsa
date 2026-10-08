package types

// The receipt is private server state. Progress and LastFAQImportResult are
// independent outputs; a catalog header's fixed code origin certifies neither.
type FAQImportOutputReadRequest struct {
	TenantID        uint64
	KnowledgeBaseID string
	KnowledgeID     string
	TaskID          string
	Kind            string
}
type FAQImportOutputSnapshot struct {
	Request                 FAQImportOutputReadRequest
	AdmissionID             string
	SessionID               string
	HeadRevision            uint64
	MaterialRevision        uint64
	BodyDigest              string
	SourceLineage           *SourceLineage
	OriginalKnowledgeInputs []*KnowledgeBuildInputProof
	Progress                *FAQImportProgress
	Result                  *FAQImportResult
}
type FAQImportOutputReadPlan struct {
	Request        FAQImportOutputReadRequest
	AdmissionID    string
	HeadRevision   uint64
	MetadataDigest string
	Scopes         []KnowledgeSearchScope
	SourceLineage  *SourceLineage
}

// A metadata-only plan for every actual FAQ result in scoped/explicit-ID outputs.
type FAQImportOutputReadSetPlan struct {
	KnowledgeScopes []KnowledgeSearchScope
	KnowledgeIDs    []string
	Plans           []*FAQImportOutputReadPlan
	Scopes          []KnowledgeSearchScope
	SourceLineage   *SourceLineage
	MetadataDigest  string
}
