package types

// FAQImportOutputReadRequest selects an original import output under trusted server scope.
//
// The receipt is private server state. Progress and LastFAQImportResult are
// independent outputs; a catalog header's fixed code origin certifies neither.
type FAQImportOutputReadRequest struct {
	TenantID        uint64
	KnowledgeBaseID string
	KnowledgeID     string
	TaskID          string
	Kind            string
}

// FAQImportOutputSnapshot holds the retained original import result and its influencing inputs.
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

// FAQImportOutputReadPlan describes the metadata and scopes required before reading an import output.
type FAQImportOutputReadPlan struct {
	MessagePlans   []*MessageMaterialReadPlan
	Request        FAQImportOutputReadRequest
	AdmissionID    string
	HeadRevision   uint64
	MetadataDigest string
	Scopes         []KnowledgeSearchScope
	SourceLineage  *SourceLineage
}

// FAQImportOutputReadSetPlan plans all original import results selected by a scoped output.
//
// A metadata-only plan for every actual FAQ result in scoped/explicit-ID outputs.
type FAQImportOutputReadSetPlan struct {
	MessagePlans    []*MessageMaterialReadPlan
	KnowledgeScopes []KnowledgeSearchScope
	KnowledgeIDs    []string
	Plans           []*FAQImportOutputReadPlan
	Scopes          []KnowledgeSearchScope
	SourceLineage   *SourceLineage
	MetadataDigest  string
}
