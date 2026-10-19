package types

// KnowledgeCollectionReadRequest fixes the collection scopes before selection.
// Collection receipts are private request data. Candidate membership is captured
// before any caller keyword, filter, order, aggregate or pagination is evaluated.
// They deliberately retain one bounded original lineage per producer rather than
// unioning an entire knowledge base into one SourceLineage JSON value.
type KnowledgeCollectionReadRequest struct {
	Scopes []KnowledgeSearchScope
}

// KnowledgeCollectionScopeCheckpoint pins one scope and its configuration authority.
type KnowledgeCollectionScopeCheckpoint struct {
	Scope                 KnowledgeSearchScope
	Revision              uint64
	ConfigurationRevision uint64
	ConfigurationDigest   string
	ConfigurationLineage  *SourceLineage
}

// KnowledgeCollectionCandidate binds a candidate to its original producer proof.
type KnowledgeCollectionCandidate struct {
	Knowledge *Knowledge
	Proof     *KnowledgeBuildInputProof
}

// KnowledgeCollectionAuxiliaryProof pins an auxiliary display input.
type KnowledgeCollectionAuxiliaryProof struct {
	Kind, ID        string
	TenantID        uint64
	KnowledgeBaseID string
	Revision        uint64
	Digest          string
	SourceLineage   *SourceLineage
}

// KnowledgeCollectionAuxiliary contains the admitted names, tags and relations.
type KnowledgeCollectionAuxiliary struct {
	Proofs             []*KnowledgeCollectionAuxiliaryProof
	KnowledgeBaseNames map[string]string
	Tags               map[string][]*KnowledgeTag
	Relations          []*KnowledgeTagRelation
}

// KnowledgeCollectionInputSnapshot captures candidates and original auxiliary inputs.
type KnowledgeCollectionInputSnapshot struct {
	Scopes     []KnowledgeCollectionScopeCheckpoint
	Candidates []*KnowledgeCollectionCandidate
	Auxiliary  *KnowledgeCollectionAuxiliary
}

// KnowledgeCollectionSelectionRequest describes selection over admitted candidates.
type KnowledgeCollectionSelectionRequest struct {
	Mode          string // list, search, folders
	Filter        KnowledgeListFilter
	Page          Pagination
	Keyword       string
	Offset, Limit int
	FileTypes     []string
}

// KnowledgeCollectionSelection records the exact selected collection output.
type KnowledgeCollectionSelection struct {
	Request          KnowledgeCollectionSelectionRequest
	Knowledge        []*Knowledge
	Folders          *KnowledgeFolderTree
	Total            int64
	HasMore          bool
	Page, PageSize   int
	FAQImportResults []*FAQImportOutputSnapshot
}

// KnowledgeOutputDisplaySelection limits auxiliary fields consumed by an explicit-ID response.
// Metadata display selectors describe only fields the actual explicit-ID
// response consumed. They cannot widen a document read to an entire KB.
type KnowledgeOutputDisplaySelection struct {
	TenantID                     uint64
	KnowledgeBaseID, KnowledgeID string
	Tags, KnowledgeBaseName      bool
}

// KnowledgeOutputAuxiliaryInputSnapshot binds explicit output to admitted auxiliary inputs.
type KnowledgeOutputAuxiliaryInputSnapshot struct {
	Proofs     []*KnowledgeBuildInputProof
	Selections []KnowledgeOutputDisplaySelection
	Knowledge  []*Knowledge
	Auxiliary  *KnowledgeCollectionAuxiliary
}
