package types

// Collection receipts are private request data. Candidate membership is captured
// before any caller keyword, filter, order, aggregate or pagination is evaluated.
// They deliberately retain one bounded original lineage per producer rather than
// unioning an entire knowledge base into one SourceLineage JSON value.
type KnowledgeCollectionReadRequest struct {
	Scopes []KnowledgeSearchScope
}

type KnowledgeCollectionScopeCheckpoint struct {
	Scope                 KnowledgeSearchScope
	Revision              uint64
	ConfigurationRevision uint64
	ConfigurationDigest   string
	ConfigurationLineage  *SourceLineage
}

type KnowledgeCollectionCandidate struct {
	Knowledge *Knowledge
	Proof     *KnowledgeBuildInputProof
}

type KnowledgeCollectionAuxiliaryProof struct {
	Kind, ID        string
	TenantID        uint64
	KnowledgeBaseID string
	Revision        uint64
	Digest          string
	SourceLineage   *SourceLineage
}

type KnowledgeCollectionAuxiliary struct {
	Proofs             []*KnowledgeCollectionAuxiliaryProof
	KnowledgeBaseNames map[string]string
	Tags               map[string][]*KnowledgeTag
	Relations          []*KnowledgeTagRelation
}

type KnowledgeCollectionInputSnapshot struct {
	Scopes     []KnowledgeCollectionScopeCheckpoint
	Candidates []*KnowledgeCollectionCandidate
	Auxiliary  *KnowledgeCollectionAuxiliary
}

type KnowledgeCollectionSelectionRequest struct {
	Mode          string // list, search, folders
	Filter        KnowledgeListFilter
	Page          Pagination
	Keyword       string
	Offset, Limit int
	FileTypes     []string
}

type KnowledgeCollectionSelection struct {
	Request        KnowledgeCollectionSelectionRequest
	Knowledge      []*Knowledge
	Folders        *KnowledgeFolderTree
	Total          int64
	HasMore        bool
	Page, PageSize int
}

// Metadata display selectors describe only fields the actual explicit-ID
// response consumed. They cannot widen a document read to an entire KB.
type KnowledgeOutputDisplaySelection struct {
	TenantID                     uint64
	KnowledgeBaseID, KnowledgeID string
	Tags, KnowledgeBaseName      bool
}

type KnowledgeOutputAuxiliaryInputSnapshot struct {
	Proofs     []*KnowledgeBuildInputProof
	Selections []KnowledgeOutputDisplaySelection
	Knowledge  []*Knowledge
	Auxiliary  *KnowledgeCollectionAuxiliary
}
