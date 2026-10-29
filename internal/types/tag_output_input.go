package types

// TagOutputReadRequest selects original tags and all influencing statistics candidates.
//
// Private tag receipts retain one bounded original lineage per actual producer.
// Statistics capture all influencing rows before keyword/order/page selection.
type TagOutputReadRequest struct {
	TenantID        uint64
	KnowledgeBaseID string
	TagID           string // empty means the complete list/statistics candidate set
}

// TagOutputChunkCandidate holds an authenticated original chunk used by tag statistics.
type TagOutputChunkCandidate struct {
	Chunk                   *Chunk
	Revision                uint64
	Digest                  string
	SourceLineage           *SourceLineage
	OriginalKnowledgeInputs []*KnowledgeBuildInputProof
}

// TagOutputInputSnapshot retains the complete original candidate material for tag output.
type TagOutputInputSnapshot struct {
	Request       TagOutputReadRequest
	ScopeRevision uint64
	InFlight      bool
	Tags          []*KnowledgeTag
	TagProofs     []*KnowledgeCollectionAuxiliaryProof
	Knowledge     []*KnowledgeCollectionCandidate
	Chunks        []*TagOutputChunkCandidate
	Auxiliary     *KnowledgeCollectionAuxiliary
}

// TagOutputSelectionRequest defines the keyword and pagination applied to retained tag candidates.
type TagOutputSelectionRequest struct {
	Page    Pagination
	Keyword string
}

// TagOutputSelection contains the selected tags and the count of authenticated matches.
type TagOutputSelection struct {
	Request TagOutputSelectionRequest
	Tags    []*KnowledgeTagWithStats
	Total   int64
}
