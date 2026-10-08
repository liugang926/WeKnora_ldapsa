package types

// Private tag receipts retain one bounded original lineage per actual producer.
// Statistics capture all influencing rows before keyword/order/page selection.
type TagOutputReadRequest struct {
	TenantID        uint64
	KnowledgeBaseID string
	TagID           string // empty means the complete list/statistics candidate set
}
type TagOutputChunkCandidate struct {
	Chunk    *Chunk
	Revision uint64
	Digest   string
}
type TagOutputInputSnapshot struct {
	Request       TagOutputReadRequest
	ScopeRevision uint64
	Tags          []*KnowledgeTag
	TagProofs     []*KnowledgeCollectionAuxiliaryProof
	Knowledge     []*KnowledgeCollectionCandidate
	Chunks        []*TagOutputChunkCandidate
	Auxiliary     *KnowledgeCollectionAuxiliary
}
type TagOutputSelectionRequest struct {
	Page    Pagination
	Keyword string
}
type TagOutputSelection struct {
	Request TagOutputSelectionRequest
	Tags    []*KnowledgeTagWithStats
	Total   int64
}
