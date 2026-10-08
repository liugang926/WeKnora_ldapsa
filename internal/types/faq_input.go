package types

// FAQEntryInputProof identifies this entry's immutable original producer.
// A FAQ catalog header never authenticates the bodies of its entries.
type FAQEntryInputProof struct {
	AdmissionID             string
	SessionID               string
	TenantID                uint64
	KnowledgeBaseID         string
	KnowledgeID             string
	ChunkID                 string
	InputDigest             string
	ConfigDigest            string
	ConfigRevision          uint64
	Revision                uint64
	BodyDigest              string
	SourceLineage           *SourceLineage
	OriginalKnowledgeInputs []*KnowledgeBuildInputProof
}

type FAQInputReadRequest struct {
	TenantID        uint64
	KnowledgeBaseID string
	ChunkIDs        []string
	AllEntries      bool
}

type FAQInputSnapshot struct {
	KnowledgeBase     *KnowledgeBase
	Request           FAQInputReadRequest
	ScopeRevision     uint64
	Knowledge         *Knowledge
	KnowledgeDigest   string
	KnowledgeRevision uint64
	ConfigDigest      string
	ConfigRevision    uint64
	ConfigLineage     *SourceLineage
	Chunks            []*Chunk
	Proofs            []*FAQEntryInputProof
	Tags              []*KnowledgeTag
	TagProofs         []*KnowledgeCollectionAuxiliaryProof
}

// FAQInputOperation is server constructed from the actual authenticated body.
// It is never decoded as a receipt or accepted from client metadata.
type FAQInputOperation struct {
	Kind       string
	Entry      *FAQEntryPayload
	Batch      *FAQBatchUpsertPayload
	EntrySeqID int64
	EntryID    string
	Questions  []string
	Enabled    *bool
	TagID      *string
	Fields     *FAQEntryFieldsBatchUpdate
	DeleteIDs  []int64
}
