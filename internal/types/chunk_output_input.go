package types

// ChunkOutputReadRequest is trusted server scope plus row IDs. AllChunks asks
// the reader to account its complete candidate membership before filtering,
// ordering, pagination and totals. Public body/lineage JSON is never an input.
type ChunkOutputReadRequest struct {
	TenantID        uint64
	KnowledgeBaseID string
	KnowledgeID     string
	ChunkIDs        []string
	AllChunks       bool
}
type ChunkOutputInputSnapshot struct {
	Knowledge *Knowledge                `json:"-"`
	Chunks    []*Chunk                  `json:"-"`
	Proof     *KnowledgeBuildInputProof `json:"-"`
	AllChunks bool                      `json:"-"`
}
