package types

import "time"

// RAGExactReadLease is an internal copy of the original server-acquired
// durable read token. Receipts are never parsed from client/model metadata.
type RAGExactReadLease struct {
	ID              string    `json:"-"`
	TenantID        uint64    `json:"-"`
	KnowledgeBaseID string    `json:"-"`
	KnowledgeID     string    `json:"-"`
	Kind            string    `json:"-"`
	Epoch           int64     `json:"-"`
	OwnerID         string    `json:"-"`
	ExpiresAt       time.Time `json:"-"`
}

// RAGChunkReadRequest carries row IDs and a real lease, not supplied text.
// MetadataOnly has a nil Chunk in its returned receipt. ExpectedLineage is
// the original receipt when augmenting previously captured material.
type RAGChunkReadRequest struct {
	TenantID        uint64             `json:"-"`
	KnowledgeBaseID string             `json:"-"`
	KnowledgeID     string             `json:"-"`
	ChunkIDs        []string           `json:"-"`
	Lease           *RAGExactReadLease `json:"-"`
	ExpectedLineage *SourceLineage     `json:"-"`
	MetadataOnly    bool               `json:"-"`
}

// RAGChunkInputSnapshot holds an original chunk snapshot and its verified producer material.
type RAGChunkInputSnapshot struct {
	Knowledge     *Knowledge     `json:"-"`
	Chunk         *Chunk         `json:"-"`
	SourceLineage *SourceLineage `json:"-"`
	// Set only after the trusted snapshot reader verifies the exact persisted
	// producer material, including its digest/revision. Current publication
	// identity by itself cannot authorize generated summary text.
	ProducerMaterialVerified bool `json:"-"`
}
