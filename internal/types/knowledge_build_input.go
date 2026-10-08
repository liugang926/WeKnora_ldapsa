package types

// KnowledgeBuildInputProof is a private original producer receipt. AdmissionID
// identifies an immutable server origin; BuildGeneration is its monotonic
// admitted generation (Nextcloud uses the exact original build-fence epoch).
// It never authenticates cached text by obtaining a new current receipt.
type KnowledgeBuildInputProof struct {
	Version         int            `json:"version"`
	Projection      string         `json:"projection,omitempty"`
	TenantID        uint64         `json:"tenant_id"`
	KnowledgeBaseID string         `json:"knowledge_base_id"`
	KnowledgeID     string         `json:"knowledge_id"`
	AdmissionID     string         `json:"admission_id"`
	BuildGeneration uint64         `json:"build_generation"`
	InputDigest     string         `json:"input_digest"`
	ConfigDigest    string         `json:"config_digest"`
	BodyDigest      string         `json:"body_digest"`
	ChunkIDs        []string       `json:"chunk_ids"`
	SourceLineage   *SourceLineage `json:"source_lineage"`
}

type KnowledgeBuildInputCaptureRequest struct {
	Projection      string
	TenantID        uint64
	KnowledgeBaseID string
	KnowledgeID     string
	ChunkIDs        []string
	// Expected is the original admitted task receipt, not a client claim.
	Expected *KnowledgeBuildInputProof
}

const KnowledgeBuildProjectionGraph = "graph_model_inputs"

// Enrichment authenticates all document/model input fields while generated
// summary and question outputs retain the same immutable original admission.
const KnowledgeBuildProjectionEnrichment = "enrichment_model_inputs"

// KnowledgeProducerInputProof refers to an immutable private receipt emitted
// by the actual producer, never a digest accepted from an HTTP client.
type KnowledgeProducerInputProof struct {
	ID      string `json:"id"`
	Purpose string `json:"purpose"`
	Digest  string `json:"digest"`
}

// Snapshot includes actual persisted material/config used for model input.
// Chunk metadata, titles and graph/config controls belong in its body digest.
type KnowledgeBuildInputSnapshot struct {
	Knowledge     *Knowledge                `json:"-"`
	KnowledgeBase *KnowledgeBase            `json:"-"`
	Chunks        []*Chunk                  `json:"-"`
	Proof         *KnowledgeBuildInputProof `json:"-"`
}
