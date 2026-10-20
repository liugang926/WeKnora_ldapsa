package types

// KnowledgeAutoTagTaskInputProof identifies the original classification receipt.
type KnowledgeAutoTagTaskInputProof struct{ ID, Digest string }

// KnowledgeAutoTagInputSnapshot is a server-only snapshot persisted before the classification call. The
// manifest includes every candidate name/control and every existing membership
// that can affect classification, including candidates the model does not select.
type KnowledgeAutoTagInputSnapshot struct {
	ManifestID          string
	PublicationRevision uint64 `json:"publication_revision,omitempty"`
	InputDigest         string
	Original            *KnowledgeAutoTagOriginalInput
	Tags                []*KnowledgeTag
	TotalTags           int64
	TagListRevision     uint64
	TagProofs           []*KnowledgeCollectionAuxiliaryProof
	Relations           []*KnowledgeTagRelation
	RelationProof       *KnowledgeCollectionAuxiliaryProof
	SourceLineage       *SourceLineage
	DataSourceName      *DataSourceNameInputProof `json:"datasource_name_input,omitempty"`
	ModelDigest         string                    `json:"model_digest,omitempty"`
	// Runtime-only actual SQL context. ModelDigest seals the tenant's required
	// routing/secret configuration digest and monotonic revision; credentials
	// must never be copied into the immutable model-input JSON.
	Tenant *Tenant `json:"-"`
}

// KnowledgeAutoTagOriginalInput holds the authenticated original classification material.
type KnowledgeAutoTagOriginalInput struct {
	Knowledge     *Knowledge
	KnowledgeBase *KnowledgeBase
	Chunks        []*Chunk
	Proof         *KnowledgeBuildInputProof
}
