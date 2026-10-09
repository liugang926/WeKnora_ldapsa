package types

type KnowledgeAutoTagTaskInputProof struct{ ID, Digest string }

// This server-only snapshot is persisted before the classification call. The
// manifest includes every candidate name/control and every existing membership
// that can affect classification, including candidates the model does not select.
type KnowledgeAutoTagInputSnapshot struct {
	ManifestID      string
	InputDigest     string
	Original        *KnowledgeAutoTagOriginalInput
	Tags            []*KnowledgeTag
	TotalTags       int64
	TagListRevision uint64
	TagProofs       []*KnowledgeCollectionAuxiliaryProof
	Relations       []*KnowledgeTagRelation
	RelationProof   *KnowledgeCollectionAuxiliaryProof
	SourceLineage   *SourceLineage
	DataSourceName  *DataSourceNameInputProof `json:"datasource_name_input,omitempty"`
	ModelDigest     string                    `json:"model_digest,omitempty"`
	// Runtime-only actual SQL context. ModelDigest seals the tenant's required
	// routing/secret configuration digest and monotonic revision; credentials
	// must never be copied into the immutable model-input JSON.
	Tenant *Tenant `json:"-"`
}

type KnowledgeAutoTagOriginalInput struct {
	Knowledge     *Knowledge
	KnowledgeBase *KnowledgeBase
	Chunks        []*Chunk
	Proof         *KnowledgeBuildInputProof
}
