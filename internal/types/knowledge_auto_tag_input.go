package types

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
}

type KnowledgeAutoTagOriginalInput struct {
	Knowledge     *Knowledge
	KnowledgeBase *KnowledgeBase
	Chunks        []*Chunk
	Proof         *KnowledgeBuildInputProof
}
