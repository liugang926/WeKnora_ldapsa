package service

import "github.com/Tencent/WeKnora/internal/types/interfaces"

// ConfigureKnowledgeInputProvenance wires actual producer capture and writer
// capabilities. They do not classify old rows or authorize caller grants.
func ConfigureKnowledgeInputProvenance(knowledge interfaces.KnowledgeService, reader interfaces.KnowledgeBuildInputReader, writer interfaces.KnowledgeInputWriter) {
	if impl, ok := knowledge.(*knowledgeService); ok {
		impl.knowledgeInputs = reader
		impl.knowledgeInputWriter = writer
	}
}

func ConfigureKnowledgeBaseInputProvenance(base interfaces.KnowledgeBaseService, writer interfaces.KnowledgeBaseInputWriter) {
	if impl, ok := base.(*knowledgeBaseService); ok {
		impl.knowledgeInputWriter = writer
	}
}
