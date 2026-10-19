package service

import (
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// ConfigureKnowledgeInputProvenance wires actual producer capture and writer
// capabilities. They do not classify old rows or authorize caller grants.
func ConfigureKnowledgeInputProvenance(
	knowledge interfaces.KnowledgeService,
	reader interfaces.KnowledgeBuildInputReader,
	writer interfaces.KnowledgeInputWriter,
) {
	if impl, ok := knowledge.(*knowledgeService); ok {
		impl.knowledgeInputs = reader
		impl.knowledgeInputWriter = writer
		impl.faqInputs, _ = reader.(*repository.KnowledgeInputProvenanceStore)
	}
}

// ConfigureKnowledgeBaseInputProvenance configures the knowledge base input provenance boundary.
func ConfigureKnowledgeBaseInputProvenance(
	base interfaces.KnowledgeBaseService,
	writer interfaces.KnowledgeBaseInputWriter,
) {
	if impl, ok := base.(*knowledgeBaseService); ok {
		impl.knowledgeInputWriter = writer
	}
}

// ConfigureChunkSummaryInputProvenance configures the chunk summary input provenance boundary.
func ConfigureChunkSummaryInputProvenance(chunks interfaces.ChunkService, knowledge interfaces.KnowledgeService) {
	if c, ok := chunks.(*chunkService); ok {
		if k, ok := knowledge.(*knowledgeService); ok {
			c.summaryInputPrepare = k.prepareSummaryGenerationPayload
		}
	}
}
