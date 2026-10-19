package service

import (
	"context"
	"github.com/Tencent/WeKnora/internal/types"
)

func (s *knowledgeService) commitKnowledgeQuestionMetadata(ctx context.Context, chunks []*types.Chunk) error {
	writer, ok := s.chunkRepo.(interface {
		UpdateKnowledgeGeneratedQuestionMetadata(context.Context, []*types.Chunk) error
	})
	if !ok {
		return types.ErrSourceLineageUnknown
	}
	return writer.UpdateKnowledgeGeneratedQuestionMetadata(ctx, chunks)
}
