// Package postgres implements retrieval against its configured vector backend.
package postgres

import (
	"context"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
)

func (g *pgRepository) MoveKnowledgeIndices(
	ctx context.Context,
	sourceKB, targetKB, knowledgeID string,
	_ []string,
	_ int,
	_ string,
) error {
	return apprepo.KnowledgeMoveDB(ctx, g.db).Model(&pgVector{}).
		Where("knowledge_base_id = ? AND knowledge_id = ?", sourceKB, knowledgeID).
		Updates(map[string]any{"knowledge_base_id": targetKB, "tag_id": ""}).Error
}
