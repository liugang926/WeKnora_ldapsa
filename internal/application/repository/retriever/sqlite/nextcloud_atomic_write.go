package sqlite

import (
	"context"
	"strconv"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"gorm.io/gorm"
)

func sameSQLiteIndexOwner(a, b *sqliteEmbedding) bool {
	return a.KnowledgeBaseID == b.KnowledgeBaseID && a.KnowledgeID == b.KnowledgeID && a.ChunkID == b.ChunkID
}

func recordSQLiteIndexArtifact(ctx context.Context, tx *gorm.DB, row *sqliteEmbedding) error {
	return apprepo.RecordNextcloudDerivedArtifactInTx(
		ctx, tx, row.KnowledgeID, row.KnowledgeBaseID, "sqlite_embedding",
		strconv.FormatUint(uint64(row.ID), 10), int64(row.Dimension),
	)
}
