package sqlite

import (
	"context"
	"fmt"
	"strconv"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	sqlite_vec "github.com/asg017/sqlite-vec-go-bindings/cgo"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (r *sqliteRepository) saveRowsAtomic(
	ctx context.Context,
	infos []*types.IndexInfo,
	rows []*sqliteEmbedding,
	embeddings [][]float32,
) error {
	if r.initErr != nil {
		return r.initErr
	}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := apprepo.GuardNextcloudLocalIndexWrites(ctx, tx, infos); e != nil {
			return e
		}
		for i, row := range rows {
			if e := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(row).Error; e != nil {
				return e
			}
			if row.ID == 0 {
				if e := tx.Where("source_id=? AND source_type=?", row.SourceID, row.SourceType).
					Take(row).
					Error; e != nil {
					return e
				}
			}
			if row.KnowledgeID != infos[i].KnowledgeID || row.KnowledgeBaseID != infos[i].KnowledgeBaseID ||
				row.ChunkID != infos[i].ChunkID {
				return apprepo.ErrNextcloudContentLeaseDenied
			}
			if e := apprepo.RecordNextcloudDerivedArtifactInTx(
				ctx,
				tx,
				row.KnowledgeID,
				row.KnowledgeBaseID,
				"sqlite_embedding",
				strconv.FormatUint(uint64(row.ID), 10),
				int64(row.Dimension),
			); e != nil {
				return e
			}
			// Both the fulltext and vec0 rows share the same commit/rollback boundary.
			if e := tx.Exec("DELETE FROM lite_embeddings_fts WHERE rowid=?", row.ID).Error; e != nil {
				return e
			}
			if e := tx.Exec(
				("INSERT INTO lite_embeddings_fts(rowid,content,source_id,chunk_id,knowledge" +
					"_id,knowledge_base_id) VALUES(?,?,?,?,?,?)"),
				row.ID,
				tokenizeCJKBigram(row.Content),
				row.SourceID,
				row.ChunkID,
				row.KnowledgeID,
				row.KnowledgeBaseID,
			).Error; e != nil {
				return e
			}
			if row.Dimension > 0 {
				if row.Dimension > 65536 {
					return fmt.Errorf("invalid vector dimension")
				}
				table := vecTableName(row.Dimension)
				if e := tx.Exec(
					fmt.Sprintf(
						"CREATE VIRTUAL TABLE IF NOT EXISTS %s USING vec0(embedding float[%d] distance_metric=cosine)",
						table,
						row.Dimension,
					),
				).Error; e != nil {
					return e
				}
				if e := tx.Exec(fmt.Sprintf("DELETE FROM %s WHERE rowid=?", table), row.ID).Error; e != nil {
					return e
				}
				blob, e := sqlite_vec.SerializeFloat32(embeddings[i])
				if e != nil {
					return e
				}
				if e = tx.Exec(
					fmt.Sprintf("INSERT INTO %s(rowid,embedding) VALUES(?,?)", table),
					row.ID,
					blob,
				).Error; e != nil {
					return e
				}
			}
		}
		return nil
	})
	if err == nil {
		r.vecMutex.Lock()
		for _, row := range rows {
			if row.Dimension > 0 {
				r.vecTables[row.Dimension] = true
			}
		}
		r.vecMutex.Unlock()
	}
	return err
}
