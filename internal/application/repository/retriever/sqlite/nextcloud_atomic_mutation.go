package sqlite

import (
	"context"
	"fmt"
	"strconv"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func sqliteRowInfos(rows []sqliteEmbedding) []*types.IndexInfo {
	infos := make([]*types.IndexInfo, 0, len(rows))
	for _, r := range rows {
		infos = append(
			infos,
			&types.IndexInfo{KnowledgeID: r.KnowledgeID, KnowledgeBaseID: r.KnowledgeBaseID, ChunkID: r.ChunkID},
		)
	}
	return infos
}

func (r *sqliteRepository) mutateRowsAtomic(ctx context.Context, where string, arg any, updates map[string]any) error {
	if r.initErr != nil {
		return r.initErr
	}
	return r.db.WithContext(ctx).
		Transaction(func(tx *gorm.DB) error { return r.mutateRowsInTx(ctx, tx, where, arg, updates) })
}

func (r *sqliteRepository) mutateRowsInTx(
	ctx context.Context,
	tx *gorm.DB,
	where string,
	arg any,
	updates map[string]any,
) error {
	var rows []sqliteEmbedding
	if e := tx.Where(where, arg).Find(&rows).Error; e != nil {
		return e
	}
	if e := apprepo.GuardNextcloudLocalIndexWrites(ctx, tx, sqliteRowInfos(rows)); e != nil {
		return e
	}
	for _, row := range rows {
		if updates != nil {
			if e := tx.Model(&sqliteEmbedding{}).Where("id=?", row.ID).Updates(updates).Error; e != nil {
				return e
			}
			continue
		}
		if e := tx.Exec("DELETE FROM lite_embeddings_fts WHERE rowid=?", row.ID).Error; e != nil {
			return e
		}
		if row.Dimension > 0 {
			if row.Dimension > 65536 {
				return fmt.Errorf("invalid vector dimension")
			}
			if e := tx.Exec(
				fmt.Sprintf("DELETE FROM vec_embeddings_%d WHERE rowid=?", row.Dimension),
				row.ID,
			).Error; e != nil {
				return e
			}
		}
		if e := tx.Where("id=?", row.ID).Delete(&sqliteEmbedding{}).Error; e != nil {
			return e
		}
	}
	return nil
}

func (r *sqliteRepository) copyRowsAtomic(
	ctx context.Context,
	knowledgeMap, chunkMap map[string]string,
	kb string,
) error {
	if r.initErr != nil {
		return r.initErr
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for source, target := range chunkMap {
			var rows []sqliteEmbedding
			if e := tx.Where("chunk_id=?", source).Find(&rows).Error; e != nil {
				return e
			}
			for _, src := range rows {
				row := sqliteEmbedding{
					SourceID:        uuid.NewString(),
					SourceType:      src.SourceType,
					ChunkID:         target,
					KnowledgeID:     knowledgeMap[src.KnowledgeID],
					KnowledgeBaseID: kb,
					TagID:           src.TagID,
					Content:         src.Content,
					Dimension:       src.Dimension,
					IsEnabled:       src.IsEnabled,
				}
				if e := apprepo.GuardNextcloudLocalIndexWrites(
					ctx,
					tx,
					sqliteRowInfos([]sqliteEmbedding{row}),
				); e != nil {
					return e
				}
				if e := tx.Create(&row).Error; e != nil {
					return e
				}
				if e := apprepo.RecordNextcloudDerivedArtifactInTx(
					ctx,
					tx,
					row.KnowledgeID,
					kb,
					"sqlite_embedding",
					strconv.FormatUint(uint64(row.ID), 10),
					int64(row.Dimension),
				); e != nil {
					return e
				}
				if e := tx.Exec(
					"INSERT INTO lite_embeddings_fts(rowid,content,source_id,chunk_id,knowledge_id,knowledge_base_id) VALUES(?,?,?,?,?,?)",
					row.ID,
					tokenizeCJKBigram(row.Content),
					row.SourceID,
					row.ChunkID,
					row.KnowledgeID,
					kb,
				).Error; e != nil {
					return e
				}
				if row.Dimension > 0 {
					if row.Dimension > 65536 {
						return fmt.Errorf("invalid vector dimension")
					}
					result := tx.Exec(
						fmt.Sprintf(
							"INSERT INTO vec_embeddings_%d(rowid,embedding) SELECT ?,embedding FROM vec_embeddings_%d WHERE rowid=?",
							row.Dimension,
							row.Dimension,
						),
						row.ID,
						src.ID,
					)
					if result.Error != nil {
						return result.Error
					}
					if result.RowsAffected != 1 {
						return fmt.Errorf("copy vector physical row missing")
					}
				}
			}
		}
		return nil
	})
}
