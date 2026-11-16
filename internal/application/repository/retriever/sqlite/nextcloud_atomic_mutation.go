package sqlite

import (
	"context"
	"errors"
	"fmt"
	"github.com/Tencent/WeKnora/internal/logger"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
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
	if len(rows) == 0 && updates != nil {
		for column := range updates {
			logger.GetLogger(ctx).Warnf(
				"[SQLite] update of lite_embeddings.%s matched 0 rows for chunk %v", column, arg,
			)
		}
	}
	for _, row := range rows {
		if updates != nil {
			if e := tx.Model(&sqliteEmbedding{}).Where("id=?", row.ID).Updates(updates).Error; e != nil {
				return e
			}
			continue
		}
		if e := r.deleteRowsAndVecs(tx, []sqliteEmbedding{row}); e != nil {
			return e
		}
		if e := tx.Where("id=?", row.ID).Delete(&sqliteEmbedding{}).Error; e != nil {
			return e
		}
	}
	return nil
}

func (r *sqliteRepository) copyRowsAtomic(
	ctx context.Context,
	sourceKB string, knowledgeMap, chunkMap map[string]string,
	kb string,
) error {
	if r.initErr != nil {
		return r.initErr
	}
	var partial []error
	guarded := apprepo.CheckNextcloudBuildIdentity(ctx, 0, "", "") != nil
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for source, target := range chunkMap {
			var rows []sqliteEmbedding
			if e := tx.Where("chunk_id=? AND knowledge_base_id=?", source, sourceKB).Find(&rows).Error; e != nil {
				return e
			}
			if e := apprepo.GuardNextcloudLocalIndexWrites(ctx, tx, sqliteRowInfos(rows)); e != nil {
				return e
			}
			for _, src := range rows {
				row := sqliteEmbedding{
					SourceID:        copiedSourceID(src.SourceID, source, target),
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
				rowError := func(e error) error {
					if e == nil {
						return nil
					}
					wrapped := fmt.Errorf(
						"[SQLite] copy source %s (chunk %s -> %s): %w", src.SourceID, source, target, e,
					)
					if guarded {
						return wrapped
					}
					partial = append(partial, wrapped)
					return nil
				}
				if e := tx.Create(&row).Error; e != nil {
					if err := rowError(e); err != nil {
						return err
					}
					continue
				}
				if e := recordSQLiteIndexArtifact(ctx, tx, &row); e != nil {
					return e
				}
				if e := rowError(r.syncFTS5Insert(tx, &row)); e != nil {
					return e
				}
				if row.Dimension > 0 {
					if e := rowError(r.copyVec(tx, src.ID, row.ID, row.Dimension)); e != nil {
						return e
					}
				}
			}
		}
		return nil
	})
	return errors.Join(append(partial, err)...)
}
