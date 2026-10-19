package repository

import (
	"context"
	"errors"
	"sort"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Shared by the SQLite metadata/FTS/vec0 transaction. Ordinary unrelated rows
// retain their existing API, but an actual Nextcloud row requires its worker.
func GuardNextcloudLocalIndexWrites(ctx context.Context, tx *gorm.DB, infos []*types.IndexInfo) error {
	ids := map[string]*types.IndexInfo{}
	for _, info := range infos {
		if info == nil {
			return ErrNextcloudContentLeaseInvalid
		}
		ids[info.KnowledgeID] = info
	}
	keys := []string{}
	for id := range ids {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	if !tx.Migrator().HasTable("knowledges") {
		return CheckNextcloudBuildIdentity(ctx, 0, "", "")
	}
	for _, id := range keys {
		info := ids[id]
		var k types.Knowledge
		e := tx.Unscoped().Where("id=? AND knowledge_base_id=?", id, info.KnowledgeBaseID).Take(&k).Error
		if errors.Is(e, gorm.ErrRecordNotFound) {
			if e = CheckNextcloudBuildIdentity(ctx, 0, "", ""); e != nil {
				return e
			}
			continue
		}
		if e != nil {
			return e
		}
		if k.Channel != types.ConnectorTypeNextcloud {
			continue
		}
		if !NextcloudBuildLeasePresent(ctx, k.TenantID, k.KnowledgeBaseID, k.ID) {
			return ErrNextcloudContentLeaseInvalid
		}
		if e = LockNextcloudIndexKnowledgeBaseInTx(ctx, tx, k.TenantID, k.KnowledgeBaseID, k.ID); e != nil {
			return e
		}
		if e = ValidateNextcloudBuildWrite(ctx, tx, k.TenantID, k.KnowledgeBaseID, k.ID); e != nil {
			return e
		}
		metadata := k.GetMetadata()
		var source nextcloudSourceVersion
		if e = nextcloudVersionQuery(
			tx.Clauses(clause.Locking{Strength: "UPDATE"}),
			k.TenantID,
			k.KnowledgeBaseID,
			metadata["datasource_id"],
			metadata["external_id"],
		).Take(&source).
			Error; e != nil {
			return e
		}
		if source.State == "tombstone" || source.CandidateKnowledgeID != k.ID {
			return ErrNextcloudContentLeaseDenied
		}
		if e = ValidateNextcloudIndexInputWriteInTx(ctx, tx, k.TenantID, k.KnowledgeBaseID, k.ID); e != nil {
			return e
		}
	}
	return nil
}
