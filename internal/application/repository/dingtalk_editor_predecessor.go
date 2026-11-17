package repository

import (
	"context"
	"crypto/sha256"
	"errors"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type dingTalkEditorPredecessorKey struct{}

type dingTalkEditorPredecessor struct {
	tenant                                     uint64
	source, kb, kind, schedule, mode, conflict string
	paused, enabled, deletions                 bool
	config                                     [sha256.Size]byte
}

func dingTalkEditorState(ds *types.DataSource) dingTalkEditorPredecessor {
	return dingTalkEditorPredecessor{
		tenant: ds.TenantID, source: ds.ID, kb: ds.KnowledgeBaseID, kind: ds.Type,
		schedule: ds.SyncSchedule, mode: ds.SyncMode, conflict: ds.ConflictStrategy,
		deletions: ds.SyncDeletions, paused: ds.Status == types.DataSourceStatusPaused,
		enabled: ds.SyncEnabled == nil || *ds.SyncEnabled, config: sha256.Sum256(ds.Config),
	}
}

// WithDingTalkEditorPredecessor binds an editor write to the saved scope and credentials
// observed before validation. Worker progress and transient errors are independent.
func WithDingTalkEditorPredecessor(ctx context.Context, before *types.DataSource) context.Context {
	// Keep the HTTP authorization lookup's snapshot when the service reads the source again.
	if _, exists := ctx.Value(dingTalkEditorPredecessorKey{}).(dingTalkEditorPredecessor); exists {
		return ctx
	}
	return context.WithValue(ctx, dingTalkEditorPredecessorKey{}, dingTalkEditorState(before))
}

func (r *connectorScheduleSourceRepository) writeDingTalkEditorChange(ctx context.Context, ds *types.DataSource,
	before dingTalkEditorPredecessor, write func(*DataSourceRepository) error,
) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Use the same KB -> source lock ordering as the configuration and credential writers.
		if err := admitDataSourceKnowledgeBase(ctx, tx, ds, true); err != nil {
			return err
		}
		var current types.DataSource
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", ds.ID, ds.TenantID, ds.KnowledgeBaseID).
			Take(&current).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return types.ErrDataSourceSettingsConflict
		}
		if err != nil {
			return err
		}
		if before != dingTalkEditorState(&current) {
			return types.ErrDataSourceSettingsConflict
		}
		scoped := *r.DataSourceRepository
		scoped.db = tx
		return write(&scoped)
	})
}
