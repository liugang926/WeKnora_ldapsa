package repository

import (
	"context"
	"errors"
	"fmt"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

// Configuration rebuild uses the same authenticated fresh-artifact publication
// transaction as content replacement; old derived output is never re-admitted.
func (s *KnowledgeInputProvenanceStore) PublishDingTalkConfigurationRebuild(ctx context.Context, k *types.Knowledge, input []byte) (*DingTalkPublicationResult, error) {
	return s.PublishDingTalkOriginal(ctx, k, input)
}

// RetireDingTalkDocument must only be called by the service for a complete,
// successful remote enumeration or explicit authoritative deletion. No client
// metadata is accepted as enumeration evidence. The snapshot fences stale runs.
func (s *KnowledgeInputProvenanceStore) RetireDingTalkDocument(ctx context.Context, external string, complete bool) error {
	snap, ok := ctx.Value(dingTalkPublicationKey{}).(dingTalkPublicationSnapshot)
	if !ok || !complete || external == "" {
		return ErrDingTalkPublicationConflict
	}
	f := snap.Fetch
	current, present := ctx.Value(dingTalkFetchKey{}).(dingTalkFetch)
	if !present || current.Invocation != f.Invocation {
		return ErrDingTalkPublicationConflict
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := lockDingTalkPublicationSource(tx, f); e != nil {
			return e
		}
		var p dingTalkPublication
		if e := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND data_source_id = ? AND external_id = ?", f.Tenant, f.KB, f.Source, external).Take(&p).Error; e != nil {
			return e
		}
		if p.Generation != snap.Generations[external] {
			return ErrDingTalkPublicationConflict
		}
		if p.Tombstone {
			return nil
		}
		var k types.Knowledge
		e := tx.Unscoped().Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", p.KnowledgeID, f.Tenant, f.KB).Take(&k).Error
		if e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
			return e
		}
		exists := e == nil
		if exists {
			externallyRetired := k.DeletedAt.Valid
			if !k.DeletedAt.Valid {
				if e := dingTalkCAS(tx.Model(&types.Knowledge{}).Where("id = ? AND tenant_id = ?", k.ID, f.Tenant).Update("deleted_at", time.Now().UTC())); e != nil {
					return e
				}
			}
			intent := DingTalkWorkIntent{ID: uuid.NewString(), TenantID: f.Tenant, KnowledgeBaseID: f.KB, DataSourceID: f.Source, ExternalID: external, KnowledgeID: k.ID, Kind: "cleanup", Generation: p.Generation, Version: 1, State: "pending", ProcessAttempt: 1, CapacitySettled: externallyRetired}
			if e := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&intent).Error; e != nil {
				return e
			}
		}
		return dingTalkCAS(tx.Model(&dingTalkPublication{}).Where("tenant_id = ? AND knowledge_base_id = ? AND data_source_id = ? AND external_id = ? AND generation = ?", f.Tenant, f.KB, f.Source, external, p.Generation).Updates(map[string]any{"tombstone": true, "generation": p.Generation + 1}))
	})
}

// DingTalkQueueKey is stable across uncertain enqueue acknowledgements.
func DingTalkQueueKey(i DingTalkWorkIntent) string {
	return fmt.Sprintf("dingtalk:%s:%d:%d", i.KnowledgeID, i.Generation, i.ProcessAttempt)
}

// RetryDingTalkProcessing allocates a new key only after a confirmed failed
// processing attempt; uncertain enqueue repeats the old key instead.
func (s *KnowledgeInputProvenanceStore) RetryDingTalkProcessing(ctx context.Context, i DingTalkWorkIntent, previousTaskTerminal bool) (DingTalkWorkIntent, error) {
	k, _, e := s.LoadDingTalkWork(ctx, i)
	if e != nil {
		return i, e
	}
	if !previousTaskTerminal || i.Kind != "process" || i.State != "done" || k.ParseStatus != "failed" {
		return i, ErrDingTalkPublicationConflict
	}
	f := ctx.Value(dingTalkFetchKey{}).(dingTalkFetch)
	e = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := lockDingTalkPublicationSource(tx, f); e != nil {
			return e
		}
		var current types.Knowledge
		if e := tx.Where("id = ? AND parse_status = 'failed'", k.ID).Take(&current).Error; e != nil {
			return e
		}
		return dingTalkCAS(tx.Model(&DingTalkWorkIntent{}).Where("id = ? AND version = ? AND state = 'done' AND generation = ?", i.ID, i.Version, i.Generation).Updates(map[string]any{"state": "pending", "version": i.Version + 1, "process_attempt": i.ProcessAttempt + 1}))
	})
	if e == nil {
		i.State = "pending"
		i.Version++
		i.ProcessAttempt++
	}
	return i, e
}

// SettleDingTalkCleanupCapacity is called after idempotent external deletion.
// Do not also invoke the generic cleanup helper's AdjustStorageUsed path.
func (s *KnowledgeInputProvenanceStore) SettleDingTalkCleanupCapacity(ctx context.Context, i DingTalkWorkIntent) error {
	if i.Kind != "cleanup" {
		return ErrDingTalkPublicationConflict
	}
	if _, _, e := s.LoadDingTalkWork(ctx, i); e != nil {
		return e
	}
	f := ctx.Value(dingTalkFetchKey{}).(dingTalkFetch)
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := lockDingTalkPublicationSource(tx, f); e != nil {
			return e
		}
		var stored DingTalkWorkIntent
		if e := tx.Where("id = ? AND version = ?", i.ID, i.Version).Take(&stored).Error; e != nil {
			return e
		}
		if stored.CapacitySettled {
			return nil
		}
		if stored != i {
			return ErrDingTalkPublicationConflict
		}
		var k types.Knowledge
		if e := tx.Unscoped().Where("id = ? AND tenant_id = ?", i.KnowledgeID, f.Tenant).Take(&k).Error; e != nil {
			return e
		}
		var tenant types.Tenant
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", f.Tenant).Take(&tenant).Error; e != nil {
			return e
		}
		amount := k.StorageSize
		if amount < 0 {
			return ErrKnowledgeInputChanged
		}
		remaining := tenant.StorageUsed - amount
		if remaining < 0 {
			remaining = 0
		}
		if e := dingTalkCAS(tx.Model(&types.Tenant{}).Where("id = ?", f.Tenant).Update("storage_used", remaining)); e != nil {
			return e
		}
		if e := dingTalkCAS(tx.Unscoped().Model(&types.Knowledge{}).Where("id = ? AND tenant_id = ?", k.ID, f.Tenant).Update("storage_size", 0)); e != nil {
			return e
		}
		return dingTalkCAS(tx.Model(&DingTalkWorkIntent{}).Where("id = ? AND version = ? AND capacity_settled = ?", i.ID, i.Version, false).Updates(map[string]any{"capacity_settled": true, "version": i.Version + 1}))
	})
}

// ReconcileDingTalkDeletedPublications observes existing local deletion facts.
// Call before a NEW Begin/fetch: retirement invalidates the old fetch snapshot.
// A physically absent row has no recoverable resource metadata; no invented
// file path or capacity adjustment is emitted for it.
func (s *KnowledgeInputProvenanceStore) ReconcileDingTalkDeletedPublications(ctx context.Context) error {
	snap, ok := ctx.Value(dingTalkPublicationKey{}).(dingTalkPublicationSnapshot)
	if !ok {
		return ErrDingTalkPublicationConflict
	}
	for external := range snap.Generations {
		var p dingTalkPublication
		if e := s.db.WithContext(ctx).Where("tenant_id = ? AND knowledge_base_id = ? AND data_source_id = ? AND external_id = ?", snap.Fetch.Tenant, snap.Fetch.KB, snap.Fetch.Source, external).Take(&p).Error; e != nil {
			return e
		}
		if p.Tombstone {
			continue
		}
		var k types.Knowledge
		e := s.db.WithContext(ctx).Unscoped().Where("id = ? AND tenant_id = ?", p.KnowledgeID, snap.Fetch.Tenant).Take(&k).Error
		if e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
			return e
		}
		if errors.Is(e, gorm.ErrRecordNotFound) || k.DeletedAt.Valid {
			if e := s.RetireDingTalkDocument(ctx, external, true); e != nil {
				return e
			}
		}
	}
	return nil
}
