package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type NextcloudCapacityFileResolver func(context.Context, *types.KnowledgeBase) (interfaces.FileService, error)
type NextcloudCapacityAdmission struct {
	store   *repository.NextcloudCapacityStore
	resolve NextcloudCapacityFileResolver
}

func NewNextcloudCapacityAdmission(store *repository.NextcloudCapacityStore, resolve NextcloudCapacityFileResolver) *NextcloudCapacityAdmission {
	return &NextcloudCapacityAdmission{store: store, resolve: resolve}
}
func (a *NextcloudCapacityAdmission) Begin(ctx context.Context, kb *types.KnowledgeBase, ds, external, etag string, bytes int64) (*repository.NextcloudCapacityReservation, error) {
	if a == nil || a.store == nil || a.resolve == nil || kb == nil || kb.StorageBackendID == nil {
		return nil, repository.ErrNextcloudCapacityUnavailable
	}
	fs, err := a.resolve(ctx, kb)
	if err != nil {
		return nil, err
	}
	provider, ok := fs.(interfaces.StorageCapacityProvider)
	if !ok {
		return nil, repository.ErrNextcloudCapacityUnavailable
	}
	measured, err := provider.MeasureStorageCapacity(ctx)
	if err != nil {
		return nil, errors.Join(repository.ErrNextcloudCapacityUnavailable, err)
	}
	if err := a.store.Observe(ctx, measured); err != nil {
		return nil, err
	}
	key := sha256.Sum256([]byte(ds + "\x00" + external + "\x00" + etag))
	return a.store.Reserve(ctx, kb.TenantID, kb.ID, *kb.StorageBackendID, hex.EncodeToString(key[:]), bytes, measured)
}
func (a *NextcloudCapacityAdmission) Finish(ctx context.Context, r *repository.NextcloudCapacityReservation, k string, ok bool) error {
	return a.store.Finish(ctx, r, k, ok)
}
func ConfigureDataSourceCapacityAdmission(source interfaces.DataSourceService, admission *NextcloudCapacityAdmission) {
	if service, ok := source.(*DataSourceService); ok {
		service.capacityAdmission = admission
	}
}

func (a *NextcloudCapacityAdmission) Status(ctx context.Context, kb *types.KnowledgeBase) (repository.NextcloudCapacityStatus, error) {
	if a == nil || a.resolve == nil || kb == nil || kb.StorageBackendID == nil {
		return repository.NextcloudCapacityStatus{}, repository.ErrNextcloudCapacityUnavailable
	}
	fs, err := a.resolve(ctx, kb)
	if err != nil {
		return repository.NextcloudCapacityStatus{}, err
	}
	provider, ok := fs.(interfaces.StorageCapacityProvider)
	if !ok {
		return repository.NextcloudCapacityStatus{}, repository.ErrNextcloudCapacityUnavailable
	}
	m, err := provider.MeasureStorageCapacity(ctx)
	if err != nil {
		return repository.NextcloudCapacityStatus{}, err
	}
	if err = a.store.Observe(ctx, m); err != nil {
		return repository.NextcloudCapacityStatus{}, err
	}
	return a.store.Status(ctx, kb.TenantID, kb.ID, *kb.StorageBackendID, m)
}
