package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type (
	// NextcloudCapacityFileResolver loads files considered by source capacity admission.
	NextcloudCapacityFileResolver func(context.Context, *types.KnowledgeBase) (interfaces.FileService, error)
	// NextcloudCapacityAdmission accounts for source capacity before accepting file processing.
	NextcloudCapacityAdmission struct {
		store   *repository.NextcloudCapacityStore
		resolve NextcloudCapacityFileResolver
	}
)

// NewNextcloudCapacityAdmission constructs the configured NextcloudCapacityAdmission.
func NewNextcloudCapacityAdmission(
	store *repository.NextcloudCapacityStore,
	resolve NextcloudCapacityFileResolver,
) *NextcloudCapacityAdmission {
	return &NextcloudCapacityAdmission{store: store, resolve: resolve}
}

// Begin captures the original inputs and current controls for this operation.
func (a *NextcloudCapacityAdmission) Begin(
	ctx context.Context,
	kb *types.KnowledgeBase,
	ds, external, etag string,
	bytes int64,
) (*repository.NextcloudCapacityReservation, error) {
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
	local, ok := fs.(interfaces.LocalCapacityIntentProvider)
	if !ok || !local.SupportsLocalCapacityIntents() {
		return nil, repository.ErrNextcloudCapacityUnavailable
	}
	measured, err := provider.MeasureStorageCapacity(ctx)
	if err != nil {
		return nil, errors.Join(repository.ErrNextcloudCapacityUnavailable, err)
	}
	if err := a.store.Observe(ctx, measured); err != nil {
		return nil, err
	}
	key := sha256.Sum256([]byte(ds + "\x00" + external + "\x00" + etag + "\x00" + uuid.NewString()))
	return a.store.ReserveDurable(
		ctx,
		kb.TenantID,
		kb.ID,
		*kb.StorageBackendID,
		hex.EncodeToString(key[:]),
		bytes,
		measured,
	)
}

// Finish records the result of source capacity admission.
func (a *NextcloudCapacityAdmission) Finish(
	ctx context.Context,
	r *repository.NextcloudCapacityReservation,
	k string,
	ok bool,
) error {
	return a.store.Finish(ctx, r, k, ok)
}

type capacityWriteSession struct {
	store       *repository.NextcloudCapacityStore
	reservation *repository.NextcloudCapacityReservation
	writer      string
	mutex       sync.Mutex
	lock        io.Closer
	closed      bool
}

func (s *capacityWriteSession) Token() string   { return s.reservation.Token }
func (s *capacityWriteSession) Backend() string { return s.reservation.BackendID }
func (s *capacityWriteSession) RetainWriterLock(lock io.Closer) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.closed || s.lock != nil {
		return repository.ErrNextcloudCapacityUnavailable
	}
	s.lock = lock
	return nil
}

func (s *capacityWriteSession) PrepareLocal(ctx context.Context, k, path, root, scope string, size int64) error {
	return s.store.PrepareLocal(ctx, s.reservation, s.writer, k, path, root, scope, size)
}

func (s *capacityWriteSession) LocalCreated(ctx context.Context, path, identity string) error {
	return s.store.LocalCreated(ctx, s.reservation, s.writer, path, identity)
}

func (s *capacityWriteSession) PhysicalWritten(ctx context.Context, path string, size int64, sha string) error {
	return s.store.PhysicalWritten(ctx, s.reservation, s.writer, path, size, sha)
}

func (s *capacityWriteSession) ValidateCatalog(ctx context.Context, path, k string) error {
	return s.store.ValidateWriter(ctx, s.reservation, s.writer, path, k)
}

func (s *capacityWriteSession) Cataloged(ctx context.Context, path, k, ref string) error {
	return s.store.Cataloged(ctx, s.reservation, s.writer, path, k, ref)
}

// WriteContext returns the context carrying the current capacity admission.
func (a *NextcloudCapacityAdmission) WriteContext(
	ctx context.Context,
	r *repository.NextcloudCapacityReservation,
) context.Context {
	return types.WithCapacityWriteSession(
		ctx,
		&capacityWriteSession{store: a.store, reservation: r, writer: uuid.NewString()},
	)
}

// End runs on every ingestion exit, including failures before Stage. The
// original-object writer lock spans storage, catalog, knowledge and staging.
func (a *NextcloudCapacityAdmission) End(
	ctx context.Context,
	r *repository.NextcloudCapacityReservation,
	kb *types.KnowledgeBase,
) {
	session, ok := types.CapacityWriteSessionFromContext(ctx).(*capacityWriteSession)
	if !ok {
		return
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	_ = a.store.MarkWriteFailed(cleanup, r, session.writer)
	session.mutex.Lock()
	if session.lock != nil {
		_ = session.lock.Close()
		session.lock = nil
	}
	session.closed = true
	session.mutex.Unlock()
	if e := a.store.CancelUnstarted(cleanup, r); e == nil {
		return
	}
	// A failure can leave a real partial file or an intact retained generation.
	// Reconciliation observes that exact object; errors leave its charge intact.
	_ = a.Reconcile(cleanup, kb, r)
}

// Reconcile reconciles source capacity accounting with current artifacts.
func (a *NextcloudCapacityAdmission) Reconcile(
	ctx context.Context,
	kb *types.KnowledgeBase,
	r *repository.NextcloudCapacityReservation,
) error {
	if a == nil || kb == nil || r == nil || kb.TenantID != r.TenantID || kb.ID != r.KnowledgeBaseID ||
		kb.StorageBackendID == nil ||
		*kb.StorageBackendID != r.BackendID {
		return repository.ErrNextcloudCapacityUnavailable
	}
	intent, e := a.store.Intent(ctx, r)
	if errors.Is(e, gorm.ErrRecordNotFound) {
		return a.store.CancelUnstarted(ctx, r)
	}
	if e != nil {
		return e
	}
	fs, e := a.resolve(ctx, kb)
	if e != nil {
		return e
	}
	provider, ok := fs.(interfaces.LocalCapacityIntentProvider)
	if !ok || !provider.SupportsLocalCapacityIntents() {
		return repository.ErrNextcloudCapacityUnavailable
	}
	expected := types.CapacityLocalObject{
		Token:             r.Token,
		TenantKnowledgeID: intent.KnowledgeID,
		BackendID:         r.BackendID,
		PhysicalPath:      intent.PhysicalPath,
		RootIdentity:      intent.RootIdentity,
		ScopeID:           r.ScopeID,
	}
	return provider.InspectCapacityIntent(
		ctx,
		expected,
		func(o types.CapacityLocalObject) error { return a.store.ReconcileLocal(ctx, r, intent, o) },
	)
}

// ConfigureDataSourceCapacityAdmission configures the data source capacity admission boundary.
func ConfigureDataSourceCapacityAdmission(source interfaces.DataSourceService, admission *NextcloudCapacityAdmission) {
	if service, ok := source.(*DataSourceService); ok {
		service.capacityAdmission = admission
	}
}

// Status returns the current operation state under its scoped controls.
func (a *NextcloudCapacityAdmission) Status(
	ctx context.Context,
	kb *types.KnowledgeBase,
) (repository.NextcloudCapacityStatus, error) {
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
