package repository

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

const independentConfigurationReadTTL = 2 * time.Minute

// Empty source lineage removes external source authorization, never physical
// protection. This capability needs the real fetch/build purpose and original
// config manifest, including on a generic pair with no onboarding operation.
type NextcloudIndependentConfigurationRead struct {
	mu         sync.Mutex
	store      *KnowledgeInputProvenanceStore
	tenant     uint64
	kb         string
	origin     knowledgeConfigurationInputOrigin
	plan       *KnowledgeConfigurationOriginalBodyReadPlan
	broad      NextcloudContentLease
	bodyLeases []string
	lifetime   context.Context
	cancel     context.CancelCauseFunc
	stop, done chan struct{}
	contexts   []func()
	err        error
	closed     bool
}

func (s *KnowledgeInputProvenanceStore) BeginScopedIndependentConfigurationRead(
	ctx context.Context,
	tenant uint64,
	kb string,
	lineage *types.SourceLineage,
) (*NextcloudIndependentConfigurationRead, error) {
	if s == nil || s.db == nil || ctx == nil || lineage.RequireComplete() != nil || len(lineage.Sources) != 0 {
		return nil, types.ErrSourceLineageUnknown
	}
	if _, err := scopedOnboardingConfigurationPurposeFromContext(ctx, tenant, kb); err != nil {
		return nil, err
	}
	var origin knowledgeConfigurationInputOrigin
	revision, err := knowledgeConfigurationControlRevision(s.db.WithContext(ctx), kb)
	if err != nil {
		return nil, err
	}
	if err := s.db.WithContext(ctx).
		Where("tenant_id = ? AND knowledge_base_id = ? AND config_revision = ?", tenant, kb, revision).
		Take(&origin).
		Error; err != nil {
		return nil, err
	}
	var saved types.SourceLineage
	if json.Unmarshal([]byte(origin.SourceLineage), &saved) != nil || saved.RequireComplete() != nil ||
		len(saved.Sources) != 0 ||
		origin.OriginID == "" ||
		origin.InputDigest == "" {
		return nil, types.ErrSourceLineageUnknown
	}
	lifetime, cancel := context.WithCancelCause(ctx)
	read := &NextcloudIndependentConfigurationRead{
		store:    s,
		tenant:   tenant,
		kb:       kb,
		origin:   origin,
		lifetime: lifetime,
		cancel:   cancel,
	}
	fail := func(err error) (*NextcloudIndependentConfigurationRead, error) { _ = read.Close(); return nil, err }
	// Plans are metadata only; bytes are selected only after their owned
	// durable body leases and the enclosing actual KB scope are acquired.
	read.plan, err = s.KnowledgeConfigurationBodyReadPlan(lifetime, tenant, kb)
	if err != nil {
		return fail(err)
	}
	if read.plan.OriginID != origin.OriginID || read.plan.ConfigRevision != origin.ConfigRevision ||
		read.plan.ConfigDigest != origin.InputDigest {
		return fail(ErrKnowledgeInputChanged)
	}
	owner := "nc-independent-config:" + uuid.NewString()
	read.broad, err = NewNextcloudContentLeaseStore(
		s.db,
	).AcquireKBRead(lifetime, tenant, kb, owner, independentConfigurationReadTTL)
	if err != nil {
		return fail(err)
	}
	read.bodyLeases, err = s.AcquireKnowledgeConfigurationBodyReadLeases(
		lifetime,
		read.plan,
		owner,
		independentConfigurationReadTTL,
	)
	if err != nil {
		return fail(err)
	}
	if err := read.Checkpoint(ctx); err != nil {
		return fail(err)
	}
	read.startHeartbeat()
	return read, nil
}

func (r *NextcloudIndependentConfigurationRead) checkMetadataInTx(ctx context.Context, tx *gorm.DB) error {
	if err := scopedOnboardingConfigurationPurposeInTx(ctx, tx, r.tenant, r.kb); err != nil {
		return err
	}
	if err := requireNextcloudTargetOpenInTx(tx, r.tenant, r.kb); err != nil {
		return err
	}
	revision, err := knowledgeConfigurationControlRevision(tx, r.kb)
	if err != nil || revision != r.origin.ConfigRevision {
		return ErrKnowledgeInputChanged
	}
	var origin knowledgeConfigurationInputOrigin
	if err := tx.Where("origin_id = ? AND tenant_id = ? AND knowledge_base_id = ? AND config_revision = ?", r.origin.OriginID, r.tenant, r.kb, revision).
		Take(&origin).
		Error; err != nil {
		return err
	}
	if !reflect.DeepEqual(origin, r.origin) {
		return ErrKnowledgeInputChanged
	}
	var lineage types.SourceLineage
	if json.Unmarshal([]byte(origin.SourceLineage), &lineage) != nil || lineage.RequireComplete() != nil ||
		len(lineage.Sources) != 0 {
		return types.ErrSourceLineageUnknown
	}
	leased := context.WithValue(
		ctx,
		faqReadLeaseCoverageKey{},
		faqReadLeaseCoverage{Leases: []NextcloudContentLease{r.broad}},
	)
	if err := validateFAQReadLeaseCoverageInTx(
		tx.WithContext(leased),
		[]types.KnowledgeSearchScope{{TenantID: r.tenant, KBID: r.kb}},
		&lineage,
	); err != nil {
		return err
	}
	if err := r.store.RecheckKnowledgeConfigurationBodyReadLeasesInTx(ctx, tx, r.plan, r.bodyLeases); err != nil {
		return err
	}
	return nil
}

func (r *NextcloudIndependentConfigurationRead) projectionInTx(
	ctx context.Context,
	tx *gorm.DB,
) (*types.KnowledgeBase, error) {
	if err := r.checkMetadataInTx(ctx, tx); err != nil {
		return nil, err
	}
	var current types.KnowledgeBase
	if err := tx.Select(knowledgeWorkerConfigurationColumns).
		Where("id = ? AND tenant_id = ?", r.kb, r.tenant).
		Take(&current).
		Error; err != nil {
		return nil, err
	}
	digest, err := knowledgeConfigurationOriginDigest(&current)
	if err != nil || digest != r.origin.InputDigest {
		return nil, ErrKnowledgeInputChanged
	}
	if err := r.checkMetadataInTx(ctx, tx); err != nil {
		return nil, err
	}
	return &current, nil
}

func (r *NextcloudIndependentConfigurationRead) checkInTx(ctx context.Context, tx *gorm.DB) error {
	_, err := r.projectionInTx(ctx, tx)
	return err
}

// Return one independently decoded body snapshot from the guarded transaction.
// Original-input proof callers use the InTx variant with their existing pool-1
// transaction; neither method recursively enters a body snapshot.
func (r *NextcloudIndependentConfigurationRead) ConfigurationProjection(
	ctx context.Context,
) (*types.KnowledgeBase, error) {
	if err := r.Checkpoint(ctx); err != nil {
		return nil, err
	}
	var current *types.KnowledgeBase
	err := r.store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		current, err = r.ConfigurationProjectionInTx(ctx, tx)
		return err
	})
	return current, err
}

func (r *NextcloudIndependentConfigurationRead) ConfigurationProjectionInTx(
	ctx context.Context,
	tx *gorm.DB,
) (*types.KnowledgeBase, error) {
	if r == nil || ctx == nil || tx == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	// Immutable capture fields and context cancellation are safe to inspect
	// without r.mu. Heartbeat may hold that mutex while waiting for this
	// pool-1 SQL connection: an InTx reader must never wait on it.
	if r.lifetime == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	if r.lifetime.Err() != nil {
		return nil, context.Cause(r.lifetime)
	}
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	current, err := r.projectionInTx(ctx, tx)
	if err != nil {
		r.cancel(err)
	}
	if r.lifetime.Err() != nil {
		return nil, context.Cause(r.lifetime)
	}
	return current, err
}

func (r *NextcloudIndependentConfigurationRead) renew(ctx context.Context) error {
	if _, err := NewNextcloudContentLeaseStore(
		r.store.db,
	).RenewLease(ctx, r.broad.ID, independentConfigurationReadTTL); err != nil {
		return err
	}
	return r.store.RecheckKnowledgeConfigurationBodyReadLeases(
		ctx,
		r.plan,
		r.bodyLeases,
		independentConfigurationReadTTL,
	)
}

func (r *NextcloudIndependentConfigurationRead) failureLocked(ctx context.Context) error {
	if r.err != nil {
		return r.err
	}
	if r.closed {
		return ErrNextcloudContentLeaseExpired
	}
	if r.lifetime.Err() != nil {
		return context.Cause(r.lifetime)
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return nil
}

func (r *NextcloudIndependentConfigurationRead) Checkpoint(ctx context.Context) error {
	if r == nil || ctx == nil {
		return types.ErrSourceLineageUnknown
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.failureLocked(ctx); err != nil {
		return err
	}
	if _, err := scopedOnboardingConfigurationPurposeFromContext(ctx, r.tenant, r.kb); err != nil {
		return err
	}
	if err := r.renew(ctx); err != nil {
		r.err = err
		r.cancel(err)
		return err
	}
	if err := r.store.db.WithContext(ctx).
		Transaction(func(tx *gorm.DB) error { return r.checkInTx(ctx, tx) }); err != nil {
		r.err = err
		r.cancel(err)
		return err
	}
	return nil
}

func (r *NextcloudIndependentConfigurationRead) Context(ctx context.Context) context.Context {
	if r == nil || ctx == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(r.lifetime, func() { cancel(context.Cause(r.lifetime)) })
	r.contexts = append(r.contexts, func() { stop(); cancel(ErrNextcloudContentLeaseExpired) })
	if r.closed {
		cancel(ErrNextcloudContentLeaseExpired)
	}
	if r.err != nil {
		cancel(r.err)
	}
	coverage, _ := ctx.Value(faqReadLeaseCoverageKey{}).(faqReadLeaseCoverage)
	all := append([]NextcloudContentLease{}, coverage.Leases...)
	found := false
	for _, lease := range all {
		found = found || lease.ID == r.broad.ID
	}
	if !found {
		all = append(all, r.broad)
	}
	return context.WithValue(current, faqReadLeaseCoverageKey{}, faqReadLeaseCoverage{Leases: all})
}

func (r *NextcloudIndependentConfigurationRead) startHeartbeat() {
	r.stop, r.done = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(r.done)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-r.stop:
				return
			case <-r.lifetime.Done():
				return
			case <-ticker.C:
				r.mu.Lock()
				if r.closed {
					r.mu.Unlock()
					return
				}
				ctx, cancel := context.WithTimeout(r.lifetime, 30*time.Second)
				err := r.renew(ctx)
				cancel()
				if err != nil {
					r.err = err
					r.cancel(err)
					r.mu.Unlock()
					return
				}
				r.mu.Unlock()
			}
		}
	}()
}

func (r *NextcloudIndependentConfigurationRead) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.cancel(ErrNextcloudContentLeaseExpired)
	if r.stop != nil {
		close(r.stop)
	}
	done := r.done
	contexts := append([]func(){}, r.contexts...)
	r.mu.Unlock()
	for _, close := range contexts {
		close()
	}
	if done != nil {
		<-done
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var errs []error
	if r.broad.ID != "" {
		errs = append(errs, NewNextcloudContentLeaseStore(r.store.db).ReleaseLease(ctx, r.broad.ID))
	}
	for _, id := range r.bodyLeases {
		errs = append(errs, NewOriginalBodyRetentionStore(r.store.db).ReleaseLease(ctx, id))
	}
	return errors.Join(errs...)
}
