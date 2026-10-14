// Package readlease holds durable source-content fences during retrieval and response assembly.
package readlease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
)

const (
	nextcloudReadTTL       = 2 * time.Minute
	nextcloudReadHeartbeat = 30 * time.Second
)

// NextcloudReadLeaseStore is deliberately smaller than the repository. It
// lets retrieval tests force retirement or a failed renewal at a checkpoint.
type NextcloudReadLeaseStore interface {
	AcquireKBRead(context.Context, uint64, string, string, time.Duration) (repository.NextcloudContentLease, error)
	UpgradeKBRead(context.Context, string, repository.NextcloudContentScope, string, time.Duration) (
		repository.NextcloudContentLease,
		error,
	)
	RenewLease(context.Context, string, time.Duration) (repository.NextcloudContentLease, error)
	ReleaseLease(context.Context, string) error
}

// NextcloudKBReadScope identifies the tenant and knowledge base protected by a broad read lease.
type NextcloudKBReadScope struct {
	TenantID uint64
	KBID     string
}

// NextcloudReadGuard holds the broad KB fences while a vector/keyword lookup
// discovers document IDs. An exact lease is added before a result is hydrated.
// The broad leases remain through the response assembly; this is conservative
// for GC, and avoids a gap while other result IDs are still being discovered.
type NextcloudReadGuard struct {
	store     NextcloudReadLeaseStore
	parentCtx context.Context
	ctx       context.Context
	cancel    context.CancelFunc
	owner     string
	mu        sync.Mutex
	rows      []repository.NextcloudContentLease
	broad     map[NextcloudKBReadScope]string
	exact     map[repository.NextcloudContentScope]bool
	err       error
	stop      chan struct{}
	stopped   chan struct{}
	started   bool
	closeOnce sync.Once
	heartbeat time.Duration
}

type nextcloudReadGuardContextKey struct{}

// NextcloudReadGuardFromContext returns the read guard attached to the request context.
func NextcloudReadGuardFromContext(ctx context.Context) *NextcloudReadGuard {
	guard, _ := ctx.Value(nextcloudReadGuardContextKey{}).(*NextcloudReadGuard)
	return guard
}

// BeginNextcloudKBRead starts before index or document rows are loaded. Scope
// tenant IDs come from trusted KB/search-target rows, never a model handle.
func BeginNextcloudKBRead(ctx context.Context, store NextcloudReadLeaseStore,
	scopes []NextcloudKBReadScope,
) (*NextcloudReadGuard, error) {
	if store == nil {
		return nil, apperrors.NewProtocolError(errors.New(
			"nextcloud read lease store unavailable",
		), "Nextcloud read lease store unavailable")
	}
	unique := make(map[NextcloudKBReadScope]bool, len(scopes))
	ordered := make([]NextcloudKBReadScope, 0, len(scopes))
	for _, scope := range scopes {
		if scope.TenantID == 0 || scope.KBID == "" {
			return nil, repository.ErrNextcloudContentLeaseInvalid
		}
		if !unique[scope] {
			unique[scope] = true
			ordered = append(ordered, scope)
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].TenantID != ordered[j].TenantID {
			return ordered[i].TenantID < ordered[j].TenantID
		}
		return ordered[i].KBID < ordered[j].KBID
	})
	leaseCtx, cancel := context.WithCancel(ctx)
	g := &NextcloudReadGuard{
		store: store, parentCtx: ctx, ctx: leaseCtx, cancel: cancel,
		owner: uuid.NewString(), broad: make(map[NextcloudKBReadScope]string),
		exact: make(map[repository.NextcloudContentScope]bool),
		stop:  make(chan struct{}), stopped: make(chan struct{}), heartbeat: nextcloudReadHeartbeat,
	}
	for _, scope := range ordered {
		row, err := store.AcquireKBRead(leaseCtx, scope.TenantID, scope.KBID, g.owner, nextcloudReadTTL)
		if err != nil {
			_ = g.Close()
			return nil, err
		}
		g.rows = append(g.rows, row)
		g.broad[scope] = row.ID
	}
	g.started = true
	go g.heartbeatLoop()
	return g, nil
}

// Context returns the lease-guarded request context.
func (g *NextcloudReadGuard) Context() context.Context {
	if g == nil {
		return nil
	}
	return context.WithValue(g.ctx, nextcloudReadGuardContextKey{}, g)
}

// Covers reports whether the guard holds every requested broad read scope.
func (g *NextcloudReadGuard) Covers(scopes []NextcloudKBReadScope) bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, scope := range scopes {
		if g.broad[scope] == "" {
			return false
		}
	}
	return true
}

// NextcloudKnowledgeLeaseScope returns a durable generation identity. Marked
// rows with malformed or incomplete identity fail closed, including a row
// whose Nextcloud metadata survives an incorrect channel rewrite.
func NextcloudKnowledgeLeaseScope(k *types.Knowledge) (repository.NextcloudContentScope, bool, error) {
	var scope repository.NextcloudContentScope
	if k == nil {
		return scope, false, errors.New("knowledge is missing")
	}
	var metadata map[string]json.RawMessage
	if len(k.Metadata) > 0 {
		if err := json.Unmarshal(k.Metadata, &metadata); err != nil {
			return scope, true, fmt.Errorf("invalid knowledge metadata: %w", err)
		}
	}
	marked := k.Channel == types.ConnectorTypeNextcloud || metadata["nextcloud_file_id"] != nil ||
		metadata["nextcloud_binding_id"] != nil || metadata["nextcloud_instance_id"] != nil
	if !marked {
		return scope, false, nil
	}
	var dsID, externalID string
	if err := json.Unmarshal(metadata["datasource_id"], &dsID); err != nil || dsID == "" {
		return scope, true, apperrors.NewProtocolError(errors.New(
			"nextcloud data source identity is missing",
		), "Nextcloud data source identity is missing")
	}
	if err := json.Unmarshal(metadata["external_id"], &externalID); err != nil || externalID == "" {
		return scope, true, apperrors.NewProtocolError(errors.New(
			"nextcloud external identity is missing",
		), "Nextcloud external identity is missing")
	}
	if k.TenantID == 0 || k.KnowledgeBaseID == "" || k.ID == "" {
		return scope, true, repository.ErrNextcloudContentLeaseInvalid
	}
	return repository.NextcloudContentScope{
		TenantID:        k.TenantID,
		KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID,
		DataSourceID: dsID, ExternalID: externalID,
	}, true, nil
}

// PinKnowledge converts a broad search lease into an exact generation fence
// before the caller reads its derived chunks. Ordinary rows need no exact
// fence. The source publication guard remains a separate authorization check.
func (g *NextcloudReadGuard) PinKnowledge(k *types.Knowledge) error {
	scope, marked, err := NextcloudKnowledgeLeaseScope(k)
	if err != nil || !marked {
		return err
	}
	if g == nil {
		return apperrors.NewProtocolError(errors.New(
			"nextcloud knowledge has no read lease",
		), "Nextcloud knowledge has no read lease")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.currentErrorLocked(); err != nil {
		return err
	}
	if g.exact[scope] {
		return nil
	}
	broadID := g.broad[NextcloudKBReadScope{TenantID: scope.TenantID, KBID: scope.KnowledgeBaseID}]
	if broadID == "" {
		return g.failLocked(repository.ErrNextcloudContentLeaseDenied)
	}
	row, err := g.store.UpgradeKBRead(g.ctx, broadID, scope, g.owner, nextcloudReadTTL)
	if err != nil {
		return g.failLocked(err)
	}
	g.rows = append(g.rows, row)
	g.exact[scope] = true
	return nil
}

func (g *NextcloudReadGuard) currentErrorLocked() error {
	// The child context is also cancelled by failLocked and Close. Only the
	// original request context can distinguish an external cancellation from
	// an internal lease failure, whose concrete error must remain visible.
	if err := g.parentCtx.Err(); err != nil {
		return err
	}
	if g.err != nil {
		return g.err
	}
	return g.ctx.Err()
}

func (g *NextcloudReadGuard) failLocked(err error) error {
	if g.err == nil {
		g.err = err
		g.cancel()
	}
	return g.currentErrorLocked()
}

// Verify is a response checkpoint. A retirement, expiration, failed DB call,
// or cancellation suppresses source-derived output even if a prior lease was
// successfully acquired.
func (g *NextcloudReadGuard) Verify() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.currentErrorLocked(); err != nil {
		return err
	}
	for _, row := range g.rows {
		if _, err := g.store.RenewLease(g.ctx, row.ID, nextcloudReadTTL); err != nil {
			return g.failLocked(err)
		}
	}
	return g.currentErrorLocked()
}

func (g *NextcloudReadGuard) heartbeatLoop() {
	defer close(g.stopped)
	ticker := time.NewTicker(g.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-g.stop:
			return
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			if g.Verify() != nil {
				return
			}
		}
	}
}

// Close releases all rows with a bounded detached context so a cancelled
// request does not unnecessarily hold GC until TTL expiry.
func (g *NextcloudReadGuard) Close() error {
	if g == nil {
		return nil
	}
	var closeErr error
	g.closeOnce.Do(func() {
		close(g.stop)
		g.cancel()
		// A partial acquisition has no heartbeat goroutine yet.
		if g.started {
			<-g.stopped
		}
		g.mu.Lock()
		rows := append([]repository.NextcloudContentLease(nil), g.rows...)
		g.mu.Unlock()
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(g.ctx), 5*time.Second)
		defer cancel()
		for _, row := range rows {
			if err := g.store.ReleaseLease(releaseCtx, row.ID); err != nil {
				closeErr = errors.Join(closeErr, err)
			}
		}
	})
	return closeErr
}
