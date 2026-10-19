package service

import (
	"context"
	"encoding/json"
	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Original live producer holder. Its metadata snapshots are private native
// values, never reconstructed from Redis frames or current message bodies.
func (s *messageService) MessageNativeLiveIdentity(ctx context.Context, session, message string) (*types.Message, string, error) {
	if s.materialStore == nil {
		return nil, "", access.ErrNextcloudPublicationUnavailable
	}
	return s.materialStore.MessageLiveControl(ctx, session, message)
}
func (s *messageService) CaptureNativeLiveMaterialParents(ctx context.Context) ([]*types.MessageMaterialSnapshot, error) {
	parents, err := messageMaterialBoundarySnapshots(ctx)
	if err != nil {
		return nil, err
	}
	for _, parent := range parents {
		parent.Messages = nil
	}
	return parents, nil
}
func (s *messageService) BeginNativeLiveMaterialReceiver(ctx context.Context, parents []*types.MessageMaterialSnapshot) (func() error, func(), error) {
	if s.materialStore == nil || s.sourceLineageGuard == nil || len(parents) == 0 {
		return nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	guards := []*MessageMaterialGuard{}
	var closed atomic.Bool
	var once sync.Once
	close := func() {
		once.Do(func() {
			closed.Store(true)
			for _, guard := range guards {
				guard.Close()
			}
		})
	}
	for _, parent := range parents {
		if parent == nil {
			close()
			return nil, nil, types.ErrSourceLineageUnknown
		}
		guard, err := s.beginMessageMaterialRead(ctx, parent.Request)
		if err != nil {
			close()
			return nil, nil, err
		}
		actual, err := guard.OriginalSnapshot()
		if err != nil {
			guard.Close()
			close()
			return nil, nil, err
		}
		a, b := *actual, *parent
		a.Messages = nil
		b.Messages = nil
		// Deep serialized metadata equality includes receipt/control/owner counters.
		if !messageLiveParentEqual(&a, &b) {
			guard.Close()
			close()
			return nil, nil, access.ErrNextcloudPublicationDenied
		}
		guards = append(guards, guard)
	}
	check := func() error {
		if closed.Load() {
			return access.ErrNextcloudPublicationUnavailable
		}
		for _, guard := range guards {
			if err := guard.Recheck(); err != nil {
				return err
			}
		}
		return nil
	}
	return check, close, nil
}

func messageLiveParentEqual(a, b *types.MessageMaterialSnapshot) bool {
	x, err := json.Marshal(a)
	if err != nil {
		return false
	}
	y, err := json.Marshal(b)
	return err == nil && string(x) == string(y)
}

// A registered frame uses the actual immutable producer execution selectors.
// Current receiver permissions are reloaded independently from the producer.
func (s *messageService) CheckNativeLiveFramePublication(ctx context.Context, control *types.Message, lineage *types.SourceLineage) error {
	if control == nil || lineage.RequireComplete() != nil {
		return access.ErrNextcloudPublicationDenied
	}
	actual := *control
	actual.SourceLineage = lineage
	return s.CheckLiveMessagePublication(ctx, &actual)
}

func (s *messageService) CheckNativeLiveIdentity(ctx context.Context, original *types.Message, generation string) error {
	if original == nil || generation == "" {
		return types.ErrSourceLineageUnknown
	}
	actual, current, err := s.MessageNativeLiveIdentity(ctx, original.SessionID, original.ID)
	if err != nil {
		return err
	}
	if current != generation {
		return access.ErrNextcloudPublicationDenied
	}
	x, err := json.Marshal(struct {
		Body        *types.Message
		Execution   types.MessageExecutionContext
		AgentTenant uint64
	}{original, original.ExecutionContext, original.AgentTenantID})
	if err != nil {
		return err
	}
	y, err := json.Marshal(struct {
		Body        *types.Message
		Execution   types.MessageExecutionContext
		AgentTenant uint64
	}{actual, actual.ExecutionContext, actual.AgentTenantID})
	if err != nil {
		return err
	}
	if string(x) != string(y) {
		return access.ErrNextcloudPublicationDenied
	}
	return nil
}

func (s *messageService) BeginNativeLiveFrameSources(ctx context.Context, control *types.Message, lineage *types.SourceLineage) (func() error, func(), error) {
	originalLineage, err := types.UnionSourceLineage(lineage)
	if err != nil {
		return nil, nil, err
	}
	lineage = originalLineage
	originalControl, err := cloneCollection(control)
	if err != nil {
		return nil, nil, err
	}
	if control == nil {
		return nil, nil, access.ErrNextcloudPublicationDenied
	}
	originalControl.ExecutionContext, err = cloneCollection(control.ExecutionContext)
	if err != nil {
		return nil, nil, err
	}
	originalControl.AgentTenantID = control.AgentTenantID
	control = originalControl
	if err := s.CheckNativeLiveFramePublication(ctx, control, lineage); err != nil {
		return nil, nil, err
	}
	if s.materialStore == nil {
		return nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	store := s.materialStore.ContentLeaseStore()
	scopes := map[types.KnowledgeSearchScope]bool{}
	for _, source := range lineage.Sources {
		scopes[types.KnowledgeSearchScope{TenantID: source.TenantID, KBID: source.KnowledgeBaseID}] = true
	}
	ordered := []types.KnowledgeSearchScope{}
	for scope := range scopes {
		ordered = append(ordered, scope)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].TenantID != ordered[j].TenantID {
			return ordered[i].TenantID < ordered[j].TenantID
		}
		return ordered[i].KBID < ordered[j].KBID
	})
	owner := uuid.NewString()
	leases := []repository.NextcloudContentLease{}
	broad := map[types.KnowledgeSearchScope]repository.NextcloudContentLease{}
	var closed atomic.Bool
	var once sync.Once
	close := func() {
		once.Do(func() {
			closed.Store(true)
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for i := len(leases) - 1; i >= 0; i-- {
				_ = store.ReleaseLease(cleanup, leases[i].ID)
			}
		})
	}
	for _, scope := range ordered {
		lease, err := store.AcquireKBRead(ctx, scope.TenantID, scope.KBID, owner, 2*time.Minute)
		if err != nil {
			close()
			return nil, nil, err
		}
		leases = append(leases, lease)
		broad[scope] = lease
	}
	for _, source := range lineage.Sources {
		kb := broad[types.KnowledgeSearchScope{TenantID: source.TenantID, KBID: source.KnowledgeBaseID}]
		lease, err := store.UpgradeKBRead(ctx, kb.ID, repository.NextcloudContentScope{TenantID: source.TenantID, KnowledgeBaseID: source.KnowledgeBaseID, KnowledgeID: source.KnowledgeID, DataSourceID: source.DataSourceID, ExternalID: source.ExternalID}, owner, 2*time.Minute)
		if err != nil {
			close()
			return nil, nil, err
		}
		leases = append(leases, lease)
		if uint64(lease.Epoch) != source.Revision {
			close()
			return nil, nil, repository.ErrKnowledgeInputChanged
		}
	}
	check := func() error {
		if closed.Load() {
			return access.ErrNextcloudPublicationUnavailable
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, lease := range leases {
			if _, err := store.RenewLease(ctx, lease.ID, 2*time.Minute); err != nil {
				return err
			}
		}
		return s.CheckNativeLiveFramePublication(ctx, control, lineage)
	}
	if err := check(); err != nil {
		close()
		return nil, nil, err
	}
	return check, close, nil
}
