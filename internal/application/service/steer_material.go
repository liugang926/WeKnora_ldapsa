package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
)

type (
	actualSteerHTTPInputKey      struct{}
	actualSteerHTTPInputBoundary struct{ Digest string }
)

// WithActualSteerHTTPInput attaches the actual injected HTTP input receipt to a context.
func WithActualSteerHTTPInput(ctx context.Context, request *types.SteerAuthoredRequest) (context.Context, error) {
	if request == nil {
		return ctx, types.ErrSourceLineageUnknown
	}
	digest, err := repository.KnowledgeInputDigest(request)
	if err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, actualSteerHTTPInputKey{}, actualSteerHTTPInputBoundary{digest}), nil
}

func (s *messageService) AdmitActualSteerInput(
	ctx context.Context,
	request *types.SteerAuthoredRequest,
) (*types.SteerMaterialInput, error) {
	if s.materialStore == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	boundary, ok := ctx.Value(actualSteerHTTPInputKey{}).(actualSteerHTTPInputBoundary)
	digest, err := repository.KnowledgeInputDigest(request)
	if err != nil {
		return nil, err
	}
	if !ok || digest != boundary.Digest {
		return nil, types.ErrSourceLineageUnknown
	}
	return s.materialStore.AdmitSteerAuthoredInput(ctx, request)
}

func (s *messageService) ApplyActualSteerControl(
	ctx context.Context,
	request *types.SteerControlRequest,
) (*types.SteerCandidateControl, error) {
	if s.materialStore == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	return s.materialStore.ApplySteerControl(ctx, request)
}

func (s *messageService) CheckActualSteerControl(ctx context.Context, control *types.SteerCandidateControl) error {
	if s.materialStore == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	return s.materialStore.CheckSteerControl(ctx, control)
}

func (s *messageService) BeginSteerMaterialRead(
	ctx context.Context,
	request *types.SteerMaterialRequest,
) (*types.SteerMaterialSnapshot, func() error, func(), error) {
	snapshot, _, check, closeRead, err := s.beginSteerMaterialRead(ctx, request)
	return snapshot, check, closeRead, err
}

func (s *messageService) beginSteerMaterialRead(
	ctx context.Context,
	request *types.SteerMaterialRequest,
) (*types.SteerMaterialSnapshot, context.Context, func() error, func(), error) {
	if s.materialStore == nil || s.sourceLineageGuard == nil {
		return nil, ctx, nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	plan, err := s.materialStore.PeekSteerMaterialPlan(ctx, request)
	if err != nil {
		return nil, ctx, nil, nil, err
	}
	owner := uuid.NewString()
	bodyLeases, err := s.materialStore.AcquireSteerOriginalBodyLeases(ctx, plan, owner, 2*time.Minute)
	if err != nil {
		return nil, ctx, nil, nil, err
	}
	leases := []repository.NextcloudContentLease{}
	var once sync.Once
	closed := false
	var mu sync.Mutex
	closeRead2 := func() {
		once.Do(func() {
			mu.Lock()
			closed = true
			mu.Unlock()
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for i := len(leases) - 1; i >= 0; i-- {
				_ = s.materialStore.ContentLeaseStore().ReleaseLease(cleanup, leases[i].ID)
			}
			for i := len(bodyLeases) - 1; i >= 0; i-- {
				_ = s.materialStore.ReleaseMessageBodyReadLease(cleanup, bodyLeases[i])
			}
		})
	}
	broad := map[types.KnowledgeSearchScope]repository.NextcloudContentLease{}
	for _, scope := range plan.Scopes {
		lease, err := s.materialStore.ContentLeaseStore().
			AcquireKBRead(ctx, scope.TenantID, scope.KBID, owner, 2*time.Minute)
		if err != nil {
			closeRead2()
			return nil, ctx, nil, nil, err
		}
		leases = append(leases, lease)
		broad[scope] = lease
	}
	for _, source := range plan.SourceLineage.Sources {
		kb := broad[types.KnowledgeSearchScope{TenantID: source.TenantID, KBID: source.KnowledgeBaseID}]
		lease, err := s.materialStore.ContentLeaseStore().
			UpgradeKBRead(ctx, kb.ID, repository.NextcloudContentScope{TenantID: source.TenantID, KnowledgeBaseID: source.KnowledgeBaseID, KnowledgeID: source.KnowledgeID, DataSourceID: source.DataSourceID, ExternalID: source.ExternalID}, owner, 2*time.Minute)
		if err != nil {
			closeRead2()
			return nil, ctx, nil, nil, err
		}
		leases = append(leases, lease)
		if uint64(lease.Epoch) != source.Revision {
			closeRead2()
			return nil, ctx, nil, nil, repository.ErrKnowledgeInputChanged
		}
	}
	if len(leases) > 0 {
		ctx, err = repository.WithFAQReadLeaseCoverage(ctx, leases)
		if err != nil {
			closeRead2()
			return nil, ctx, nil, nil, err
		}
	}
	ctx, err = repository.WithSteerMaterialReadPlan(ctx, plan, bodyLeases)
	if err != nil {
		closeRead2()
		return nil, ctx, nil, nil, err
	}
	snapshot, err := s.materialStore.CaptureSteerMaterial(ctx, plan)
	if err != nil {
		closeRead2()
		return nil, ctx, nil, nil, err
	}
	expectedBody, err := steerSnapshotBody(snapshot)
	if err != nil {
		closeRead2()
		return nil, ctx, nil, nil, err
	}
	check := func() error {
		mu.Lock()
		defer mu.Unlock()
		if closed {
			return access.ErrNextcloudPublicationUnavailable
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, id := range bodyLeases {
			if err := s.materialStore.RenewMessageBodyReadLease(ctx, id, 2*time.Minute); err != nil {
				return err
			}
		}
		for _, lease := range leases {
			if _, err := s.materialStore.ContentLeaseStore().RenewLease(ctx, lease.ID, 2*time.Minute); err != nil {
				return err
			}
		}
		if err := s.sourceLineageGuard.Check(ctx, plan.SourceLineage, access.LineageReadScope{}); err != nil {
			return err
		}
		actual, err := s.materialStore.CaptureSteerMaterial(ctx, plan)
		if err != nil {
			return err
		}
		body, err := steerSnapshotBody(snapshot)
		if err != nil {
			return err
		}
		if string(body) != string(expectedBody) {
			return access.ErrNextcloudPublicationDenied
		}
		freshBody, err := steerSnapshotBody(actual)
		if err != nil {
			return err
		}
		if string(freshBody) != string(expectedBody) {
			return access.ErrNextcloudPublicationDenied
		}
		return s.sourceLineageGuard.Check(ctx, plan.SourceLineage, access.LineageReadScope{})
	}
	if err := check(); err != nil {
		closeRead2()
		return nil, ctx, nil, nil, err
	}
	return snapshot, ctx, check, closeRead2, nil
}

func steerSnapshotBody(snapshot *types.SteerMaterialSnapshot) ([]byte, error) {
	type value struct {
		Input       *types.SteerMaterialInput
		Source      *types.SourceLineage
		Execution   types.MessageExecutionContext
		AgentTenant uint64
	}
	values := []value{}
	for _, input := range snapshot.Inputs {
		values = append(
			values,
			value{input, input.Proof.SourceLineage, input.Proof.ExecutionContext, input.Proof.AgentTenantID},
		)
	}
	return json.Marshal(values)
}

func (s *messageService) ConsumeActualSteerIntoMessage(
	ctx context.Context,
	input *types.SteerMaterialInput,
	run, generation, request string,
) (*types.Message, *types.SteerConsumptionProof, func() error, func(), error) {
	if input == nil || input.Proof == nil || s.materialStore == nil {
		return nil, nil, nil, nil, types.ErrSourceLineageUnknown
	}
	existing, err := s.materialStore.ExistingSteerConsumption(ctx, input, run, generation, request)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if existing != nil {
		retainedCtx := ctx
		if slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot); ok &&
			slot.OwnerContext != nil {
			retainedCtx = slot.OwnerContext
		}
		guard, err := s.beginMessageMaterialRead(
			retainedCtx,
			types.MessageMaterialReadRequest{
				TenantID:                  input.Proof.TenantID,
				SessionID:                 input.Proof.SessionID,
				MessageIDs:                []string{existing.UserMessageID},
				IncludeOriginalUserFields: true,
			},
		)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		rows, err := guard.OriginalSnapshot()
		if err != nil || len(rows.Messages) != 1 {
			guard.Close()
			if err == nil {
				err = types.ErrSourceLineageUnknown
			}
			return nil, nil, nil, nil, err
		}
		check := func() error {
			if err := s.materialStore.CheckSteerConsumption(retainedCtx, existing); err != nil {
				return err
			}
			return guard.Recheck()
		}
		if slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot); ok {
			body, err := messageBoundaryBodyDigest(guard.snapshot.Messages)
			if err != nil {
				guard.Close()
				return nil, nil, nil, nil, err
			}
			slot.mu.Lock()
			if slot.Closed {
				slot.mu.Unlock()
				guard.Close()
				return nil, nil, nil, nil, access.ErrNextcloudPublicationUnavailable
			}
			slot.Guards = append(slot.Guards, guard)
			slot.Bodies = append(slot.Bodies, string(body))
			slot.mu.Unlock()
			return rows.Messages[0], existing, check, func() {}, nil
		}
		return rows.Messages[0], existing, check, guard.Close, nil
	}
	snapshot, readCtx, oldCheck, closeSteer, err := s.beginSteerMaterialRead(
		ctx,
		&types.SteerMaterialRequest{
			TenantID:        input.Proof.TenantID,
			SessionID:       input.Proof.SessionID,
			RunMessageID:    run,
			RunGenerationID: generation,
			SteerIDs:        []string{input.Proof.SteerID},
			Use:             types.SteerMaterialInject,
		},
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if len(snapshot.Inputs) != 1 {
		closeSteer()
		return nil, nil, nil, nil, types.ErrSourceLineageUnknown
	}
	if err := oldCheck(); err != nil {
		closeSteer()
		return nil, nil, nil, nil, err
	}
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	if !ok {
		closeSteer()
		return nil, nil, nil, nil, types.ErrSourceLineageUnknown
	}
	slot.mu.Lock()
	if slot.Completion == nil || slot.Completion.Assistant != run || slot.Completion.Generation != generation ||
		slot.Completion.Request != request {
		slot.mu.Unlock()
		closeSteer()
		return nil, nil, nil, nil, types.ErrSourceLineageUnknown
	}
	locked := []*MessageMaterialGuard{}
	release := func() {
		for i := len(locked) - 1; i >= 0; i-- {
			locked[i].mu.Unlock()
		}
		slot.mu.Unlock()
	}
	for _, guard := range slot.Guards {
		if err := guard.Recheck(); err != nil {
			slot.mu.Unlock()
			closeSteer()
			return nil, nil, nil, nil, err
		}
	}
	physical := []repository.NextcloudContentLease{}
	for _, guard := range slot.Guards {
		guard.mu.Lock()
		locked = append(locked, guard)
		var err error
		readCtx, err = repository.WithMessageMaterialReadPlan(readCtx, guard.plan)
		if err != nil {
			release()
			closeSteer()
			return nil, nil, nil, nil, err
		}
		physical = append(physical, guard.leases...)
	}
	if len(physical) > 0 {
		readCtx, err = repository.WithFAQReadLeaseCoverage(readCtx, physical)
		if err != nil {
			release()
			closeSteer()
			return nil, nil, nil, nil, err
		}
	}
	message, proof, err := s.materialStore.ConsumeSteerIntoMessage(readCtx, input, run, generation, request)
	if err != nil {
		release()
		closeSteer()
		return nil, nil, nil, nil, fmt.Errorf("actual steer atomic consume: %w", err)
	}
	for i, guard := range slot.Guards {
		updated, err := s.materialStore.AdvanceMessageMaterialAfterOwnSteerInsertion(readCtx, guard.snapshot, proof)
		if err != nil {
			slot.Failed = err
			release()
			closeSteer()
			return nil, nil, nil, nil, fmt.Errorf("actual steer own insertion transfer: %w", err)
		}
		plan, err := s.materialStore.PeekMessageMaterialReadPlan(readCtx, updated.Request)
		if err != nil {
			slot.Failed = err
			release()
			closeSteer()
			return nil, nil, nil, nil, err
		}
		marked, err := repository.WithMessageMaterialReadPlan(guard.ctx, plan)
		if err != nil {
			slot.Failed = err
			release()
			closeSteer()
			return nil, nil, nil, nil, err
		}
		bodyLeases, err := s.materialStore.AcquireMessageBodyReadLeases(
			marked,
			plan,
			uuid.NewString(),
			2*time.Minute,
			guard.bodyLeases,
		)
		if err != nil {
			slot.Failed = err
			release()
			closeSteer()
			return nil, nil, nil, nil, err
		}
		guard.bodyLeases = append(guard.bodyLeases, bodyLeases...)
		guard.ctx = marked
		guard.plan = plan
		guard.snapshot = updated
		body, err := messageBoundaryBodyDigest(updated.Messages)
		if err != nil {
			release()
			closeSteer()
			return nil, nil, nil, nil, err
		}
		slot.Bodies[i] = string(body)
	}
	release()
	// Old queue head advanced only through the same atomic consume. Retain its
	// original body leases, and check the exact new head plus sealed new user.
	retainedCtx := slot.OwnerContext
	if retainedCtx == nil {
		closeSteer()
		return nil, nil, nil, nil, types.ErrSourceLineageUnknown
	}
	userGuard, err := s.beginMessageMaterialRead(
		retainedCtx,
		types.MessageMaterialReadRequest{
			TenantID:                  input.Proof.TenantID,
			SessionID:                 input.Proof.SessionID,
			MessageIDs:                []string{message.ID},
			IncludeOriginalUserFields: true,
		},
	)
	if err != nil {
		closeSteer()
		return nil, nil, nil, nil, fmt.Errorf("actual steer new original user guard: %w", err)
	}
	body, err := messageBoundaryBodyDigest(userGuard.snapshot.Messages)
	if err != nil {
		userGuard.Close()
		closeSteer()
		return nil, nil, nil, nil, err
	}
	slot.mu.Lock()
	if slot.Closed {
		slot.mu.Unlock()
		userGuard.Close()
		closeSteer()
		return nil, nil, nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	slot.Guards = append(slot.Guards, userGuard)
	slot.Bodies = append(slot.Bodies, string(body))
	if slot.Completion == nil || slot.Completion.Assistant != run || slot.Completion.Generation != generation ||
		slot.Completion.Request != request {
		slot.mu.Unlock()
		closeSteer()
		return nil, nil, nil, nil, types.ErrSourceLineageUnknown
	}
	if len(userGuard.snapshot.Proofs) != 1 {
		slot.mu.Unlock()
		closeSteer()
		return nil, nil, nil, nil, types.ErrSourceLineageUnknown
	}
	selectionDigest, err := s.materialStore.SteerConsumptionSelectionDigest(ctx, proof)
	if err != nil {
		slot.mu.Unlock()
		closeSteer()
		return nil, nil, nil, nil, err
	}
	slot.Completion.Steers = append(
		slot.Completion.Steers,
		&types.MessageCompletionSteerParent{
			Consumption:         proof,
			Original:            input.Proof,
			OriginalSource:      input.Proof.SourceLineage,
			OriginalExecution:   input.Proof.ExecutionContext,
			OriginalAgentTenant: input.Proof.AgentTenantID,
			User:                userGuard.snapshot.Proofs[0],
			SelectionDigest:     selectionDigest,
		},
	)
	slot.mu.Unlock()
	// Shared producer/HTTP ownership now retains this exact new user's bodies,
	// including its original steer parent. The queue read can release.
	closeSteer()
	closeRead3 := func() {}
	check := func() error {
		if err := s.materialStore.CheckSteerConsumption(retainedCtx, proof); err != nil {
			return err
		}
		if err := userGuard.Recheck(); err != nil {
			return err
		}
		return CheckMessageMaterialOutputBoundary(context.WithValue(retainedCtx, messageMaterialOutputKey{}, slot))
	}
	if err := check(); err != nil {
		closeRead3()
		return nil, nil, nil, nil, err
	}
	return message, proof, check, closeRead3, nil
}
