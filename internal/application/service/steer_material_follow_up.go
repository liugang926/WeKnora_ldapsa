package service

import (
	"context"
	"fmt"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
)

// MessageMaterialOwnerContext starts another actual request under the same
// authenticated caller without borrowing the previous request's output slot.
func MessageMaterialOwnerContext(ctx context.Context) context.Context {
	if slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot); ok && slot.OwnerContext != nil {
		return context.WithoutCancel(slot.OwnerContext)
	}
	return context.WithoutCancel(ctx)
}

func (s *messageService) advanceFollowUpMaterialRowLocked(
	_ context.Context,
	slot *messageMaterialOutputSlot,
	id, generation, admission, request, role string,
) error {
	for i, guard := range slot.Guards {
		guard.mu.Lock()
		marked, err := repository.WithMessageMaterialReadPlan(guard.ctx, guard.plan)
		if err == nil {
			var updated *types.MessageMaterialSnapshot
			updated, err = s.materialStore.AdvanceMessageMaterialAfterOwnFollowUpRow(
				marked,
				guard.snapshot,
				id,
				generation,
				admission,
				request,
				role,
			)
			if err == nil {
				var plan *types.MessageMaterialReadPlan
				plan, err = s.materialStore.PeekMessageMaterialReadPlan(marked, updated.Request)
				if err == nil {
					marked, err = repository.WithMessageMaterialReadPlan(guard.ctx, plan)
					if err == nil {
						var body []byte
						body, err = messageBoundaryBodyDigest(updated.Messages)
						if err == nil {
							guard.ctx, guard.plan, guard.snapshot = marked, plan, updated
							slot.Bodies[i] = string(body)
						}
					}
				}
			}
		}
		guard.mu.Unlock()
		if err != nil {
			slot.Failed = err
			return err
		}
	}
	return nil
}

func (s *messageService) CreateActualSteerFollowUpAssistant(
	ctx context.Context,
	input *types.SteerMaterialInput,
	message *types.Message,
) (*types.Message, error) {
	if input == nil || input.Control == nil || message == nil || message.Role != "assistant" || message.IsCompleted ||
		message.Content != "" ||
		message.RequestID == input.Control.RunRequestID {
		return nil, types.ErrSourceLineageUnknown
	}
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	if !ok {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.Closed || slot.Failed != nil || !slot.Ready || slot.Completion == nil ||
		slot.Completion.Assistant != input.Control.RunMessageID ||
		slot.Completion.Generation != input.Control.RunGenerationID ||
		slot.Completion.Request != input.Control.RunRequestID {
		return nil, types.ErrSourceLineageUnknown
	}
	for _, guard := range slot.Guards {
		if err := guard.Recheck(); err != nil {
			slot.Failed = err
			return nil, err
		}
	}
	created, err := s.CreateMessage(ctx, message)
	if err != nil {
		return nil, err
	}
	_, generation, err := s.materialStore.MessageLiveControl(ctx, created.SessionID, created.ID)
	if err != nil {
		slot.Failed = err
		return nil, err
	}
	if err := s.advanceFollowUpMaterialRowLocked(
		ctx,
		slot,
		created.ID,
		generation,
		"",
		created.RequestID,
		"assistant",
	); err != nil {
		return nil, err
	}
	return created, nil
}

// ConsumeActualSteerFollowUpIntoMessage derives the successor's real initial
// user from the old authored receipt. The old model gets no new input: only
// the exact owned assistant and consumed user advance its membership witness.
func (s *messageService) ConsumeActualSteerFollowUpIntoMessage(
	ctx context.Context,
	input *types.SteerMaterialInput,
	run, generation, request string,
) (*types.Message, *types.SteerConsumptionProof, func() error, func(), error) {
	if input == nil || input.Proof == nil || input.Control == nil || s.materialStore == nil {
		return nil, nil, nil, nil, types.ErrSourceLineageUnknown
	}
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	if !ok {
		return nil, nil, nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	slot.mu.Lock()
	if slot.Closed || slot.Failed != nil || !slot.Ready || slot.Completion == nil ||
		slot.Completion.Assistant != input.Control.RunMessageID ||
		slot.Completion.Generation != input.Control.RunGenerationID ||
		slot.Completion.Request != input.Control.RunRequestID ||
		slot.Completion.Assistant == run ||
		slot.Completion.Request == request {
		slot.mu.Unlock()
		return nil, nil, nil, nil, types.ErrSourceLineageUnknown
	}
	for _, physical := range slot.PhysicalReads {
		if err := physical.Verify(); err != nil {
			slot.Failed = err
			slot.mu.Unlock()
			return nil, nil, nil, nil, err
		}
	}
	slot.mu.Unlock()
	if err := CheckMessageMaterialOutputBoundary(ctx); err != nil {
		return nil, nil, nil, nil, err
	}
	snapshot, readCtx, checkOriginal, closeOriginal, err := s.beginSteerMaterialRead(
		ctx,
		&types.SteerMaterialRequest{
			TenantID:        input.Proof.TenantID,
			SessionID:       input.Proof.SessionID,
			RunMessageID:    input.Control.RunMessageID,
			RunGenerationID: input.Control.RunGenerationID,
			SteerIDs:        []string{input.Proof.SteerID},
			Use:             types.SteerMaterialFollowUp,
		},
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	defer closeOriginal()
	if len(snapshot.Inputs) != 1 {
		return nil, nil, nil, nil, types.ErrSourceLineageUnknown
	}
	if err := checkOriginal(); err != nil {
		return nil, nil, nil, nil, err
	}
	slot.mu.Lock()
	for _, guard := range slot.Guards {
		if err := guard.Recheck(); err != nil {
			slot.Failed = err
			slot.mu.Unlock()
			return nil, nil, nil, nil, err
		}
	}
	message, proof, err := s.materialStore.ConsumeSteerIntoFollowUp(readCtx, input, run, generation, request)
	if err != nil {
		slot.mu.Unlock()
		return nil, nil, nil, nil, fmt.Errorf("followup exact original consumption: %w", err)
	}
	err = s.advanceFollowUpMaterialRowLocked(
		ctx,
		slot,
		message.ID,
		proof.UserGenerationID,
		proof.UserAdmissionID,
		request,
		"user",
	)
	slot.mu.Unlock()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("followup exact user membership transfer: %w", err)
	}
	if err := CheckMessageMaterialOutputBoundary(ctx); err != nil {
		return nil, nil, nil, nil, err
	}
	guard, err := s.beginMessageMaterialRead(
		MessageMaterialOwnerContext(ctx),
		types.MessageMaterialReadRequest{
			TenantID:                  input.Proof.TenantID,
			SessionID:                 input.Proof.SessionID,
			MessageIDs:                []string{message.ID},
			IncludeOriginalUserFields: true,
		},
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("followup original initial read: %w", err)
	}
	check := func() error {
		if err := s.materialStore.CheckSteerConsumption(ctx, proof); err != nil {
			return err
		}
		return guard.Recheck()
	}
	if err := check(); err != nil {
		guard.Close()
		return nil, nil, nil, nil, err
	}
	return message, proof, check, guard.Close, nil
}
