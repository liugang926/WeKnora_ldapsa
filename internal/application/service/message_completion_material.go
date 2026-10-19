package service

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

type messageCompletionBinding struct {
	Assistant, Generation, Request string
	Initial                        *types.MessageMaterialProof
	Steers                         []*types.MessageCompletionSteerParent
}

func (s *messageService) bindInitialMessageMaterial(ctx context.Context, assistant, user, request string) error {
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	if !ok {
		return types.ErrSourceLineageUnknown
	}
	var control *types.Message
	var generation string
	var err error
	// The exact requested single-user guard, not the latest same-request row,
	// supplies the initial input identity already checked by the real reader.
	slot.mu.Lock()
	defer slot.mu.Unlock()
	var proof *types.MessageMaterialProof
	session := ""
	for _, guard := range slot.Guards {
		guard.mu.Lock()
		if len(guard.snapshot.Proofs) == 1 && len(guard.snapshot.Messages) == 1 &&
			guard.snapshot.Proofs[0].MessageID == user &&
			guard.snapshot.Messages[0].Role == "user" &&
			guard.snapshot.Messages[0].RequestID == request {
			copied := *guard.snapshot.Proofs[0]
			proof = &copied
			session = copied.SessionID
		}
		guard.mu.Unlock()
	}
	if proof == nil {
		return types.ErrSourceLineageUnknown
	}
	control, generation, err = s.materialStore.MessageLiveControl(ctx, session, assistant)
	if err != nil {
		return err
	}
	if control.RequestID != request || control.IsCompleted {
		return types.ErrSourceLineageUnknown
	}
	if slot.Completion != nil {
		if slot.Completion.Assistant != assistant || slot.Completion.Initial.MessageID != user {
			return types.ErrSourceLineageUnknown
		}
		return nil
	}
	slot.Completion = &messageCompletionBinding{
		Assistant:  assistant,
		Generation: generation,
		Request:    request,
		Initial:    proof,
		Steers:     []*types.MessageCompletionSteerParent{},
	}
	return nil
}

func (s *messageService) markCompletionInputsLocked(
	ctx context.Context,
	slot *messageMaterialOutputSlot,
	message *types.Message,
) (context.Context, error) {
	binding := slot.Completion
	if binding == nil || binding.Assistant != message.ID || binding.Request != message.RequestID {
		return ctx, types.ErrSourceLineageUnknown
	}
	snapshots := []*types.MessageMaterialSnapshot{}
	sources := []*types.SourceLineage{message.SourceLineage}
	var initial *types.MessageMaterialProof
	for _, guard := range slot.Guards {
		parent, err := cloneCollection(guard.snapshot)
		if err != nil {
			return ctx, err
		}
		parent.Messages = nil
		snapshots = append(snapshots, parent)
		for _, proof := range guard.snapshot.Proofs {
			sources = append(sources, proof.SourceLineage)
			if proof.MessageID == binding.Initial.MessageID {
				copied2 := *proof
				initial = &copied2
			}
		}
	}
	if initial == nil {
		return ctx, types.ErrSourceLineageUnknown
	}
	for _, parent := range binding.Steers {
		sources = append(sources, parent.OriginalSource, parent.User.SourceLineage)
	}
	source, err := types.UnionSourceLineage(sources...)
	if err != nil {
		return ctx, err
	}
	manifest := &types.MessageCompletionInputManifest{
		Version:               1,
		Recipe:                "history-initial-and-consumed-steer-v1",
		AssistantMessageID:    binding.Assistant,
		AssistantGenerationID: binding.Generation,
		SessionID:             initial.SessionID,
		RequestID:             binding.Request,
		OwnerID:               initial.OwnerID,
		TenantID:              initial.TenantID,
		OwnerRevision:         initial.OwnerRevision,
		Initial:               initial,
		Steers:                binding.Steers,
		Inputs:                snapshots,
		SourceLineage:         source,
	}
	return s.materialStore.WithActualMessageCompletionInputs(ctx, manifest)
}
