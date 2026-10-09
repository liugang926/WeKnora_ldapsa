package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
)

type messageMaterialOutputKey struct{}
type messageMaterialOutputSlot struct {
	Guards       []*MessageMaterialGuard
	Bodies       []string
	Outputs      [][]*types.Message
	OutputBodies []string
}

func WithMessageMaterialOutputBoundary(ctx context.Context) context.Context {
	return context.WithValue(ctx, messageMaterialOutputKey{}, &messageMaterialOutputSlot{})
}
func CloseMessageMaterialOutputBoundary(ctx context.Context) {
	if slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot); ok {
		for _, guard := range slot.Guards {
			guard.Close()
		}
	}
}
func CheckMessageMaterialOutputBoundary(ctx context.Context) error {
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	if !ok {
		return access.ErrNextcloudPublicationUnavailable
	}
	for i, guard := range slot.Guards {
		if err := guard.Recheck(); err != nil {
			return err
		}
		body, err := json.Marshal(guard.snapshot.Messages)
		if err != nil {
			return err
		}
		if string(body) != slot.Bodies[i] {
			return access.ErrNextcloudPublicationDenied
		}
	}
	for i, output := range slot.Outputs {
		body, err := json.Marshal(output)
		if err != nil {
			return err
		}
		if string(body) != slot.OutputBodies[i] {
			return access.ErrNextcloudPublicationDenied
		}
	}
	return nil
}
func (s *messageService) admittedMessagePage(ctx context.Context, sessionID string, before time.Time, limit, page int, descending bool) ([]*types.Message, error) {
	tenant, ok := sessionTenantIDForLookup(ctx)
	if !ok {
		return nil, access.ErrNextcloudPublicationDenied
	}
	request, err := s.materialStore.MessagePageMaterialRequest(ctx, tenant, sessionID, before, limit, page, descending)
	if err != nil {
		return nil, err
	}
	if len(request.MessageIDs) == 0 {
		return []*types.Message{}, nil
	}
	return s.admittedMessages(ctx, request)
}
func (s *messageService) admittedMessages(ctx context.Context, request types.MessageMaterialReadRequest) ([]*types.Message, error) {
	guard, err := s.beginMessageMaterialRead(ctx, request)
	if err != nil {
		return nil, err
	}
	if slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot); ok {
		body, err := json.Marshal(guard.snapshot.Messages)
		if err != nil {
			guard.Close()
			return nil, err
		}
		slot.Guards = append(slot.Guards, guard)
		slot.Bodies = append(slot.Bodies, string(body))
	} else {
		defer guard.Close()
	}
	copied, err := cloneCollection(guard.snapshot.Messages)
	if err != nil {
		guard.Close()
		return nil, err
	}
	copied = rawUserContentOnly(copied)
	if slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot); ok {
		slot.Outputs = append(slot.Outputs, copied)
		body, err := json.Marshal(copied)
		if err != nil {
			return nil, err
		}
		slot.OutputBodies = append(slot.OutputBodies, string(body))
	}
	return copied, nil
}
