package service

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
)

type messageMaterialOutputKey struct{}
type messageMaterialOutputSlot struct {
	mu           sync.Mutex
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
		slot.mu.Lock()
		guards := append([]*MessageMaterialGuard{}, slot.Guards...)
		slot.mu.Unlock()
		for _, guard := range guards {
			guard.Close()
		}
	}
}
func CheckMessageMaterialOutputBoundary(ctx context.Context) error {
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	if !ok {
		return access.ErrNextcloudPublicationUnavailable
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	for i, guard := range slot.Guards {
		if err := guard.Recheck(); err != nil {
			return err
		}
		guard.mu.Lock()
		body, err := messageBoundaryBodyDigest(guard.snapshot.Messages)
		guard.mu.Unlock()
		if err != nil {
			return err
		}
		if string(body) != slot.Bodies[i] {
			return access.ErrNextcloudPublicationDenied
		}
	}
	for i, output := range slot.Outputs {
		body, err := messageBoundaryBodyDigest(output)
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
	exclude, _ := ctx.Value(agentHistoryCurrentRequestKey{}).(string)
	if exclude != "" {
		return s.admittedMessages(ctx, types.MessageMaterialReadRequest{TenantID: tenant, SessionID: sessionID, AllSession: true, CompletedOnly: true, ExcludeRequestID: exclude, IncludeOriginalUserFields: true})
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
		body, err := messageBoundaryBodyDigest(guard.snapshot.Messages)
		if err != nil {
			guard.Close()
			return nil, err
		}
		slot.mu.Lock()
		slot.Guards = append(slot.Guards, guard)
		slot.Bodies = append(slot.Bodies, string(body))
		slot.mu.Unlock()
	} else {
		defer guard.Close()
	}
	copied, err := cloneCollection(guard.snapshot.Messages)
	if err != nil {
		guard.Close()
		return nil, err
	}
	for i, row := range guard.snapshot.Messages {
		if row == nil {
			continue
		}
		copied[i].SourceLineage, _ = types.UnionSourceLineage(row.SourceLineage)
		copied[i].ExecutionContext = row.ExecutionContext
		copied[i].AgentTenantID = row.AgentTenantID
		copied[i].RenderedContent = row.RenderedContent
		copied[i].ContextCheckpoint, err = cloneCollection(row.ContextCheckpoint)
		if err != nil {
			return nil, err
		}
	}
	if !request.IncludeOriginalUserFields {
		copied = rawUserContentOnly(copied)
	}
	if slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot); ok {
		body, err := messageBoundaryBodyDigest(copied)
		if err != nil {
			return nil, err
		}
		slot.mu.Lock()
		slot.Outputs = append(slot.Outputs, copied)
		slot.OutputBodies = append(slot.OutputBodies, string(body))
		slot.mu.Unlock()
	}
	return copied, nil
}

func messageMaterialBoundarySnapshots(ctx context.Context) ([]*types.MessageMaterialSnapshot, error) {
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	if !ok {
		return nil, types.ErrSourceLineageUnknown
	}
	if err := CheckMessageMaterialOutputBoundary(ctx); err != nil {
		return nil, err
	}
	slot.mu.Lock()
	guards := append([]*MessageMaterialGuard{}, slot.Guards...)
	slot.mu.Unlock()
	if len(guards) == 0 {
		return nil, types.ErrSourceLineageUnknown
	}
	result := []*types.MessageMaterialSnapshot{}
	for _, guard := range guards {
		copy, err := guard.OriginalSnapshot()
		if err != nil {
			return nil, err
		}
		result = append(result, copy)
	}
	return result, nil
}

func advanceMessageMaterialBoundaryAfterOwnField(ctx context.Context) error {
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	if !ok {
		return types.ErrSourceLineageUnknown
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	for i, guard := range slot.Guards {
		if guard.Recheck() == nil {
			continue
		}
		if err := guard.advanceOwnGeneratedField(ctx); err != nil {
			return err
		}
		body, err := messageBoundaryBodyDigest(guard.snapshot.Messages)
		if err != nil {
			return err
		}
		slot.Bodies[i] = string(body)
	}
	return nil
}

func messageMaterialGeneratedWriteContext(ctx context.Context) (context.Context, error) {
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	if !ok {
		return ctx, types.ErrSourceLineageUnknown
	}
	slot.mu.Lock()
	guards := append([]*MessageMaterialGuard{}, slot.Guards...)
	slot.mu.Unlock()
	if len(guards) == 0 {
		return ctx, types.ErrSourceLineageUnknown
	}
	leases := []repository.NextcloudContentLease{}
	var err error
	for _, guard := range guards {
		guard.mu.Lock()
		plan := guard.plan
		rows := append([]repository.NextcloudContentLease{}, guard.leases...)
		guard.mu.Unlock()
		ctx, err = repository.WithMessageMaterialReadPlan(ctx, plan)
		if err != nil {
			return ctx, err
		}
		leases = append(leases, rows...)
	}
	if len(leases) > 0 {
		ctx, err = repository.WithFAQReadLeaseCoverage(ctx, leases)
	}
	return ctx, err
}

func messageBoundaryBodyDigest(messages []*types.Message) ([]byte, error) {
	type material struct {
		Body        *types.Message
		Lineage     *types.SourceLineage
		Execution   types.MessageExecutionContext
		AgentTenant uint64
		Rendered    string
		Checkpoint  *types.ContextCheckpoint
	}
	values := []material{}
	for _, message := range messages {
		if message == nil {
			values = append(values, material{})
			continue
		}
		values = append(values, material{message, message.SourceLineage, message.ExecutionContext, message.AgentTenantID, message.RenderedContent, message.ContextCheckpoint})
	}
	return json.Marshal(values)
}

func (s *messageService) actualGeneratedMessageFieldContext(ctx context.Context, session, message, field string, value any) (context.Context, bool, error) {
	turn := types.RAGSourceLineageFromContext(ctx)
	if turn == nil {
		return ctx, false, nil
	}
	lineage, err := turn.CheckedSnapshot(ctx)
	if err != nil {
		return ctx, true, err
	}
	if s.materialStore == nil {
		return ctx, true, types.ErrSourceLineageUnknown
	}
	ctx, err = messageMaterialGeneratedWriteContext(ctx)
	if err != nil {
		return ctx, true, err
	}
	parents, err := messageMaterialBoundarySnapshots(ctx)
	if err != nil {
		return ctx, true, err
	}
	ctx, err = s.materialStore.WithActualMessageGeneratedField(ctx, session, message, field, value, parents, lineage)
	return ctx, true, err
}
