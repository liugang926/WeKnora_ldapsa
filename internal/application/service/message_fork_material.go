package service

import (
	"context"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
)

// The actual repository verifies the original read plan and fixed copy mapping
// in the same transaction that inserts and seals the destination messages.
type forkMaterialSessionStore interface {
	CreateForkedFromMaterial(context.Context, *types.Session, []*types.Message, *types.MessageMaterialSnapshot) error
}

// Only service-package algorithm fixtures implement this private port. Actual
// repositories cannot use it to bypass original producer receipts or leases.
type forkAlgorithmMaterialReader interface {
	readForkAlgorithmMaterial(context.Context, string, string) (*types.Message, []*types.Message, error)
}

func (s *SessionForkService) beginForkMessageMaterial(ctx context.Context, tenant uint64, sessionID, pointID string) (*types.Message, []*types.Message, *MessageMaterialGuard, error) {
	actual, ok := s.lineageVerifier.(*messageService)
	if !ok {
		if algorithm, ok := s.messages.(forkAlgorithmMaterialReader); ok {
			point, history, err := algorithm.readForkAlgorithmMaterial(ctx, sessionID, pointID)
			return point, history, nil, err
		}
		return nil, nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	if actual.materialStore == nil || actual.sourceLineageGuard == nil {
		return nil, nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	if _, ok := s.sessions.(forkMaterialSessionStore); !ok {
		return nil, nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	point, request, err := actual.materialStore.MessageForkMaterialRequest(ctx, tenant, sessionID, pointID)
	if err != nil {
		return nil, nil, nil, err
	}
	if point == nil || point.ID != pointID || request.TenantID != tenant || request.SessionID != sessionID || request.AllSession {
		return nil, nil, nil, access.ErrNextcloudPublicationDenied
	}
	if point.Role != "user" && point.Role != "assistant" {
		return nil, nil, nil, ErrForkMessageNotUser
	}
	if point.Role == "assistant" && !point.Complete {
		return nil, nil, nil, ErrForkSourceBusy
	}
	// The point is authenticated even when it is the first user message and
	// the copied prefix is empty. It is excluded only by the fixed copy recipe.
	request.IncludeOriginalUserFields = true
	guard, err := actual.beginMessageMaterialRead(ctx, request)
	if err != nil {
		return nil, nil, nil, err
	}
	history := make([]*types.Message, 0, len(guard.snapshot.Messages))
	var originalPoint *types.Message
	for _, original := range guard.snapshot.Messages {
		if original == nil || original.SessionID != sessionID {
			guard.Close()
			return nil, nil, nil, access.ErrNextcloudPublicationDenied
		}
		if original.ID == point.ID {
			if original.Role != point.Role || original.RequestID != point.RequestID || original.IsCompleted != point.Complete || !original.CreatedAt.Equal(point.CreatedAt) {
				guard.Close()
				return nil, nil, nil, access.ErrNextcloudPublicationDenied
			}
			originalPoint = original
			if original.Role == "user" {
				continue
			}
		} else if original.CreatedAt.After(point.CreatedAt) || original.CreatedAt.Equal(point.CreatedAt) && original.ID >= point.ID {
			guard.Close()
			return nil, nil, nil, access.ErrNextcloudPublicationDenied
		}
		copy, err := cloneForkOriginalMessage(original)
		if err != nil {
			guard.Close()
			return nil, nil, nil, err
		}
		history = append(history, copy)
	}
	if originalPoint == nil {
		guard.Close()
		return nil, nil, nil, access.ErrNextcloudPublicationDenied
	}
	metadataPoint := &types.Message{ID: point.ID, SessionID: sessionID, RequestID: point.RequestID, Role: point.Role, IsCompleted: point.Complete, CreatedAt: point.CreatedAt}
	return metadataPoint, history, guard, nil
}

// Explicitly include every hidden native field in the deep copy. Serializing
// Message alone would lose its original source envelope and execution scope.
func cloneForkOriginalMessage(original *types.Message) (*types.Message, error) {
	copy, err := cloneCollection(struct {
		Message           *types.Message
		SourceLineage     *types.SourceLineage
		ExecutionContext  types.MessageExecutionContext
		AgentTenantID     uint64
		RenderedContent   string
		ContextCheckpoint *types.ContextCheckpoint
	}{original, original.SourceLineage, original.ExecutionContext, original.AgentTenantID, original.RenderedContent, original.ContextCheckpoint})
	if err != nil {
		return nil, err
	}
	copy.Message.SourceLineage = copy.SourceLineage
	copy.Message.ExecutionContext = copy.ExecutionContext
	copy.Message.AgentTenantID = copy.AgentTenantID
	copy.Message.RenderedContent = copy.RenderedContent
	copy.Message.ContextCheckpoint = copy.ContextCheckpoint
	copy.Message.Artifacts = nil
	return copy.Message, nil
}
