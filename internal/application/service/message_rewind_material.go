package service

import (
	"context"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
)

// The writer derives the fixed cut from the authenticated point, checks the
// source plan and actual current authority in its delete transaction, and
// returns only cleanup metadata. It never reloads deleted message bodies.
type rewindMaterialMessageStore interface {
	DeleteMessagesFromMaterial(
		context.Context,
		string,
		string,
		*types.MessageMaterialSnapshot,
	) ([]*types.Message, error)
}

type rewindAlgorithmMaterialReader interface {
	rewindAlgorithmMaterial()
}

func (s *SessionRewindService) beginRewindMessageMaterial(
	ctx context.Context,
	tenant uint64,
	sessionID, pointID string,
) (*types.Message, []*types.Message, *MessageMaterialGuard, error) {
	actual, ok := s.knowledge.(*messageService)
	if !ok {
		if _, algorithm := s.messages.(rewindAlgorithmMaterialReader); algorithm {
			point, err := s.messages.GetMessage(ctx, sessionID, pointID)
			if err != nil || point == nil {
				return point, nil, nil, err
			}
			history, err := s.keptCheckpointHistory(ctx, sessionID, point)
			return point, history, nil, err
		}
		return nil, nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	if actual.materialStore == nil || actual.sourceLineageGuard == nil {
		return nil, nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	if _, ok := s.messages.(rewindMaterialMessageStore); !ok {
		return nil, nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	point, request, err := actual.materialStore.MessageRewindMaterialRequest(ctx, tenant, sessionID, pointID)
	if err != nil {
		return nil, nil, nil, err
	}
	if point == nil || point.ID != pointID || request.TenantID != tenant || request.SessionID != sessionID ||
		!request.AllSession {
		return nil, nil, nil, access.ErrNextcloudPublicationDenied
	}
	if point.Role != "user" && point.Role != "assistant" {
		return nil, nil, nil, ErrRewindMessageRole
	}
	if point.Role == "assistant" && !point.Complete {
		return nil, nil, nil, ErrRewindSourceBusy
	}
	request.IncludeOriginalUserFields = true
	guard, err := actual.beginMessageMaterialRead(ctx, request)
	if err != nil {
		return nil, nil, nil, err
	}
	history := make([]*types.Message, 0, len(guard.snapshot.Messages))
	foundPoint := false
	for _, original := range guard.snapshot.Messages {
		if original == nil || original.SessionID != sessionID {
			guard.Close()
			return nil, nil, nil, access.ErrNextcloudPublicationDenied
		}
		if original.ID == point.ID {
			if original.Role != point.Role || original.RequestID != point.RequestID ||
				original.IsCompleted != point.Complete ||
				!original.CreatedAt.Equal(point.CreatedAt) {
				guard.Close()
				return nil, nil, nil, access.ErrNextcloudPublicationDenied
			}
			foundPoint = true
			if original.Role == "user" {
				continue
			}
		} else if original.CreatedAt.After(point.CreatedAt) || original.CreatedAt.Equal(point.CreatedAt) && original.ID >= point.ID {
			continue // Original suffix remains authenticated for the atomic delete.
		}
		// There is currently no actual typed code/checkpoint producer. A saved
		// generic message receipt cannot certify a cached reset instruction.
		if original.SandboxCheckpoint != nil {
			guard.Close()
			return nil, nil, nil, types.ErrSourceLineageUnknown
		}
		copied, err := cloneForkOriginalMessage(original)
		if err != nil {
			guard.Close()
			return nil, nil, nil, err
		}
		history = append(history, copied)
	}
	if !foundPoint {
		guard.Close()
		return nil, nil, nil, access.ErrNextcloudPublicationDenied
	}
	metadataPoint := &types.Message{
		ID:          point.ID,
		SessionID:   sessionID,
		RequestID:   point.RequestID,
		Role:        point.Role,
		IsCompleted: point.Complete,
		CreatedAt:   point.CreatedAt,
	}
	return metadataPoint, history, guard, nil
}

type rewindHTTPOutputKey struct{}

// WithRewindOriginalHTTPOutput binds retained original rewind inputs to HTTP output.
func WithRewindOriginalHTTPOutput(ctx context.Context) context.Context {
	return context.WithValue(ctx, rewindHTTPOutputKey{}, true)
}
