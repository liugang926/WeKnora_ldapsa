package service

import (
	"bytes"
	"context"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
)

// The completed replay sender gets only original authenticated material. The
// metadata probe never fetches text, references, steps, or checkpoint JSON.
// A nil message represents an actual current running turn, not a legacy grant.
func (s *messageService) BeginCompletedMessageMaterialReplay(
	ctx context.Context,
	sessionID, messageID string,
) (*types.Message, func() error, func(), error) {
	if s.materialStore == nil || s.sourceLineageGuard == nil {
		return nil, nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	complete, err := s.materialStore.MessageCompletionControl(ctx, sessionID, messageID)
	if err != nil {
		return nil, nil, nil, err
	}
	if !complete {
		return nil, nil, nil, nil
	}
	tenant, ok := sessionTenantIDForLookup(ctx)
	if !ok {
		return nil, nil, nil, access.ErrNextcloudPublicationDenied
	}
	guard, err := s.beginMessageMaterialRead(
		ctx,
		types.MessageMaterialReadRequest{TenantID: tenant, SessionID: sessionID, MessageIDs: []string{messageID}},
	)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(guard.snapshot.Messages) != 1 || !guard.snapshot.Messages[0].IsCompleted {
		guard.Close()
		return nil, nil, nil, access.ErrNextcloudPublicationDenied
	}
	source := guard.snapshot.Messages[0]
	message, err := cloneCollection(source)
	if err != nil {
		guard.Close()
		return nil, nil, nil, err
	}
	message.SourceLineage, _ = types.UnionSourceLineage(source.SourceLineage)
	message.ExecutionContext = source.ExecutionContext
	message.AgentTenantID = source.AgentTenantID
	message.RenderedContent = source.RenderedContent
	message.ContextCheckpoint, err = cloneCollection(source.ContextCheckpoint)
	if err != nil {
		guard.Close()
		return nil, nil, nil, err
	}
	expected, err := messageBoundaryBodyDigest([]*types.Message{message})
	if err != nil {
		guard.Close()
		return nil, nil, nil, err
	}
	check := func() error {
		if err := guard.Recheck(); err != nil {
			return err
		}
		actual, err := messageBoundaryBodyDigest([]*types.Message{message})
		if err != nil {
			return err
		}
		if !bytes.Equal(actual, expected) {
			return access.ErrNextcloudPublicationDenied
		}
		return s.CheckMessagePublication(guard.Context(), message)
	}
	return message, check, guard.Close, nil
}

func (s *messageService) HasOriginalMessageMaterialStore() bool {
	return s.materialStore != nil && s.sourceLineageGuard != nil
}
