package service

import (
	"context"
	"slices"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
)

const maxAutomationCarryDepth = 64

type automationCarryCheckKey struct{}

// While the outer walk follows the immutable chain, each real message guard
// verifies one original turn. Do not recursively restart the walk for that turn.
type automationCarryReadingKey struct{}

func (s *messageService) openAutomationSource(
	ctx context.Context, source types.AutomationCarrySource,
) (*MessageMaterialGuard, error) {
	caller := types.CallerFromContext(ctx)
	if source.SessionID == "" || source.MessageID == "" || caller.TenantID == 0 || caller.UserID == "" {
		return nil, access.ErrNextcloudPublicationDenied
	}
	request := types.MessageMaterialReadRequest{
		TenantID: caller.TenantID, SessionID: source.SessionID, MessageIDs: []string{source.MessageID},
	}
	readCtx := context.WithValue(ctx, automationCarryReadingKey{}, source.MessageID)
	guard, err := s.beginMessageMaterialRead(readCtx, request)
	if err != nil {
		return nil, err
	}
	if len(guard.snapshot.Messages) != 1 || len(guard.snapshot.Proofs) != 1 {
		guard.Close()
		return nil, access.ErrNextcloudPublicationDenied
	}
	msg, proof := guard.snapshot.Messages[0], guard.snapshot.Proofs[0]
	if proof == nil || proof.OwnerID != caller.UserID || proof.TenantID != caller.TenantID ||
		proof.AdmissionID == "" || source.AdmissionID != "" && proof.AdmissionID != source.AdmissionID ||
		msg == nil || msg.SessionID != source.SessionID || msg.ID != source.MessageID ||
		!msg.IsCompleted || msg.Role != "assistant" || msg.ExecutionContext.AutomationCarry == nil {
		guard.Close()
		return nil, access.ErrNextcloudPublicationDenied
	}
	for _, ref := range msg.KnowledgeReferences {
		if ref == nil || ref.KnowledgeID == "" || !slices.Contains(msg.ExecutionContext.KnowledgeIDs, ref.KnowledgeID) {
			guard.Close()
			return nil, access.ErrNextcloudPublicationDenied
		}
	}
	return guard, nil
}

// Read the parent identities from each authenticated answer. Holding only one
// at a time keeps long-running automations from accumulating leases or stack frames.
func (s *messageService) checkAutomationCarry(ctx context.Context, message *types.Message) error {
	return s.checkAutomationParents(ctx, message, maxAutomationCarryDepth)
}

func (s *messageService) checkAutomationParents(ctx context.Context, message *types.Message, limit int) error {
	if message == nil || message.ExecutionContext.AutomationCarry == nil {
		return nil
	}
	if checking, _ := ctx.Value(automationCarryReadingKey{}).(string); checking != "" && checking == message.ID {
		return nil
	}
	seen := map[string]bool{message.ID: true}
	for source := message.ExecutionContext.AutomationCarry; *source != (types.AutomationCarrySource{}); {
		if source.AdmissionID == "" || seen[source.MessageID] || len(seen) > limit {
			return access.ErrNextcloudPublicationDenied
		}
		guard, err := s.openAutomationSource(ctx, *source)
		if err != nil {
			return err
		}
		seen[source.MessageID] = true
		source = guard.snapshot.Messages[0].ExecutionContext.AutomationCarry
		guard.Close()
	}
	return nil
}

// Scheduled conversations can also be opened through the normal chat history.
// Load the admitted version of each scheduled answer, including its source link.
func (s *messageService) automationHistory(
	ctx context.Context, session *types.Session, messages []*types.Message,
) ([]*types.Message, error) {
	for i, m := range messages {
		if m == nil || m.Role != "assistant" {
			continue
		}
		if m.ExecutionContext.AutomationCarry == nil {
			if session == nil || session.Kind != types.SessionKindAutomation {
				continue
			}
			if s.materialStore == nil {
				return nil, access.ErrNextcloudPublicationUnavailable
			}
			legacy, err := s.materialStore.IsAutomationAnswer(ctx, m)
			if err != nil {
				return nil, err
			}
			if legacy {
				return nil, access.ErrNextcloudPublicationDenied
			}
			continue
		}
		if m.IsCompleted {
			source := types.AutomationCarrySource{SessionID: m.SessionID, MessageID: m.ID}
			guard, err := s.openAutomationSource(ctx, source)
			if err != nil {
				return nil, err
			}
			m = guard.snapshot.Messages[0]
			guard.Close()
			messages[i] = m
		}
		if err := s.checkAutomationCarry(ctx, m); err != nil {
			return nil, err
		}
	}
	return s.filterHistoryMessages(ctx, messages)
}

func (s *messageService) checkAutomationStreamControl(ctx context.Context, control *types.Message) error {
	if control == nil || control.Role != "assistant" || control.ExecutionContext.AutomationCarry != nil {
		return nil
	}
	legacy, err := s.materialStore.IsAutomationAnswer(ctx, control)
	if err != nil {
		return err
	}
	if legacy {
		return access.ErrNextcloudPublicationDenied
	}
	return nil
}
