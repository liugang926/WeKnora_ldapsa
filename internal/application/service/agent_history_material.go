package service

import (
	"context"
	"sort"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type agentHistoryOriginalAdmission interface {
	OriginalHistoryRepository(context.Context, string) (interfaces.MessageRepository, error)
}

// This adapter serves only immutable captured material. The loader's budget,
// checkpoint, ordering and replay decisions cannot fall back to current SQL.
type agentOriginalHistoryRepository struct {
	interfaces.MessageRepository
	rows []*types.Message
}

func (a messageLineageHistoryAdmission) OriginalHistoryRepository(
	ctx context.Context,
	sessionID string,
) (interfaces.MessageRepository, error) {
	actual, ok := a.verifier.(*messageService)
	if !ok || actual.materialStore == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	if _, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot); !ok {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	tenant, ok := sessionTenantIDForLookup(ctx)
	if !ok {
		return nil, access.ErrNextcloudPublicationDenied
	}
	exclude, _ := ctx.Value(agentHistoryCurrentRequestKey{}).(string)
	rows, err := actual.admittedMessages(
		ctx,
		types.MessageMaterialReadRequest{
			TenantID:                  tenant,
			SessionID:                 sessionID,
			AllSession:                true,
			CompletedOnly:             true,
			ExcludeRequestID:          exclude,
			IncludeOriginalUserFields: true,
		},
	)
	if err != nil {
		return nil, err
	}
	copied, err := cloneCollection(rows)
	if err != nil {
		return nil, err
	}
	for i, row := range rows {
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

	return agentOriginalHistoryRepository{MessageRepository: actual.messageRepo, rows: copied}, nil
}

type agentHistoryCurrentRequestKey struct{}

func (r agentOriginalHistoryRepository) GetLatestContextCheckpoint(
	ctx context.Context,
	sessionID string,
) (*types.Message, error) {
	if err := CheckMessageMaterialOutputBoundary(ctx); err != nil {
		return nil, err
	}
	var newest *types.Message
	for _, row := range r.rows {
		if row.SessionID != sessionID || row.Role != "assistant" || row.ContextCheckpoint == nil {
			continue
		}
		if newest == nil || row.CreatedAt.After(newest.CreatedAt) ||
			row.CreatedAt.Equal(newest.CreatedAt) && row.ID > newest.ID {
			newest = row
		}
	}
	return newest, nil
}

func (r agentOriginalHistoryRepository) ListMessagesBySessionBeforeCursor(
	ctx context.Context,
	sessionID string,
	before time.Time,
	beforeID string,
	limit int,
) ([]*types.Message, error) {
	if err := CheckMessageMaterialOutputBoundary(ctx); err != nil {
		return nil, err
	}
	selected := []*types.Message{}
	for _, row := range r.rows {
		if row.SessionID != sessionID {
			continue
		}
		if (!before.IsZero() || beforeID != "") &&
			!row.CreatedAt.Before(before) && (!row.CreatedAt.Equal(before) || row.ID >= beforeID) {
			continue
		}
		selected = append(selected, row)
	}
	sort.Slice(selected, func(i, j int) bool {
		if selected[i].CreatedAt.Equal(selected[j].CreatedAt) {
			return selected[i].ID > selected[j].ID
		}
		return selected[i].CreatedAt.After(selected[j].CreatedAt)
	})
	if limit > 0 && len(selected) > limit {
		selected = selected[:limit]
	}
	return selected, nil
}

func admittedAgentSessionAttachments(
	ctx context.Context,
	verifier interfaces.MessageSourceLineageVerifier,
	sessionID string,
) (types.MessageAttachments, error) {
	actual, ok := verifier.(*messageService)
	if !ok || actual.materialStore == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	tenant, ok := sessionTenantIDForLookup(ctx)
	if !ok {
		return nil, access.ErrNextcloudPublicationDenied
	}
	exclude, _ := ctx.Value(agentHistoryCurrentRequestKey{}).(string)
	rows, err := actual.admittedMessages(
		ctx,
		types.MessageMaterialReadRequest{
			TenantID:                  tenant,
			SessionID:                 sessionID,
			AllSession:                true,
			CompletedOnly:             true,
			ExcludeRequestID:          exclude,
			IncludeOriginalUserFields: true,
		},
	)
	if err != nil {
		return nil, err
	}
	attachments := types.MessageAttachments{}
	for _, row := range rows {
		attachments = append(attachments, row.Attachments...)
	}
	// Raw user generated attachments have no covered field producer; they were
	// omitted by the typed projection. Never fetch them from a current catalog.
	return attachments, nil
}

func originalHistoryRepositoryConfigured(repo interfaces.MessageRepository) bool {
	actual, ok := repo.(interface {
		MessageMaterialStore() *repository.KnowledgeInputProvenanceStore
	})
	return ok && actual.MessageMaterialStore() != nil
}
