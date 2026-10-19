package service

import (
	"context"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
)

type agentConversationSearchKey struct{}

type agentConversationSearchInput struct {
	service *messageService
	owner   string
	slot    *messageMaterialOutputSlot
}

// SearchConversationsWithInputs is the native Agent producer. Unlike the
// public search projection it retains every actual original body examined,
// including nonmatching candidates, until the model and HTTP owner finish.
// No result JSON or caller-supplied lineage can certify a search.
func (s *messageService) SearchConversationsWithInputs(
	ctx context.Context,
	params *types.MessageSearchParams,
) (*types.MessageSearchResult, *types.SourceLineage, error) {
	if ctx == nil || params == nil || s.materialStore == nil || s.sourceLineageGuard == nil {
		return nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	turn := types.RAGSourceLineageFromContext(ctx)
	owner := types.SessionOwnerIDFromContext(ctx)
	if !ok || slot == nil || turn == nil || owner == "" || (params.OwnerID != "" && params.OwnerID != owner) {
		return nil, nil, access.ErrNextcloudPublicationDenied
	}
	if _, ok := s.messageRepo.(historySearchCandidateReader); !ok {
		return nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	if err := CheckMessageMaterialOutputBoundary(ctx); err != nil {
		return nil, nil, err
	}
	prefix, err := turn.CheckedSnapshot(ctx)
	if err != nil {
		return nil, nil, err
	}
	// Even a genuinely empty native search must refresh its authenticated
	// reader; no saved body or citation is needed to establish this control.
	control, err := types.NewCompleteSourceLineage()
	if err != nil {
		return nil, nil, err
	}
	if err := s.sourceLineageGuard.Check(ctx, control, access.LineageReadScope{}); err != nil {
		return nil, nil, err
	}
	request := *params
	request.SessionIDs = append([]string(nil), params.SessionIDs...)
	request.OwnerID = owner
	searchCtx := context.WithValue(ctx, agentConversationSearchKey{}, &agentConversationSearchInput{
		service: s, owner: owner, slot: slot,
	})
	result, err := s.SearchMessages(searchCtx, &request)
	if err != nil {
		return nil, nil, err
	}
	if result == nil {
		return nil, nil, types.ErrSourceLineageUnknown
	}
	for _, item := range result.Items {
		// Native guarded keyword/vector search currently does not hydrate
		// session titles. A future title producer needs its own original
		// receipt; a present-day row label is not covered by message proofs.
		if item != nil && item.SessionTitle != "" {
			return nil, nil, types.ErrSourceLineageUnknown
		}
	}
	if err := CheckMessageMaterialOutputBoundary(searchCtx); err != nil {
		return nil, nil, err
	}
	slot.mu.Lock()
	guards := append([]*MessageMaterialGuard(nil), slot.Guards...)
	bridges := append([]*historyVectorMaterialBridge(nil), slot.HistoryVectorReads...)
	slot.mu.Unlock()
	inputs := []*types.SourceLineage{prefix}
	for _, guard := range guards {
		original, err := guard.OriginalSnapshot()
		if err != nil {
			return nil, nil, err
		}
		for _, proof := range original.Proofs {
			if proof == nil {
				return nil, nil, types.ErrSourceLineageUnknown
			}
			inputs = append(inputs, proof.SourceLineage)
			for _, field := range proof.GeneratedFields {
				if field == nil {
					return nil, nil, types.ErrSourceLineageUnknown
				}
				inputs = append(inputs, field.SourceLineage)
			}
		}
	}
	for _, bridge := range bridges {
		if err := bridge.recheckHeldInputs(); err != nil {
			return nil, nil, err
		}
		for _, child := range bridge.children {
			if child == nil || child.Proof == nil {
				return nil, nil, types.ErrSourceLineageUnknown
			}
			inputs = append(inputs, child.Proof.SourceLineage)
		}
	}
	lineage, err := types.UnionSourceLineage(inputs...)
	if err != nil {
		return nil, nil, err
	}
	if err := turn.Add(lineage); err != nil {
		return nil, nil, err
	}
	if err := turn.Check(searchCtx); err != nil {
		return nil, nil, err
	}
	if err := s.sourceLineageGuard.Check(searchCtx, control, access.LineageReadScope{}); err != nil {
		return nil, nil, err
	}
	if err := CheckMessageMaterialOutputBoundary(searchCtx); err != nil {
		return nil, nil, err
	}
	return result, lineage, nil
}

func (s *messageService) retainAgentConversationCandidate(
	ctx context.Context,
	guard *MessageMaterialGuard,
) ([]*types.Message, bool, error) {
	input, ok := ctx.Value(agentConversationSearchKey{}).(*agentConversationSearchInput)
	if !ok {
		return nil, false, nil
	}
	if input == nil || input.service != s || input.owner != types.SessionOwnerIDFromContext(ctx) ||
		input.slot != ctx.Value(messageMaterialOutputKey{}) {
		return nil, false, access.ErrNextcloudPublicationDenied
	}
	rows, err := s.bindAdmittedMessageGuard(ctx, guard, guard.snapshot.Request)
	return rows, err == nil, err
}
