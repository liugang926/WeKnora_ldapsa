package service

import (
	"context"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

func ConfigureFAQInputProvenance(
	knowledge interfaces.KnowledgeService,
	store *repository.KnowledgeInputProvenanceStore,
	guard *access.NextcloudSourceLineageGuard,
) {
	if s, ok := knowledge.(*knowledgeService); ok {
		s.faqInputs = store
		s.faqLineageChecker = guard
	}
}

func (s *knowledgeService) beginFAQInput(
	ctx context.Context,
	kb *types.KnowledgeBase,
	operation types.FAQInputOperation,
) (context.Context, error) {
	if s.faqInputs == nil {
		if s.knowledgeInputs != nil {
			return ctx, types.ErrSourceLineageUnknown
		}
		return ctx, nil
	}
	if s.faqInputs.FAQSessionID(ctx) == "" {
		var err error
		ctx, _, err = s.faqInputs.BeginFAQInput(ctx, kb, operation)
		if err != nil {
			return ctx, err
		}
	}
	return ctx, s.checkFAQInput(ctx)
}

func (s *knowledgeService) checkFAQInput(ctx context.Context) error {
	if s.faqInputs == nil {
		if s.knowledgeInputs != nil {
			return types.ErrSourceLineageUnknown
		}
		return nil
	}
	if s.faqLineageChecker == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	if err := s.faqInputs.CheckFAQInput(ctx); err != nil {
		return err
	}
	snapshot, err := s.faqInputs.FAQSessionSnapshot(ctx)
	if err != nil {
		return err
	}
	lineages, err := repository.FAQInputLineages(snapshot)
	if err != nil {
		return err
	}
	original, err := s.faqInputs.FAQSessionSourceLineage(ctx)
	if err != nil {
		return err
	}
	lineages = append(lineages, original)
	parents, err := s.faqInputs.FAQSessionOriginalKnowledgeInputs(ctx)
	if err != nil {
		return err
	}
	if err := checkFAQOriginalKnowledgeScopes(ctx, s.faqLineageChecker, parents); err != nil {
		return err
	}
	for _, lineage := range lineages {
		if err := s.faqLineageChecker.Check(ctx, lineage, access.LineageReadScope{}); err != nil {
			return err
		}
	}
	empty, _ := types.NewCompleteSourceLineage()
	if err := s.faqLineageChecker.Check(
		ctx,
		empty,
		access.LineageReadScope{
			Targets: types.SearchTargets{
				&types.SearchTarget{
					Type:            types.SearchTargetTypeKnowledgeBase,
					TenantID:        snapshot.Request.TenantID,
					KnowledgeBaseID: snapshot.Request.KnowledgeBaseID,
				},
			},
		},
	); err != nil {
		return err
	}
	return s.faqInputs.CheckFAQInput(ctx)
}

func checkFAQOriginalKnowledgeScopes(
	ctx context.Context,
	checker knowledgeOutputLineageChecker,
	parents []*types.KnowledgeBuildInputProof,
) error {
	for _, p := range parents {
		if p == nil || p.SourceLineage.RequireComplete() != nil {
			return types.ErrSourceLineageUnknown
		}
		readonly := types.WithAPIKeyOperationRequirement(
			ctx,
			types.APIKeyOperationRequirement{
				RequireFullAccess: true,
				Capabilities: []types.APIKeyCapability{
					types.APIKeyCapabilityRetrieve,
					types.APIKeyCapabilityChat,
					types.APIKeyCapabilityMessageHistory,
				},
			},
		)
		if err := checker.Check(
			readonly,
			p.SourceLineage,
			access.LineageReadScope{
				Targets: types.SearchTargets{
					&types.SearchTarget{
						Type:            types.SearchTargetTypeKnowledgeBase,
						TenantID:        p.TenantID,
						KnowledgeBaseID: p.KnowledgeBaseID,
					},
				},
			},
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *knowledgeService) saveFAQChunks(ctx context.Context, chunks []*types.Chunk, create bool) error {
	if s.faqInputs == nil {
		if create {
			return s.chunkService.CreateChunks(ctx, chunks)
		}
		if len(chunks) == 1 {
			return s.chunkService.UpdateChunk(ctx, chunks[0])
		}
		return s.chunkRepo.SaveChunks(ctx, chunks)
	}
	if err := s.checkFAQInput(ctx); err != nil {
		return err
	}
	return s.faqInputs.WriteFAQChunks(ctx, chunks, create)
}

func (s *knowledgeService) deleteFAQChunks(ctx context.Context, tenant uint64, ids []string) error {
	if s.faqInputs == nil {
		return s.chunkRepo.DeleteChunks(ctx, tenant, ids)
	}
	if err := s.checkFAQInput(ctx); err != nil {
		return err
	}
	return s.faqInputs.DeleteFAQChunks(ctx, ids)
}
