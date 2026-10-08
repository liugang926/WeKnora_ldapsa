package service

import (
	"context"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type knowledgeEnrichmentInputStore interface {
	interfaces.KnowledgeBuildInputReader
	CaptureKnowledgeEnrichmentInputs(context.Context, types.KnowledgeBuildInputCaptureRequest) (*types.KnowledgeBuildInputSnapshot, error)
	AdmitKnowledgeProducerInput(context.Context, *types.KnowledgeBuildInputProof, string, any) (*types.KnowledgeProducerInputProof, error)
	RecheckKnowledgeProducerInput(context.Context, *types.KnowledgeBuildInputProof, *types.KnowledgeProducerInputProof, any) error
	RestoreKnowledgeProducerActor(context.Context, *types.KnowledgeBuildInputProof, *types.KnowledgeProducerInputProof) (context.Context, error)
	ReadKnowledgeProducerTenant(context.Context, *types.KnowledgeBuildInputProof) (*types.Tenant, error)
}
type enrichmentRuntime struct {
	SummaryPrompt, QuestionPrompt           string
	Summary                                 *config.SummaryConfig
	SummaryOriginKnown, QuestionOriginKnown bool
}

func currentEnrichmentRuntime(cfg *config.Config) enrichmentRuntime {
	if cfg == nil || cfg.Conversation == nil {
		return enrichmentRuntime{}
	}
	_, summaryErr := config.KnowledgeEnrichmentPromptLineage(cfg, "summary")
	_, questionErr := config.KnowledgeEnrichmentPromptLineage(cfg, "question")
	return enrichmentRuntime{cfg.Conversation.GenerateSummaryPrompt, cfg.Conversation.GenerateQuestionsPrompt, cfg.Conversation.Summary, summaryErr == nil, questionErr == nil}
}
func summaryProducerControls(cfg *config.Config, p types.SummaryGenerationPayload) any {
	return struct {
		Runtime          enrichmentRuntime
		Tenant           uint64
		KB, KG, Language string
		Attempt          int
		Refresh          bool
	}{currentEnrichmentRuntime(cfg), p.TenantID, p.KnowledgeBaseID, p.KnowledgeID, p.Language, p.Attempt, p.Refresh}
}
func questionProducerControls(cfg *config.Config, p types.QuestionGenerationPayload) any {
	return struct {
		Runtime                            enrichmentRuntime
		Tenant                             uint64
		KB, KG, Language, Chunk, Pre, Next string
		IDs                                []string
		Count, Attempt, Batch              int
	}{currentEnrichmentRuntime(cfg), p.TenantID, p.KnowledgeBaseID, p.KnowledgeID, p.Language, p.ChunkID, p.PrevChunkID, p.NextChunkID, p.ChunkIDs, p.QuestionCount, p.Attempt, p.BatchIndex}
}
func imageProducerControls(p types.ImageMultimodalPayload) any {
	return struct {
		Tenant                                               uint64
		KB, KG, Parent, URL, Local, Digest, Language, Source string
		OCR, Caption                                         bool
		Attempt, Index                                       int
	}{p.TenantID, p.KnowledgeBaseID, p.KnowledgeID, p.ChunkID, p.ImageURL, p.ImageLocalPath, p.ImageDigest, p.Language, p.ImageSourceType, p.EnableOCR, p.EnableCaption, p.Attempt, p.ImageIndex}
}
func resumeKnowledgeEnrichment(ctx context.Context, reader interfaces.KnowledgeBuildInputReader, original *types.KnowledgeBuildInputProof, producer *types.KnowledgeProducerInputProof, input func() any) (context.Context, error) {
	store, ok := reader.(knowledgeEnrichmentInputStore)
	if !ok || original == nil || producer == nil || original.Projection != types.KnowledgeBuildProjectionEnrichment {
		return ctx, types.ErrSourceLineageUnknown
	}
	var err error
	ctx, err = store.RestoreKnowledgeProducerActor(ctx, original, producer)
	if err != nil {
		return ctx, err
	}
	ctx = repository.WithKnowledgeBuildInputProof(ctx, reader, original)
	ctx = repository.WithKnowledgeProducerInput(ctx, producer, input)
	tenant, err := store.ReadKnowledgeProducerTenant(ctx, original)
	if err != nil {
		return ctx, err
	}
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
	if err := requireActualKnowledgeBuildInput(ctx); err != nil {
		return ctx, err
	}
	return ctx, nil
}
func (s *knowledgeService) captureFreshEnrichment(ctx context.Context, tenant uint64, kb, id string, ids []string) (context.Context, *types.KnowledgeBuildInputSnapshot, error) {
	store, ok := s.knowledgeInputs.(knowledgeEnrichmentInputStore)
	if !ok {
		return ctx, nil, types.ErrSourceLineageUnknown
	}
	if ctx.Value(types.CallerContextKey) != nil && !repository.KnowledgeProducerInputPresent(ctx) {
		currentKB, e := s.kbService.GetKnowledgeBaseByID(ctx, kb)
		if e != nil {
			return ctx, nil, e
		}
		if currentKB == nil || currentKB.ID != kb || currentKB.TenantID != tenant {
			return ctx, nil, types.ErrSourceLineageUnknown
		}
		authority, ok := s.knowledgeInputs.(interface {
			AuthorizeKnowledgeCurrentProducerWrite(context.Context, *types.KnowledgeBase) (context.Context, error)
		})
		if !ok {
			return ctx, nil, types.ErrSourceLineageUnknown
		}
		var e2 error
		ctx, e2 = authority.AuthorizeKnowledgeCurrentProducerWrite(ctx, currentKB)
		if e2 != nil {
			return ctx, nil, e2
		}
	}
	snapshot, err := store.CaptureKnowledgeEnrichmentInputs(ctx, types.KnowledgeBuildInputCaptureRequest{TenantID: tenant, KnowledgeBaseID: kb, KnowledgeID: id, ChunkIDs: ids, Expected: repository.KnowledgeBuildInputProofFromContext(ctx)})
	if err != nil {
		return ctx, nil, err
	}
	ctx = repository.WithKnowledgeBuildInputProof(ctx, s.knowledgeInputs, snapshot.Proof)
	if err := requireActualKnowledgeBuildInput(ctx); err != nil {
		return ctx, nil, err
	}
	return ctx, snapshot, nil
}
func enrichmentInputChunkIDs(chunks []*types.Chunk) []string {
	ids := []string{}
	for _, c := range chunks {
		if c != nil && (c.ChunkType == types.ChunkTypeText || c.ChunkType == types.ChunkTypeImageOCR || c.ChunkType == types.ChunkTypeImageCaption) {
			ids = append(ids, c.ID)
		}
	}
	return ids
}

// ConfigureKnowledgeEnrichmentRuntime supplies the exact resolved templates
// consumed by the model handler when the private job is admitted.
func ConfigureKnowledgeEnrichmentRuntime(handler interfaces.TaskHandler, cfg *config.Config) {
	if impl, ok := handler.(*KnowledgePostProcessService); ok {
		impl.enrichmentConfig = cfg
	}
}
func (s *knowledgeService) prepareSummaryGenerationPayload(ctx context.Context, p *types.SummaryGenerationPayload) error {
	if s.knowledgeInputs == nil {
		return nil
	}
	if _, err := config.KnowledgeEnrichmentPromptLineage(s.config, "summary"); err != nil {
		return err
	}
	rows, err := s.chunkRepo.ListChunksByKnowledgeID(ctx, p.TenantID, p.KnowledgeID)
	if err != nil {
		return err
	}
	ctx, actual, err := s.captureFreshEnrichment(ctx, p.TenantID, p.KnowledgeBaseID, p.KnowledgeID, enrichmentInputChunkIDs(rows))
	if err != nil {
		return err
	}
	p.OriginalInput = actual.Proof
	store := s.knowledgeInputs.(knowledgeEnrichmentInputStore)
	p.ProducerInput, err = store.AdmitKnowledgeProducerInput(ctx, actual.Proof, "summary", summaryProducerControls(s.config, *p))
	return err
}
