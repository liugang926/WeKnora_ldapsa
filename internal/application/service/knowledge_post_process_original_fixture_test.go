package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// attachPostProcessOriginalInputs supplies actual owned originals and current
// SQL controls. The state/queue spies retain only the failure/count contracts;
// they never sign or verify body, producer, actor or model receipts themselves.
func attachPostProcessOriginalInputs(
	t *testing.T,
	svc *KnowledgePostProcessService,
	repo *wikiEnqueueFailureKnowledgeRepo,
	desired *types.KnowledgeBase,
	chunks []*types.Chunk,
) *enrichmentTestFixture {
	t.Helper()
	f := newEnrichmentTestFixture(t, false)
	applyEnrichmentTemplateMigration(t, f, false)
	require.NoError(t, f.db.AutoMigrate(&types.TaskPendingOp{}, &types.KnowledgeProcessingSpan{}))

	// Start with a fresh real actor, without retaining the setup KB grant.
	ctx := types.WithCaller(context.Background(), types.CallerFromContext(f.ctx))
	principal, ok := types.PrincipalFromContext(f.ctx)
	require.True(t, ok)
	ctx = types.WithPrincipal(ctx, principal)
	ctx = types.WithExecutionTenant(ctx, 7)
	actual := &types.KnowledgeBase{}
	err := f.db.Where("id = ?", desired.ID).Take(actual).Error
	if err == gorm.ErrRecordNotFound {
		actual = &types.KnowledgeBase{
			ID: desired.ID, TenantID: 7, Type: types.KnowledgeBaseTypeDocument,
			CreatorID: types.CallerFromContext(ctx).UserID,
			Name:      "Original postprocess fixture KB", SummaryModelID: "summary", EmbeddingModelID: "embed",
		}
	} else {
		require.NoError(t, err)
	}
	actual.IndexingStrategy = desired.IndexingStrategy
	actual.QuestionGenerationConfig = desired.QuestionGenerationConfig
	actual.ExtractConfig = desired.ExtractConfig
	actual.AutoTagConfig = desired.AutoTagConfig
	input := repository.WithIndependentKnowledgeInput(ctx)
	if err == gorm.ErrRecordNotFound {
		require.NoError(t, f.store.CreateKnowledgeBaseWithInput(
			repository.WithIndependentKnowledgeBaseNameInput(input, "", actual.Name), actual,
		))
	} else {
		require.NoError(t, f.store.UpdateKnowledgeBaseWithInput(input, actual))
	}
	ctx, err = f.store.AuthorizeCurrentKnowledgeInputWrite(ctx, actual)
	require.NoError(t, err)

	// All body and override fields are authored before the original admission.
	// No cached knowledge/chunk row is retroactively certified as independent.
	const body = "Actual authored postprocess document body for the local parser fixture."
	knowledge := repo.knowledge
	metadata := map[string]any{}
	if len(knowledge.Metadata) != 0 {
		require.NoError(t, json.Unmarshal(knowledge.Metadata, &metadata))
	}
	metadata["manual"] = map[string]any{"content": body, "version": 1}
	raw, err := json.Marshal(metadata)
	require.NoError(t, err)
	knowledge.Metadata = types.JSON(raw)
	knowledge.CustomMetadata = types.JSON(`{}`)
	knowledge.Type, knowledge.Channel, knowledge.Source = types.KnowledgeTypeManual, "web", "manual"
	knowledge.Title = "Actual postprocess original document"
	knowledge.EnableStatus = "enabled"
	original, err := f.store.CreateKnowledgeWithInput(
		repository.WithIndependentKnowledgeInput(ctx), knowledge, body,
	)
	require.NoError(t, err)
	build := repository.WithKnowledgeBuildInputProof(ctx, f.store, original)
	for index, chunk := range chunks {
		chunk.TenantID, chunk.KnowledgeBaseID, chunk.KnowledgeID = 7, actual.ID, knowledge.ID
		chunk.ChunkIndex, chunk.IsEnabled, chunk.IndexStatus = index, true, "ready"
		chunk.Metadata = types.JSON(`{}`)
	}
	require.NoError(t, f.s.chunkRepo.CreateChunks(build, chunks))
	repo.KnowledgeRepository = f.s.repo
	svc.kbService = enrichmentTestBases{db: f.db}
	svc.chunkRepo, svc.chunkService = f.s.chunkRepo, enrichmentTestChunks{repo: f.s.chunkRepo}
	svc.knowledgeInputs, svc.enrichmentConfig = f.store, f.s.config
	// Completion owns the automatic-tag handoff in the real repository transaction.
	ConfigureKnowledgeAutoTagCompletion(svc, f.s.repo, f.store)
	if pending, ok := svc.pendingRepo.(*wikiEnqueueFailurePendingRepo); ok {
		pending.TaskPendingOpsRepository = repository.NewTaskPendingOpsRepository(f.db)
	}
	f.k, f.chunks, f.build = knowledge, chunks, build
	return f
}

func postProcessOriginalTask(t *testing.T, f *enrichmentTestFixture) *asynq.Task {
	t.Helper()
	payload, err := json.Marshal(types.KnowledgePostProcessPayload{
		TenantID: 7, KnowledgeBaseID: f.k.KnowledgeBaseID, KnowledgeID: f.k.ID,
		OriginalInput: repository.KnowledgeBuildInputProofFromContext(f.build),
		Initiator:     types.TaskInitiatorFromContext(f.build),
	})
	require.NoError(t, err)
	return asynq.NewTask(types.TypeKnowledgePostProcess, payload)
}

func assertPostProcessQueuedOriginals(t *testing.T, f *enrichmentTestFixture, queue *wikiEnqueueFailureTaskQueue) {
	t.Helper()
	for _, task := range queue.tasks {
		var original *types.KnowledgeBuildInputProof
		var initiator types.TaskInitiator
		switch task.Type() {
		case types.TypeSummaryGeneration:
			var p types.SummaryGenerationPayload
			require.NoError(t, json.Unmarshal(task.Payload(), &p))
			original, initiator = p.OriginalInput, p.Initiator
			require.NotNil(t, p.ProducerInput)
			require.NoError(t, f.store.RecheckKnowledgeProducerInput(
				f.build, p.OriginalInput, p.ProducerInput, summaryProducerControls(f.s.config, p),
			))
		case types.TypeQuestionGeneration:
			var p types.QuestionGenerationPayload
			require.NoError(t, json.Unmarshal(task.Payload(), &p))
			original, initiator = p.OriginalInput, p.Initiator
			require.NotNil(t, p.ProducerInput)
			require.NoError(t, f.store.RecheckKnowledgeProducerInput(
				f.build, p.OriginalInput, p.ProducerInput, questionProducerControls(f.s.config, p),
			))
		case types.TypeChunkExtract:
			var p types.ExtractChunkPayload
			require.NoError(t, json.Unmarshal(task.Payload(), &p))
			original, initiator = p.OriginalInput, p.Initiator
		case types.TypeKnowledgeAutoTag:
			var p types.KnowledgeAutoTagPayload
			require.NoError(t, json.Unmarshal(task.Payload(), &p))
			original, initiator = p.OriginalInput, p.Initiator
			require.NotEmpty(t, p.CompletionInput)
			require.NotNil(t, p.TaskInput)
			require.NoError(t, f.store.VerifyKnowledgeAutoTagTaskInput(f.build, p))
		default:
			continue
		}
		require.NotNil(t, original)
		require.NotEmpty(t, original.BodyDigest)
		require.Equal(t, f.k.ID, original.KnowledgeID)
		require.Equal(t, f.k.KnowledgeBaseID, original.KnowledgeBaseID)
		require.NoError(t, original.SourceLineage.RequireComplete())
		if task.Type() == types.TypeKnowledgeAutoTag {
			// The immutable completion envelope carries its original actor selector;
			// recovery does not forge a human TaskInitiator for the machine handoff.
			require.Empty(t, initiator.UserID)
		} else {
			require.Equal(t, "enrich-human", initiator.UserID)
		}
		require.NoError(t, f.store.RecheckKnowledgeBuildInputs(f.build, original))
	}
}
