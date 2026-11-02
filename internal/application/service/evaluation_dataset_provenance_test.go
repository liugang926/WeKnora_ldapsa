package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type evaluationImportStore struct {
	interfaces.KnowledgeService
	store *repository.KnowledgeInputProvenanceStore
	repo  interfaces.KnowledgeRepository
}

func (s evaluationImportStore) CreateKnowledgeFromPassageSync(ctx context.Context, kb string, passages []string, channel string) (*types.Knowledge, error) {
	k := &types.Knowledge{ID: "benchmark-import", TenantID: 7, KnowledgeBaseID: kb, Type: "passage", Channel: "web", ParseStatus: "completed", EnableStatus: "enabled", EmbeddingModelID: "embed"}
	_, err := s.store.CreateKnowledgeWithInput(ctx, k, passages)
	return k, err
}
func (s evaluationImportStore) DeleteKnowledge(context.Context, string) error       { return nil }
func (s evaluationImportStore) DeleteKnowledgeList(context.Context, []string) error { return nil }
func (s evaluationImportStore) GetRepository() interfaces.KnowledgeRepository       { return s.repo }

type evaluationFixtureKB struct{ evaluationProvenanceKB }

func (s evaluationFixtureKB) DeleteKnowledgeBase(context.Context, string) error { return nil }

type evaluationFixtureSession struct{ interfaces.SessionService }

func (s evaluationFixtureSession) KnowledgeQAByEvent(ctx context.Context, cm *types.ChatManage, events []types.EventType) error {
	cm.ChatResponse = &types.ChatResponse{Content: "synthetic answer"}
	return nil
}
func TestEvaluationLocalDatasetAdmissionAndCompletion(t *testing.T) {
	t.Setenv("EVALUATION_DATASET_DIR", filepath.Join("..", "..", "..", "dataset", "benchmarks"))
	_, store, db, ctx, _, _, _ := historyActorIntegrationFixture(t, "web")
	for _, migration := range []string{"000059_knowledge_collection_display_inputs.up.sql", "000034_evaluation_runs.up.sql"} {
		ddl, err := os.ReadFile("../../../migrations/sqlite/" + migration)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
	// Exactly the production background detach must retain the authenticated actor.
	ctx = logger.CloneContext(context.WithoutCancel(ctx))
	svc := &EvaluationService{dataset: &DatasetService{}, knowledgeBaseService: evaluationFixtureKB{evaluationProvenanceKB{store: store}}, knowledgeService: evaluationImportStore{store: store, repo: repository.NewKnowledgeRepository(db)}, sessionService: evaluationFixtureSession{}, runRepository: repository.NewEvaluationRunRepository(db), evaluationMemoryStorage: newEvaluationMemoryStorage()}
	kb, err := svc.createEvaluationKnowledgeBase(ctx, "embed", "chat")
	require.NoError(t, err)
	fixture, inputCtx, err := svc.loadEvaluationDatasetInput(ctx, "synthetic-zh-v1")
	require.NoError(t, err)
	denied := &types.Knowledge{ID: "unadmitted-import", TenantID: 7, KnowledgeBaseID: kb.ID, Type: "passage"}
	_, err = store.CreateKnowledgeWithInput(ctx, denied, fixture.Corpus)
	require.ErrorIs(t, err, types.ErrSourceLineageUnknown)
	require.NotSame(t, ctx, inputCtx)
	detail := &types.EvaluationDetail{Task: &types.EvaluationTask{ID: "fixture-run", TenantID: 7, DatasetID: "synthetic-zh-v1", StartTime: time.Now()}, Params: &types.ChatManage{}}
	require.NoError(t, svc.runRepository.Save(ctx, detail))
	svc.evaluationMemoryStorage.register(detail)
	require.NoError(t, svc.EvalDataset(ctx, detail, kb.ID))
	saved, err := svc.runRepository.Get(ctx, 7, "fixture-run")
	require.NoError(t, err)
	require.Equal(t, 16, saved.Task.Finished)
	require.Len(t, saved.Cases, 16)
	for _, c := range saved.Cases {
		require.NotNil(t, c)
		require.Equal(t, "synthetic answer", c.GeneratedAnswer)
	}
	require.NotEmpty(t, saved.Task.DatasetSHA256)
	svc.dataset = nil
	_, _, err = svc.loadEvaluationDatasetInput(ctx, "synthetic-zh-v1")
	require.ErrorIs(t, err, types.ErrSourceLineageUnknown)
	svc.dataset = &DatasetService{}
	_, _, err = svc.loadEvaluationDatasetInput(ctx, "../private")
	require.Error(t, err)
}
