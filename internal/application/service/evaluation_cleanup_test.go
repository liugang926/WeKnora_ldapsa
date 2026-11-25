package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type evaluationCleanupKnowledgeRepo struct {
	interfaces.KnowledgeRepository
	knowledge *types.Knowledge
}

func (r *evaluationCleanupKnowledgeRepo) GetKnowledgeByID(
	_ context.Context, tenant uint64, id string,
) (*types.Knowledge, error) {
	if r.knowledge != nil && r.knowledge.TenantID == tenant && r.knowledge.ID == id {
		return r.knowledge, nil
	}
	return nil, nil
}

func (r *evaluationCleanupKnowledgeRepo) GetKnowledgeBatch(
	context.Context, uint64, []string,
) ([]*types.Knowledge, error) {
	if r.knowledge == nil {
		return nil, nil
	}
	return []*types.Knowledge{r.knowledge}, nil
}

type evaluationCleanupKnowledge struct {
	interfaces.KnowledgeService
	repo       *evaluationCleanupKnowledgeRepo
	knowledge  *types.Knowledge
	createErr  error
	deletedIDs []string
}

func (s *evaluationCleanupKnowledge) CreateKnowledgeFromPassageSync(
	context.Context, string, []string, string,
) (*types.Knowledge, error) {
	return s.knowledge, s.createErr
}

func (s *evaluationCleanupKnowledge) GetRepository() interfaces.KnowledgeRepository {
	return s.repo
}

func (s *evaluationCleanupKnowledge) DeleteKnowledgeList(_ context.Context, ids []string) error {
	s.deletedIDs = append(s.deletedIDs, ids...)
	return nil
}

type evaluationCleanupKB struct {
	interfaces.KnowledgeBaseService
	deletedIDs []string
}

func (s *evaluationCleanupKB) DeleteKnowledgeBase(_ context.Context, id string) error {
	s.deletedIDs = append(s.deletedIDs, id)
	return nil
}

func TestEvalDatasetCleansUpTemporaryResources(t *testing.T) {
	t.Setenv("EVALUATION_DATASET_DIR", filepath.Join("..", "..", "..", "dataset", "benchmarks"))
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "eval.db")), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	ddl, err := os.ReadFile("../../../migrations/sqlite/000034_evaluation_runs.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(ddl)).Error)
	processingErr := errors.New("indexing failed")
	for _, tc := range []struct {
		name                 string
		withoutKnowledge     bool
		createErr            error
		expectedKnowledgeIDs []string
	}{
		{
			name: "processing failure", createErr: processingErr,
			expectedKnowledgeIDs: []string{"knowledge-1"},
		},
		{
			name: "processing failure without knowledge", withoutKnowledge: true, createErr: processingErr,
		},
		{
			name: "successful evaluation", expectedKnowledgeIDs: []string{"knowledge-1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var knowledge *types.Knowledge
			if !tc.withoutKnowledge {
				knowledge = &types.Knowledge{
					ID: "knowledge-1", TenantID: 7, KnowledgeBaseID: "kb-1",
					EnableStatus: "enabled", ParseStatus: types.ParseStatusCompleted,
				}
			}
			knowledgeSvc := &evaluationCleanupKnowledge{
				repo:      &evaluationCleanupKnowledgeRepo{knowledge: knowledge},
				knowledge: knowledge, createErr: tc.createErr,
			}
			kbSvc := &evaluationCleanupKB{}
			svc := &EvaluationService{
				dataset: NewDatasetService(), knowledgeService: knowledgeSvc,
				runRepository: repository.NewEvaluationRunRepository(db), sessionService: evaluationFixtureSession{},
				knowledgeBaseService: kbSvc, evaluationMemoryStorage: newEvaluationMemoryStorage(),
			}
			detail := &types.EvaluationDetail{Task: &types.EvaluationTask{
				ID: "task-1", TenantID: 7, DatasetID: "synthetic-zh-v1",
			}, Params: &types.ChatManage{}}
			svc.evaluationMemoryStorage.register(detail)

			err := svc.EvalDataset(types.WithExecutionTenant(t.Context(), 7), detail, "kb-1")

			if tc.createErr != nil {
				require.ErrorIs(t, err, tc.createErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expectedKnowledgeIDs, knowledgeSvc.deletedIDs)
			require.Equal(t, []string{"kb-1"}, kbSvc.deletedIDs)
		})
	}
}
