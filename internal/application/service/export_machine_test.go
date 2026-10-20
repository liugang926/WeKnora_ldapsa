package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sqlitevec "github.com/asg017/sqlite-vec-go-bindings/cgo"
	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/Tencent/WeKnora/internal/application/repository"
	sqliteindex "github.com/Tencent/WeKnora/internal/application/repository/retriever/sqlite"
	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// Export only test ports so the external service test can use the actual
// router middleware without introducing a production service→router cycle.
// MachineQueueFixture exposes actual test ports to the external router integration test.
type MachineQueueFixture struct {
	repeatTableProducer bool
	table               interfaces.TaskHandler
	m                   *machinePipelineFixture
	revoked             interface{ Store(bool) }
	embedder            *machineTableEmbedder
}

// NewMachineQueueFixture pairs a real signed source under an actual restricted policy.
func NewMachineQueueFixture(t *testing.T) *MachineQueueFixture {
	m, revoked := automaticSourceFixture(t)
	return &MachineQueueFixture{m: m, revoked: revoked}
}

// RevokeManager withdraws the signed source manager decision in the controlled server.
func (f *MachineQueueFixture) RevokeManager() { f.revoked.Store(true) }

// Database returns the isolated fixture database.
func (f *MachineQueueFixture) Database() *gorm.DB { return f.m.f.db }

// Inputs returns the actual configured original input store.
func (f *MachineQueueFixture) Inputs() *repository.KnowledgeInputProvenanceStore { return f.m.f.store }

// KnowledgeRepository returns the production knowledge repository.
func (f *MachineQueueFixture) KnowledgeRepository() interfaces.KnowledgeRepository {
	return f.m.f.s.repo
}

// ChunkRepository returns the production chunk repository.
func (f *MachineQueueFixture) ChunkRepository() interfaces.ChunkRepository { return f.m.f.s.chunkRepo }

// KnowledgeBases returns the guarded KB service.
func (f *MachineQueueFixture) KnowledgeBases() interfaces.KnowledgeBaseService {
	return f.m.f.s.kbService
}

// Groups returns the production group authorization service.
func (f *MachineQueueFixture) Groups() interfaces.GroupAccessService { return f.m.f.s.groupAccess }

// Tracker returns the production processing span tracker.
func (f *MachineQueueFixture) Tracker() SpanTracker { return f.m.f.s.spanTracker }

// Tasks returns tasks actually emitted by the production producers.
func (f *MachineQueueFixture) Tasks() []*asynq.Task {
	return append([]*asynq.Task{}, f.m.f.queue.tasks...)
}

// ModelCalls counts controlled model invocations.
func (f *MachineQueueFixture) ModelCalls() int { return f.m.f.model.calls + f.m.model.calls }

// Ingest executes an actual durable event through the connector and file producer.
func (f *MachineQueueFixture) Ingest(t *testing.T) *types.Knowledge {
	payload := automaticSourcePayload(t, f.m, "nextcloud_event")
	ctx := types.WithTaskRetryMetadata(types.WithTaskExecutionID(context.Background(),
		"ncevent:"+payload.NextcloudEventConnectionID+":"+payload.SyncLogID), 0, 0)
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	require.NoError(t, f.m.source.ProcessSync(ctx, asynq.NewTask(types.TypeDataSourceSync, body)))
	var k types.Knowledge
	require.NoError(t, f.m.f.db.Where("channel = ?", types.ConnectorTypeNextcloud).Take(&k).Error)
	require.NotEmpty(t, k.FilePath)
	return &k
}

// Handler selects the actual processing service for the given task type.
func (f *MachineQueueFixture) Handler(kind string) func(context.Context, *asynq.Task) error {
	switch kind {
	case types.TypeDataTableSummary:
		return f.table.Handle
	case types.TypeDocumentProcess:
		return func(ctx context.Context, task *asynq.Task) error {
			if err := f.m.f.s.ProcessDocument(ctx, task); err != nil {
				return err
			}
			if !f.repeatTableProducer {
				return nil
			}
			var payload struct {
				Tenant    uint64 `json:"tenant_id"`
				KB        string `json:"knowledge_base_id"`
				Knowledge string `json:"knowledge_id"`
			}
			if err := json.Unmarshal(task.Payload(), &payload); err != nil {
				return err
			}
			actual, err := f.m.f.store.CaptureKnowledgeOutputInputs(ctx, types.KnowledgeBuildInputCaptureRequest{
				TenantID: payload.Tenant, KnowledgeBaseID: payload.KB, KnowledgeID: payload.Knowledge,
			})
			if err != nil {
				return err
			}
			current := repository.WithKnowledgeBuildInputProof(ctx, f.m.f.store, actual.Proof)
			current = withAttempt(current, f.m.f.s.tracker().LatestAttempt(ctx, payload.Knowledge))
			return f.m.f.s.enqueueNextcloudTableSummary(current, actual.KnowledgeBase, actual.Knowledge)
		}
	case types.TypeKnowledgePostProcess:
		return f.m.f.post.Handle
	case types.TypeSummaryGeneration:
		return f.m.f.s.ProcessSummaryGeneration
	case types.TypeQuestionGeneration:
		return f.m.f.s.ProcessQuestionGeneration
	case types.TypeKnowledgeAutoTag:
		return f.m.auto.Handle
	}
	return nil
}

// RepeatTableProducerAfterDocument models a reentered child producer while the
// real parent's exact lease, current manager and machine claim are still live.
func (f *MachineQueueFixture) RepeatTableProducerAfterDocument() { f.repeatTableProducer = true }

// NewMachineTableQueueFixture creates actual CSV transport, file storage and DuckDB consumers.
func NewMachineTableQueueFixture(t *testing.T) *MachineQueueFixture {
	// Production registers vec0 before opening its SQLite pool. This fixture
	// must do the same instead of relying on another test's initialization.
	sqlitevec.Auto()
	m, revoked := automaticSourceFixture(t, true)
	index := sqliteindex.NewSQLiteRetrieveEngineRepository(m.f.db)
	m.f.s.retrieveEngine = parentChildRetrieveRegistry{engine: retriever.NewKVHybridRetrieveEngine(index,
		types.SQLiteRetrieverEngineType)}
	embedder := &machineTableEmbedder{}
	m.f.s.modelService = machineTableModels{ModelService: m.f.s.modelService, embedder: embedder}
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Exec("SET memory_limit='256MB'; SET threads=1")
	require.NoError(t, err)
	table := NewDataTableSummaryService(m.f.s.modelService, m.f.s.kbService, m.f.s, m.files,
		machineTableChunks{repo: m.f.s.chunkRepo},
		machineQueueTenantService{repo: m.f.s.tenantRepo}, m.f.s.retrieveEngine, m.f.s.ownership, db, nil)
	ConfigureDataTableInputProvenance(table, m.f.store)
	return &MachineQueueFixture{m: m, revoked: revoked, table: table, embedder: embedder}
}

type machineTableChunks struct {
	interfaces.ChunkService
	repo interfaces.ChunkRepository
}

func (c machineTableChunks) GetRepository() interfaces.ChunkRepository { return c.repo }
func (c machineTableChunks) CreateChunks(ctx context.Context, chunks []*types.Chunk) error {
	return c.repo.CreateChunks(ctx, chunks)
}

func (c machineTableChunks) UpdateChunks(ctx context.Context, chunks []*types.Chunk) error {
	return c.repo.UpdateChunks(ctx, chunks)
}

type machineTableModels struct {
	interfaces.ModelService
	embedder *machineTableEmbedder
}

func (m machineTableModels) GetEmbeddingModel(context.Context, string) (embedding.Embedder, error) {
	return m.embedder, nil
}

type machineTableEmbedder struct {
	beforeReturn func()
	calls        int
}

func (m *machineTableEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	rows, err := m.BatchEmbed(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	return rows[0], nil
}

func (m *machineTableEmbedder) BatchEmbed(_ context.Context, texts []string) ([][]float32, error) {
	m.calls++
	if m.beforeReturn != nil {
		m.beforeReturn()
	}
	rows := make([][]float32, len(texts))
	for i := range rows {
		rows[i] = []float32{1, 0}
	}
	return rows, nil
}

func (m *machineTableEmbedder) BatchEmbedWithPool(
	ctx context.Context,
	model embedding.Embedder,
	texts []string,
) ([][]float32, error) {
	return model.BatchEmbed(ctx, texts)
}
func (*machineTableEmbedder) GetModelName() string { return "local embed" }
func (*machineTableEmbedder) GetModelID() string   { return "embed" }
func (*machineTableEmbedder) GetDimensions() int   { return 2 }

// BeforeEmbeddingReturn runs only after the actual table embedding provider is invoked.
func (f *MachineQueueFixture) BeforeEmbeddingReturn(hook func()) { f.embedder.beforeReturn = hook }

// EmbeddingCalls counts actual controlled embedding provider calls.
func (f *MachineQueueFixture) EmbeddingCalls() int { return f.embedder.calls }

type machineQueueTenantService struct {
	interfaces.TenantService
	repo interfaces.TenantRepository
}

func (s machineQueueTenantService) GetTenantByID(ctx context.Context, id uint64) (*types.Tenant, error) {
	return s.repo.GetTenantByID(ctx, id)
}

// TamperTableArtifact changes the actual isolated local provider file, without changing its sealed source receipt.
func (f *MachineQueueFixture) TamperTableArtifact(t *testing.T) {
	files := 0
	err := filepath.WalkDir(f.m.base, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(path, ".csv") {
			files++
			return os.WriteFile(path, []byte("column_a,column_b\n99,100\n"), 0o600)
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, files)
}

// BeforeSummaryModelReturn installs a controlled hook after the actual provider was called.
func (f *MachineQueueFixture) BeforeSummaryModelReturn(hook func()) { f.m.f.model.beforeReturn = hook }
