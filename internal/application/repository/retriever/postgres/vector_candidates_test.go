package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	gormlogger "gorm.io/gorm/logger"
)

type vectorCandidatePGSQLRecorder struct {
	gormlogger.Interface
	Statements []string
}

func (r *vectorCandidatePGSQLRecorder) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	query, _ := fc()
	r.Statements = append(r.Statements, query)
}

func TestNativePGVectorCandidatesFilterBeforeTopKAndNeverSelectBody(t *testing.T) {
	db := nextcloudVectorTestDB(t)
	require.NoError(t, db.Exec("CREATE TABLE knowledge_bases(id TEXT PRIMARY KEY,tenant_id BIGINT NOT NULL,deleted_at TIMESTAMPTZ)").Error)
	require.NoError(t, db.Exec("INSERT INTO knowledge_bases(id,tenant_id) VALUES('kb-target',7),('kb-foreign',8)").Error)
	for _, item := range []struct {
		ID, KB string
		Tenant uint64
	}{{"target", "kb-target", 7}, {"not-owned", "kb-target", 7}, {"foreign", "kb-foreign", 8}} {
		require.NoError(t, db.Create(&types.Knowledge{ID: item.ID, TenantID: item.Tenant, KnowledgeBaseID: item.KB, Type: "passage", Channel: "internal_history", ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled", Metadata: types.JSON(`{}`)}).Error)
	}
	repo := &pgRepository{db: db}
	for _, item := range []struct {
		ID, KB string
		Vector []float32
	}{{"target", "kb-target", []float32{0.8, 0.6}}, {"not-owned", "kb-target", []float32{1, 0}}, {"foreign", "kb-foreign", []float32{1, 0}}} {
		info := &types.IndexInfo{SourceID: item.ID, SourceType: types.ChunkSourceType, ChunkID: "chunk-" + item.ID, KnowledgeID: item.ID, KnowledgeBaseID: item.KB, Content: "PRIVATE INDEX BODY NEVER SELECTED", IsEnabled: true}
		require.NoError(t, repo.Save(context.Background(), info, map[string]any{"embedding": map[string][]float32{item.ID: item.Vector}}))
	}
	recorder := &vectorCandidatePGSQLRecorder{Interface: db.Logger}
	db.Logger = recorder
	ctx := types.WithExecutionTenant(context.Background(), 7)
	p := types.VectorCandidateParams{TenantID: 7, KnowledgeBaseIDs: []string{"kb-target", "kb-foreign"}, KnowledgeIDs: []string{"target", "foreign"}, Embedding: []float32{1, 0}, TopK: 1}
	rows, err := repo.RetrieveVectorCandidates(ctx, p)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "target", rows[0].KnowledgeID)
	selected := false
	for _, statement := range recorder.Statements {
		if strings.Contains(statement, "SELECT candidates.id") {
			selected = true
			require.NotContains(t, strings.ToLower(statement), "content")
			require.NotContains(t, strings.ToLower(statement), "title")
			require.NotContains(t, statement, "PRIVATE")
		}
	}
	require.True(t, selected)
	p.KnowledgeIDs = nil
	rows, err = repo.RetrieveVectorCandidates(ctx, p)
	require.ErrorIs(t, err, types.ErrVectorCandidateScope)
	require.Nil(t, rows)
	// A separate vector store without authoritative control tables cannot
	// claim tenant-scoped native candidates through this same-database port.
	require.NoError(t, db.Exec("DROP TABLE knowledge_bases").Error)
	p.KnowledgeIDs = []string{"target"}
	rows, err = repo.RetrieveVectorCandidates(ctx, p)
	require.ErrorIs(t, err, types.ErrVectorCandidateRetrievalUnavailable)
	require.Nil(t, rows)
}
