package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	gormlogger "gorm.io/gorm/logger"
)

type vectorCandidateSQLRecorder struct {
	gormlogger.Interface
	Statements []string
}

func (r *vectorCandidateSQLRecorder) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	query, _ := fc()
	r.Statements = append(r.Statements, query)
}

func TestNativeSQLiteVectorCandidatesFilterBeforeTopKAndNeverSelectBody(t *testing.T) {
	for _, mode := range []string{"knowledge-base", "tenant", "owned-whitelist", "index-disabled", "knowledge-disabled"} {
		t.Run(mode, func(t *testing.T) {
			r := newSQLiteRetrieverTestRepository(t)
			require.NoError(t, r.db.Exec(`CREATE TABLE knowledge_bases(id TEXT PRIMARY KEY,tenant_id BIGINT NOT NULL,deleted_at DATETIME);
				CREATE TABLE knowledges(id TEXT PRIMARY KEY,tenant_id BIGINT NOT NULL,knowledge_base_id TEXT NOT NULL,parse_status TEXT NOT NULL,enable_status TEXT NOT NULL,deleted_at DATETIME)`).Error)
			require.NoError(t, r.db.Exec("INSERT INTO knowledge_bases(id,tenant_id) VALUES('kb-target',7),('kb-other',7),('kb-foreign',8)").Error)
			blockerKB, blockerTenant, blockerEnabled, blockerKnowledgeEnabled := "kb-target", uint64(7), true, "enabled"
			p := types.VectorCandidateParams{TenantID: 7, KnowledgeBaseIDs: []string{"kb-target"}, KnowledgeIDs: []string{"target", "blocker"}, Embedding: []float32{1, 0}, TopK: 1}
			switch mode {
			case "knowledge-base":
				blockerKB = "kb-other"
			case "tenant":
				blockerKB, blockerTenant = "kb-foreign", 8
				p.KnowledgeBaseIDs = append(p.KnowledgeBaseIDs, blockerKB)
			case "owned-whitelist":
				p.KnowledgeIDs = []string{"target"}
			case "index-disabled":
				blockerEnabled = false
			case "knowledge-disabled":
				blockerKnowledgeEnabled = "disabled"
			}
			require.NoError(t, r.db.Exec("INSERT INTO knowledges(id,tenant_id,knowledge_base_id,parse_status,enable_status) VALUES('target',7,'kb-target','completed','enabled')").Error)
			require.NoError(t, r.db.Exec("INSERT INTO knowledges(id,tenant_id,knowledge_base_id,parse_status,enable_status) VALUES(?,?,?,'completed',?)", "blocker", blockerTenant, blockerKB, blockerKnowledgeEnabled).Error)
			blocker := sqliteTestIndex("blocker-chunk", blockerKB, "blocker", "", blockerEnabled)
			blocker.Content = "PRIVATE BLOCKER CONTENT NEVER SELECTED"
			target := sqliteTestIndex("target-chunk", "kb-target", "target", "", true)
			target.Content = "PRIVATE TARGET CONTENT NEVER SELECTED"
			saveSQLiteTestVector(t, r, blocker, []float32{1, 0})
			saveSQLiteTestVector(t, r, target, []float32{0.8, 0.6})
			recorder := &vectorCandidateSQLRecorder{Interface: r.db.Logger}
			r.db.Logger = recorder
			rows, err := r.RetrieveVectorCandidates(types.WithExecutionTenant(context.Background(), 7), p)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, "target-chunk", rows[0].ChunkID)
			selected := false
			for _, statement := range recorder.Statements {
				if strings.Contains(statement, "SELECT v.rowid") {
					selected = true
					require.NotContains(t, strings.ToLower(statement), "content")
					require.NotContains(t, strings.ToLower(statement), "title")
					require.NotContains(t, statement, "PRIVATE")
				}
			}
			require.True(t, selected, "must execute the real native ID-only query")
		})
	}
}

func TestNativeSQLiteVectorCandidatesMissingControlTablesAndEmptyWhitelistRefuse(t *testing.T) {
	r := newSQLiteRetrieverTestRepository(t)
	p := types.VectorCandidateParams{TenantID: 7, KnowledgeBaseIDs: []string{"kb"}, KnowledgeIDs: []string{"knowledge"}, Embedding: []float32{1, 0}, TopK: 1}
	ctx := types.WithExecutionTenant(context.Background(), 7)
	rows, err := r.RetrieveVectorCandidates(ctx, p)
	require.ErrorIs(t, err, types.ErrVectorCandidateRetrievalUnavailable)
	require.Nil(t, rows)
	p.KnowledgeIDs = nil
	rows, err = r.RetrieveVectorCandidates(ctx, p)
	require.ErrorIs(t, err, types.ErrVectorCandidateScope)
	require.Nil(t, rows)
}
