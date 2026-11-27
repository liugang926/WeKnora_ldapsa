package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type analysisWorkspaceKnowledge struct {
	interfaces.KnowledgeService
	doc types.Knowledge
}

func (s *analysisWorkspaceKnowledge) GetKnowledgeByIDOnly(_ context.Context, _ string) (*types.Knowledge, error) {
	doc := s.doc
	return &doc, nil
}

type analysisWorkspaceFile struct {
	interfaces.FileService
	opens  atomic.Int32
	err    error
	onOpen func()
}

func (f *analysisWorkspaceFile) GetFile(context.Context, string) (io.ReadCloser, error) {
	f.opens.Add(1)
	if f.err != nil {
		return nil, f.err
	}
	if f.onOpen != nil {
		f.onOpen()
	}
	return io.NopCloser(strings.NewReader("label,amount\nfirst,14\nsecond,28\n")), nil
}

func TestDataAnalysisWorkspaceResult(t *testing.T) {
	for _, scenario := range []string{
		"parallel", "follow_up", "revoked", "moved", "replaced", "disabled", "storage_failed",
		"outside_scope", "wrong_tenant", "strict", "unscoped",
	} {
		t.Run(scenario, func(t *testing.T) {
			svc := &analysisWorkspaceKnowledge{doc: types.Knowledge{
				ID: "doc", KnowledgeBaseID: "kb", TenantID: 7, FilePath: "test.csv", FileType: "csv",
				EnableStatus: "enabled",
			}}
			db := newPlainDuckDB(t)
			file := &analysisWorkspaceFile{}
			tool := NewDataAnalysisTool(nil, svc, nil, file, db, "test")
			targets := types.SearchTargets{{
				Type: types.SearchTargetTypeKnowledgeBase, KnowledgeBaseID: "kb", TenantID: 7,
			}}
			var revoked atomic.Bool
			check := func(context.Context, *types.SourceLineage) error {
				if revoked.Load() {
					return types.ErrSourceLineageUnknown
				}
				return nil
			}
			turn := types.NewWeKnoraACLSourceLineage(check)
			switch scenario {
			case "revoked":
				file.onOpen = func() { revoked.Store(true) }
			case "moved":
				file.onOpen = func() { svc.doc.KnowledgeBaseID = "other" }
			case "disabled":
				svc.doc.EnableStatus = "disabled"
			case "storage_failed":
				file.err = fmt.Errorf("storage unavailable")
			case "replaced":
				file.onOpen = func() { svc.doc.FilePath = "replacement.csv" }
			case "outside_scope":
				targets[0].KnowledgeIDs = []string{"other-doc"}
			case "wrong_tenant":
				targets[0].TenantID++
			case "strict":
				turn = types.NewRAGSourceLineage(check)
			}
			if scenario != "unscoped" {
				tool.WithSearchTargets(targets)
			}
			registry := NewToolRegistry()
			registry.RegisterTool(tool)
			ctx := types.WithRAGSourceLineage(t.Context(), turn)
			args, err := json.Marshal(DataAnalysisInput{
				KnowledgeID: "doc", SQL: "SELECT SUM(CAST(amount AS BIGINT)) AS total FROM dataset",
			})
			require.NoError(t, err)
			if scenario == "follow_up" {
				for range 2 {
					result, err := registry.ExecuteToolWithSourceLineage(ctx, ToolDataAnalysis, args)
					require.NoError(t, err)
					require.True(t, result.Success)
					require.Contains(t, result.Output, `"total":"42"`)
					turn = types.NewWeKnoraACLSourceLineage(check)
					require.NoError(t, turn.Add(result.SourceLineage))
					ctx = types.WithRAGSourceLineage(t.Context(), turn)
				}
				return
			}
			if scenario == "parallel" {
				const calls = 3
				done := make(chan error, calls)
				for range calls {
					go func() {
						result, err := registry.ExecuteToolWithSourceLineage(ctx, ToolDataAnalysis, args)
						if err == nil && (result == nil || !result.Success ||
							!strings.Contains(result.Output, `"total":"42"`)) {
							err = fmt.Errorf("unexpected result: %+v", result)
						}
						if err == nil {
							err = result.SourceLineage.RequireComplete()
						}
						done <- err
					}()
				}
				for range calls {
					require.NoError(t, <-done)
				}
				require.NoError(t, turn.Check(ctx))
				return
			}
			result, err := registry.ExecuteToolWithSourceLineage(ctx, ToolDataAnalysis, args)
			require.Error(t, err)
			require.Nil(t, result)
			if scenario == "revoked" || scenario == "moved" || scenario == "replaced" || scenario == "storage_failed" {
				require.EqualValues(t, 1, file.opens.Load())
				require.Error(t, turn.Check(ctx))
			} else {
				require.Zero(t, file.opens.Load())
			}
		})
	}
}

// Metadata-only and follow-up calculations use the same registry, document and workspace.
func TestDataAnalysisAfterWorkspaceSchema(t *testing.T) {
	t.Setenv("WEKNORA_NEXTCLOUD_READ_AUTHORITY", "weknora")
	read, svc, db := actualAgentDocumentFixture(t)
	doc := svc.docs["doc"]
	doc.FileType, doc.FilePath = "csv", "test.csv"
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", doc.ID).
		Updates(map[string]any{"file_type": doc.FileType, "file_path": doc.FilePath}).Error)
	require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", "c1").
		Updates(map[string]any{"chunk_type": types.ChunkTypeTableSummary, "content": "label, amount"}).Error)
	require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", "c2").
		Updates(map[string]any{"chunk_type": types.ChunkTypeTableColumn, "content": "numeric amount"}).Error)
	file := &analysisWorkspaceFile{}
	r := NewToolRegistry()
	r.RegisterTool(NewDataSchemaTool(svc, read.chunkService.(*readDocChunkService).repo).
		WithSearchTargets(read.searchTargets))
	r.RegisterTool(NewDataAnalysisTool(nil, svc, nil, file, newPlainDuckDB(t), "test").
		WithSearchTargets(read.searchTargets))
	r.SetRAGChunkSnapshotReader(read.snapshotReader)
	check := func(context.Context, *types.SourceLineage) error { return nil }
	ctx := types.WithRAGSourceLineage(t.Context(), types.NewWeKnoraACLSourceLineage(check))
	schema, err := r.ExecuteToolWithSourceLineage(ctx, ToolDataSchema, json.RawMessage(`{"knowledge_id":"doc"}`))
	require.NoError(t, err)
	require.True(t, schema.Success)
	require.Contains(t, schema.Output, "label, amount")
	require.Zero(t, file.opens.Load())

	for _, query := range []struct{ sql, want string }{
		{"SELECT COUNT(*) AS n FROM dataset", `"n":"2"`},
		{"SELECT SUM(CAST(amount AS BIGINT)) AS total FROM dataset", `"total":"42"`},
	} {
		args, err := json.Marshal(DataAnalysisInput{KnowledgeID: doc.ID, SQL: query.sql})
		require.NoError(t, err)
		result, err := r.ExecuteToolWithSourceLineage(ctx, ToolDataAnalysis, args)
		require.NoError(t, err)
		require.True(t, result.Success)
		require.Contains(t, result.Output, query.want)
		require.NoError(t, result.SourceLineage.RequireComplete())
	}
}
