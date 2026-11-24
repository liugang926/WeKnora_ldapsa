package tools

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestDataSchemaRegisteredSnapshotAndAccess(t *testing.T) {
	for _, scenario := range []string{
		"allowed", "workspace_authority", "missing_schema", "outside_scope", "wrong_tenant", "revoked",
		"strict_unproven", "reindexed",
	} {
		t.Run(scenario, func(t *testing.T) {
			read, svc, db := actualAgentDocumentFixture(t)
			chunks := read.chunkService.(*readDocChunkService).repo
			require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", "c1").
				Updates(map[string]any{"chunk_type": types.ChunkTypeTableSummary, "content": "verified summary"}).Error)
			require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", "c2").
				Updates(map[string]any{"chunk_type": types.ChunkTypeTableColumn, "content": "verified columns"}).Error)
			targets := read.searchTargets
			turn := types.NewWeKnoraACLSourceLineage(func(context.Context, *types.SourceLineage) error { return nil })
			switch scenario {
			case "workspace_authority":
				t.Setenv("WEKNORA_NEXTCLOUD_READ_AUTHORITY", "weknora")
			case "missing_schema":
				chunks.ordered = nil
			case "outside_scope":
				targets[0].KnowledgeIDs = []string{"other"}
			case "wrong_tenant":
				targets[0].TenantID++
			case "revoked":
				chunks.onList = func() { svc.revoked.Store(true) }
			case "reindexed":
				chunks.onList = func() {
					require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", "c2").
						Update("status", types.ChunkStatusStored).Error)
				}
			case "strict_unproven":
				// This fixture pins the correct source revision but supplies no
				// generated-material receipt. A strict turn cannot publish these bytes.
				turn, _ = toolTurn()
			}
			registry := NewToolRegistry()
			schema := NewDataSchemaTool(svc, chunks).WithSearchTargets(targets)
			registry.RegisterTool(schema)
			registry.SetRAGChunkSnapshotReader(read.snapshotReader)
			ctx := types.WithRAGSourceLineage(t.Context(), turn)
			result, err := registry.ExecuteToolWithSourceLineage(ctx, ToolDataSchema, json.RawMessage(`{"knowledge_id":"doc"}`))
			switch scenario {
			case "allowed", "workspace_authority":
				require.NoError(t, err)
				require.True(t, result.Success)
				require.Equal(t, "verified summary\n\nverified columns", result.Output)
				require.Equal(t, "verified columns", result.Data["columns"])
				require.Equal(t, "verified summary", result.Data["summary"])
				if scenario == "allowed" {
					require.Len(t, result.SourceLineage.Sources, 1)
					require.Equal(t, "original", result.SourceLineage.Sources[0].ETag)
				}
				require.NoError(t, turn.Check(ctx))
			case "missing_schema":
				require.NoError(t, err)
				require.False(t, result.Success)
				require.Contains(t, result.Error, "No table schema")
				require.Len(t, result.SourceLineage.Sources, 1)
				require.NoError(t, turn.Check(ctx))
			default:
				require.Error(t, err)
				require.Nil(t, result)
				require.Error(t, turn.Check(ctx))
			}
		})
	}
}
