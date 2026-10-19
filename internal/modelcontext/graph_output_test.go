package modelcontext

import (
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestKnowledgeGraphModelProjectionKeepsActualGraphAttributesAndChunkHandles(t *testing.T) {
	r := newSourceRegistry()
	output := r.ModelOutput(&types.ToolResult{Success: true, Output: "raw fallback", Data: map[string]any{"display_type": "graph_query_results", "graph": map[string]any{"nodes": []map[string]any{{"name": "Graph <entity>", "attributes": []string{"model-independent actual graph fact"}}}, "edges": []map[string]any{{"source": "Graph <entity>", "target": "Other", "type": "REL"}}}, "results": []map[string]any{{"chunk_id": "real-chunk", "knowledge_id": "real-doc", "knowledge_base_id": "real-kb", "knowledge_title": "actual title", "content": "actual snapshot body", "chunk_type": "text", "score": 1}}}})
	require.Contains(t, output, "model-independent actual graph fact")
	require.Contains(t, output, "Graph &lt;entity&gt;")
	require.Contains(t, output, `type="REL"`)
	require.Contains(t, output, "actual snapshot body")
	require.Contains(t, output, `id="c1"`)
	require.NotContains(t, output, "raw fallback")
}
