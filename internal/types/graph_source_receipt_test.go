package types

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func graphReceiptFixture(t *testing.T) (NameSpace, *GraphNode) {
	t.Helper()
	empty, _ := NewCompleteSourceLineage()
	namespace := NameSpace{KnowledgeBase: "kb", Knowledge: "doc"}
	proof := &KnowledgeBuildInputProof{Version: 1, TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "doc", AdmissionID: "immutable-origin", BuildGeneration: 1, InputDigest: GraphDigest("body"), ConfigDigest: GraphDigest("config"), BodyDigest: GraphDigest("snapshot"), ChunkIDs: []string{"chunk"}, SourceLineage: empty}
	node := &GraphNode{Name: "entity", Chunks: []string{"chunk"}, Attributes: []string{"fact"}}
	var err error
	node.SourceReceipt, err = BindGraphSourceReceipt(&GraphSourceReceipt{Version: 1, OriginalInput: proof, TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "doc", BuildGeneration: 1, InputDigest: proof.InputDigest, ConfigDigest: GraphDigest("actual template"), WireDigest: GraphDigest("actual messages/options"), SourceLineage: empty}, GraphNodeBodyDigest(namespace, node))
	require.NoError(t, err)
	return namespace, node
}
func TestKnowledgeGraphReceiptPrivateAndOriginalBinding(t *testing.T) {
	namespace, node := graphReceiptFixture(t)
	require.NoError(t, CheckGraphNodeReceipt(namespace, node))
	encoded, err := EncodeGraphSourceReceipt(node.SourceReceipt)
	require.NoError(t, err)
	decoded, err := DecodeGraphSourceReceipt(encoded)
	require.NoError(t, err)
	require.Equal(t, node.SourceReceipt, decoded)
	public, err := json.Marshal(node)
	require.NoError(t, err)
	require.NotContains(t, string(public), "source_receipt")
	var forged GraphNode
	require.NoError(t, json.Unmarshal([]byte(`{"name":"entity","source_receipt":{"version":1,"source_lineage":{"version":1,"state":"complete","sources":[]}}}`), &forged))
	require.Nil(t, forged.SourceReceipt)
	node.Attributes[0] = "changed"
	require.ErrorIs(t, CheckGraphNodeReceipt(namespace, node), ErrGraphSourceReceiptUnknown)
}
func TestKnowledgeGraphReceiptMalformedOriginalAndLegacyClosed(t *testing.T) {
	for _, mode := range []string{"nil", "generation", "admission", "source unknown", "input digest", "duplicate key", "extra key"} {
		t.Run(mode, func(t *testing.T) {
			_, node := graphReceiptFixture(t)
			r := node.SourceReceipt
			encoded, err := EncodeGraphSourceReceipt(r)
			require.NoError(t, err)
			switch mode {
			case "nil":
				r.OriginalInput = nil
			case "generation":
				r.OriginalInput.BuildGeneration++
			case "admission":
				r.OriginalInput.AdmissionID = ""
			case "source unknown":
				r.OriginalInput.SourceLineage = UnknownSourceLineage()
			case "input digest":
				r.OriginalInput.InputDigest = GraphDigest("new input")
			case "duplicate key":
				encoded = strings.Replace(encoded, `"tenant_id":7`, `"tenant_id":7,"tenant_id":7`, 1)
			case "extra key":
				encoded = strings.Replace(encoded, `"version":1`, `"version":1,"fake":true`, 1)
			}
			if mode == "duplicate key" || mode == "extra key" {
				_, err := DecodeGraphSourceReceipt(encoded)
				require.Error(t, err)
			} else {
				require.Error(t, r.Validate())
			}
		})
	}
}
func TestKnowledgeGraphPromptOriginRequiresExactActualMaterial(t *testing.T) {
	empty, _ := NewCompleteSourceLineage()
	template, err := BindGraphPromptTemplateOrigin(&PromptTemplateStructured{Description: "trusted human raw instructions", Tags: []string{"tag"}, Examples: []GraphData{{Text: "trusted human example", Node: []*GraphNode{{Name: "example"}}}}}, empty)
	require.NoError(t, err)
	lineage, err := GraphPromptTemplateLineage(template)
	require.NoError(t, err)
	require.Empty(t, lineage.Sources)
	template.Examples[0].Node[0].Name = "derived name"
	_, err = GraphPromptTemplateLineage(template)
	require.Error(t, err)
	var incoming PromptTemplateStructured
	require.NoError(t, json.Unmarshal([]byte(`{"description":"x","source_origin":{"source_lineage":{"version":1,"state":"complete","sources":[]}}}`), &incoming))
	require.Nil(t, incoming.SourceOrigin)
}
