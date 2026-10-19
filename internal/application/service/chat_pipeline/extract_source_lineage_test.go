package chatpipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type sourceGraphModel struct {
	call  func([]chat.Message, *chat.ChatOptions) (*types.ChatResponse, error)
	calls int
}

func (m *sourceGraphModel) Chat(
	_ context.Context,
	msg []chat.Message,
	opts *chat.ChatOptions,
) (*types.ChatResponse, error) {
	m.calls++
	return m.call(msg, opts)
}

func (m *sourceGraphModel) ChatStream(
	context.Context,
	[]chat.Message,
	*chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	panic("unused")
}
func (m *sourceGraphModel) GetModelID() string   { return "local-source-stub" }
func (m *sourceGraphModel) GetModelName() string { return "local-source-stub" }
func extractionLineage(t *testing.T, file, etag string, epoch uint64) *types.SourceLineage {
	t.Helper()
	l, err := types.NewCompleteSourceLineage(
		types.SourceIdentity{
			Provider:        types.ConnectorTypeNextcloud,
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     "doc",
			DataSourceID:    "ds",
			PairOperationID: "pair",
			InstanceID:      "instance",
			BindingID:       "binding",
			FileID:          file,
			ExternalID:      "nextcloud:instance:" + file,
			ETag:            etag,
			Revision:        epoch,
		},
	)
	require.NoError(t, err)
	return l
}

func TestKnowledgeGraphExtractorAccountsActualWireAndIgnoresModelAuthority(t *testing.T) {
	body := extractionLineage(t, "77", "old", 1)
	config := extractionLineage(t, "77", "new", 2)
	template := &types.PromptTemplateStructured{
		Description: "actual system %s",
		Tags:        []string{"actual-tag"},
		Examples: []types.GraphData{
			{
				Text: "actual example",
				Node: []*types.GraphNode{{Name: "example", Attributes: []string{"actual attributes"}}},
			},
		},
	}
	model := &sourceGraphModel{call: func(msg []chat.Message, opts *chat.ChatOptions) (*types.ChatResponse, error) {
		require.Contains(t, msg[0].Content, "actual example")
		require.Contains(t, msg[0].Content, "actual attributes")
		require.Contains(t, msg[0].Content, "actual-tag")
		require.Contains(t, msg[1].Content, "actual input")
		require.Len(t, msg[0].SourceLineage.Sources, 2)
		require.Equal(t, 0.3, opts.Temperature)
		return &types.ChatResponse{
			Content: ("[{\"entity\":\"citation-free\",\"source_receipt\":{\"source_lineage\":{\"v" +
				"ersion\":1,\"state\":\"complete\",\"sources\":[]}}}]"),
		}, nil
	}}
	extractor := NewExtractor(model, template)
	checks := 0
	graph, err := extractor.ExtractSourceLineage(
		context.Background(),
		"actual input",
		body,
		config,
		func(context.Context, *types.SourceLineage) error {
			checks++
			template.Examples[0].Text = "mutated after authentication"
			extractor.chatOpt.MaxTokens = 1
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, 3, checks)
	require.Len(t, graph.SourceLineage.Sources, 2)
	require.Nil(t, graph.Node[0].SourceReceipt)
	require.NotEmpty(t, graph.WireDigest)
	require.Equal(t, 1, model.calls)
}

func TestKnowledgeGraphExtractorFreshOutputCheckAndUnknownNeverCallModel(t *testing.T) {
	empty, _ := types.NewCompleteSourceLineage()
	denied := errors.New("withdrawn during actual model call")
	for _, mode := range []string{"unknown body", "unknown config", "withdraw model", "withdraw parse"} {
		t.Run(mode, func(t *testing.T) {
			revoked := false
			model := &sourceGraphModel{call: func([]chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
				if mode == "withdraw model" {
					revoked = true
				}
				return &types.ChatResponse{Content: `[{"entity":"derived"}]`}, nil
			}}
			extractor := NewExtractor(model, &types.PromptTemplateStructured{Description: "literal"})
			body, config := empty, empty
			if mode == "unknown body" {
				body = nil
			}
			if mode == "unknown config" {
				config = types.UnknownSourceLineage()
			}
			checks := 0
			graph, err := extractor.ExtractSourceLineage(
				context.Background(),
				"actual source",
				body,
				config,
				func(context.Context, *types.SourceLineage) error {
					checks++
					if revoked || (mode == "withdraw parse" && checks == 3) {
						return denied
					}
					return nil
				},
			)
			require.Error(t, err)
			require.Nil(t, graph)
			if mode == "unknown body" || mode == "unknown config" {
				require.Zero(t, model.calls)
			} else {
				require.ErrorIs(t, err, denied)
				require.Equal(t, 1, model.calls)
			}
		})
	}
}
