package neo4j_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	graphrepo "github.com/Tencent/WeKnora/internal/application/repository/retriever/neo4j"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/stretchr/testify/require"
)

type localGraphModel struct {
	beforeReturn func()
	messages     []chat.Message
	options      *chat.ChatOptions
	output       string
}

func (m *localGraphModel) Chat(
	_ context.Context,
	messages []chat.Message,
	options *chat.ChatOptions,
) (*types.ChatResponse, error) {
	m.messages = messages
	m.options = options
	if m.beforeReturn != nil {
		m.beforeReturn()
	}
	return &types.ChatResponse{Content: m.output}, nil
}

func (m *localGraphModel) ChatStream(
	context.Context,
	[]chat.Message,
	*chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	panic("unused local model method")
}
func (m *localGraphModel) GetModelID() string   { return "local-graph-stub" }
func (m *localGraphModel) GetModelName() string { return "local-graph-stub" }

func neoGraphFixture(
	t *testing.T,
) (context.Context, neo4j.Driver, interfaces.RetrieveGraphRepository, types.NameSpace) {
	t.Helper()
	dsn := os.Getenv("WEKNORA_KG_TEST_NEO4J_URI")
	if dsn == "" {
		t.Skip("owned Neo4j fixture is not configured")
	}
	ctx := context.Background()
	driver, err := neo4j.NewDriver(dsn, neo4j.NoAuth())
	require.NoError(t, err)
	require.NoError(t, driver.VerifyConnectivity(ctx))
	repo := graphrepo.NewNeo4jRepository(driver)
	namespace := types.NameSpace{
		KnowledgeBase: "kg_lineage_" + strings.ReplaceAll(uuid.NewString(), "-", ""),
		Knowledge:     "doc",
	}
	t.Cleanup(func() {
		require.NoError(t, repo.DelGraph(ctx, []types.NameSpace{{KnowledgeBase: namespace.KnowledgeBase}}))
		require.NoError(t, driver.Close(ctx))
	})
	return ctx, driver, repo, namespace
}

func graphTestLineage(t *testing.T, namespace types.NameSpace, file, etag string, epoch uint64) *types.SourceLineage {
	t.Helper()
	lineage, err := types.NewCompleteSourceLineage(
		types.SourceIdentity{
			Provider:        types.ConnectorTypeNextcloud,
			TenantID:        7,
			KnowledgeBaseID: namespace.KnowledgeBase,
			KnowledgeID:     namespace.Knowledge,
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
	return lineage
}

func extractGraph(
	ctx context.Context,
	t *testing.T,
	namespace types.NameSpace,
	body,
	config *types.SourceLineage,
	chunk string,
	authorize func(context.Context, *types.SourceLineage) error,
) *types.GraphData {
	t.Helper()
	model := &localGraphModel{
		output: ("```json\n[{\"entity\":\"shared\",\"entity_attributes\":[\"extracted fact\"" +
			"]},{\"entity1\":\"shared\",\"entity2\":\"target\",\"relation\":\"REL\"}]\n" +
			"```"),
	}
	template := &types.PromptTemplateStructured{
		Description: "custom extraction instructions %s",
		Tags:        []string{"REL"},
		Examples: []types.GraphData{
			{
				Text: "actual config example",
				Node: []*types.GraphNode{{Name: "example", Attributes: []string{"example fact"}}},
			},
		},
	}
	extractor := chatpipeline.NewExtractor(model, template)
	graph, err := extractor.ExtractSourceLineage(ctx, "actual source input", body, config, authorize)
	require.NoError(t, err)
	require.Contains(t, model.messages[0].Content, "actual config example")
	require.Contains(t, model.messages[0].Content, "example fact")
	require.Contains(t, model.messages[0].Content, "REL")
	require.Equal(t, 8192, model.options.MaxTokens)
	require.Equal(t, graph.SourceLineage, model.messages[0].SourceLineage)
	proof := &types.KnowledgeBuildInputProof{
		Version:         1,
		TenantID:        7,
		KnowledgeBaseID: namespace.KnowledgeBase,
		KnowledgeID:     namespace.Knowledge,
		AdmissionID:     "local-original-admission",
		BuildGeneration: 1,
		InputDigest:     types.GraphDigest(chunk),
		ConfigDigest:    types.GraphDigest(template),
		BodyDigest:      types.GraphDigest("actual source input"),
		ChunkIDs:        []string{chunk},
		SourceLineage:   graph.SourceLineage,
	}
	base := &types.GraphSourceReceipt{
		OriginalInput:   proof,
		Version:         1,
		TenantID:        7,
		KnowledgeBaseID: namespace.KnowledgeBase,
		KnowledgeID:     namespace.Knowledge,
		BuildGeneration: 1,
		InputDigest:     types.GraphDigest(chunk),
		ConfigDigest:    types.GraphDigest(template),
		WireDigest:      graph.WireDigest,
		SourceLineage:   graph.SourceLineage,
	}
	for _, node := range graph.Node {
		node.Chunks = []string{chunk}
		if node.Attributes == nil {
			node.Attributes = []string{}
		}
		node.SourceReceipt, err = types.BindGraphSourceReceipt(base, types.GraphNodeBodyDigest(namespace, node))
		require.NoError(t, err)
	}
	for _, edge := range graph.Relation {
		edge.SourceReceipt, err = types.BindGraphSourceReceipt(base, types.GraphRelationBodyDigest(namespace, edge))
		require.NoError(t, err)
	}
	graph.SourceReceipt, err = types.BindGraphSourceReceipt(base, types.GraphDataBodyDigest(namespace, graph))
	require.NoError(t, err)
	return graph
}

func TestKnowledgeGraphExtractorNeo4jOriginalUnionEntry(t *testing.T) {
	ctx, _, repo, namespace := neoGraphFixture(t)
	a := graphTestLineage(t, namespace, "77", "original-etag", 1)
	b := graphTestLineage(t, namespace, "78", "config-etag", 2)
	revoked := ""
	authorize := func(_ context.Context, lineage *types.SourceLineage) error {
		for _, source := range lineage.Sources {
			if source.FileID == revoked {
				return types.ErrSourceLineageUnknown
			}
		}
		return nil
	}
	ctx = types.WithGraphWriteAuthorization(
		ctx,
		func(ctx context.Context, r *types.GraphSourceReceipt) error { return authorize(ctx, r.SourceLineage) },
	)
	first := extractGraph(ctx, t, namespace, a, b, "chunk-a", authorize)
	require.NoError(t, repo.AddGraph(ctx, namespace, []*types.GraphData{first}))
	secondNamespace := types.NameSpace{KnowledgeBase: namespace.KnowledgeBase, Knowledge: "other-doc"}
	c := graphTestLineage(t, secondNamespace, "79", "other-etag", 3)
	empty, _ := types.NewCompleteSourceLineage()
	second := extractGraph(ctx, t, secondNamespace, c, empty, "chunk-b", authorize)
	require.NoError(t, repo.AddGraph(ctx, secondNamespace, []*types.GraphData{second}))
	graph, err := repo.SearchNode(ctx, types.NameSpace{KnowledgeBase: namespace.KnowledgeBase}, []string{"shared"})
	require.NoError(t, err)
	require.Len(t, graph.Node, 4, "equal display names retain both original writer units")
	require.Len(t, graph.Relation, 2)
	lineage, err := types.ValidateGraphMaterial(graph)
	require.NoError(t, err)
	require.Len(t, lineage.Sources, 3)
	require.NoError(t, authorize(ctx, lineage))
	revoked = "78"
	require.Error(t, authorize(ctx, lineage), "withdrawn example/config source still controls graph without a citation")
	none, err := repo.SearchNode(ctx, types.NameSpace{KnowledgeBase: namespace.KnowledgeBase}, []string{"absent"})
	require.NoError(t, err)
	require.Empty(t, none.Node)
	require.Len(
		t,
		none.SourceLineage.Sources,
		3,
		"predicates inspect source-derived names before empty-result filtering",
	)
	require.Error(t, authorize(ctx, none.SourceLineage))
}

func TestKnowledgeGraphSameFileEpochsNeverRestamped(t *testing.T) {
	ctx, _, repo, namespace := neoGraphFixture(t)
	empty, _ := types.NewCompleteSourceLineage()
	check := func(context.Context, *types.SourceLineage) error { return nil }
	ctx = types.WithGraphWriteAuthorization(ctx, func(context.Context, *types.GraphSourceReceipt) error { return nil })
	old := extractGraph(
		ctx,
		t,
		namespace,
		graphTestLineage(t, namespace, "77", "old-etag", 1),
		empty,
		"old-chunk",
		check,
	)
	require.NoError(t, repo.AddGraph(ctx, namespace, []*types.GraphData{old}))
	fresh := extractGraph(
		ctx,
		t,
		namespace,
		graphTestLineage(t, namespace, "77", "fresh-etag", 2),
		empty,
		"fresh-chunk",
		check,
	)
	require.NoError(t, repo.AddGraph(ctx, namespace, []*types.GraphData{fresh}))
	graph, err := repo.SearchNode(ctx, namespace, []string{"shared"})
	require.NoError(t, err)
	require.Len(t, graph.SourceLineage.Sources, 2)
	require.NotEqual(t, graph.SourceLineage.Sources[0].ETag, graph.SourceLineage.Sources[1].ETag)
	require.NotEqual(t, graph.SourceLineage.Sources[0].Revision, graph.SourceLineage.Sources[1].Revision)
	for _, node := range graph.Node {
		if node.Chunks[0] == "old-chunk" {
			require.Equal(t, "old-etag", node.SourceReceipt.SourceLineage.Sources[0].ETag)
		}
	}
}

func TestKnowledgeGraphWriteWithdrawalRollsBackAllUnits(t *testing.T) {
	ctx, _, repo, namespace := neoGraphFixture(t)
	empty, _ := types.NewCompleteSourceLineage()
	graph := extractGraph(
		ctx,
		t,
		namespace,
		empty,
		empty,
		"ordinary-chunk",
		func(context.Context, *types.SourceLineage) error { return nil },
	)
	calls := 0
	denied := errors.New("input generation withdrawn inside write transaction")
	ctx = types.WithGraphWriteAuthorization(ctx, func(context.Context, *types.GraphSourceReceipt) error {
		calls++
		if calls > 2*(len(graph.Node)+len(graph.Relation)+1) {
			return denied
		}
		return nil
	})
	require.ErrorIs(t, repo.AddGraph(ctx, namespace, []*types.GraphData{graph}), denied)
	read, err := repo.SearchNode(ctx, namespace, []string{"shared"})
	require.NoError(t, err)
	require.Empty(t, read.Node)
	require.Empty(t, read.Relation)
}

func TestKnowledgeGraphLegacyOrChangedBodyCannotBorrowChunkProof(t *testing.T) {
	for _, mode := range []string{"legacy", "changed body", "orphan legacy"} {
		t.Run(mode, func(t *testing.T) {
			ctx, driver, repo, namespace := neoGraphFixture(t)
			empty, _ := types.NewCompleteSourceLineage()
			graph := extractGraph(
				ctx,
				t,
				namespace,
				empty,
				empty,
				"current-chunk",
				func(context.Context, *types.SourceLineage) error { return nil },
			)
			ctx = types.WithGraphWriteAuthorization(
				ctx,
				func(context.Context, *types.GraphSourceReceipt) error { return nil },
			)
			require.NoError(t, repo.AddGraph(ctx, namespace, []*types.GraphData{graph}))
			session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
			defer func() { require.NoError(t, session.Close(ctx)) }()
			label := "`ENTITY" + namespace.KnowledgeBase + "`:`ENTITY" + namespace.Knowledge + "`"
			query := `MATCH (n:` + label + `) SET n.attributes=['rewritten attributes']`
			if mode != "changed body" {
				query = `CREATE (n:` + label + (" {name:'legacy',kg:$kg,chunks:['" +
					"current-chunk'],attributes:['unknown graph" +
					"']})")
			}
			_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
				result, err := tx.Run(ctx, query, map[string]any{"kg": namespace.Knowledge})
				if err != nil {
					return nil, err
				}
				_, err = result.Consume(ctx)
				return nil, err
			})
			require.NoError(t, err)
			read, err := repo.SearchNode(ctx, namespace, []string{"shared"})
			require.ErrorIs(t, err, types.ErrGraphSourceReceiptUnknown)
			require.Nil(t, read)
		})
	}
}

func TestKnowledgeGraphEmptyModelOutputRetainsOriginalControlReceipt(t *testing.T) {
	ctx, _, repo, namespace := neoGraphFixture(t)
	lineage := graphTestLineage(t, namespace, "77", "empty-source-etag", 1)
	empty, _ := types.NewCompleteSourceLineage()
	model := &localGraphModel{output: "[]"}
	template := &types.PromptTemplateStructured{Description: "literal instructions"}
	extractor := chatpipeline.NewExtractor(model, template)
	graph, err := extractor.ExtractSourceLineage(
		ctx,
		"actual source that extracted no nodes",
		lineage,
		empty,
		func(context.Context, *types.SourceLineage) error { return nil },
	)
	require.NoError(t, err)
	require.Empty(t, graph.Node)
	proof := &types.KnowledgeBuildInputProof{
		Version:         1,
		TenantID:        7,
		KnowledgeBaseID: namespace.KnowledgeBase,
		KnowledgeID:     namespace.Knowledge,
		AdmissionID:     "original-empty-model-input",
		BuildGeneration: 1,
		InputDigest:     types.GraphDigest("empty-source-input"),
		ConfigDigest:    types.GraphDigest(template),
		BodyDigest:      types.GraphDigest("actual source that extracted no nodes"),
		ChunkIDs:        []string{"source-chunk"},
		SourceLineage:   lineage,
	}
	graph.SourceReceipt, err = types.BindGraphSourceReceipt(
		&types.GraphSourceReceipt{
			OriginalInput:   proof,
			Version:         1,
			TenantID:        7,
			KnowledgeBaseID: namespace.KnowledgeBase,
			KnowledgeID:     namespace.Knowledge,
			BuildGeneration: 1,
			InputDigest:     proof.InputDigest,
			ConfigDigest:    proof.ConfigDigest,
			WireDigest:      graph.WireDigest,
			SourceLineage:   lineage,
		},
		types.GraphDataBodyDigest(namespace, graph),
	)
	require.NoError(t, err)
	ctx = types.WithGraphWriteAuthorization(ctx, func(context.Context, *types.GraphSourceReceipt) error { return nil })
	require.NoError(t, repo.AddGraph(ctx, namespace, []*types.GraphData{graph}))
	read, err := repo.SearchNode(ctx, namespace, []string{"no matching entity"})
	require.NoError(t, err)
	require.Empty(t, read.Node)
	require.Empty(t, read.Relation)
	require.Equal(t, lineage, read.SourceLineage)
	require.Len(t, read.InputReceipts, 1)
}

func TestKnowledgeGraphOrphanChildrenCannotOutliveTheirInputReceipt(t *testing.T) {
	ctx, driver, repo, namespace := neoGraphFixture(t)
	empty, _ := types.NewCompleteSourceLineage()
	graph := extractGraph(
		ctx,
		t,
		namespace,
		empty,
		empty,
		"chunk",
		func(context.Context, *types.SourceLineage) error { return nil },
	)
	ctx = types.WithGraphWriteAuthorization(ctx, func(context.Context, *types.GraphSourceReceipt) error { return nil })
	require.NoError(t, repo.AddGraph(ctx, namespace, []*types.GraphData{graph}))
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer func() { require.NoError(t, session.Close(ctx)) }()
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(
			ctx,
			`MATCH (g:WEKNORA_GRAPH_INPUT {kb:$kb}) DELETE g`,
			map[string]any{"kb": namespace.KnowledgeBase},
		)
		if err != nil {
			return nil, err
		}
		_, err = result.Consume(ctx)
		return nil, err
	})
	require.NoError(t, err)
	read, err := repo.SearchNode(ctx, namespace, []string{"shared"})
	require.ErrorIs(t, err, types.ErrGraphSourceReceiptUnknown)
	require.Nil(t, read)
}
