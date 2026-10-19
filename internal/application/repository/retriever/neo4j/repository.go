package neo4j

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

// Neo4jRepository represents neo4j repository repository data.
// Records are immutable extraction units. Collapsing equal display names is a
// read projection, never a write that can erase an older generation's proof.
type Neo4jRepository struct {
	driver     neo4j.Driver
	nodePrefix string
}

func NewNeo4jRepository(driver neo4j.Driver) interfaces.RetrieveGraphRepository {
	return &Neo4jRepository{driver: driver, nodePrefix: "ENTITY"}
}
func _removeHyphen(s string) string { return strings.ReplaceAll(s, "-", "_") }
func (n *Neo4jRepository) Labels(namespace types.NameSpace) []string {
	labels := []string{}
	for _, label := range namespace.Labels() {
		labels = append(labels, n.nodePrefix+_removeHyphen(label))
	}
	return labels
}

func (n *Neo4jRepository) Label(namespace types.NameSpace) string {
	labels := n.Labels(namespace)
	for i := range labels {
		labels[i] = "`" + strings.ReplaceAll(labels[i], "`", "``") + "`"
	}
	return strings.Join(labels, ":")
}

func (n *Neo4jRepository) AddGraph(
	ctx context.Context,
	namespace types.NameSpace,
	graphs []*types.GraphData,
) (err error) {
	if n.driver == nil || namespace.KnowledgeBase == "" || namespace.Knowledge == "" {
		return types.ErrGraphSourceReceiptUnknown
	}
	var nodeRows, edgeRows, controlRows []map[string]any
	receipts := []*types.GraphSourceReceipt{}
	for _, graph := range graphs {
		allInputs, err := types.ValidateGraphMaterial(graph)
		if err != nil {
			return err
		}
		if err := types.CheckGraphDataReceipt(namespace, graph); err != nil {
			return err
		}
		if types.GraphDigest(graph.SourceReceipt.SourceLineage) != types.GraphDigest(allInputs) {
			return types.ErrGraphSourceReceiptUnknown
		}
		controlReceipt, err := types.EncodeGraphSourceReceipt(graph.SourceReceipt)
		if err != nil {
			return err
		}
		controlKey := types.GraphDigest(controlReceipt)
		graphBody, err := json.Marshal(graph)
		if err != nil {
			return err
		}
		controlRows = append(controlRows, map[string]any{
			"key": controlKey, "kg": namespace.Knowledge, "kb": namespace.KnowledgeBase,
			"receipt": controlReceipt, "body": string(graphBody),
		})
		receipts = append(receipts, graph.SourceReceipt)
		keys := map[string]string{}
		for _, node := range graph.Node {
			if err := types.CheckGraphNodeReceipt(namespace, node); err != nil {
				return err
			}
			if types.GraphDigest(node.SourceReceipt.SourceLineage) != types.GraphDigest(allInputs) {
				return types.ErrGraphSourceReceiptUnknown
			}
			if _, duplicate := keys[node.Name]; duplicate {
				return types.ErrGraphSourceReceiptUnknown
			}
			encoded, err := types.EncodeGraphSourceReceipt(node.SourceReceipt)
			if err != nil {
				return err
			}
			key := types.GraphDigest(encoded)
			keys[node.Name] = key
			nodeRows = append(nodeRows, map[string]any{
				"input_key": controlKey, "key": key, "name": node.Name,
				"kg": namespace.Knowledge, "kb": namespace.KnowledgeBase,
				"chunks": node.Chunks, "attributes": node.Attributes, "receipt": encoded,
			})
			receipts = append(receipts, node.SourceReceipt)
		}
		for _, edge := range graph.Relation {
			if err := types.CheckGraphRelationReceipt(namespace, edge); err != nil {
				return err
			}
			if types.GraphDigest(edge.SourceReceipt.SourceLineage) != types.GraphDigest(allInputs) {
				return types.ErrGraphSourceReceiptUnknown
			}
			source, sourceOK := keys[edge.Node1]
			target, targetOK := keys[edge.Node2]
			if !sourceOK || !targetOK {
				return types.ErrGraphSourceReceiptUnknown
			}
			encoded, err := types.EncodeGraphSourceReceipt(edge.SourceReceipt)
			if err != nil {
				return err
			}
			edgeRows = append(edgeRows, map[string]any{
				"input_key": controlKey, "key": types.GraphDigest(encoded), "source": source, "target": target,
				"kind": edge.Type, "receipt": encoded, "kg": namespace.Knowledge, "kb": namespace.KnowledgeBase,
			})
			receipts = append(receipts, edge.SourceReceipt)
		}
	}
	for _, receipt := range receipts {
		if err := types.CheckGraphWriteAuthorization(ctx, receipt); err != nil {
			return err
		}
	}
	session := n.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer func() { err = errors.Join(err, session.Close(ctx)) }()
	_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		for _, receipt := range receipts {
			if err := types.CheckGraphWriteAuthorization(ctx, receipt); err != nil {
				return nil, err
			}
		}
		controls, err := tx.Run(ctx, `UNWIND $data AS row
			MERGE (g:WEKNORA_GRAPH_INPUT {receipt_key:row.key})
			ON CREATE SET g.kb=row.kb,g.kg=row.kg,g._source_receipt=row.receipt,g.body=row.body RETURN g`,
			map[string]any{"data": controlRows})
		if err != nil {
			return nil, err
		}
		count := 0
		for controls.Next(ctx) {
			v, _ := controls.Record().Get("g")
			g, ok := v.(neo4j.Node)
			if !ok {
				return nil, types.ErrGraphSourceReceiptUnknown
			}
			if _, err := decodeControl(g); err != nil {
				return nil, err
			}
			count++
		}
		if err := controls.Err(); err != nil {
			return nil, err
		}
		if count != len(controlRows) {
			return nil, types.ErrGraphSourceReceiptUnknown
		}
		nodes, err := tx.Run(ctx, `UNWIND $data AS row MERGE (n:`+n.Label(namespace)+` {receipt_key:row.key})
			ON CREATE SET n.name=row.name,n.kg=row.kg,n.kb=row.kb,n.chunks=row.chunks,
			 n.attributes=row.attributes,n._source_receipt=row.receipt,n._input_key=row.input_key RETURN n`,
			map[string]any{"data": nodeRows})
		if err != nil {
			return nil, err
		}
		count = 0
		for nodes.Next(ctx) {
			value, _ := nodes.Record().Get("n")
			raw, ok := value.(neo4j.Node)
			if !ok {
				return nil, types.ErrGraphSourceReceiptUnknown
			}
			if _, err := decodeNode(raw); err != nil {
				return nil, err
			}
			count++
		}
		if err := nodes.Err(); err != nil {
			return nil, err
		}
		if count != len(nodeRows) {
			return nil, types.ErrGraphSourceReceiptUnknown
		}
		edges, err := tx.Run(ctx, `UNWIND $data AS row
			MATCH (s:`+n.Label(namespace)+` {receipt_key:row.source}),
			 (t:`+n.Label(namespace)+` {receipt_key:row.target})
			MERGE (s)-[r:WEKNORA_REL {receipt_key:row.key}]->(t)
			ON CREATE SET r.kind=row.kind,r.kg=row.kg,r.kb=row.kb,
			 r._source_receipt=row.receipt,r._input_key=row.input_key RETURN r,s,t`, map[string]any{"data": edgeRows})
		if err != nil {
			return nil, err
		}
		count = 0
		for edges.Next(ctx) {
			record := edges.Record()
			rv, _ := record.Get("r")
			sv, _ := record.Get("s")
			tv, _ := record.Get("t")
			r, rok := rv.(neo4j.Relationship)
			s, sok := sv.(neo4j.Node)
			t, tok := tv.(neo4j.Node)
			if !rok || !sok || !tok {
				return nil, types.ErrGraphSourceReceiptUnknown
			}
			if _, err := decodeEdge(r, s, t); err != nil {
				return nil, err
			}
			count++
		}
		if err := edges.Err(); err != nil {
			return nil, err
		}
		if count != len(edgeRows) {
			return nil, types.ErrGraphSourceReceiptUnknown
		}
		// A denial here rolls back the whole batch, including nodes and relations.
		for _, receipt := range receipts {
			if err := types.CheckGraphWriteAuthorization(ctx, receipt); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	return err
}

func (n *Neo4jRepository) DelGraph(ctx context.Context, namespaces []types.NameSpace) error {
	if n.driver == nil {
		return nil
	}
	session := n.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		for _, namespace := range namespaces {
			if namespace.KnowledgeBase == "" {
				return nil, types.ErrGraphSourceReceiptUnknown
			}
			result, err := tx.Run(ctx, `MATCH (n:`+n.Label(namespace)+`) WHERE $kg='' OR n.kg=$kg DETACH DELETE n`,
				map[string]any{"kg": namespace.Knowledge})
			if err != nil {
				return nil, err
			}
			if _, err := result.Consume(ctx); err != nil {
				return nil, err
			}
		}
		for _, namespace := range namespaces {
			result, err := tx.Run(ctx, `MATCH (g:WEKNORA_GRAPH_INPUT {kb:$kb}) WHERE $kg='' OR g.kg=$kg DELETE g`,
				map[string]any{"kb": namespace.KnowledgeBase, "kg": namespace.Knowledge})
			if err != nil {
				return nil, err
			}
			if _, err := result.Consume(ctx); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	return err
}

const graphReadLimit = 4096

func (n *Neo4jRepository) SearchNode(
	ctx context.Context, namespace types.NameSpace, names []string,
) (*types.GraphData, error) {
	if n.driver == nil || namespace.KnowledgeBase == "" {
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	session := n.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)
	value, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		empty, _ := types.NewCompleteSourceLineage()
		graph := &types.GraphData{SourceLineage: empty}
		controlKeys := map[string]*types.GraphSourceReceipt{}
		controls, err := tx.Run(ctx, `MATCH (g:WEKNORA_GRAPH_INPUT {kb:$kb})
			WHERE $kg='' OR g.kg=$kg RETURN g LIMIT $limit`,
			map[string]any{"kb": namespace.KnowledgeBase, "kg": namespace.Knowledge, "limit": graphReadLimit + 1})
		if err != nil {
			return nil, err
		}
		controlCount := 0
		for controls.Next(ctx) {
			controlCount++
			if controlCount > graphReadLimit {
				return nil, types.ErrSourceLineageLimit
			}
			v, _ := controls.Record().Get("g")
			raw, ok := v.(neo4j.Node)
			if !ok {
				return nil, types.ErrGraphSourceReceiptUnknown
			}
			receipt, err := decodeControl(raw)
			if err != nil {
				return nil, err
			}
			key, err := stringProperty(raw.Props, "receipt_key")
			if err != nil {
				return nil, err
			}
			controlKeys[key] = receipt
			graph.InputReceipts = append(graph.InputReceipts, receipt)
			graph.SourceLineage, err = types.UnionSourceLineage(graph.SourceLineage, receipt.SourceLineage)
			if err != nil {
				return nil, err
			}
		}
		if err := controls.Err(); err != nil {
			return nil, err
		}
		result, err := tx.Run(ctx, `MATCH (n:`+n.Label(namespace)+`) WHERE ($kg='' OR n.kg=$kg)
			OPTIONAL MATCH (n)-[r]-(m:`+n.Label(namespace)+`)
			RETURN n,r,m,startNode(r) AS source,endNode(r) AS target,
			 ANY(text IN $names WHERE n.name CONTAINS text) AS matched LIMIT $limit`,
			map[string]any{"kg": namespace.Knowledge, "names": names, "limit": graphReadLimit + 1})
		if err != nil {
			return nil, err
		}
		nodeSeen := map[string]bool{}
		edgeSeen := map[string]bool{}
		count := 0
		addNode := func(raw neo4j.Node, matched bool) error {
			node, err := decodeNode(raw)
			if err != nil {
				return err
			}
			inputKey, err := stringProperty(raw.Props, "_input_key")
			if err != nil || !sameControlProof(controlKeys[inputKey], node.SourceReceipt) {
				return types.ErrGraphSourceReceiptUnknown
			}
			if node.SourceReceipt.KnowledgeBaseID != namespace.KnowledgeBase ||
				(namespace.Knowledge != "" && node.SourceReceipt.KnowledgeID != namespace.Knowledge) {
				return types.ErrGraphSourceReceiptUnknown
			}
			graph.InputReceipts = append(graph.InputReceipts, node.SourceReceipt)
			graph.SourceLineage, err = types.UnionSourceLineage(graph.SourceLineage, node.SourceReceipt.SourceLineage)
			if err != nil {
				return err
			}
			if matched && !nodeSeen[raw.ElementId] {
				nodeSeen[raw.ElementId] = true
				graph.Node = append(graph.Node, node)
			}
			return nil
		}
		for result.Next(ctx) {
			count++
			if count > graphReadLimit {
				return nil, types.ErrSourceLineageLimit
			}
			record := result.Record()
			v, _ := record.Get("n")
			raw, ok := v.(neo4j.Node)
			if !ok {
				return nil, types.ErrGraphSourceReceiptUnknown
			}
			matchValue, _ := record.Get("matched")
			matched, ok := matchValue.(bool)
			if !ok {
				return nil, types.ErrGraphSourceReceiptUnknown
			}
			if err := addNode(raw, matched); err != nil {
				return nil, err
			}
			rv, _ := record.Get("r")
			if rv == nil {
				continue
			}
			rel, ok := rv.(neo4j.Relationship)
			if !ok {
				return nil, types.ErrGraphSourceReceiptUnknown
			}
			sv, _ := record.Get("source")
			tv, _ := record.Get("target")
			source, sok := sv.(neo4j.Node)
			target, tok := tv.(neo4j.Node)
			if !sok || !tok {
				return nil, types.ErrGraphSourceReceiptUnknown
			}
			if err := addNode(source, matched); err != nil {
				return nil, err
			}
			if err := addNode(target, matched); err != nil {
				return nil, err
			}
			edge, err := decodeEdge(rel, source, target)
			if err != nil {
				return nil, err
			}
			inputKey, err := stringProperty(rel.Props, "_input_key")
			if err != nil || !sameControlProof(controlKeys[inputKey], edge.SourceReceipt) {
				return nil, types.ErrGraphSourceReceiptUnknown
			}
			graph.InputReceipts = append(graph.InputReceipts, edge.SourceReceipt)
			graph.SourceLineage, err = types.UnionSourceLineage(graph.SourceLineage, edge.SourceReceipt.SourceLineage)
			if err != nil {
				return nil, err
			}
			if matched && !edgeSeen[rel.ElementId] {
				edgeSeen[rel.ElementId] = true
				graph.Relation = append(graph.Relation, edge)
			}
		}
		if err := result.Err(); err != nil {
			return nil, err
		}
		if _, err := types.ValidateGraphMaterial(graph); err != nil {
			return nil, err
		}
		return graph, nil
	})
	if err != nil {
		return nil, err
	}
	graph, ok := value.(*types.GraphData)
	if !ok {
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	return graph, nil
}

func stringProperty(props map[string]any, key string) (string, error) {
	value, ok := props[key].(string)
	if !ok {
		return "", types.ErrGraphSourceReceiptUnknown
	}
	return value, nil
}

func stringListProperty(props map[string]any, key string) ([]string, error) {
	v, ok := props[key].([]any)
	if !ok {
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	values := make([]string, len(v))
	for i, item := range v {
		s, ok := item.(string)
		if !ok {
			return nil, types.ErrGraphSourceReceiptUnknown
		}
		values[i] = s
	}
	return values, nil
}

func decodeNode(raw neo4j.Node) (*types.GraphNode, error) {
	name, err := stringProperty(raw.Props, "name")
	if err != nil {
		return nil, err
	}
	chunks, err := stringListProperty(raw.Props, "chunks")
	if err != nil {
		return nil, err
	}
	attributes, err := stringListProperty(raw.Props, "attributes")
	if err != nil {
		return nil, err
	}
	encoded, err := stringProperty(raw.Props, "_source_receipt")
	if err != nil {
		return nil, err
	}
	receipt, err := types.DecodeGraphSourceReceipt(encoded)
	if err != nil {
		return nil, err
	}
	kb, err := stringProperty(raw.Props, "kb")
	if err != nil {
		return nil, err
	}
	kg, err := stringProperty(raw.Props, "kg")
	if err != nil {
		return nil, err
	}
	node := &types.GraphNode{Name: name, Chunks: chunks, Attributes: attributes, SourceReceipt: receipt}
	if err := types.CheckGraphNodeReceipt(types.NameSpace{KnowledgeBase: kb, Knowledge: kg}, node); err != nil {
		return nil, err
	}
	key, err := stringProperty(raw.Props, "receipt_key")
	if err != nil || key != types.GraphDigest(encoded) {
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	return node, nil
}

func decodeEdge(raw neo4j.Relationship, source, target neo4j.Node) (*types.GraphRelation, error) {
	if raw.Type != "WEKNORA_REL" {
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	name1, err := stringProperty(source.Props, "name")
	if err != nil {
		return nil, err
	}
	name2, err := stringProperty(target.Props, "name")
	if err != nil {
		return nil, err
	}
	kind, err := stringProperty(raw.Props, "kind")
	if err != nil {
		return nil, err
	}
	encoded, err := stringProperty(raw.Props, "_source_receipt")
	if err != nil {
		return nil, err
	}
	receipt, err := types.DecodeGraphSourceReceipt(encoded)
	if err != nil {
		return nil, err
	}
	kb, err := stringProperty(raw.Props, "kb")
	if err != nil {
		return nil, err
	}
	kg, err := stringProperty(raw.Props, "kg")
	if err != nil {
		return nil, err
	}
	edge := &types.GraphRelation{Node1: name1, Node2: name2, Type: kind, SourceReceipt: receipt}
	if err := types.CheckGraphRelationReceipt(types.NameSpace{KnowledgeBase: kb, Knowledge: kg}, edge); err != nil {
		return nil, err
	}
	for _, node := range []neo4j.Node{source, target} {
		if node.Props["kb"] != kb || node.Props["kg"] != kg {
			return nil, types.ErrGraphSourceReceiptUnknown
		}
	}
	key, err := stringProperty(raw.Props, "receipt_key")
	if err != nil || key != types.GraphDigest(encoded) {
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	return edge, nil
}

func listI2listS(list []any) []string {
	result := make([]string, len(list))
	for i, v := range list {
		result[i] = fmt.Sprintf("%v", v)
	}
	return result
}

func decodeControl(raw neo4j.Node) (*types.GraphSourceReceipt, error) {
	encoded, err := stringProperty(raw.Props, "_source_receipt")
	if err != nil {
		return nil, err
	}
	receipt, err := types.DecodeGraphSourceReceipt(encoded)
	if err != nil {
		return nil, err
	}
	kb, err := stringProperty(raw.Props, "kb")
	if err != nil {
		return nil, err
	}
	kg, err := stringProperty(raw.Props, "kg")
	if err != nil {
		return nil, err
	}
	body, err := stringProperty(raw.Props, "body")
	if err != nil {
		return nil, err
	}
	var graph types.GraphData
	if err := json.Unmarshal([]byte(body), &graph); err != nil {
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	graph.SourceReceipt = receipt
	if err := types.CheckGraphDataReceipt(types.NameSpace{KnowledgeBase: kb, Knowledge: kg}, &graph); err != nil {
		return nil, err
	}
	key, err := stringProperty(raw.Props, "receipt_key")
	if err != nil || key != types.GraphDigest(encoded) {
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	return receipt, nil
}

func sameControlProof(control, child *types.GraphSourceReceipt) bool {
	return control != nil && child != nil && control.TenantID == child.TenantID &&
		control.KnowledgeBaseID == child.KnowledgeBaseID && control.KnowledgeID == child.KnowledgeID &&
		control.BuildGeneration == child.BuildGeneration && control.InputDigest == child.InputDigest &&
		control.ConfigDigest == child.ConfigDigest && control.WireDigest == child.WireDigest &&
		types.GraphDigest(control.OriginalInput) == types.GraphDigest(child.OriginalInput) &&
		types.GraphDigest(control.SourceLineage) == types.GraphDigest(child.SourceLineage)
}
