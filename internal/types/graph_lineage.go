package types

import (
	"context"
	"encoding/json"
)

// KnowledgeGraphInputSnapshot is internal material, never hydrated from client
// JSON. Graph keeps every raw contributor until its receipts are checked.
type KnowledgeGraphInputSnapshot struct {
	Recheck       func(context.Context) error `json:"-"`
	Close         func() error                `json:"-"`
	Graph         *GraphData                  `json:"-"`
	Chunks        []*SearchResult             `json:"-"`
	SourceLineage *SourceLineage              `json:"-"`
}

// CloneKnowledgeGraphInputSnapshot detaches every actual field before an
// authorization callback or renderer can touch shared producer-owned values.
func CloneKnowledgeGraphInputSnapshot(input *KnowledgeGraphInputSnapshot) (*KnowledgeGraphInputSnapshot, error) {
	if input == nil || input.Graph == nil || input.SourceLineage.RequireComplete() != nil {
		return nil, ErrGraphSourceReceiptUnknown
	}
	out := &KnowledgeGraphInputSnapshot{Recheck: input.Recheck, Close: input.Close}
	var err error
	out.SourceLineage, err = UnionSourceLineage(input.SourceLineage)
	if err != nil {
		return nil, err
	}
	graphCopy := *input.Graph
	out.Graph = &graphCopy
	out.Graph.Node = make([]*GraphNode, len(input.Graph.Node))
	for i, node := range input.Graph.Node {
		if node == nil {
			return nil, ErrGraphSourceReceiptUnknown
		}
		copy := *node
		if node.Chunks != nil {
			copy.Chunks = append([]string{}, node.Chunks...)
		}
		if node.Attributes != nil {
			copy.Attributes = append([]string{}, node.Attributes...)
		}
		out.Graph.Node[i] = &copy
	}
	out.Graph.Relation = make([]*GraphRelation, len(input.Graph.Relation))
	for i, edge := range input.Graph.Relation {
		if edge == nil {
			return nil, ErrGraphSourceReceiptUnknown
		}
		copy := *edge
		out.Graph.Relation[i] = &copy
	}
	out.Graph.InputReceipts = nil
	out.Graph.SourceLineage, err = UnionSourceLineage(input.Graph.SourceLineage)
	if err != nil {
		return nil, err
	}
	out.Graph.WireDigest = input.Graph.WireDigest
	if input.Graph.SourceReceipt != nil {
		out.Graph.SourceReceipt, err = CopyGraphSourceReceipt(input.Graph.SourceReceipt)
		if err != nil {
			return nil, err
		}
	}
	for i, node := range input.Graph.Node {
		if node == nil {
			return nil, ErrGraphSourceReceiptUnknown
		}
		out.Graph.Node[i].SourceReceipt, err = CopyGraphSourceReceipt(node.SourceReceipt)
		if err != nil {
			return nil, err
		}
	}
	for i, edge := range input.Graph.Relation {
		if edge == nil {
			return nil, ErrGraphSourceReceiptUnknown
		}
		out.Graph.Relation[i].SourceReceipt, err = CopyGraphSourceReceipt(edge.SourceReceipt)
		if err != nil {
			return nil, err
		}
	}
	for _, receipt := range input.Graph.InputReceipts {
		copy, err := CopyGraphSourceReceipt(receipt)
		if err != nil {
			return nil, err
		}
		out.Graph.InputReceipts = append(out.Graph.InputReceipts, copy)
	}
	data, err := json.Marshal(input.Chunks)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &out.Chunks); err != nil {
		return nil, err
	}
	for i, result := range input.Chunks {
		if result == nil {
			return nil, ErrGraphSourceReceiptUnknown
		}
		out.Chunks[i].SourceLineage, err = UnionSourceLineage(result.SourceLineage)
		if err != nil {
			return nil, err
		}
	}
	if _, err := ValidateGraphMaterial(out.Graph); err != nil {
		return nil, err
	}
	return out, nil
}
