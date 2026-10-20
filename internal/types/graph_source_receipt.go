package types

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
)

// GraphSourceReceipt is an immutable original writer receipt, persisted in a
// private graph property. Public graph JSON (including model output) cannot
// supply it. The digest binds the exact original body and namespace, not an
// endpoint chunk's current revision.
type GraphSourceReceipt struct {
	OriginalInput   *KnowledgeBuildInputProof `json:"original_input"`
	Version         int                       `json:"version"`
	TenantID        uint64                    `json:"tenant_id"`
	KnowledgeBaseID string                    `json:"knowledge_base_id"`
	KnowledgeID     string                    `json:"knowledge_id"`
	BuildGeneration uint64                    `json:"build_generation"`
	InputDigest     string                    `json:"input_digest"`
	ConfigDigest    string                    `json:"config_digest"`
	WireDigest      string                    `json:"wire_digest"`
	BodyDigest      string                    `json:"body_digest"`
	SourceLineage   *SourceLineage            `json:"source_lineage"`
}

// ErrGraphSourceReceiptUnknown reports missing or changed original graph material.
var ErrGraphSourceReceiptUnknown = errors.New("graph source receipt is unknown or changed")

// Validate checks the receipt envelope and its original input proof.
func (r *GraphSourceReceipt) Validate() error {
	if r == nil || r.Version != 1 || r.TenantID == 0 || r.KnowledgeBaseID == "" || r.KnowledgeID == "" ||
		r.BuildGeneration == 0 ||
		r.SourceLineage.RequireComplete() != nil {
		return ErrGraphSourceReceiptUnknown
	}
	if err := checkGraphOriginalInput(r); err != nil {
		return err
	}
	for _, value := range []string{r.InputDigest, r.ConfigDigest, r.WireDigest, r.BodyDigest} {
		decoded, err := hex.DecodeString(value)
		if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != value {
			return ErrGraphSourceReceiptUnknown
		}
	}
	return nil
}

// GraphDigest returns the SHA-256 digest of a canonical JSON value.
func GraphDigest(body any) string {
	payload, err := json.Marshal(body)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func graphBodyDigest(kind string, namespace NameSpace, body any) string {
	return GraphDigest(struct {
		Kind      string
		Namespace NameSpace
		Body      any
	}{kind, namespace, body})
}

// GraphNodeBodyDigest binds original node material to its namespace.
func GraphNodeBodyDigest(namespace NameSpace, node *GraphNode) string {
	if node == nil {
		return ""
	}
	return graphBodyDigest("node", namespace, struct {
		Name       string
		Chunks     []string
		Attributes []string
	}{node.Name, node.Chunks, node.Attributes})
}

// GraphRelationBodyDigest binds original relation material to its namespace.
func GraphRelationBodyDigest(namespace NameSpace, relation *GraphRelation) string {
	if relation == nil {
		return ""
	}
	return graphBodyDigest("relation", namespace, struct {
		Node1 string
		Node2 string
		Type  string
	}{relation.Node1, relation.Node2, relation.Type})
}

// BindGraphSourceReceipt copies an original receipt and binds its exact body digest.
func BindGraphSourceReceipt(base *GraphSourceReceipt, bodyDigest string) (*GraphSourceReceipt, error) {
	if base == nil {
		return nil, ErrGraphSourceReceiptUnknown
	}
	receipt := *base
	var err error
	receipt.SourceLineage, err = UnionSourceLineage(base.SourceLineage)
	if base.OriginalInput != nil {
		p := *base.OriginalInput
		p.ChunkIDs = append([]string(nil), p.ChunkIDs...)
		p.SourceLineage, err = UnionSourceLineage(p.SourceLineage)
		receipt.OriginalInput = &p
	}
	if err != nil {
		return nil, err
	}
	receipt.BodyDigest = bodyDigest
	if err := receipt.Validate(); err != nil {
		return nil, err
	}
	return &receipt, nil
}

// CheckGraphNodeReceipt checks a node against its original receipt and namespace.
func CheckGraphNodeReceipt(namespace NameSpace, node *GraphNode) error {
	if node == nil || node.SourceReceipt.Validate() != nil ||
		node.SourceReceipt.KnowledgeBaseID != namespace.KnowledgeBase ||
		node.SourceReceipt.KnowledgeID != namespace.Knowledge ||
		node.SourceReceipt.BodyDigest != GraphNodeBodyDigest(namespace, node) {
		return ErrGraphSourceReceiptUnknown
	}
	return nil
}

// CheckGraphRelationReceipt checks a relation against its original receipt and namespace.
func CheckGraphRelationReceipt(namespace NameSpace, relation *GraphRelation) error {
	if relation == nil || relation.SourceReceipt.Validate() != nil ||
		relation.SourceReceipt.KnowledgeBaseID != namespace.KnowledgeBase ||
		relation.SourceReceipt.KnowledgeID != namespace.Knowledge ||
		relation.SourceReceipt.BodyDigest != GraphRelationBodyDigest(namespace, relation) {
		return ErrGraphSourceReceiptUnknown
	}
	return nil
}

// EncodeGraphSourceReceipt serializes a valid bounded original receipt.
func EncodeGraphSourceReceipt(receipt *GraphSourceReceipt) (string, error) {
	if err := receipt.Validate(); err != nil {
		return "", err
	}
	b, err := json.Marshal(receipt)
	if err != nil || len(b) > SourceLineageMaxBytes {
		return "", ErrGraphSourceReceiptUnknown
	}
	return string(b), nil
}

// DecodeGraphSourceReceipt decodes a bounded original receipt and rejects ambiguous fields.
func DecodeGraphSourceReceipt(encoded string) (*GraphSourceReceipt, error) {
	if encoded == "" || len(encoded) > SourceLineageMaxBytes {
		return nil, ErrGraphSourceReceiptUnknown
	}
	// Detect duplicate keys at every depth, including aliases. Receipts only use
	// canonical ASCII keys and typed lineage has its own stricter decoder.
	dec := json.NewDecoder(bytes.NewBufferString(encoded))
	var walk func() error
	depth := 0
	walk = func() error {
		if depth > 16 {
			return ErrGraphSourceReceiptUnknown
		}
		depth++
		defer func() { depth-- }()
		token, err := dec.Token()
		if err != nil {
			return err
		}
		d, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch d {
		case '{':
			seen := map[string]bool{}
			for dec.More() {
				k, err := dec.Token()
				if err != nil {
					return err
				}
				s, ok := k.(string)
				if !ok || seen[s] {
					return ErrGraphSourceReceiptUnknown
				}
				if depth == 1 {
					switch s {
					case "version",
						"original_input",
						"tenant_id",
						"knowledge_base_id",
						"knowledge_id",
						"build_generation",
						"input_digest",
						"config_digest",
						"wire_digest",
						"body_digest",
						"source_lineage":
					default:
						return ErrGraphSourceReceiptUnknown
					}
				}
				if depth == 2 {
					switch s {
					case "version",
						"projection",
						"tenant_id",
						"knowledge_base_id",
						"knowledge_id",
						"admission_id",
						"build_generation",
						"input_digest",
						"config_digest",
						"body_digest",
						"chunk_ids",
						"source_lineage",
						"state",
						"sources":
					default:
						return ErrGraphSourceReceiptUnknown
					}
				}
				seen[s] = true
				if err := walk(); err != nil {
					return err
				}
			}
		case '[':
			for dec.More() {
				if err := walk(); err != nil {
					return err
				}
			}
		default:
			return ErrGraphSourceReceiptUnknown
		}
		_, err = dec.Token()
		return err
	}
	if err := walk(); err != nil {
		return nil, ErrGraphSourceReceiptUnknown
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, ErrGraphSourceReceiptUnknown
	}
	dec = json.NewDecoder(bytes.NewBufferString(encoded))
	dec.DisallowUnknownFields()
	var receipt GraphSourceReceipt
	if err := dec.Decode(&receipt); err != nil {
		return nil, ErrGraphSourceReceiptUnknown
	}
	if err := receipt.Validate(); err != nil {
		return nil, err
	}
	return &receipt, nil
}

// GraphWriteAuthorization is installed by the real build writer. It checks
// the original admitted input proof/lease again within the graph transaction.
// A background task JSON or model output never installs this callback.
type graphWriteAuthorizationKey struct{}

// WithGraphWriteAuthorization attaches the actual build writer's authorization checker.
func WithGraphWriteAuthorization(
	ctx context.Context,
	check func(context.Context, *GraphSourceReceipt) error,
) context.Context {
	return context.WithValue(ctx, graphWriteAuthorizationKey{}, check)
}

// CheckGraphWriteAuthorization rechecks a copied original receipt using the actual build writer.
func CheckGraphWriteAuthorization(ctx context.Context, receipt *GraphSourceReceipt) error {
	check, _ := ctx.Value(graphWriteAuthorizationKey{}).(func(context.Context, *GraphSourceReceipt) error)
	if check == nil || receipt.Validate() != nil {
		return ErrGraphSourceReceiptUnknown
	}
	original, err := BindGraphSourceReceipt(receipt, receipt.BodyDigest)
	if err != nil {
		return err
	}
	return check(ctx, original)
}

// ValidateGraphMaterial runs before any name-based collapse. Even a duplicate
// or non-rendered contributor retains its full original input union.
func ValidateGraphMaterial(graph *GraphData) (*SourceLineage, error) {
	if graph == nil || graph.SourceLineage.RequireComplete() != nil {
		return UnknownSourceLineage(), ErrGraphSourceReceiptUnknown
	}
	inputs := []*SourceLineage{graph.SourceLineage}
	for _, receipt := range graph.InputReceipts {
		if receipt.Validate() != nil {
			return UnknownSourceLineage(), ErrGraphSourceReceiptUnknown
		}
		inputs = append(inputs, receipt.SourceLineage)
	}
	for _, node := range graph.Node {
		if node == nil || node.SourceReceipt == nil {
			return UnknownSourceLineage(), ErrGraphSourceReceiptUnknown
		}
		namespace := NameSpace{
			KnowledgeBase: node.SourceReceipt.KnowledgeBaseID,
			Knowledge:     node.SourceReceipt.KnowledgeID,
		}
		if err := CheckGraphNodeReceipt(namespace, node); err != nil {
			return UnknownSourceLineage(), err
		}
		inputs = append(inputs, node.SourceReceipt.SourceLineage)
	}
	for _, edge := range graph.Relation {
		if edge == nil || edge.SourceReceipt == nil {
			return UnknownSourceLineage(), ErrGraphSourceReceiptUnknown
		}
		namespace := NameSpace{
			KnowledgeBase: edge.SourceReceipt.KnowledgeBaseID,
			Knowledge:     edge.SourceReceipt.KnowledgeID,
		}
		if err := CheckGraphRelationReceipt(namespace, edge); err != nil {
			return UnknownSourceLineage(), err
		}
		inputs = append(inputs, edge.SourceReceipt.SourceLineage)
	}
	union, err := UnionSourceLineage(inputs...)
	if err != nil || union.RequireComplete() != nil {
		return UnknownSourceLineage(), ErrGraphSourceReceiptUnknown
	}
	return union, nil
}

func checkGraphOriginalInput(r *GraphSourceReceipt) error {
	p := r.OriginalInput
	if p == nil || p.Version != 1 || p.AdmissionID == "" || p.TenantID != r.TenantID ||
		p.KnowledgeBaseID != r.KnowledgeBaseID ||
		p.KnowledgeID != r.KnowledgeID ||
		p.BuildGeneration != r.BuildGeneration ||
		p.InputDigest != r.InputDigest ||
		len(p.ChunkIDs) == 0 ||
		p.SourceLineage.RequireComplete() != nil {
		return ErrGraphSourceReceiptUnknown
	}
	for _, value := range []string{p.InputDigest, p.ConfigDigest, p.BodyDigest} {
		decoded, err := hex.DecodeString(value)
		if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != value {
			return ErrGraphSourceReceiptUnknown
		}
	}
	union, err := UnionSourceLineage(r.SourceLineage, p.SourceLineage)
	if err != nil || !sameRAGSourceLineage(union, r.SourceLineage) {
		return ErrGraphSourceReceiptUnknown
	}
	return nil
}

// CopyGraphSourceReceipt detaches a storage receipt before current callbacks.
func CopyGraphSourceReceipt(r *GraphSourceReceipt) (*GraphSourceReceipt, error) {
	encoded, err := EncodeGraphSourceReceipt(r)
	if err != nil {
		return nil, err
	}
	return DecodeGraphSourceReceipt(encoded)
}

// GraphDataBodyDigest binds original graph material to its namespace.
func GraphDataBodyDigest(namespace NameSpace, graph *GraphData) string {
	if graph == nil {
		return ""
	}
	return graphBodyDigest("graph", namespace, graph)
}

// CheckGraphDataReceipt checks a graph against its original receipt and namespace.
func CheckGraphDataReceipt(namespace NameSpace, graph *GraphData) error {
	if graph == nil || graph.SourceReceipt.Validate() != nil ||
		graph.SourceReceipt.KnowledgeBaseID != namespace.KnowledgeBase ||
		graph.SourceReceipt.KnowledgeID != namespace.Knowledge ||
		graph.SourceReceipt.BodyDigest != GraphDataBodyDigest(namespace, graph) {
		return ErrGraphSourceReceiptUnknown
	}
	return nil
}
