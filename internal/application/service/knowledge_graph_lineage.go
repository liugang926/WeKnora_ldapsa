package service

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/readlease"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type graphReadLeaseProvider interface {
	BeginNextcloudRead(context.Context, types.SearchTargets) (*readlease.NextcloudReadGuard, error)
	CheckNextcloudReadTargets(context.Context, types.SearchTargets) error
}

// GraphOriginalInputVerifier compares the persisted original generation to its
// trusted writer ledger. It must not resolve current chunks to mint old proof.
type GraphOriginalInputVerifier interface {
	RecheckGraphOriginalInputs(context.Context, *types.GraphSourceReceipt) error
}

// KnowledgeGraphLineageReader hydrates graph material with its original input receipts.
type KnowledgeGraphLineageReader struct {
	knowledge interfaces.KnowledgeService
	bases     interfaces.KnowledgeBaseService
	graph     interfaces.RetrieveGraphRepository
	chunks    interfaces.RAGChunkSnapshotReader
	originals GraphOriginalInputVerifier
}

// NewKnowledgeGraphLineageReader constructs the configured KnowledgeGraphLineageReader.
func NewKnowledgeGraphLineageReader(
	knowledge interfaces.KnowledgeService,
	bases interfaces.KnowledgeBaseService,
	graph interfaces.RetrieveGraphRepository,
	chunks interfaces.RAGChunkSnapshotReader,
	originals GraphOriginalInputVerifier,
) *KnowledgeGraphLineageReader {
	return &KnowledgeGraphLineageReader{
		knowledge: knowledge,
		bases:     bases,
		graph:     graph,
		chunks:    chunks,
		originals: originals,
	}
}

// ConfigureKnowledgeBaseGraphLineage configures the knowledge base graph lineage boundary.
func ConfigureKnowledgeBaseGraphLineage(base interfaces.KnowledgeBaseService, reader *KnowledgeGraphLineageReader) {
	if impl, ok := base.(*knowledgeBaseService); ok {
		impl.graphLineageReader = reader
	}
}

func (s *knowledgeBaseService) ReadKnowledgeGraphSnapshot(
	ctx context.Context,
	targets types.SearchTargets,
	namespace types.NameSpace,
	names []string,
) (*types.KnowledgeGraphInputSnapshot, error) {
	if s.graphLineageReader == nil {
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	return s.graphLineageReader.ReadKnowledgeGraphSnapshot(ctx, targets, namespace, names)
}

// ReadKnowledgeGraphSnapshot hydrates graph material and retains its original input controls.
func (r *KnowledgeGraphLineageReader) ReadKnowledgeGraphSnapshot(
	ctx context.Context,
	targets types.SearchTargets,
	namespace types.NameSpace,
	names []string,
) (out *types.KnowledgeGraphInputSnapshot, err error) {
	turn := types.RAGSourceLineageFromContext(ctx)
	defer func() {
		if err != nil && turn != nil {
			_ = turn.Add(types.UnknownSourceLineage())
		}
	}()
	provider, ok := r.knowledge.(graphReadLeaseProvider)
	if !ok || r.bases == nil || r.graph == nil || r.chunks == nil || r.originals == nil || turn == nil ||
		namespace.KnowledgeBase == "" ||
		len(names) == 0 {
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	if err := turn.Check(ctx); err != nil {
		return nil, err
	}
	kb, err := r.bases.GetKnowledgeBaseByIDOnly(ctx, namespace.KnowledgeBase)
	if err != nil || kb == nil || kb.ID != namespace.KnowledgeBase || kb.TenantID != targets.GetTenantIDForKB(kb.ID) {
		return nil, access.ErrNextcloudPublicationDenied
	}
	namespaces, err := r.namespaces(ctx, targets, namespace, kb.TenantID)
	if err != nil {
		return nil, err
	}
	scopes := types.SearchTargets{
		&types.SearchTarget{Type: types.SearchTargetTypeKnowledgeBase, TenantID: kb.TenantID, KnowledgeBaseID: kb.ID},
	}
	ownedGuard := false
	completed := false
	guard := readlease.NextcloudReadGuardFromContext(ctx)
	if guard == nil || !guard.Covers([]readlease.NextcloudKBReadScope{{TenantID: kb.TenantID, KBID: kb.ID}}) {
		guard, err = provider.BeginNextcloudRead(ctx, scopes)
		if err != nil {
			return nil, err
		}
		ownedGuard = true
		defer func() {
			if !completed {
				_ = guard.Close()
			}
		}()
		ctx = guard.Context()
	}
	empty, _ := types.NewCompleteSourceLineage()
	graph := &types.GraphData{SourceLineage: empty}
	for _, scope := range namespaces {
		material, err := r.graph.SearchNode(ctx, scope, names)
		if err != nil {
			return nil, err
		}
		union, err := types.ValidateGraphMaterial(material)
		if err != nil {
			return nil, err
		}
		graph.SourceLineage, err = types.UnionSourceLineage(graph.SourceLineage, union)
		if err != nil {
			return nil, err
		}
		graph.Node = append(graph.Node, material.Node...)
		graph.Relation = append(graph.Relation, material.Relation...)
		graph.InputReceipts = append(graph.InputReceipts, material.InputReceipts...)
	}
	// Every row inspected by name predicates, every edge, and each endpoint is
	// rechecked before collapse. Older ordinary rows are covered as well as NC.
	receipts := append([]*types.GraphSourceReceipt{}, graph.InputReceipts...)
	for _, node := range graph.Node {
		receipts = append(receipts, node.SourceReceipt)
	}
	for _, edge := range graph.Relation {
		receipts = append(receipts, edge.SourceReceipt)
	}
	docs := map[string]*types.Knowledge{}
	expected := map[string]*types.SourceLineage{}
	for _, receipt := range receipts {
		if receipt.Validate() != nil || receipt.TenantID != kb.TenantID || receipt.KnowledgeBaseID != kb.ID {
			return nil, types.ErrGraphSourceReceiptUnknown
		}
		if err := r.originals.RecheckGraphOriginalInputs(ctx, receipt); err != nil {
			return nil, err
		}
		k, err := r.knowledge.GetKnowledgeByIDOnly(ctx, receipt.KnowledgeID)
		if err != nil || k == nil || k.TenantID != receipt.TenantID || k.KnowledgeBaseID != receipt.KnowledgeBaseID ||
			k.DeletedAt.Valid ||
			k.ParseStatus != types.ParseStatusCompleted ||
			k.EnableStatus != "enabled" {
			return nil, access.ErrNextcloudPublicationDenied
		}
		if allowed, err := r.allowed(ctx, targets, k.ID, k.KnowledgeBaseID); err != nil || !allowed {
			return nil, access.ErrNextcloudPublicationDenied
		}
		docs[k.ID] = k
		prior := expected[k.ID]
		if prior == nil {
			prior = empty
		}
		expected[k.ID], err = types.UnionSourceLineage(prior, receipt.SourceLineage)
		if err != nil {
			return nil, err
		}
	}
	if err := turn.Add(graph.SourceLineage); err != nil {
		return nil, err
	}
	if err := turn.Check(ctx); err != nil {
		return nil, err
	}
	// Keep all underlying controlled sources pinned, including extraction
	// examples/config dependencies from another document or effective tenant.
	for _, source := range graph.SourceLineage.Sources {
		scopes = append(
			scopes,
			&types.SearchTarget{
				Type:            types.SearchTargetTypeKnowledgeBase,
				TenantID:        source.TenantID,
				KnowledgeBaseID: source.KnowledgeBaseID,
			},
		)
	}
	expanded, err := provider.BeginNextcloudRead(ctx, scopes)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !completed {
			_ = expanded.Close()
		}
	}()
	ctx = expanded.Context()
	for _, source := range graph.SourceLineage.Sources {
		k, err := r.knowledge.GetKnowledgeByIDOnly(ctx, source.KnowledgeID)
		if err != nil || k == nil || k.TenantID != source.TenantID || k.KnowledgeBaseID != source.KnowledgeBaseID {
			return nil, access.ErrNextcloudPublicationDenied
		}
		if err := expanded.PinKnowledge(k); err != nil {
			return nil, err
		}
	}
	ids := map[string][]string{}
	seen := map[string]bool{}
	for _, node := range graph.Node {
		for _, id := range node.Chunks {
			if id == "" {
				return nil, types.ErrGraphSourceReceiptUnknown
			}
			if !seen[id] {
				seen[id] = true
				ids[node.SourceReceipt.KnowledgeID] = append(ids[node.SourceReceipt.KnowledgeID], id)
			}
		}
	}
	keys := []string{}
	for id := range ids {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	requests := []types.RAGChunkReadRequest{}
	for _, id := range keys {
		k := docs[id]
		if k == nil {
			return nil, types.ErrGraphSourceReceiptUnknown
		}
		if err := expanded.PinKnowledge(k); err != nil {
			return nil, err
		}
		lease, err := expanded.ExactLease(k)
		if err != nil {
			return nil, err
		}
		requests = append(
			requests,
			types.RAGChunkReadRequest{
				TenantID:        k.TenantID,
				KnowledgeBaseID: k.KnowledgeBaseID,
				KnowledgeID:     k.ID,
				ChunkIDs:        ids[id],
				Lease:           lease,
				ExpectedLineage: expected[id],
			},
		)
	}
	rows := []types.RAGChunkInputSnapshot{}
	if len(requests) > 0 {
		rows, err = r.chunks.HydrateRAGChunkSnapshots(ctx, requests)
		if err != nil {
			return nil, err
		}
	}
	if len(rows) != len(seen) {
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	results := []*types.SearchResult{}
	returned := map[string]bool{}
	for _, row := range rows {
		k, c := row.Knowledge, row.Chunk
		if k == nil || c == nil || !seen[c.ID] || returned[c.ID] || c.KnowledgeID != k.ID ||
			c.KnowledgeBaseID != k.KnowledgeBaseID ||
			c.TenantID != k.TenantID ||
			k.TenantID != kb.TenantID ||
			k.KnowledgeBaseID != kb.ID ||
			row.SourceLineage.RequireComplete() != nil {
			return nil, types.ErrGraphSourceReceiptUnknown
		}
		returned[c.ID] = true
		if c.ChunkType != types.ChunkTypeText ||
			(strings.TrimSpace(c.ImageInfo) != "" && strings.TrimSpace(c.ImageInfo) != "[]") ||
			(len(c.Metadata) > 0 && strings.TrimSpace(string(c.Metadata)) != "{}") {
			return nil, types.ErrGraphSourceReceiptUnknown
		}
		graph.SourceLineage, err = types.UnionSourceLineage(graph.SourceLineage, row.SourceLineage)
		if err != nil {
			return nil, err
		}
		if err := turn.Add(row.SourceLineage); err != nil {
			return nil, err
		}
		results = append(
			results,
			&types.SearchResult{
				ID:                c.ID,
				KnowledgeID:       k.ID,
				KnowledgeBaseID:   k.KnowledgeBaseID,
				KnowledgeTitle:    k.Title,
				KnowledgeFilename: k.FileName,
				KnowledgeSource:   k.Source,
				KnowledgeChannel:  k.Channel,
				Content:           c.Content,
				ContentRevision:   c.ContentRevision,
				ChunkType:         string(c.ChunkType),
				ChunkIndex:        c.ChunkIndex,
				StartAt:           c.StartAt,
				EndAt:             c.EndAt,
				MatchType:         types.MatchTypeGraph,
				Score:             1,
				SourceLineage:     graph.SourceLineage,
				Metadata:          map[string]string{},
			},
		)
	}
	for _, receipt := range receipts {
		if err := r.originals.RecheckGraphOriginalInputs(ctx, receipt); err != nil {
			return nil, err
		}
	}
	for _, k := range docs {
		if allowed, err := r.allowed(ctx, targets, k.ID, k.KnowledgeBaseID); err != nil || !allowed {
			return nil, access.ErrNextcloudPublicationDenied
		}
	}
	if err := provider.CheckNextcloudReadTargets(ctx, scopes); err != nil {
		return nil, err
	}
	if err := expanded.Verify(); err != nil {
		return nil, err
	}
	if err := guard.Verify(); err != nil {
		return nil, err
	}
	if err := turn.Check(ctx); err != nil {
		return nil, err
	}
	for _, result := range results {
		result.SourceLineage, err = types.UnionSourceLineage(graph.SourceLineage)
		if err != nil {
			return nil, err
		}
	}
	recheck := func(boundaryCtx context.Context) error {
		for _, receipt := range receipts {
			if err := r.originals.RecheckGraphOriginalInputs(boundaryCtx, receipt); err != nil {
				return err
			}
		}
		for _, k := range docs {
			allowed, err := r.allowed(boundaryCtx, targets, k.ID, k.KnowledgeBaseID)
			if err != nil || !allowed {
				return access.ErrNextcloudPublicationDenied
			}
		}
		if err := provider.CheckNextcloudReadTargets(boundaryCtx, scopes); err != nil {
			return err
		}
		if err := expanded.Verify(); err != nil {
			return err
		}
		if err := guard.Verify(); err != nil {
			return err
		}
		return turn.Check(boundaryCtx)
	}
	var once sync.Once
	var closeErr error
	closeRead := func() error {
		once.Do(func() {
			closeErr = expanded.Close()
			if ownedGuard {
				if err := guard.Close(); closeErr == nil {
					closeErr = err
				}
			}
		})
		return closeErr
	}
	completed = true
	return &types.KnowledgeGraphInputSnapshot{
		Graph:         graph,
		Chunks:        results,
		SourceLineage: graph.SourceLineage,
		Recheck:       recheck,
		Close:         closeRead,
	}, nil
}

func (r *KnowledgeGraphLineageReader) allowed(
	ctx context.Context,
	targets types.SearchTargets,
	knowledge, kb string,
) (bool, error) {
	for _, target := range targets {
		if target == nil || target.KnowledgeBaseID != kb {
			continue
		}
		if target.Type == types.SearchTargetTypeKnowledgeBase && len(target.TagIDs) == 0 &&
			len(target.ScopeTagIDs) == 0 {
			return true, nil
		}
		if slices.Contains(target.KnowledgeIDs, knowledge) {
			return true, nil
		}
		tags := append(append([]string{}, target.TagIDs...), target.ScopeTagIDs...)
		if len(tags) > 0 {
			rows, err := r.knowledge.GetKnowledgeTags(ctx, []string{knowledge})
			if err != nil {
				return false, err
			}
			for _, tag := range rows[knowledge] {
				if tag != nil && slices.Contains(tags, tag.ID) {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func (r *KnowledgeGraphLineageReader) namespaces(
	ctx context.Context,
	targets types.SearchTargets,
	wanted types.NameSpace,
	tenant uint64,
) ([]types.NameSpace, error) {
	whole := false
	ids := map[string]bool{}
	for _, target := range targets {
		if target == nil || target.KnowledgeBaseID != wanted.KnowledgeBase {
			continue
		}
		if target.TenantID != tenant {
			return nil, access.ErrNextcloudPublicationDenied
		}
		if target.Type == types.SearchTargetTypeKnowledgeBase && len(target.TagIDs) == 0 &&
			len(target.ScopeTagIDs) == 0 {
			whole = true
		}
		for _, id := range target.KnowledgeIDs {
			ids[id] = true
		}
		tags := append(append([]string{}, target.TagIDs...), target.ScopeTagIDs...)
		if len(tags) > 0 {
			matching, err := r.knowledge.ListKnowledgeIDsByTagIDs(ctx, tenant, wanted.KnowledgeBase, tags)
			if err != nil {
				return nil, err
			}
			for _, id := range matching {
				ids[id] = true
			}
		}
	}
	if wanted.Knowledge != "" {
		allowed, err := r.allowed(ctx, targets, wanted.Knowledge, wanted.KnowledgeBase)
		if err != nil || !allowed {
			return nil, access.ErrNextcloudPublicationDenied
		}
		return []types.NameSpace{wanted}, nil
	}
	if whole {
		return []types.NameSpace{wanted}, nil
	}
	if len(ids) > 4096 {
		return nil, types.ErrSourceLineageLimit
	}
	keys := []string{}
	for id := range ids {
		if id != "" {
			keys = append(keys, id)
		}
	}
	sort.Strings(keys)
	out := []types.NameSpace{}
	for _, id := range keys {
		out = append(out, types.NameSpace{KnowledgeBase: wanted.KnowledgeBase, Knowledge: id})
	}
	if len(out) == 0 && !targets.ContainsKB(wanted.KnowledgeBase) {
		return nil, fmt.Errorf("graph source scope is unavailable")
	}
	return out, nil
}
