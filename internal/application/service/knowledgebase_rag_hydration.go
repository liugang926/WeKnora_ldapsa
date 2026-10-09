package service

import (
	"context"
	"fmt"
	"sort"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/readlease"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// ConfigureKnowledgeBaseRAGChunkHydration is wired by trusted DI. A missing
// reader keeps result provenance unknown; it never certifies old text through
// a current knowledge-ID lookup.
func ConfigureKnowledgeBaseRAGChunkHydration(base interfaces.KnowledgeBaseService, reader interfaces.RAGChunkSnapshotReader, guard *access.NextcloudSourceLineageGuard) {
	if impl, ok := base.(*knowledgeBaseService); ok {
		impl.ragChunkSnapshots = reader
		impl.ragChunkLineageGuard = guard
	}
}

type ragChunkReadScopeKey struct{}

// WithRAGChunkReadScope carries only a selector resolved and authorized by a
// trusted entry point. It is reauthorized below, never accepted as a grant.
func WithRAGChunkReadScope(ctx context.Context, scope access.LineageReadScope) context.Context {
	return context.WithValue(ctx, ragChunkReadScopeKey{}, scope)
}

// HydrateRAGChunkAugmentations is the parent/neighbor read boundary. It returns
// fresh actual material while requiring the original document receipt. Old
// history results with no receipt remain unknown rather than being restamped.
func (s *knowledgeBaseService) HydrateRAGChunkAugmentations(ctx context.Context, original *types.SearchResult, ids []string) ([]types.RAGChunkInputSnapshot, error) {
	if original == nil || original.SourceLineage.RequireComplete() != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	if s.ragChunkSnapshots == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	knowledgeMap, err := s.fetchKnowledgeDataWithShared(ctx, types.MustTenantIDFromContext(ctx), []string{original.KnowledgeID})
	if err != nil {
		return nil, err
	}
	knowledge := knowledgeMap[original.KnowledgeID]
	if knowledge == nil || knowledge.KnowledgeBaseID != original.KnowledgeBaseID {
		return nil, access.ErrNextcloudPublicationDenied
	}
	scopes := []readlease.NextcloudKBReadScope{{TenantID: knowledge.TenantID, KBID: knowledge.KnowledgeBaseID}}
	guard := readlease.NextcloudReadGuardFromContext(ctx)
	if owned, turn := ragTurnPhysicalReadGuard(ctx, scopes); turn {
		if guard != nil && owned != guard {
			ctx = guard.ParentContext(ctx)
		}
		guard = owned
	}
	if guard == nil || !guard.Covers(scopes) {
		guard, err = readlease.BeginNextcloudKBRead(ctx, s.contentLeases, scopes)
		if err != nil {
			return nil, err
		}
		held, holdErr := holdRAGPhysicalReadGuard(ctx, guard)
		if holdErr != nil {
			_ = guard.Close()
			return nil, holdErr
		}
		if !held {
			defer guard.Close()
		}
		ctx = guard.Context()
	}
	if err := guard.PinKnowledge(knowledge); err != nil {
		return nil, err
	}
	lease, err := guard.ExactLease(knowledge)
	if err != nil {
		return nil, err
	}
	rows, err := s.ragChunkSnapshots.HydrateRAGChunkSnapshots(ctx, []types.RAGChunkReadRequest{{TenantID: knowledge.TenantID,
		KnowledgeBaseID: knowledge.KnowledgeBaseID, KnowledgeID: knowledge.ID, ChunkIDs: ids, Lease: lease, ExpectedLineage: original.SourceLineage}})
	if err != nil {
		return nil, err
	}
	if err := s.accountRAGInputSnapshots(ctx, rows); err != nil {
		return nil, err
	}
	if err := guard.Verify(); err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *knowledgeBaseService) accountRAGInputSnapshots(ctx context.Context, snapshots []types.RAGChunkInputSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}
	var inputs []*types.SourceLineage
	scope, _ := ctx.Value(ragChunkReadScopeKey{}).(access.LineageReadScope)
	for _, snapshot := range snapshots {
		inputs = append(inputs, snapshot.SourceLineage)
		if snapshot.Knowledge != nil {
			scope.Targets = append(scope.Targets, &types.SearchTarget{Type: types.SearchTargetTypeKnowledgeBase,
				TenantID: snapshot.Knowledge.TenantID, KnowledgeBaseID: snapshot.Knowledge.KnowledgeBaseID})
		}
	}
	union, err := types.UnionSourceLineage(inputs...)
	if err != nil {
		return err
	}
	if turn := types.RAGSourceLineageFromContext(ctx); turn != nil {
		if err := turn.Add(union); err != nil {
			return err
		}
		_, err := turn.CheckedSnapshot(ctx)
		return err
	}
	if s.ragChunkLineageGuard == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	return s.ragChunkLineageGuard.Check(ctx, union, scope)
}

// hydrateRAGSearchChunks takes only scoped row IDs. The knowledge map supplies
// already-authorized server scope hints, whose material is replaced by the
// coherent snapshot's actual descriptor. Expected lineages are original
// receipts for augmentations, not newly inferred from an old cached result.
func (s *knowledgeBaseService) hydrateRAGSearchChunks(ctx context.Context, byKnowledge map[string][]string,
	knowledgeMap map[string]*types.Knowledge, expected map[string]*types.SourceLineage,
) ([]*types.Chunk, map[string]*types.SourceLineage, error) {
	lineages := map[string]*types.SourceLineage{}
	if s.ragChunkSnapshots == nil {
		var ids []string
		for _, chunkIDs := range byKnowledge {
			ids = append(ids, chunkIDs...)
		}
		rows, err := s.listChunksByIDWithShared(ctx, types.MustTenantIDFromContext(ctx), ids)
		for _, row := range rows {
			lineages[row.ID] = types.UnknownSourceLineage()
		}
		if turn := types.RAGSourceLineageFromContext(ctx); turn != nil && len(rows) > 0 {
			if err := turn.Add(types.UnknownSourceLineage()); err != nil {
				return nil, nil, err
			}
			if _, err := turn.CheckedSnapshot(ctx); err != nil {
				return nil, nil, err
			}
		}
		return rows, lineages, err
	}
	var requests []repository.RAGChunkReadRequest
	guard := readlease.NextcloudReadGuardFromContext(ctx)
	keys := make([]string, 0, len(byKnowledge))
	for id := range byKnowledge {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	for _, id := range keys {
		knowledge := knowledgeMap[id]
		if knowledge == nil {
			continue
		} // Unavailable/ungranted index hits carry no material.
		if err := guard.PinKnowledge(knowledge); err != nil {
			return nil, nil, err
		}
		lease, err := guard.ExactLease(knowledge)
		if err != nil {
			return nil, nil, err
		}
		requests = append(requests, repository.RAGChunkReadRequest{TenantID: knowledge.TenantID,
			KnowledgeBaseID: knowledge.KnowledgeBaseID, KnowledgeID: knowledge.ID, ChunkIDs: byKnowledge[id], Lease: lease, ExpectedLineage: expected[id]})
	}
	snapshots, err := s.ragChunkSnapshots.HydrateRAGChunkSnapshots(ctx, requests)
	if err != nil {
		return nil, nil, err
	}
	// Metadata/progress and rerank can precede final prompt assembly. Account
	// and authorize the actual captured material before it becomes a result.
	if err := s.accountRAGInputSnapshots(ctx, snapshots); err != nil {
		return nil, nil, err
	}
	var rows []*types.Chunk
	for _, snapshot := range snapshots {
		if snapshot.Knowledge == nil || snapshot.Chunk == nil || snapshot.Chunk.KnowledgeID != snapshot.Knowledge.ID ||
			snapshot.Chunk.TenantID != snapshot.Knowledge.TenantID || snapshot.Chunk.KnowledgeBaseID != snapshot.Knowledge.KnowledgeBaseID {
			return nil, nil, fmt.Errorf("RAG hydration returned an invalid scope")
		}
		knowledgeMap[snapshot.Knowledge.ID] = snapshot.Knowledge
		rows = append(rows, snapshot.Chunk)
		lineages[snapshot.Chunk.ID] = snapshot.SourceLineage
	}
	if err := guard.Verify(); err != nil {
		return nil, nil, err
	}
	return rows, lineages, nil
}

func ragPrimaryChunkScopes(chunks []*types.IndexWithScore) map[string][]string {
	byKnowledge := map[string][]string{}
	seen := map[string]bool{}
	for _, chunk := range chunks {
		if chunk == nil || seen[chunk.ChunkID] {
			continue
		}
		seen[chunk.ChunkID] = true
		byKnowledge[chunk.KnowledgeID] = append(byKnowledge[chunk.KnowledgeID], chunk.ChunkID)
	}
	return byKnowledge
}

// Enrichment references remain constrained to their originating document.
// A reference to another owner/document is rejected by the snapshot reader;
// a chunk handle is not permission to pivot to that document's text.
func ragEnrichmentChunkScopes(chunks []*types.Chunk, wanted []string) map[string][]string {
	wants := map[string]bool{}
	for _, id := range wanted {
		wants[id] = true
	}
	byKnowledge := map[string][]string{}
	seen := map[string]bool{}
	add := func(knowledge, id string) {
		if wants[id] && !seen[id] {
			byKnowledge[knowledge] = append(byKnowledge[knowledge], id)
			seen[id] = true
		}
	}
	for _, chunk := range chunks {
		add(chunk.KnowledgeID, chunk.ParentChunkID)
		add(chunk.KnowledgeID, chunk.PreChunkID)
		add(chunk.KnowledgeID, chunk.NextChunkID)
		for _, id := range (&knowledgeBaseService{}).collectRelatedChunkIDs(chunk, map[string]bool{}) {
			add(chunk.KnowledgeID, id)
		}
	}
	return byKnowledge
}

func ragKnowledgeReceiptMap(chunks []*types.Chunk, lineages map[string]*types.SourceLineage) map[string]*types.SourceLineage {
	byKnowledge := map[string]*types.SourceLineage{}
	for _, chunk := range chunks {
		prior, found := byKnowledge[chunk.KnowledgeID]
		if !found {
			byKnowledge[chunk.KnowledgeID] = lineages[chunk.ID]
			continue
		}
		union, err := types.UnionSourceLineage(prior, lineages[chunk.ID])
		if err != nil {
			union = types.UnknownSourceLineage()
		}
		byKnowledge[chunk.KnowledgeID] = union
	}
	return byKnowledge
}
