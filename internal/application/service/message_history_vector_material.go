package service

import (
	"context"
	"errors"
	"math"
	"sort"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/readlease"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/logprivacy"
	"github.com/Tencent/WeKnora/internal/types"
)

type historyVectorMaterialBridgeKey struct{}

type historyVectorOriginal struct {
	header    types.HistoryKnowledgeParentPlan
	guard     *MessageMaterialGuard
	cohort    []*types.Message
	assistant *types.Message
}

// These fields are filled only by actual original guards and the production
// output capture. A metadata header never becomes a full child proof.
type historyVectorMaterialBridge struct {
	ctx         context.Context
	cancel      context.CancelFunc
	service     *messageService
	tenant      uint64
	owner       string
	kbID        string
	originals   map[string]*historyVectorOriginal
	children    map[string]*types.KnowledgeBuildInputSnapshot
	childBodies map[string]string
	hydrated    map[string]string
	kbCurrent   func(context.Context) error
}

func historyVectorRequestKey(request types.MessageMaterialReadRequest) (string, error) {
	request.MessageIDs = append([]string{}, request.MessageIDs...)
	request.ExcludeRequestIDs = append([]string{}, request.ExcludeRequestIDs...)
	sort.Strings(request.MessageIDs)
	sort.Strings(request.ExcludeRequestIDs)
	return repository.KnowledgeInputDigest(request)
}

// Count control IDs before original bytes are read, including recursively
// recorded input parents. This is a resource bound, never a truncated success.
func historyVectorPlanBudget(
	plan *types.MessageMaterialReadPlan,
	tenant uint64,
	seen map[string]bool,
	depth int,
) error {
	if plan == nil || depth > 32 || plan.MetadataDigest == "" || plan.Request.TenantID != tenant ||
		plan.Request.SessionID == "" ||
		plan.Request.AllSession ||
		len(plan.Request.MessageIDs) == 0 ||
		len(plan.Request.MessageIDs) > types.MessageSearchMaximumExchangeMessages {
		return types.ErrSourceLineageUnknown
	}
	for _, id := range plan.Request.MessageIDs {
		if id == "" || len(id) > 128 {
			return types.ErrSourceLineageUnknown
		}
		seen[plan.Request.SessionID+"\x00"+id] = true
	}
	if len(seen) > historySearchCandidateBudget {
		return access.ErrNextcloudPublicationUnavailable
	}
	for _, parent := range plan.ParentPlans {
		if err := historyVectorPlanBudget(parent, tenant, seen, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// beginHistoryVectorMaterialBridge admits immutable history parents before
// any child body is loaded. Current row JSON is never a source of provenance.
// Guards transferred to the existing output slot remain there through HTTP.
func (s *messageService) beginHistoryVectorMaterialBridge(
	ctx context.Context,
	params *types.MessageSearchParams,
	kbID string,
	parents []*types.HistoryKnowledgeParentPlan,
) (*historyVectorMaterialBridge, error) {
	if ctx == nil || params == nil || s.materialStore == nil || s.sourceLineageGuard == nil || kbID == "" ||
		len(parents) > maxRetrievalPoolSize {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	tenant, ok := types.TenantIDFromContext(ctx)
	owner := types.SessionOwnerIDFromContext(ctx)
	if !ok || tenant == 0 || owner == "" || params.OwnerID != owner {
		return nil, access.ErrNextcloudPublicationDenied
	}
	if slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot); !ok || slot == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	b := &historyVectorMaterialBridge{
		service:     s,
		tenant:      tenant,
		owner:       owner,
		kbID:        kbID,
		originals:   map[string]*historyVectorOriginal{},
		children:    map[string]*types.KnowledgeBuildInputSnapshot{},
		childBodies: map[string]string{},
		hydrated:    map[string]string{},
	}
	allowedSessions := map[string]bool{}
	for _, id := range params.SessionIDs {
		allowedSessions[id] = true
	}
	seenMaterial := map[string]bool{}
	for _, parent := range parents {
		if parent == nil || parent.KnowledgeID == "" || parent.AdmissionID == "" || parent.InputDigest == "" ||
			parent.BuildGeneration == 0 ||
			b.originals[parent.KnowledgeID] != nil {
			return nil, types.ErrSourceLineageUnknown
		}
		if err := historyVectorPlanBudget(parent.MessagePlan, tenant, seenMaterial, 0); err != nil {
			return nil, err
		}
		if len(allowedSessions) > 0 && !allowedSessions[parent.MessagePlan.Request.SessionID] {
			return nil, access.ErrNextcloudPublicationDenied
		}
		// Reserve selectors before obtaining any body's guard, even on failure.
		b.originals[parent.KnowledgeID] = &historyVectorOriginal{header: *parent}
	}
	remaining := historySearchCandidateBudget - len(seenMaterial)
	ids := b.KnowledgeIDs()
	for _, id := range ids {
		original := b.originals[id]
		request := original.header.MessagePlan.Request
		guard, err := s.beginMessageMaterialRead(ctx, request)
		if err != nil {
			return nil, err
		}
		actualKey, err := historyVectorRequestKey(guard.plan.Request)
		expectedKey, keyErr := historyVectorRequestKey(request)
		if err != nil || keyErr != nil || actualKey != expectedKey ||
			guard.plan.MetadataDigest != original.header.MessagePlan.MetadataDigest ||
			guard.snapshot.SessionOwnerID != owner {
			guard.Close()
			return nil, repository.ErrKnowledgeInputChanged
		}
		original.guard = guard
		var assistant *types.Message
		for _, row := range guard.snapshot.Messages {
			if row != nil && row.Role == "assistant" && row.KnowledgeID == id && row.IsCompleted {
				if assistant != nil {
					guard.Close()
					return nil, access.ErrNextcloudPublicationDenied
				}
				assistant = row
			}
		}
		if assistant == nil || assistant.RequestID == "" || assistant.SessionID != request.SessionID {
			guard.Close()
			return nil, access.ErrNextcloudPublicationDenied
		}
		parentRows, err := s.bindAdmittedMessageGuard(ctx, guard, request)
		if err != nil {
			return nil, err
		}
		// The immutable child may record its initial pair while the display
		// exchange includes consumed steer inputs. The existing metadata-only
		// partner port obtains a separately admitted complete current cohort.
		candidate := &types.MessageSearchCandidate{
			ID: assistant.ID, SessionID: assistant.SessionID, RequestID: assistant.RequestID,
			Role: assistant.Role, KnowledgeID: assistant.KnowledgeID, CreatedAt: assistant.CreatedAt,
		}
		reader, ok := s.messageRepo.(historySearchCandidateReader)
		if !ok {
			return nil, access.ErrNextcloudPublicationUnavailable
		}
		metadata, err := reader.HistorySearchPartnerCandidates(ctx, tenant, owner, candidate)
		if err != nil {
			return nil, err
		}
		if len(metadata) == 0 {
			return nil, access.ErrNextcloudPublicationDenied
		}
		if len(metadata) > types.MessageSearchMaximumExchangeMessages {
			return nil, access.ErrNextcloudPublicationUnavailable
		}
		parentIDs := map[string]bool{}
		for _, row := range parentRows {
			parentIDs[row.ID] = true
		}
		same := len(metadata) == len(parentIDs)
		partnerIDs := map[string]bool{}
		for _, row := range metadata {
			if row == nil || row.ID == "" || row.SessionID != assistant.SessionID ||
				row.RequestID != assistant.RequestID ||
				partnerIDs[row.ID] ||
				(row.Role != "user" && row.Role != "assistant") {
				return nil, access.ErrNextcloudPublicationDenied
			}
			partnerIDs[row.ID] = true
			if !parentIDs[row.ID] {
				same = false
			}
		}
		if same {
			original.cohort = parentRows
			for _, row := range parentRows {
				if row.ID == assistant.ID {
					original.assistant = row
				}
			}
			continue
		}
		cohortGuard, cohortAssistant, err := s.beginHistorySearchPair(ctx, params, candidate, &remaining)
		if err != nil {
			if err == errHistorySearchPageMaterialBudget {
				return nil, access.ErrNextcloudPublicationUnavailable
			}
			return nil, err
		}
		if cohortAssistant.ID != assistant.ID || cohortAssistant.KnowledgeID != id {
			cohortGuard.Close()
			return nil, access.ErrNextcloudPublicationDenied
		}
		rows, err := s.bindAdmittedMessageGuard(ctx, cohortGuard, cohortGuard.snapshot.Request)
		if err != nil {
			return nil, err
		}
		original.cohort = rows
		for _, row := range rows {
			if row.ID == assistant.ID {
				original.assistant = row
			}
		}
		if original.assistant == nil {
			return nil, access.ErrNextcloudPublicationDenied
		}
	}
	if len(ids) == 0 {
		b.ctx = logprivacy.SuppressContent(context.WithValue(ctx, historyVectorMaterialBridgeKey{}, b))
		return b, nil
	}
	// This unions every output-owned guard, including earlier keyword and
	// all parent/cohort sessions, rather than retaining the last one's scope.
	merged, err := messageMaterialGeneratedWriteContext(ctx)
	if err != nil {
		return nil, err
	}
	joined, cancelCause := context.WithCancelCause(merged)
	b.cancel = func() { cancelCause(context.Canceled) }
	b.ctx = logprivacy.SuppressContent(context.WithValue(joined, historyVectorMaterialBridgeKey{}, b))
	slot := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	slot.mu.Lock()
	guards := append([]*MessageMaterialGuard{}, slot.Guards...)
	slot.mu.Unlock()
	for _, guard := range guards {
		guard.mu.Lock()
		guardContext := guard.ctx
		guard.mu.Unlock()
		stop := context.AfterFunc(guardContext, func() { cancelCause(context.Cause(guardContext)) })
		context.AfterFunc(b.ctx, func() { stop() })
	}
	if err := b.Checkpoint(b.ctx); err != nil {
		return nil, err
	}
	return b, nil
}

// This classifies only a failed/cancelled bridge; it never admits output or
// skips a physical lease checkpoint. Ordinary Close/deadline/unknown causes
// remain unavailable. Only the actual parent's typed source denial survives.
func historyVectorCancellationError(ctx context.Context) error {
	if ctx != nil && errors.Is(context.Cause(ctx), access.ErrNextcloudPublicationDenied) {
		return context.Cause(ctx)
	}
	return access.ErrNextcloudPublicationUnavailable
}

func (b *historyVectorMaterialBridge) Context() context.Context { return b.ctx }
func (b *historyVectorMaterialBridge) KnowledgeIDs() []string {
	ids := make([]string, 0, len(b.originals))
	for id := range b.originals {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (b *historyVectorMaterialBridge) Checkpoint(ctx context.Context) error {
	if b == nil || ctx == nil || b.ctx == nil || b.service == nil || b.service.materialStore == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	if ctx.Err() != nil {
		return historyVectorCancellationError(ctx)
	}
	if b.ctx.Err() != nil {
		return historyVectorCancellationError(b.ctx)
	}
	tenant, ok := types.TenantIDFromContext(ctx)
	if !ok || tenant != b.tenant || types.SessionOwnerIDFromContext(ctx) != b.owner {
		return access.ErrNextcloudPublicationDenied
	}
	if err := CheckMessageMaterialOutputBoundary(ctx); err != nil {
		return err
	}
	if err := b.recheckCurrentInputs(); err != nil {
		return err
	}
	return CheckMessageMaterialOutputBoundary(ctx)
}

// recheckCurrentInputs is also used by the final output owner while its
// mutex is held. It never checks or registers with the output slot. The
// captured context carries every admitted original plan and FAQ lease;
// the HTTP writer's context alone does not carry those producer inputs.
func (b *historyVectorMaterialBridge) recheckCurrentInputs() error {
	if b == nil || b.ctx == nil || b.service == nil || b.service.materialStore == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	if b.ctx.Err() != nil {
		return historyVectorCancellationError(b.ctx)
	}
	ctx := b.ctx
	tenant, ok := types.TenantIDFromContext(ctx)
	if !ok || tenant != b.tenant || types.SessionOwnerIDFromContext(ctx) != b.owner {
		return access.ErrNextcloudPublicationDenied
	}
	if b.kbCurrent != nil {
		if err := b.kbCurrent(ctx); err != nil {
			return err
		}
	}
	var proofs []*types.KnowledgeBuildInputProof
	for _, id := range b.KnowledgeIDs() {
		child := b.children[id]
		if child == nil {
			continue
		}
		digest, err := historyVectorChildDigest(child)
		if err != nil {
			return err
		}
		if b.childBodies[id] == "" || b.childBodies[id] != digest {
			return repository.ErrKnowledgeInputChanged
		}
		if child.Proof == nil || child.Proof.SourceLineage.RequireComplete() != nil {
			return types.ErrSourceLineageUnknown
		}
		if err := b.service.materialStore.RecheckKnowledgeBuildInputs(ctx, child.Proof); err != nil {
			return err
		}
		proofs = append(proofs, child.Proof)
	}
	if len(proofs) > 0 {
		if err := b.service.materialStore.RecheckKnowledgeOutputInputs(ctx, proofs); err != nil {
			return err
		}
	}
	return nil
}

// Capture checkpoints may run before children have been obtained. Once a
// bridge is held for output, every selected child and its KB authorizer must
// remain present; deleting a retained snapshot cannot skip its final proof.
func (b *historyVectorMaterialBridge) recheckHeldInputs() error {
	if b == nil || b.kbCurrent == nil || len(b.originals) == 0 || len(b.children) != len(b.originals) ||
		len(b.childBodies) != len(b.originals) {
		return access.ErrNextcloudPublicationUnavailable
	}
	for id, original := range b.originals {
		child := b.children[id]
		if original == nil || original.guard == nil || original.assistant == nil || child == nil ||
			child.Proof == nil ||
			b.childBodies[id] == "" {
			return access.ErrNextcloudPublicationUnavailable
		}
	}
	return b.recheckCurrentInputs()
}

// holdHistoryVectorMaterialOutput accepts only this request's hydrated
// bridge. It retains current child/KB authorization, not a caller-supplied
// callback, until the existing output owner's final release.
func holdHistoryVectorMaterialOutput(ctx context.Context, bridge *historyVectorMaterialBridge) error {
	if ctx == nil || bridge == nil || bridge.ctx == nil || bridge.kbCurrent == nil || len(bridge.originals) == 0 ||
		len(bridge.children) != len(bridge.originals) {
		return access.ErrNextcloudPublicationUnavailable
	}
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	if !ok || slot == nil || bridge.ctx.Value(messageMaterialOutputKey{}) != slot {
		return access.ErrNextcloudPublicationDenied
	}
	if err := bridge.recheckHeldInputs(); err != nil {
		return err
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.Closed || slot.Failed != nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	for _, held := range slot.HistoryVectorReads {
		if held == bridge {
			return nil
		}
	}
	slot.HistoryVectorReads = append(slot.HistoryVectorReads, bridge)
	return nil
}

func (b *historyVectorMaterialBridge) captureChildren(
	ctx context.Context,
	candidates []*types.VectorIndexCandidate,
	physical *readlease.NextcloudReadGuard,
) error {
	if physical == nil || !physical.Covers([]readlease.NextcloudKBReadScope{{TenantID: b.tenant, KBID: b.kbID}}) ||
		len(candidates) > maxRetrievalPoolSize {
		return access.ErrNextcloudPublicationUnavailable
	}
	byKnowledge := map[string][]string{}
	seen := map[string]bool{}
	for _, candidate := range candidates {
		if candidate == nil || candidate.KnowledgeBaseID != b.kbID || b.originals[candidate.KnowledgeID] == nil ||
			candidate.IndexID == "" ||
			candidate.ChunkID == "" ||
			math.IsNaN(candidate.Score) ||
			math.IsInf(candidate.Score, 0) {
			return access.ErrNextcloudPublicationDenied
		}
		key := candidate.KnowledgeID + "\x00" + candidate.ChunkID
		if !seen[key] {
			byKnowledge[candidate.KnowledgeID] = append(byKnowledge[candidate.KnowledgeID], candidate.ChunkID)
			seen[key] = true
		}
	}
	if err := b.Checkpoint(ctx); err != nil {
		return err
	}
	for _, id := range b.KnowledgeIDs() {
		chunkIDs := byKnowledge[id]
		if len(chunkIDs) == 0 {
			continue
		}
		if b.children[id] != nil {
			return access.ErrNextcloudPublicationDenied
		}
		sort.Strings(chunkIDs)
		child, err := b.service.materialStore.CaptureKnowledgeOutputInputs(ctx, types.KnowledgeBuildInputCaptureRequest{
			TenantID: b.tenant, KnowledgeBaseID: b.kbID, KnowledgeID: id, ChunkIDs: chunkIDs,
		})
		if err != nil {
			return err
		}
		header := b.originals[id].header
		if child == nil || child.Proof == nil || child.Knowledge == nil || child.KnowledgeBase == nil ||
			child.Knowledge.ID != id ||
			child.Knowledge.TenantID != b.tenant || child.Knowledge.KnowledgeBaseID != b.kbID ||
			child.KnowledgeBase.ID != b.kbID || child.KnowledgeBase.TenantID != b.tenant ||
			child.Proof.TenantID != b.tenant ||
			child.Proof.KnowledgeBaseID != b.kbID || child.Proof.KnowledgeID != id ||
			child.Proof.AdmissionID != header.AdmissionID ||
			child.Proof.InputDigest != header.InputDigest || child.Proof.BuildGeneration != header.BuildGeneration ||
			child.Proof.BodyDigest == "" || child.Proof.SourceLineage.RequireComplete() != nil || len(
			child.Chunks,
		) != len(
			chunkIDs,
		) {
			return repository.ErrKnowledgeInputChanged
		}
		wanted := map[string]bool{}
		for _, chunkID := range chunkIDs {
			wanted[chunkID] = true
		}
		for _, chunk := range child.Chunks {
			if chunk == nil || !wanted[chunk.ID] || chunk.TenantID != b.tenant || chunk.KnowledgeBaseID != b.kbID ||
				chunk.KnowledgeID != id {
				return repository.ErrKnowledgeInputChanged
			}
			delete(wanted, chunk.ID)
		}
		if len(wanted) != 0 {
			return repository.ErrKnowledgeInputChanged
		}
		if err := physical.PinKnowledge(child.Knowledge); err != nil {
			return err
		}
		digest, err := historyVectorChildDigest(child)
		if err != nil {
			return err
		}
		b.children[id] = child
		b.childBodies[id] = digest
	}
	if err := physical.Verify(); err != nil {
		return err
	}
	return b.Checkpoint(ctx)
}

func historyVectorResultKey(result *types.SearchResult) string {
	return result.KnowledgeID + "\x00" + result.ID
}

func historyVectorChildDigest(child *types.KnowledgeBuildInputSnapshot) (string, error) {
	if child == nil {
		return "", types.ErrSourceLineageUnknown
	}
	// The snapshot struct itself is JSON-private, so name each actual field.
	return repository.KnowledgeInputDigest(struct {
		Knowledge     *types.Knowledge
		KnowledgeBase *types.KnowledgeBase
		Chunks        []*types.Chunk
		Proof         *types.KnowledgeBuildInputProof
	}{child.Knowledge, child.KnowledgeBase, child.Chunks, child.Proof})
}

func historyVectorResultDigest(result *types.SearchResult) (string, error) {
	if result == nil {
		return "", types.ErrSourceLineageUnknown
	}
	return repository.KnowledgeInputDigest(struct {
		Result    *types.SearchResult
		Lineage   *types.SourceLineage
		Revision  int
		Rewritten bool
	}{result, result.SourceLineage, result.ContentRevision, result.ContentRewritten})
}

func (b *historyVectorMaterialBridge) originalChunk(result *types.SearchResult) (*types.Chunk, error) {
	if result == nil || result.KnowledgeBaseID != b.kbID || result.SourceLineage.RequireComplete() != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	child := b.children[result.KnowledgeID]
	if child == nil {
		return nil, access.ErrNextcloudPublicationDenied
	}
	digest, err := historyVectorChildDigest(child)
	if err != nil {
		return nil, err
	}
	if digest != b.childBodies[result.KnowledgeID] {
		return nil, repository.ErrKnowledgeInputChanged
	}
	for _, chunk := range child.Chunks {
		if chunk.ID == result.ID && chunk.Content == result.Content &&
			chunk.ContentRevision == result.ContentRevision &&
			!result.ContentRewritten &&
			result.MatchedContent == "" {
			return chunk, nil
		}
	}
	return nil, repository.ErrKnowledgeInputChanged
}

func (b *historyVectorMaterialBridge) bindHydratedResults(results []*types.SearchResult) error {
	if len(b.hydrated) > 0 {
		return access.ErrNextcloudPublicationDenied
	}
	for _, result := range results {
		if _, err := b.originalChunk(result); err != nil {
			return err
		}
		key := historyVectorResultKey(result)
		if b.hydrated[key] != "" {
			return access.ErrNextcloudPublicationDenied
		}
		digest, err := historyVectorResultDigest(result)
		if err != nil {
			return err
		}
		b.hydrated[key] = digest
	}
	return nil
}

// Model documents are copied directly from captured original child chunks.
// The mutable hydrated display rows must still match their actual digest.
func (b *historyVectorMaterialBridge) modelDocuments(results []*types.SearchResult) ([]string, error) {
	documents := make([]string, 0, len(results))
	for _, result := range results {
		chunk, err := b.originalChunk(result)
		if err != nil {
			return nil, err
		}
		digest, err := historyVectorResultDigest(result)
		if err != nil {
			return nil, err
		}
		if b.hydrated[historyVectorResultKey(result)] == "" || b.hydrated[historyVectorResultKey(result)] != digest {
			return nil, repository.ErrKnowledgeInputChanged
		}
		documents = append(documents, chunk.Content)
	}
	return documents, nil
}

// OwnedCandidates projects held original message/cohort bodies only. Child
// chunk text and any provider-returned Document are never search output text.
func (b *historyVectorMaterialBridge) OwnedCandidates(
	ctx context.Context,
	scores map[string]float64,
) ([]*types.MessageSearchResultItem, map[string][]*types.Message, error) {
	if err := b.Checkpoint(ctx); err != nil {
		return nil, nil, err
	}
	items := []*types.MessageSearchResultItem{}
	pairs := map[string][]*types.Message{}
	ids := make([]string, 0, len(scores))
	for id := range scores {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		original := b.originals[id]
		if original == nil || original.assistant == nil || original.assistant.KnowledgeID != id ||
			b.children[id] == nil ||
			math.IsNaN(scores[id]) ||
			math.IsInf(scores[id], 0) {
			return nil, nil, access.ErrNextcloudPublicationDenied
		}
		// Copies prevent callers from mutating the guards' retained bodies.
		rows, err := cloneCollection(original.cohort)
		if err != nil {
			return nil, nil, err
		}
		var assistant *types.Message
		for i, row := range rows {
			row.SourceLineage, err = types.UnionSourceLineage(original.cohort[i].SourceLineage)
			if err != nil {
				return nil, nil, err
			}
			row.ExecutionContext = original.cohort[i].ExecutionContext
			row.AgentTenantID = original.cohort[i].AgentTenantID
			row.RenderedContent = original.cohort[i].RenderedContent
			row.ContextCheckpoint, err = cloneCollection(original.cohort[i].ContextCheckpoint)
			if err != nil {
				return nil, nil, err
			}
			if row.ID == original.assistant.ID {
				assistant = row
			}
		}
		if assistant == nil {
			return nil, nil, access.ErrNextcloudPublicationDenied
		}
		items = append(
			items,
			&types.MessageSearchResultItem{
				MessageWithSession: types.MessageWithSession{Message: *assistant},
				Score:              scores[id],
				MatchType:          "vector",
			},
		)
		pairs[historySearchPairKey(assistant)] = rows
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].Score > items[j].Score })
	if err := b.Checkpoint(ctx); err != nil {
		return nil, nil, err
	}
	return items, pairs, nil
}
