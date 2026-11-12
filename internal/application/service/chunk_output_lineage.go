package service

import (
	"context"
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// ChunkOutputAuthorizer authorizes original chunk inputs before public output.
type ChunkOutputAuthorizer struct {
	reader  interfaces.ChunkOutputInputReader
	checker knowledgeOutputLineageChecker
}

// NewChunkOutputAuthorizer constructs the configured ChunkOutputAuthorizer.
func NewChunkOutputAuthorizer(
	store *repository.KnowledgeInputProvenanceStore,
	guard *access.NextcloudSourceLineageGuard,
) *ChunkOutputAuthorizer {
	return &ChunkOutputAuthorizer{reader: store, checker: guard}
}

// ChunkOutputGuard retains and rechecks the chunk inputs selected for output.
type ChunkOutputGuard struct {
	authorizer *ChunkOutputAuthorizer
	snapshot   *types.ChunkOutputInputSnapshot
	body       string
	scope      access.LineageReadScope
}

func cloneChunkOutputSnapshot(in *types.ChunkOutputInputSnapshot) (*types.ChunkOutputInputSnapshot, error) {
	if in == nil || in.Knowledge == nil || in.Proof == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	data, err := json.Marshal(struct {
		Knowledge *types.Knowledge
		Chunks    []*types.Chunk
		Proof     *types.KnowledgeBuildInputProof
		AllChunks bool
	}{in.Knowledge, in.Chunks, in.Proof, in.AllChunks})
	if err != nil {
		return nil, err
	}
	var out struct {
		Knowledge *types.Knowledge
		Chunks    []*types.Chunk
		Proof     *types.KnowledgeBuildInputProof
		AllChunks bool
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return &types.ChunkOutputInputSnapshot{
		Knowledge: out.Knowledge,
		Chunks:    out.Chunks,
		Proof:     out.Proof,
		AllChunks: out.AllChunks,
	}, nil
}

func chunkOutputSnapshotBody(snapshot *types.ChunkOutputInputSnapshot) (string, error) {
	if snapshot == nil {
		return "", types.ErrSourceLineageUnknown
	}
	data, err := json.Marshal(struct {
		Knowledge *types.Knowledge
		Chunks    []*types.Chunk
		AllChunks bool
	}{snapshot.Knowledge, snapshot.Chunks, snapshot.AllChunks})
	return string(data), err
}

// Begin captures the original inputs and current controls for this operation.
func (a *ChunkOutputAuthorizer) Begin(
	ctx context.Context,
	request types.ChunkOutputReadRequest,
) (*ChunkOutputGuard, error) {
	if a == nil || a.reader == nil || a.checker == nil || ctx == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	actual, err := a.reader.CaptureChunkOutputInputs(ctx, request)
	if err != nil {
		return nil, err
	}
	snapshot, err := cloneChunkOutputSnapshot(actual)
	if err != nil {
		return nil, err
	}
	if snapshot.Proof.Projection != "" || snapshot.Proof.BodyDigest == "" ||
		snapshot.Proof.SourceLineage.RequireComplete() != nil ||
		snapshot.Knowledge.TenantID != request.TenantID ||
		snapshot.Knowledge.KnowledgeBaseID != request.KnowledgeBaseID ||
		snapshot.Knowledge.ID != request.KnowledgeID {
		return nil, types.ErrSourceLineageUnknown
	}
	body, err := chunkOutputSnapshotBody(snapshot)
	if err != nil {
		return nil, err
	}
	g := &ChunkOutputGuard{
		authorizer: a,
		snapshot:   snapshot,
		body:       body,
		scope: access.LineageReadScope{
			Targets: types.SearchTargets{
				&types.SearchTarget{
					Type:            types.SearchTargetTypeKnowledgeBase,
					TenantID:        request.TenantID,
					KnowledgeBaseID: request.KnowledgeBaseID,
				},
			},
		},
	}
	if err := g.Check(ctx, snapshot); err != nil {
		return nil, err
	}
	return g, nil
}

// Snapshot returns the selected original material snapshot.
func (g *ChunkOutputGuard) Snapshot() (*types.ChunkOutputInputSnapshot, error) {
	if g == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	return cloneChunkOutputSnapshot(g.snapshot)
}

// SourceLineage returns the lineage retained by this output guard.
func (g *ChunkOutputGuard) SourceLineage() *types.SourceLineage {
	if g == nil || g.snapshot == nil {
		return types.UnknownSourceLineage()
	}
	copied, _ := types.UnionSourceLineage(g.snapshot.Proof.SourceLineage)
	return copied
}

// Check preserves the retired Nextcloud output-guard compatibility API.
func (g *ChunkOutputGuard) Check(ctx context.Context, published *types.ChunkOutputInputSnapshot) error {
	return nil // retired with the NextCloud decoupling 2026-11-09
}
