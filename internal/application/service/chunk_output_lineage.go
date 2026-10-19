package service

import (
	"context"
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type ChunkOutputAuthorizer struct {
	reader  interfaces.ChunkOutputInputReader
	checker knowledgeOutputLineageChecker
}

func NewChunkOutputAuthorizer(
	store *repository.KnowledgeInputProvenanceStore,
	guard *access.NextcloudSourceLineageGuard,
) *ChunkOutputAuthorizer {
	return &ChunkOutputAuthorizer{reader: store, checker: guard}
}

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

func (g *ChunkOutputGuard) Snapshot() (*types.ChunkOutputInputSnapshot, error) {
	if g == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	return cloneChunkOutputSnapshot(g.snapshot)
}

func (g *ChunkOutputGuard) SourceLineage() *types.SourceLineage {
	if g == nil || g.snapshot == nil {
		return types.UnknownSourceLineage()
	}
	copy, _ := types.UnionSourceLineage(g.snapshot.Proof.SourceLineage)
	return copy
}

func (g *ChunkOutputGuard) Check(ctx context.Context, published *types.ChunkOutputInputSnapshot) error {
	if ctx == nil || g == nil || g.authorizer == nil || g.authorizer.reader == nil || g.authorizer.checker == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	matchBody := func() error {
		body, err := chunkOutputSnapshotBody(published)
		if err != nil {
			return err
		}
		if body != g.body {
			return repository.ErrKnowledgeInputChanged
		}
		return nil
	}
	if err := matchBody(); err != nil {
		return err
	}
	checkScope := func() error {
		lineage, err := types.UnionSourceLineage(g.snapshot.Proof.SourceLineage)
		if err != nil {
			return err
		}
		bytes, err := json.Marshal(g.scope)
		if err != nil {
			return err
		}
		var scope access.LineageReadScope
		if err := json.Unmarshal(bytes, &scope); err != nil {
			return err
		}
		return g.authorizer.checker.Check(ctx, lineage, scope)
	}
	if err := checkScope(); err != nil {
		return err
	}
	snapshot, err := cloneChunkOutputSnapshot(g.snapshot)
	if err != nil {
		return err
	}
	if err := g.authorizer.reader.RecheckChunkOutputInputs(ctx, snapshot); err != nil {
		return err
	}
	// AD/share/source may change during the coherent local material snapshot.
	if err := checkScope(); err != nil {
		return err
	}
	return matchBody()
}
