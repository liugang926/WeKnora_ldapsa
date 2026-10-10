package repository

import (
	"context"
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/logprivacy"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type knowledgeInputOriginKey struct{}
type knowledgeBuildInputKey struct{}
type knowledgeInputOrigin struct {
	lineage *types.SourceLineage
	kind    string
}
type knowledgeBuildInputContext struct {
	reader    interfaces.KnowledgeBuildInputReader
	proof     *types.KnowledgeBuildInputProof
	admission context.Context
}

// WithIndependentKnowledgeInput is for authenticated direct human ingestion
// boundaries. It is not decoded from a request, channel or metadata. Server
// generated passages must use the original influencing input union instead.
func WithIndependentKnowledgeInput(ctx context.Context) context.Context {
	l, _ := types.NewCompleteSourceLineage()
	return context.WithValue(ctx, knowledgeInputOriginKey{}, knowledgeInputOrigin{l, "human"})
}

// WithDerivedKnowledgeInput requires the producer's already checked complete
// input envelope. It never interprets citation or client metadata as a receipt.
func WithDerivedKnowledgeInput(ctx context.Context, lineage *types.SourceLineage) (context.Context, error) {
	if err := lineage.RequireComplete(); err != nil {
		return ctx, err
	}
	copy, err := types.UnionSourceLineage(lineage)
	if err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, knowledgeInputOriginKey{}, knowledgeInputOrigin{copy, "derived"}), nil
}

func cloneKnowledgeInputProof(proof *types.KnowledgeBuildInputProof) *types.KnowledgeBuildInputProof {
	if proof == nil {
		return nil
	}
	data, err := json.Marshal(proof)
	if err != nil {
		return nil
	}
	var copy types.KnowledgeBuildInputProof
	if json.Unmarshal(data, &copy) != nil {
		return nil
	}
	return &copy
}

func WithKnowledgeBuildInputProof(ctx context.Context, reader interfaces.KnowledgeBuildInputReader, proof *types.KnowledgeBuildInputProof) context.Context {
	if proof != nil && proof.SourceLineage != nil &&
		(proof.SourceLineage.State != types.SourceLineageComplete || len(proof.SourceLineage.Sources) > 0) {
		ctx = logprivacy.SuppressContent(ctx)
	}
	return context.WithValue(ctx, knowledgeBuildInputKey{}, knowledgeBuildInputContext{reader, cloneKnowledgeInputProof(proof), ctx})
}

func KnowledgeBuildInputProofFromContext(ctx context.Context) *types.KnowledgeBuildInputProof {
	if ctx == nil {
		return nil
	}
	guard, _ := ctx.Value(knowledgeBuildInputKey{}).(knowledgeBuildInputContext)
	return cloneKnowledgeInputProof(guard.proof)
}

func CheckKnowledgeBuildInputs(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	guard, ok := ctx.Value(knowledgeBuildInputKey{}).(knowledgeBuildInputContext)
	if !ok {
		return nil
	}
	if guard.reader == nil || guard.proof == nil {
		return types.ErrSourceLineageUnknown
	}
	if err := guard.admission.Err(); err != nil {
		return err
	}
	return guard.reader.RecheckKnowledgeBuildInputs(ctx, guard.proof)
}

// NextcloudBuildInputLease exposes only a copy of the already admitted lease.
// It cannot acquire a new epoch for an old task or revive a canceled worker.
func NextcloudBuildInputLease(ctx context.Context) (NextcloudContentLease, NextcloudContentScope, error) {
	guard, ok := ctx.Value(nextcloudBuildContextKey{}).(nextcloudBuildContext)
	if !ok || guard.validator == nil {
		return NextcloudContentLease{}, NextcloudContentScope{}, ErrNextcloudContentLeaseInvalid
	}
	if err := guard.admissionCtx.Err(); err != nil {
		return NextcloudContentLease{}, NextcloudContentScope{}, err
	}
	if err := ctx.Err(); err != nil {
		return NextcloudContentLease{}, NextcloudContentScope{}, err
	}
	return guard.lease, guard.scope, nil
}

func PrecheckKnowledgeBuildWrite(ctx context.Context) error {
	if err := CheckNextcloudBuildLease(ctx); err != nil {
		return err
	}
	if err := CheckKnowledgeProducerInput(ctx); err != nil {
		return err
	}
	return CheckKnowledgeBuildInputs(ctx)
}
