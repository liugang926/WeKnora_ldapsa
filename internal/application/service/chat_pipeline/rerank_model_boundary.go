package chatpipeline

import (
	"context"
	"slices"

	"github.com/Tencent/WeKnora/internal/models/imageprep"
	"github.com/Tencent/WeKnora/internal/models/rerank"
)

// The shared v0.9 scorer can issue separate image and text requests. Authorize
// each detached payload at the real model boundary; an authorization failure
// is terminal even when the scorer attempts its normal text fallback.
type checkedReranker struct {
	rerank.Reranker
	check            func(context.Context) error
	denied, modelErr error
}

func (r *checkedReranker) authorize(ctx context.Context) error {
	if r.denied == nil {
		r.denied = r.check(ctx)
	}
	return r.denied
}

func (r *checkedReranker) Rerank(ctx context.Context, q string, docs []string) ([]rerank.RankResult, error) {
	frozen := slices.Clone(docs)
	if err := r.authorize(ctx); err != nil {
		return nil, err
	}
	out, err := r.Reranker.Rerank(ctx, q, frozen)
	r.modelErr = err
	return out, err
}

func (r *checkedReranker) MaxPassageRunes(query string) int {
	return rerank.MaxPassageRunes(r.Reranker, query)
}

func (r *checkedReranker) AcceptsImages() bool {
	_, ok := rerank.AsImageReranker(r.Reranker)
	return ok
}

func (r *checkedReranker) ImageLimits() imageprep.Limits {
	if m, ok := rerank.AsImageReranker(r.Reranker); ok {
		return m.ImageLimits()
	}
	return imageprep.Limits{}
}

func (r *checkedReranker) RerankImages(
	ctx context.Context, q string, images []rerank.Image,
) ([]rerank.RankResult, error) {
	frozen := slices.Clone(images)
	for i := range frozen {
		frozen[i].Data = slices.Clone(frozen[i].Data)
	}
	if err := r.authorize(ctx); err != nil {
		return nil, err
	}
	m, ok := rerank.AsImageReranker(r.Reranker)
	if !ok {
		return nil, rerank.ErrImagesUnsupported
	}
	return m.RerankImages(ctx, q, frozen)
}
