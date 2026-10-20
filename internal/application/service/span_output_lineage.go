package service

import (
	"context"
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// SpanOutputAuthorizer authorizes original span inputs before public output.
type SpanOutputAuthorizer struct {
	reader  interfaces.SpanOutputInputReader
	checker knowledgeOutputLineageChecker
}

// NewSpanOutputAuthorizer constructs the configured SpanOutputAuthorizer.
func NewSpanOutputAuthorizer(
	store *repository.KnowledgeInputProvenanceStore,
	guard *access.NextcloudSourceLineageGuard,
) *SpanOutputAuthorizer {
	return &SpanOutputAuthorizer{reader: store, checker: guard}
}

// SpanOutputGuard retains and rechecks original span inputs.
type SpanOutputGuard struct {
	authorizer *SpanOutputAuthorizer
	snapshot   *types.SpanOutputInputSnapshot
	body       string
	scope      access.LineageReadScope
}

func cloneSpanOutput(in *types.SpanOutputInputSnapshot) (*types.SpanOutputInputSnapshot, error) {
	if in == nil || in.Receipt == "" || in.SourceLineage.RequireComplete() != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	data, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	var out types.SpanOutputInputSnapshot
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	out.Receipt = in.Receipt
	return &out, nil
}

// Begin captures the original inputs and current controls for this operation.
func (a *SpanOutputAuthorizer) Begin(
	ctx context.Context,
	request types.SpanOutputReadRequest,
) (*SpanOutputGuard, error) {
	if ctx == nil || a == nil || a.reader == nil || a.checker == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	actual, err := a.reader.CaptureSpanOutputInputs(ctx, request)
	if err != nil {
		return nil, err
	}
	snapshot, err := cloneSpanOutput(actual)
	if err != nil {
		return nil, err
	}
	if snapshot.TenantID != request.TenantID || snapshot.KnowledgeBaseID != request.KnowledgeBaseID ||
		snapshot.KnowledgeID != request.KnowledgeID ||
		request.Attempt > 0 && snapshot.Attempt != request.Attempt {
		return nil, types.ErrSourceLineageUnknown
	}
	body, err := json.Marshal(snapshot)
	if err != nil {
		return nil, err
	}
	g := &SpanOutputGuard{
		authorizer: a,
		snapshot:   snapshot,
		body:       string(body),
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
func (g *SpanOutputGuard) Snapshot() (*types.SpanOutputInputSnapshot, error) {
	if g == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	return cloneSpanOutput(g.snapshot)
}

// Check rechecks the retained inputs and their current read controls.
func (g *SpanOutputGuard) Check(ctx context.Context, published *types.SpanOutputInputSnapshot) error {
	if ctx == nil || g == nil || g.authorizer == nil || g.authorizer.reader == nil || g.authorizer.checker == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	match := func() error {
		body, err := json.Marshal(published)
		if err != nil {
			return err
		}
		if string(body) != g.body || published.Receipt != g.snapshot.Receipt {
			return repository.ErrKnowledgeInputChanged
		}
		return nil
	}
	check := func() error {
		lineage, err := types.UnionSourceLineage(g.snapshot.SourceLineage)
		if err != nil {
			return err
		}
		data, err := json.Marshal(g.scope)
		if err != nil {
			return err
		}
		var scope access.LineageReadScope
		if err := json.Unmarshal(data, &scope); err != nil {
			return err
		}
		return g.authorizer.checker.Check(ctx, lineage, scope)
	}
	if err := match(); err != nil {
		return err
	}
	if err := check(); err != nil {
		return err
	}
	snapshot, err := cloneSpanOutput(g.snapshot)
	if err != nil {
		return err
	}
	if err := g.authorizer.reader.RecheckSpanOutputInputs(ctx, snapshot); err != nil {
		return err
	}
	if err := check(); err != nil {
		return err
	}
	// The remote authorization pass may take time or run callbacks. Finish
	// with the original entire attempt/control manifest in one local RR/
	// SQLite snapshot before the response's bytes can leave this boundary.
	if err := g.authorizer.reader.RecheckSpanOutputInputs(ctx, snapshot); err != nil {
		return err
	}
	return match()
}
