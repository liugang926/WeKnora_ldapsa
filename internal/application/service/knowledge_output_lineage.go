package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type knowledgeOutputLineageChecker interface {
	Check(context.Context, *types.SourceLineage, access.LineageReadScope) error
}

// KnowledgeOutputAuthorizer admits actual persisted metadata once per output
// operation. Every subsequent boundary keeps that original proof unchanged.
type KnowledgeOutputAuthorizer struct {
	reader  interfaces.KnowledgeOutputInputReader
	checker knowledgeOutputLineageChecker
}

// NewKnowledgeOutputAuthorizer constructs the configured KnowledgeOutputAuthorizer.
func NewKnowledgeOutputAuthorizer(
	reader *repository.KnowledgeInputProvenanceStore,
	guard *access.NextcloudSourceLineageGuard,
) *KnowledgeOutputAuthorizer {
	return &KnowledgeOutputAuthorizer{reader: reader, checker: guard}
}

type knowledgeOutputEntry struct {
	body     string
	baseBody string
	proof    *types.KnowledgeBuildInputProof
}

// KnowledgeOutputGuard retains original knowledge fields and their current read authority.
type KnowledgeOutputGuard struct {
	authorizer *KnowledgeOutputAuthorizer
	entries    map[string]knowledgeOutputEntry
	proofs     []*types.KnowledgeBuildInputProof
	lineage    *types.SourceLineage
	scope      access.LineageReadScope
	auxiliary  *types.KnowledgeOutputAuxiliaryInputSnapshot
	faqResults []*types.FAQImportOutputSnapshot
}

type knowledgeOutputGuardContextKey struct{}

// WithKnowledgeOutputGuard attaches a retained knowledge output guard to a context.
func WithKnowledgeOutputGuard(ctx context.Context, guard *KnowledgeOutputGuard) context.Context {
	return context.WithValue(ctx, knowledgeOutputGuardContextKey{}, guard)
}

// KnowledgeOutputGuardFromContext returns the knowledge output guard attached to a context.
func KnowledgeOutputGuardFromContext(ctx context.Context) *KnowledgeOutputGuard {
	if ctx == nil {
		return nil
	}
	guard, _ := ctx.Value(knowledgeOutputGuardContextKey{}).(*KnowledgeOutputGuard)
	return guard
}

func knowledgeOutputKey(k *types.Knowledge) string {
	if k == nil {
		return ""
	}
	return fmt.Sprintf("%d:%s:%s", k.TenantID, k.KnowledgeBaseID, k.ID)
}

func knowledgeOutputHasFAQResult(k *types.Knowledge) bool {
	return k != nil && len(k.LastFAQImportResult) > 0 &&
		!bytes.Equal(bytes.TrimSpace(k.LastFAQImportResult), []byte("null"))
}

func knowledgeOutputBody(k *types.Knowledge) (string, error) {
	if k == nil || len(k.Tags) > 0 || k.KnowledgeBaseName != "" || k.ErrorMessage != "" ||
		knowledgeOutputHasFAQResult(k) && k.Type != types.KnowledgeTypeFAQ {
		// Joined tag and KB display producers need their own original proof;
		// the document's origin cannot certify unrelated generated strings.
		return "", types.ErrSourceLineageUnknown
	}
	body, err := json.Marshal(k)
	return string(body), err
}

func knowledgeOutputBaseBody(k *types.Knowledge) (string, error) {
	if k == nil {
		return "", types.ErrSourceLineageUnknown
	}
	base := *k
	base.Tags = nil
	base.KnowledgeBaseName = ""
	return knowledgeOutputBody(&base)
}

func knowledgeOutputJoinedBody(k *types.Knowledge) (string, error) {
	if k == nil || k.ErrorMessage != "" || knowledgeOutputHasFAQResult(k) && k.Type != types.KnowledgeTypeFAQ {
		return "", types.ErrSourceLineageUnknown
	}
	body, err := json.Marshal(k)
	return string(body), err
}

func validateKnowledgeOutputDisplay(k *types.Knowledge, aux *types.KnowledgeCollectionAuxiliary) error {
	if k == nil || aux == nil {
		return types.ErrSourceLineageUnknown
	}
	if k.KnowledgeBaseName != "" && k.KnowledgeBaseName != aux.KnowledgeBaseNames[k.KnowledgeBaseID] {
		return repository.ErrKnowledgeInputChanged
	}
	if len(k.Tags) == 0 {
		return nil
	}
	expected := aux.Tags[k.ID]
	if len(k.Tags) != len(expected) {
		return repository.ErrKnowledgeInputChanged
	}
	actual := map[string]string{}
	for _, tag := range k.Tags {
		if tag == nil || actual[tag.ID] != "" {
			return repository.ErrKnowledgeInputChanged
		}
		body, err := json.Marshal(tag)
		if err != nil {
			return err
		}
		actual[tag.ID] = string(body)
	}
	for _, tag := range expected {
		if tag == nil {
			return repository.ErrKnowledgeInputChanged
		}
		body, err := json.Marshal(tag)
		if err != nil {
			return err
		}
		if actual[tag.ID] != string(body) {
			return repository.ErrKnowledgeInputChanged
		}
	}
	return nil
}

// Begin captures the original inputs and current controls for this operation.
func (a *KnowledgeOutputAuthorizer) Begin(ctx context.Context, rows []*types.Knowledge) (*KnowledgeOutputGuard, error) {
	if ctx == nil || a == nil || a.reader == nil || a.checker == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	if len(rows) > 4096 {
		return nil, types.ErrSourceLineageLimit
	}
	lineage, _ := types.NewCompleteSourceLineage()
	g := &KnowledgeOutputGuard{authorizer: a, entries: map[string]knowledgeOutputEntry{}, lineage: lineage}
	targets := map[string]bool{}
	var displaySelections []types.KnowledgeOutputDisplaySelection
	for _, row := range rows {
		if row == nil {
			return nil, access.ErrNextcloudPublicationDenied
		}
		supplied, err := knowledgeOutputJoinedBody(row)
		if err != nil {
			return nil, err
		}
		key := knowledgeOutputKey(row)
		if existing, ok := g.entries[key]; ok {
			if existing.body != supplied {
				return nil, repository.ErrKnowledgeInputChanged
			}
			continue
		}
		actual, err := a.reader.CaptureKnowledgeOutputInputs(
			ctx,
			types.KnowledgeBuildInputCaptureRequest{
				TenantID:        row.TenantID,
				KnowledgeBaseID: row.KnowledgeBaseID,
				KnowledgeID:     row.ID,
			},
		)
		if err != nil {
			return nil, err
		}
		if actual == nil || actual.Proof == nil || actual.Proof.SourceLineage.RequireComplete() != nil ||
			actual.Proof.Version != 1 ||
			actual.Proof.Projection != "" ||
			actual.Proof.TenantID != row.TenantID || actual.Proof.KnowledgeBaseID != row.KnowledgeBaseID ||
			actual.Proof.KnowledgeID != row.ID ||
			actual.Proof.AdmissionID == "" || actual.Proof.BuildGeneration == 0 || actual.Proof.BodyDigest == "" {
			return nil, types.ErrSourceLineageUnknown
		}
		body, err := knowledgeOutputBody(actual.Knowledge)
		if err != nil {
			return nil, err
		}
		base, err := knowledgeOutputBaseBody(row)
		if err != nil {
			return nil, err
		}
		if body != base {
			return nil, repository.ErrKnowledgeInputChanged
		}
		// Detach mutable provider fields before current authorization callbacks.
		encoded, err := json.Marshal(actual.Proof)
		if err != nil {
			return nil, err
		}
		proof := &types.KnowledgeBuildInputProof{}
		if err := json.Unmarshal(encoded, proof); err != nil {
			return nil, err
		}
		if knowledgeOutputHasFAQResult(row) {
			reader, ok := a.reader.(interfaces.FAQImportOutputInputReader)
			if !ok {
				return nil, types.ErrSourceLineageUnknown
			}
			result, err := reader.CaptureFAQImportOutputInputs(
				ctx,
				types.FAQImportOutputReadRequest{
					TenantID:        row.TenantID,
					KnowledgeBaseID: row.KnowledgeBaseID,
					KnowledgeID:     row.ID,
					Kind:            "result",
				},
			)
			if err != nil {
				return nil, err
			}
			actual, err := row.GetLastFAQImportResult()
			if err != nil {
				return nil, err
			}
			if result == nil || result.Result == nil || !faqOutputBodiesEqual(actual, result.Result) {
				return nil, repository.ErrKnowledgeInputChanged
			}
			result, err = cloneCollection(result)
			if err != nil {
				return nil, err
			}
			g.faqResults = append(g.faqResults, result)
		}
		g.entries[key] = knowledgeOutputEntry{body: supplied, baseBody: base, proof: proof}
		g.proofs = append(g.proofs, proof)
		if len(row.Tags) > 0 || row.KnowledgeBaseName != "" {
			displaySelections = append(
				displaySelections,
				types.KnowledgeOutputDisplaySelection{
					TenantID:          row.TenantID,
					KnowledgeBaseID:   row.KnowledgeBaseID,
					KnowledgeID:       row.ID,
					Tags:              len(row.Tags) > 0,
					KnowledgeBaseName: row.KnowledgeBaseName != "",
				},
			)
		}
		g.lineage, err = types.UnionSourceLineage(g.lineage, proof.SourceLineage)
		if err != nil {
			return nil, err
		}
		target := fmt.Sprintf("%d:%s", row.TenantID, row.KnowledgeBaseID)
		if !targets[target] {
			targets[target] = true
			g.scope.Targets = append(
				g.scope.Targets,
				&types.SearchTarget{
					Type:            types.SearchTargetTypeKnowledgeBase,
					TenantID:        row.TenantID,
					KnowledgeBaseID: row.KnowledgeBaseID,
				},
			)
		}
	}
	if len(displaySelections) > 0 {
		reader, ok := a.reader.(interfaces.KnowledgeOutputAuxiliaryInputReader)
		if !ok {
			return nil, types.ErrSourceLineageUnknown
		}
		proofs, err := cloneCollection(g.proofs)
		if err != nil {
			return nil, err
		}
		snapshot, err := reader.CaptureKnowledgeOutputAuxiliary(ctx, proofs, displaySelections)
		if err != nil {
			return nil, err
		}
		g.auxiliary, err = cloneCollection(snapshot)
		if err != nil {
			return nil, err
		}
		if g.auxiliary == nil || g.auxiliary.Auxiliary == nil {
			return nil, types.ErrSourceLineageUnknown
		}
		for _, actual := range g.auxiliary.Knowledge {
			base, err := knowledgeOutputBaseBody(actual)
			if err != nil {
				return nil, err
			}
			if entry, ok := g.entries[knowledgeOutputKey(actual)]; !ok || entry.baseBody != base {
				return nil, repository.ErrKnowledgeInputChanged
			}
		}
		for _, row := range rows {
			if err := validateKnowledgeOutputDisplay(row, g.auxiliary.Auxiliary); err != nil {
				return nil, err
			}
		}
		for _, proof := range g.auxiliary.Auxiliary.Proofs {
			if proof == nil {
				return nil, types.ErrSourceLineageUnknown
			}
			g.lineage, err = types.UnionSourceLineage(g.lineage, proof.SourceLineage)
			if err != nil {
				return nil, err
			}
		}
	}
	if err := g.Check(ctx, rows); err != nil {
		return nil, err
	}
	return g, nil
}

// Check rechecks the retained inputs and their current read controls.
func (g *KnowledgeOutputGuard) Check(ctx context.Context, rows []*types.Knowledge) error {
	if ctx == nil || ctx.Err() != nil {
		if ctx != nil {
			return ctx.Err()
		}
		return access.ErrNextcloudPublicationUnavailable
	}
	if g == nil || g.authorizer == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	checkBodies := func() error {
		for _, row := range rows {
			entry, ok := g.entries[knowledgeOutputKey(row)]
			if !ok {
				return repository.ErrKnowledgeInputChanged
			}
			body, err := knowledgeOutputJoinedBody(row)
			if err != nil {
				return err
			}
			if body != entry.body {
				return repository.ErrKnowledgeInputChanged
			}
		}
		return nil
	}
	if err := checkBodies(); err != nil {
		return err
	}
	if err := g.checkFAQResults(ctx); err != nil {
		return err
	}
	// Callbacks receive detached private data, never the retained authority.
	lineage, err := types.UnionSourceLineage(g.lineage)
	if err != nil {
		return err
	}
	scopeBytes, err := json.Marshal(g.scope)
	if err != nil {
		return err
	}
	var scope access.LineageReadScope
	if err := json.Unmarshal(scopeBytes, &scope); err != nil {
		return err
	}
	if err := g.authorizer.checker.Check(ctx, lineage, scope); err != nil {
		return err
	}
	if err := g.checkFAQResults(ctx); err != nil {
		return err
	}
	if g.auxiliary != nil {
		reader, ok := g.authorizer.reader.(interfaces.KnowledgeOutputAuxiliaryInputReader)
		if !ok {
			return types.ErrSourceLineageUnknown
		}
		auxiliary, err := cloneCollection(g.auxiliary)
		if err != nil {
			return err
		}
		if err := reader.RecheckKnowledgeOutputAuxiliary(ctx, auxiliary); err != nil {
			return err
		}
	}
	// Recheck all original receipts coherently after remote authorization.
	proofBytes, err := json.Marshal(g.proofs)
	if err != nil {
		return err
	}
	var proofs []*types.KnowledgeBuildInputProof
	if err := json.Unmarshal(proofBytes, &proofs); err != nil {
		return err
	}
	if err := g.authorizer.reader.RecheckKnowledgeOutputInputs(ctx, proofs); err != nil {
		return err
	}
	// A directory/share change during the local material snapshot must not
	// reuse the earlier remote decision at the final response boundary.
	lineage, err = types.UnionSourceLineage(g.lineage)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(scopeBytes, &scope); err != nil {
		return err
	}
	if err := g.authorizer.checker.Check(ctx, lineage, scope); err != nil {
		return err
	}
	// The last remote decision is followed by a coherent recheck of every
	// explicit original document and every actually consumed joined producer.
	if g.auxiliary != nil {
		reader, ok := g.authorizer.reader.(interfaces.KnowledgeOutputAuxiliaryInputReader)
		if !ok {
			return types.ErrSourceLineageUnknown
		}
		auxiliary, err := cloneCollection(g.auxiliary)
		if err != nil {
			return err
		}
		if err := reader.RecheckKnowledgeOutputAuxiliary(ctx, auxiliary); err != nil {
			return err
		}
	} else {
		proofs, err := cloneCollection(g.proofs)
		if err != nil {
			return err
		}
		if err := g.authorizer.reader.RecheckKnowledgeOutputInputs(ctx, proofs); err != nil {
			return err
		}
	}
	if err := g.checkFAQResults(ctx); err != nil {
		return err
	}
	return checkBodies()
}

func (g *KnowledgeOutputGuard) checkFAQResults(ctx context.Context) error {
	if len(g.faqResults) == 0 {
		return nil
	}
	reader, ok := g.authorizer.reader.(interfaces.FAQImportOutputInputReader)
	if !ok {
		return types.ErrSourceLineageUnknown
	}
	for _, snapshot := range g.faqResults {
		if err := checkFAQImportOutputReceipt(ctx, reader, g.authorizer.checker, snapshot); err != nil {
			return err
		}
	}
	return nil
}

// FAQResultReadPlan plans original material reads for FAQ result output.
func (a *KnowledgeOutputAuthorizer) FAQResultReadPlan(
	ctx context.Context,
	ids []string,
) (*types.FAQImportOutputReadSetPlan, error) {
	if a == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	store, ok := a.reader.(*repository.KnowledgeInputProvenanceStore)
	if !ok || store == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	return store.PeekFAQImportOutputReadSetPlan(ctx, nil, ids)
}
