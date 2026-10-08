package service

import (
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

func NewKnowledgeOutputAuthorizer(reader *repository.KnowledgeInputProvenanceStore, guard *access.NextcloudSourceLineageGuard) *KnowledgeOutputAuthorizer {
	return &KnowledgeOutputAuthorizer{reader: reader, checker: guard}
}

type knowledgeOutputEntry struct {
	body  string
	proof *types.KnowledgeBuildInputProof
}

type KnowledgeOutputGuard struct {
	authorizer *KnowledgeOutputAuthorizer
	entries    map[string]knowledgeOutputEntry
	proofs     []*types.KnowledgeBuildInputProof
	lineage    *types.SourceLineage
	scope      access.LineageReadScope
}

type knowledgeOutputGuardContextKey struct{}

func WithKnowledgeOutputGuard(ctx context.Context, guard *KnowledgeOutputGuard) context.Context {
	return context.WithValue(ctx, knowledgeOutputGuardContextKey{}, guard)
}

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

func knowledgeOutputBody(k *types.Knowledge) (string, error) {
	if k == nil || len(k.Tags) > 0 || k.KnowledgeBaseName != "" || k.ErrorMessage != "" || len(k.LastFAQImportResult) > 0 {
		// Joined tag and KB display producers need their own original proof;
		// the document's origin cannot certify unrelated generated strings.
		return "", types.ErrSourceLineageUnknown
	}
	body, err := json.Marshal(k)
	return string(body), err
}

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
	for _, row := range rows {
		if row == nil {
			return nil, access.ErrNextcloudPublicationDenied
		}
		supplied, err := knowledgeOutputBody(row)
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
		actual, err := a.reader.CaptureKnowledgeOutputInputs(ctx, types.KnowledgeBuildInputCaptureRequest{TenantID: row.TenantID, KnowledgeBaseID: row.KnowledgeBaseID, KnowledgeID: row.ID})
		if err != nil {
			return nil, err
		}
		if actual == nil || actual.Proof == nil || actual.Proof.SourceLineage.RequireComplete() != nil ||
			actual.Proof.Version != 1 || actual.Proof.Projection != "" || actual.Proof.TenantID != row.TenantID || actual.Proof.KnowledgeBaseID != row.KnowledgeBaseID ||
			actual.Proof.KnowledgeID != row.ID || actual.Proof.AdmissionID == "" || actual.Proof.BuildGeneration == 0 || actual.Proof.BodyDigest == "" {
			return nil, types.ErrSourceLineageUnknown
		}
		body, err := knowledgeOutputBody(actual.Knowledge)
		if err != nil {
			return nil, err
		}
		if body != supplied {
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
		g.entries[key] = knowledgeOutputEntry{body: body, proof: proof}
		g.proofs = append(g.proofs, proof)
		g.lineage, err = types.UnionSourceLineage(g.lineage, proof.SourceLineage)
		if err != nil {
			return nil, err
		}
		target := fmt.Sprintf("%d:%s", row.TenantID, row.KnowledgeBaseID)
		if !targets[target] {
			targets[target] = true
			g.scope.Targets = append(g.scope.Targets, &types.SearchTarget{Type: types.SearchTargetTypeKnowledgeBase, TenantID: row.TenantID, KnowledgeBaseID: row.KnowledgeBaseID})
		}
	}
	if err := g.Check(ctx, rows); err != nil {
		return nil, err
	}
	return g, nil
}

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
			body, err := knowledgeOutputBody(row)
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
	if err != nil { return err }
	if err := json.Unmarshal(scopeBytes, &scope); err != nil { return err }
	if err := g.authorizer.checker.Check(ctx, lineage, scope); err != nil { return err }
	return checkBodies()
}
