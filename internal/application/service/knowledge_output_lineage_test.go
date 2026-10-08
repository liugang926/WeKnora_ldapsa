package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type outputInputReaderFixture struct {
	snapshot          *types.KnowledgeBuildInputSnapshot
	captures          int
	expectedAdmission string
	afterRecheck      func([]*types.KnowledgeBuildInputProof)
}

func (f *outputInputReaderFixture) CaptureKnowledgeOutputInputs(context.Context, types.KnowledgeBuildInputCaptureRequest) (*types.KnowledgeBuildInputSnapshot, error) {
	f.captures++
	return f.snapshot, nil
}
func (f *outputInputReaderFixture) RecheckKnowledgeBuildInputs(context.Context, *types.KnowledgeBuildInputProof) error {
	return nil
}
func (f *outputInputReaderFixture) RecheckKnowledgeOutputInputs(_ context.Context, proofs []*types.KnowledgeBuildInputProof) error {
	for _, proof := range proofs {
		if proof.AdmissionID != f.expectedAdmission {
			return repository.ErrKnowledgeInputChanged
		}
	}
	if f.afterRecheck != nil {
		f.afterRecheck(proofs)
	}
	return nil
}

type outputCheckerFunc func(context.Context, *types.SourceLineage, access.LineageReadScope) error

func (f outputCheckerFunc) Check(ctx context.Context, lineage *types.SourceLineage, scope access.LineageReadScope) error {
	return f(ctx, lineage, scope)
}

func outputAuthorizerFixture(t *testing.T) (*KnowledgeOutputAuthorizer, *outputInputReaderFixture, *types.Knowledge) {
	t.Helper()
	lineage, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	row := &types.Knowledge{ID: "document", TenantID: 7, KnowledgeBaseID: "kb", Type: "manual", Title: "authored title", Metadata: types.JSON(`{"manual":{"content":"actual authored body"}}`)}
	reader := &outputInputReaderFixture{snapshot: &types.KnowledgeBuildInputSnapshot{Knowledge: row, Proof: &types.KnowledgeBuildInputProof{Version: 1, TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "document", AdmissionID: "original-admission", BuildGeneration: 1, BodyDigest: "actual-material-digest", SourceLineage: lineage}}, expectedAdmission: "original-admission"}
	a := &KnowledgeOutputAuthorizer{reader: reader, checker: outputCheckerFunc(func(context.Context, *types.SourceLineage, access.LineageReadScope) error { return nil })}
	return a, reader, row
}

func TestKnowledgeOutputLineageAdmitsActualMaterialAndRetainsOriginalProof(t *testing.T) {
	a, reader, row := outputAuthorizerFixture(t)
	g, err := a.Begin(context.Background(), []*types.Knowledge{row})
	require.NoError(t, err)
	require.NoError(t, g.Check(context.Background(), []*types.Knowledge{row}))
	// Even equal current bytes with a newly admitted origin cannot re-certify
	// a response whose original proof has already been captured.
	reader.snapshot.Proof.AdmissionID = "new-current-admission"
	reader.expectedAdmission = "new-current-admission"
	require.ErrorIs(t, g.Check(context.Background(), []*types.Knowledge{row}), repository.ErrKnowledgeInputChanged)
	require.Equal(t, 1, reader.captures)
}

func TestKnowledgeOutputLineageRechecksCurrentGrantsAtEveryBoundary(t *testing.T) {
	a, _, row := outputAuthorizerFixture(t)
	revoked := false
	a.checker = outputCheckerFunc(func(_ context.Context, _ *types.SourceLineage, scope access.LineageReadScope) error {
		require.Len(t, scope.Targets, 1)
		require.Equal(t, "kb", scope.Targets[0].KnowledgeBaseID)
		if revoked {
			return access.ErrNextcloudPublicationDenied
		}
		return nil
	})
	g, err := a.Begin(context.Background(), []*types.Knowledge{row})
	require.NoError(t, err)
	revoked = true
	require.ErrorIs(t, g.Check(context.Background(), []*types.Knowledge{row}), access.ErrNextcloudPublicationDenied)
}

func TestKnowledgeOutputLineageCallbacksCannotMutateRetainedAuthority(t *testing.T) {
	a, reader, row := outputAuthorizerFixture(t)
	checks := 0
	a.checker = outputCheckerFunc(func(_ context.Context, lineage *types.SourceLineage, scope access.LineageReadScope) error {
		checks++
		require.NoError(t, lineage.RequireComplete())
		require.Equal(t, "kb", scope.Targets[0].KnowledgeBaseID)
		lineage.State = types.SourceLineageUnknown
		scope.Targets[0].KnowledgeBaseID = "other"
		return nil
	})
	reader.afterRecheck = func(proofs []*types.KnowledgeBuildInputProof) { proofs[0].AdmissionID = "callback mutation" }
	g, err := a.Begin(context.Background(), []*types.Knowledge{row})
	require.NoError(t, err)
	require.NoError(t, g.Check(context.Background(), []*types.Knowledge{row}))
	require.Equal(t, 2, checks)
}

func TestKnowledgeOutputLineageRejectsWireMutationDuringAuthorization(t *testing.T) {
	a, reader, row := outputAuthorizerFixture(t)
	reader.afterRecheck = func([]*types.KnowledgeBuildInputProof) { row.Metadata[2] = 'x' }
	g, err := a.Begin(context.Background(), []*types.Knowledge{row})
	require.Error(t, err)
	require.Nil(t, g)
}

func TestKnowledgeOutputLineageRejectsUnknownForeignAndUncoveredFields(t *testing.T) {
	for _, field := range []string{"unknown", "foreign_proof", "foreign_body", "joined_tag", "joined_kb", "unaccounted_error", "faq_report"} {
		t.Run(field, func(t *testing.T) {
			a, reader, row := outputAuthorizerFixture(t)
			switch field {
			case "unknown":
				reader.snapshot.Proof.SourceLineage = types.UnknownSourceLineage()
			case "foreign_proof":
				reader.snapshot.Proof.KnowledgeID = "other"
			case "foreign_body":
				other := *row
				other.Title = "unrelated content"
				reader.snapshot.Knowledge = &other
			case "joined_tag":
				row.Tags = []*types.KnowledgeTag{{Name: "unaccounted generated tag"}}
			case "joined_kb":
				row.KnowledgeBaseName = "unaccounted KB text"
			case "unaccounted_error":
				row.ErrorMessage = "unaccounted provider body"
			case "faq_report":
				row.LastFAQImportResult = types.JSON(`{"text":"unaccounted body"}`)
			}
			g, err := a.Begin(context.Background(), []*types.Knowledge{row})
			require.Error(t, err)
			require.Nil(t, g)
		})
	}
}
