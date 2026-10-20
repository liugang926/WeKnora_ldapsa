package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type libraryMessageRepo struct {
	interfaces.MessageRepository
	query   *types.ArtifactLibraryQuery
	items   []*types.ArtifactLibraryItem
	message *types.Message
}

func (r *libraryMessageRepo) ListArtifactLibrary(
	_ context.Context, q *types.ArtifactLibraryQuery,
) ([]*types.ArtifactLibraryItem, int64, error) {
	r.query = q
	return r.items, int64(len(r.items)), nil
}

// The format/query unit owns these literal metadata values; it never loads
// a generated file or imports unknown stored message fields.
func (r *libraryMessageRepo) GetMessage(_ context.Context, sessionID, messageID string) (*types.Message, error) {
	if r.message == nil || r.message.SessionID != sessionID || r.message.ID != messageID {
		return nil, access.ErrNextcloudPublicationDenied
	}
	return r.message, nil
}

type literalLibraryLineage struct{}

func (literalLibraryLineage) Check(
	_ context.Context,
	lineage *types.SourceLineage,
	scope access.LineageReadScope,
) error {
	if lineage.RequireComplete() != nil || len(lineage.Sources) != 0 || len(scope.Targets) != 0 ||
		scope.AgentID != "" || scope.AgentTenantID != 0 {
		return access.ErrNextcloudPublicationDenied
	}
	return nil
}

func (guard literalLibraryLineage) CheckBatch(ctx context.Context, reads []access.SourceLineageRead) error {
	for _, read := range reads {
		if err := guard.Check(ctx, read.Lineage, read.Scope); err != nil {
			return err
		}
	}
	return nil
}

func TestListArtifactLibraryScopesToCallerAndNormalizesQuery(t *testing.T) {
	handle := "resource://dHZ_fFslfs0GgJGaJZGjGA"
	lineage, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	repo := &libraryMessageRepo{items: []*types.ArtifactLibraryItem{
		{SessionID: "literal-session", MessageID: "literal-answer", FileName: "deck.pptx", URL: handle},
		{SessionID: "literal-session", MessageID: "literal-answer", FileName: "raw.csv", URL: "local://tenant/raw.csv"},
	}}
	repo.message = &types.Message{
		ID: "literal-answer", SessionID: "literal-session", Role: "assistant",
		Content: "Go-owned literal answer for query and URL formatting", SourceLineage: lineage,
	}
	s := &messageService{messageRepo: repo, sourceLineageGuard: literalLibraryLineage{}}
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(7))
	ctx = context.WithValue(ctx, types.UserIDContextKey, "alice")

	result, err := s.ListArtifactLibrary(ctx, &types.ArtifactLibraryQuery{
		TenantID:  99,
		UserID:    "mallory",
		FileTypes: []string{"PDF", " .pptx ", "pdf", ""},
		PageSize:  5000,
	})
	require.NoError(t, err)

	require.Equal(t, uint64(7), repo.query.TenantID, "tenant comes from the context, not the request")
	require.Equal(t, "alice", repo.query.UserID, "owner comes from the context, not the request")
	require.Equal(t, []string{".pdf", ".pptx"}, repo.query.FileTypes)
	require.Equal(t, 1, repo.query.Page)
	require.Equal(t, artifactLibraryMaxPageSize, repo.query.PageSize)
	require.Equal(t, artifactLibraryMaxPageSize, result.PageSize)

	items := result.Data.([]*types.ArtifactLibraryItem)
	require.Equal(t, handle, items[0].Handle)
	require.Empty(t, items[1].Handle, "non-resource URLs never leak as handles")
}

// Stored/generated fields without original producer contracts remain closed;
// the literal format fixture above cannot authorize those data paths.
func TestListArtifactLibraryRefusesUnknownOrUnadmittedGeneratedFields(t *testing.T) {
	for _, mode := range []string{"unknown lineage", "unadmitted generated artifact"} {
		t.Run(mode, func(t *testing.T) {
			lineage, err := types.NewCompleteSourceLineage()
			require.NoError(t, err)
			message := &types.Message{ID: "answer", SessionID: "session", Role: "assistant", SourceLineage: lineage}
			if mode == "unknown lineage" {
				message.SourceLineage = types.UnknownSourceLineage()
			} else {
				message.Artifacts = []types.MessageArtifact{{FileName: "unadmitted.csv", URL: "local://unadmitted.csv"}}
			}
			repo := &libraryMessageRepo{message: message, items: []*types.ArtifactLibraryItem{{
				SessionID: "session", MessageID: "answer", FileName: "protected.csv", URL: "local://protected.csv",
			}}}
			service := &messageService{messageRepo: repo, sourceLineageGuard: literalLibraryLineage{}}
			ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(7))
			ctx = context.WithValue(ctx, types.UserIDContextKey, "alice")
			result, err := service.ListArtifactLibrary(ctx, &types.ArtifactLibraryQuery{})
			require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
			require.Nil(t, result)
		})
	}
}
