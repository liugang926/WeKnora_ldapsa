package tools

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type publicationProtocolKnowledgeService struct {
	*readDocKnowledgeService
	check func(context.Context, *types.Knowledge) error
}

func (s *publicationProtocolKnowledgeService) CheckKnowledgePublication(
	ctx context.Context, knowledge *types.Knowledge,
) error {
	return s.check(ctx, knowledge)
}

func requireReadDocumentPublicationProtocol(t *testing.T, id, wantPublic, wantDiagnostic string) {
	t.Helper()
	// This dependency must be the new production semantics. Running against the
	// old capitalized errors.New sentinel would miss the regression entirely.
	cause := errors.Unwrap(access.ErrNextcloudPublicationDenied)
	require.NotNil(t, cause)
	require.Equal(t, "nextcloud publication access denied", cause.Error())
	require.Equal(t, "nextcloud publication access denied", access.ErrNextcloudPublicationDenied.Error())
	require.Equal(t, "Nextcloud publication access denied", apperrors.PublicMessage(
		access.ErrNextcloudPublicationDenied,
	))

	tool, chunks := newReadDocumentFixture(2)
	base := tool.knowledgeService.(*readDocKnowledgeService)
	base.docs["doc-1"].Channel = types.ConnectorTypeNextcloud
	base.docs["doc-1"].Metadata = types.JSON(`{"datasource_id":"owned-source","nextcloud_file_id":"41"}`)
	const protectedMarker = "PROTECTED_OWNED_PROTOCOL_MARKER"
	chunks.ordered[0].Content = protectedMarker
	calls := 0
	tool.knowledgeService = &publicationProtocolKnowledgeService{
		readDocKnowledgeService: base,
		check: func(_ context.Context, knowledge *types.Knowledge) error {
			calls++
			require.Equal(t, "doc-1", knowledge.ID)
			require.Equal(t, "kb-1", knowledge.KnowledgeBaseID)
			return access.ErrNextcloudPublicationDenied
		},
	}
	args, err := json.Marshal(ReadDocumentInput{ID: id})
	require.NoError(t, err)
	result, err := tool.Execute(context.Background(), args)
	require.Error(t, err)
	require.NotNil(t, result)
	require.False(t, result.Success)
	assert.Equal(t, wantPublic, result.Error)
	assert.Equal(t, wantPublic, apperrors.PublicMessage(err))
	require.Equal(t, wantDiagnostic, err.Error())
	require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
	require.ErrorIs(t, err, cause)
	require.Equal(t, 1, calls, "actual publication callback must reject this resolved document")
	require.NotContains(t, result.Output, protectedMarker)
	require.Empty(t, result.Data)
	require.Empty(t, chunks.requests, "no protected document page may be loaded after denial")
}

func TestReadDocumentPublicationDeniedPreservesDocumentPublicError(t *testing.T) {
	const public = "document is not accessible: document doc-1 is not currently accessible: " +
		"Nextcloud publication access denied"
	const diagnostic = "document is not accessible: document doc-1 is not currently accessible: " +
		"nextcloud publication access denied"
	requireReadDocumentPublicationProtocol(t, "doc-1", public, diagnostic)
}

func TestReadDocumentPublicationDeniedPreservesChunkPublicError(t *testing.T) {
	const public = "chunk is not accessible: chunk chunk-0 is not currently accessible: " +
		"Nextcloud publication access denied"
	const diagnostic = "chunk is not accessible: chunk chunk-0 is not currently accessible: " +
		"nextcloud publication access denied"
	requireReadDocumentPublicationProtocol(t, "chunk-0", public, diagnostic)
}
