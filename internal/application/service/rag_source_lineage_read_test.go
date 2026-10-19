package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

// Account lifecycle is checked by the real SQL actor tests. This explicit
// component fixture isolates message material/provenance admission.
type ragReadActorFixture struct{}

func (ragReadActorFixture) RefreshCurrentReadActor(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

type ragReadRepo struct {
	interfaces.MessageRepository
	row *types.Message
}

func (r ragReadRepo) GetMessage(context.Context, string, string) (*types.Message, error) {
	copied := *r.row
	return &copied, nil
}

type ragReadSessions struct{ interfaces.SessionRepository }

func (ragReadSessions) Get(context.Context, uint64, string, string) (*types.Session, error) {
	return &types.Session{ID: "session", TenantID: 7, UserID: "reader"}, nil
}
func (ragReadSessions) GetIMPlatform(context.Context, uint64, string) (string, error) { return "", nil }

func ragReadContext() context.Context {
	ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(7))
	ctx = context.WithValue(ctx, types.UserIDContextKey, "reader")
	ctx = types.WithCaller(ctx, types.Caller{TenantID: 7, UserID: "reader", Role: types.TenantRoleViewer})
	return types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "reader"})
}

func TestRAGSourceLineageActualMessageReadDeniesLegacyAndOpaqueGeneratedFields(t *testing.T) {
	empty, _ := types.NewCompleteSourceLineage()
	for _, kind := range []string{"legacy NULL", "unknown", "checkpoint", "caption", "artifact", "complete plain"} {
		t.Run(kind, func(t *testing.T) {
			row := &types.Message{
				ID:            "message",
				SessionID:     "session",
				Role:          "assistant",
				Content:       "generated body",
				IsCompleted:   true,
				SourceLineage: empty,
			}
			switch kind {
			case "legacy NULL":
				row.SourceLineage = nil
			case "unknown":
				row.SourceLineage = types.UnknownSourceLineage()
			case "checkpoint":
				row.ContextCheckpoint = &types.ContextCheckpoint{Summary: "uncaptured summary"}
			case "caption":
				row.Images = types.MessageImages{{URL: "resource://secret", Caption: "copied source"}}
			case "artifact":
				row.Artifacts = types.MessageArtifacts{{FileName: "copied title"}}
			}
			svc := &messageService{
				messageRepo: ragReadRepo{row: row},
				sessionRepo: ragReadSessions{},
				sourceLineageGuard: access.NewNextcloudSourceLineageGuard(
					nil,
					nil,
					nil,
					nil,
					nil,
					nil,
					ragReadActorFixture{},
				),
			}
			got, err := svc.GetMessage(ragReadContext(), "session", "message")
			if kind == "complete plain" {
				require.NoError(t, err)
				require.Equal(t, row.Content, got.Content)
			} else {
				require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
				require.Nil(t, got)
			}
		})
	}
}

func TestRAGSourceLineageActualUserReadRetainsOnlyAuthoredContent(t *testing.T) {
	row := &types.Message{
		ID:              "message",
		SessionID:       "session",
		Role:            "user",
		Content:         "authored question",
		RenderedContent: "copied source",
		Images: types.MessageImages{
			{URL: "resource://secret", Caption: "copied source"},
		},
		Artifacts:         types.MessageArtifacts{{FileName: "secret"}},
		ContextCheckpoint: &types.ContextCheckpoint{Summary: "copied summary"},
	}
	svc := &messageService{messageRepo: ragReadRepo{row: row}, sessionRepo: ragReadSessions{}}
	got, err := svc.GetMessage(ragReadContext(), "session", "message")
	require.NoError(t, err)
	require.Equal(t, row.Content, got.Content)
	require.Empty(t, got.RenderedContent)
	require.Empty(t, got.Images)
	require.Empty(t, got.Artifacts)
	require.Nil(t, got.ContextCheckpoint)
	require.NotEmpty(t, row.Images, "read sanitization mutated stored input")
}
