package session

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type rawUserLineageMessages struct {
	interfaces.MessageService
	created *types.Message
}

func (s *rawUserLineageMessages) CreateMessage(_ context.Context, message *types.Message) (*types.Message, error) {
	s.created = message
	return message, nil
}

func TestRawUserInputLineageActualRequestWriterAdmitsOnlyPureText(t *testing.T) {
	for _, mode := range []string{"raw text", "image", "attachment"} {
		t.Run(mode, func(t *testing.T) {
			messages := &rawUserLineageMessages{}
			handler := &Handler{messageService: messages}
			var images types.MessageImages
			var attachments types.MessageAttachments
			if mode == "image" {
				images = types.MessageImages{{URL: "resource://opaque", Caption: "uncaptured caption"}}
			}
			if mode == "attachment" {
				attachments = types.MessageAttachments{{FileName: "opaque attachment"}}
			}
			created, err := handler.createUserMessage(
				context.Background(),
				"session",
				"original raw request text",
				"request",
				nil,
				images,
				attachments,
				"web",
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, "original raw request text", created.Content)
			require.True(t, created.IsCompleted)
			require.Equal(t, "user", created.Role)
			require.NotNil(t, created.SourceLineage)
			if mode == "raw text" {
				require.NoError(t, created.SourceLineage.RequireComplete())
				require.Empty(t, created.SourceLineage.Sources)
			} else {
				require.ErrorIs(t, created.SourceLineage.RequireComplete(), types.ErrSourceLineageUnknown)
			}
		})
	}
}
