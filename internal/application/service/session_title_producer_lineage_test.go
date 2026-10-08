package service

import (
	"context"
	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"os"
	"strings"
	"testing"
)

type sessionTitleLineageModel struct {
	t      *testing.T
	mutate func()
}

func (m sessionTitleLineageModel) Chat(_ context.Context, messages []chat.Message, _ *chat.ChatOptions) (*types.ChatResponse, error) {
	require.Len(m.t, messages, 2)
	require.Equal(m.t, "raw human question", messages[1].Content)
	for _, input := range messages {
		require.NoError(m.t, input.SourceLineage.RequireComplete())
		require.Empty(m.t, input.SourceLineage.Sources)
		require.NotContains(m.t, input.Content, "legacy secret")
	}
	if m.mutate != nil {
		m.mutate()
	}
	return &types.ChatResponse{Content: "new model title"}, nil
}
func (m sessionTitleLineageModel) ChatStream(context.Context, []chat.Message, *chat.ChatOptions) (<-chan types.StreamResponse, error) {
	panic("not streaming")
}
func (m sessionTitleLineageModel) GetModelName() string { return "title" }
func (m sessionTitleLineageModel) GetModelID() string   { return "title" }

type sessionTitleLineageModels struct {
	interfaces.ModelService
	model chat.Chat
}

func (s sessionTitleLineageModels) GetChatModel(context.Context, string) (chat.Chat, error) {
	return s.model, nil
}

func TestSessionTextLineageActualTitleModelWriterAndManualRace(t *testing.T) {
	for _, mutation := range []string{"none", "different", "same text", "ABA"} {
		manualRace := mutation != "none"
		t.Run(mutation, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
			require.NoError(t, err)
			require.NoError(t, db.AutoMigrate(&types.Session{}))
			triggerSQL, readErr := os.ReadFile("../../../migrations/sqlite/000052_session_text_source_lineage.up.sql")
			require.NoError(t, readErr)
			require.NoError(t, db.Exec(string(triggerSQL)[strings.Index(string(triggerSQL), "CREATE TRIGGER"):]).Error)
			empty, _ := types.NewCompleteSourceLineage()
			row := &types.Session{TenantID: 7, UserID: "reader", Title: "legacy secret title"}
			require.NoError(t, db.Create(row).Error)
			model := sessionTitleLineageModel{t: t}
			if manualRace {
				model.mutate = func() {
					if mutation == "different" {
						require.NoError(t, db.Model(&types.Session{}).Where("id = ?", row.ID).Updates(map[string]any{"title": "manual title", "title_source_lineage": empty}).Error)
					} else {
						if mutation == "ABA" {
							require.NoError(t, db.Model(&types.Session{}).Where("id = ?", row.ID).Update("title", "intermediate").Error)
						}
						require.NoError(t, db.Model(&types.Session{}).Where("id = ?", row.ID).Update("title", "legacy secret title").Error)
					}
				}
			}
			svc := &sessionService{sessionRepo: repository.NewSessionRepository(db), modelService: sessionTitleLineageModels{model: model}, cfg: &config.Config{Conversation: &config.ConversationConfig{}}, sourceLineageGuard: access.NewNextcloudSourceLineageGuard(nil, nil, nil, nil, nil, nil, ragReadActorFixture{})}
			title, err := svc.GenerateTitle(ragReadContext(), row, []types.Message{{Role: "user", Content: "raw human question"}}, "title-model")
			if manualRace {
				require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
				require.Empty(t, title)
			} else {
				require.NoError(t, err)
				require.Equal(t, "new model title", title)
			}
			stored, err := svc.sessionRepo.Get(context.Background(), 7, "reader", row.ID)
			require.NoError(t, err)
			if mutation == "same text" || mutation == "ABA" {
				require.Nil(t, stored.TitleSourceLineage)
			} else {
				require.Equal(t, empty, stored.TitleSourceLineage)
			}
			if manualRace {
				if mutation == "different" {
					require.Equal(t, "manual title", stored.Title)
				} else {
					require.Equal(t, "legacy secret title", stored.Title)
				}
			} else {
				require.Equal(t, "new model title", stored.Title)
			}
		})
	}
}
