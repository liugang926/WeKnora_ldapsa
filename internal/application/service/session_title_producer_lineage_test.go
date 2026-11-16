package service

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type sessionTitleLineageModel struct {
	t      *testing.T
	mutate func()
}

func (m sessionTitleLineageModel) Chat(
	_ context.Context,
	messages []chat.Message,
	_ *chat.ChatOptions,
) (*types.ChatResponse, error) {
	require.Len(m.t, messages, 2)
	require.Equal(m.t, "system", messages[0].Role)
	require.Equal(m.t, sessionTitleInjectionGuard, messages[0].Content)
	require.Equal(m.t, "user", messages[1].Role)
	require.Equal(m.t, "<user_message>\nraw human question\n</user_message>\n\n"+
		"Write the session title for the message above. Output only the title.", messages[1].Content)
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

func (m sessionTitleLineageModel) ChatStream(
	context.Context,
	[]chat.Message,
	*chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
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
	for _, mutation := range []string{"none", "different", "same text", "ABA", "owner transferred", "legacy assigned"} {
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
			ownerChange := mutation == "owner transferred" || mutation == "legacy assigned"
			if mutation == "legacy assigned" {
				row.UserID = ""
			}
			require.NoError(t, db.Create(row).Error)
			model := sessionTitleLineageModel{t: t}
			if manualRace {
				model.mutate = func() {
					if ownerChange {
						require.NoError(t, db.Model(row).Update("user_id", "new-owner").Error)
						return
					}
					if mutation == "different" {
						require.NoError(
							t,
							db.Model(&types.Session{}).
								Where("id = ?", row.ID).
								Updates(map[string]any{"title": "manual title", "title_source_lineage": empty}).
								Error,
						)
					} else {
						if mutation == "ABA" {
							require.NoError(
								t,
								db.Model(&types.Session{}).
									Where("id = ?", row.ID).
									Update("title", "intermediate").
									Error,
							)
						}
						require.NoError(
							t,
							db.Model(&types.Session{}).
								Where("id = ?", row.ID).
								Update("title", "legacy secret title").
								Error,
						)
					}
				}
			}
			svc := &sessionService{
				sessionRepo:  repository.NewSessionRepository(db),
				modelService: sessionTitleLineageModels{model: model},
				cfg:          &config.Config{Conversation: &config.ConversationConfig{}},
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
			// The authenticated reader's loaded row, not the caller DTO, supplies the write scope.
			input := &types.Session{ID: row.ID, TenantID: row.TenantID}
			title, err := svc.GenerateTitle(
				ragReadContext(),
				input,
				[]types.Message{{Role: "user", Content: "raw human question"}},
				"title-model",
			)
			if manualRace {
				require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
				require.Empty(t, title)
			} else {
				require.NoError(t, err)
				require.Equal(t, "new model title", title)
			}
			owner := "reader"
			if ownerChange {
				owner = "new-owner"
			}
			stored, err := svc.sessionRepo.Get(context.Background(), 7, owner, row.ID)
			require.NoError(t, err)
			if mutation == "same text" || mutation == "ABA" || ownerChange {
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
