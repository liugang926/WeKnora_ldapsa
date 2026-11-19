package im

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/types"
)

func TestDingTalkUsageIdentityIsPerSubmission(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	conn, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, db.Exec(`
		CREATE TABLE im_usage_submissions(event_key TEXT PRIMARY KEY, source TEXT, session_id TEXT,
		 actor_user_id TEXT, created_at DATETIME);
		CREATE TABLE dingtalk_identities(corp_id TEXT, dingtalk_user_id TEXT, user_id TEXT, status TEXT);
		INSERT INTO dingtalk_identities VALUES ('corp','s','a','active'),('other','s','b','active'),
		('corp','u','c','unlinked');
	`).Error)
	s := &Service{db: db}
	channel := &IMChannel{Platform: "dingtalk", ID: "channel"}
	msg := &IncomingMessage{
		Platform: PlatformDingtalk, UserID: "s", SenderCorpID: "corp", SenderStaffID: "s", MessageID: "m",
	}
	ctx := s.withTurnUsage(context.Background(), channel, msg)
	first := stampIMTurn(ctx, &types.Message{})
	require.Equal(t, "a", first.UsageActorUserID)
	require.Equal(t, "dingtalk", first.UsageSource)
	require.Len(t, first.UsageEventKey, 64)
	require.NotEqual(t, first.UsageEventKey, dingTalkEventKey("another", "m"))
	require.Equal(t, "", dingTalkEventKey(channel.ID, ""))
	require.NoError(t, db.Exec("UPDATE dingtalk_identities SET status='unlinked' WHERE user_id='a'").Error)
	require.False(t, s.recordTurnSubmission(ctx, "shared"))
	// Redelivery after an unlink is the same accepted event, not a second question.
	require.True(t, s.recordTurnSubmission(s.withTurnUsage(context.Background(), channel, msg), "shared"))
	var actors []string
	require.NoError(t, db.Table("im_usage_submissions").Pluck("actor_user_id", &actors).Error)
	require.Equal(t, []string{"a"}, actors)
	require.Equal(t, "a", imAssistantMessage(ctx, "shared", "req", nil).UsageActorUserID)
	for _, c := range []struct{ corp, staff, sender, want string }{
		{"other", "s", "s", "b"}, {"corp", "u", "u", ""}, {"corp", "", "s", ""}, {"", "s", "s", ""},
		{"other", "s", "bad", ""}, {"corp", "s", "s", ""},
	} {
		msg.SenderCorpID, msg.SenderStaffID, msg.UserID = c.corp, c.staff, c.sender
		m := stampIMTurn(s.withTurnUsage(context.Background(), channel, msg), &types.Message{})
		require.Equal(t, c.want, m.UsageActorUserID)
	}
}

type usageMessages struct {
	fullOutputMessageService
	created []*types.Message
	updated *types.Message
}

func (m *usageMessages) CreateMessage(ctx context.Context, msg *types.Message) (*types.Message, error) {
	created, err := m.fullOutputMessageService.CreateMessage(ctx, msg)
	m.created = append(m.created, created)
	return created, err
}

func (m *usageMessages) UpdateMessage(_ context.Context, msg *types.Message) error {
	m.updated = msg
	return nil
}

func TestIMQuestionAndAnswerCarryTurnUsage(t *testing.T) {
	for _, stream := range []bool{false, true} {
		msgs := &usageMessages{}
		s := &Service{
			messageService: msgs, streamManager: &fullOutputStreamManager{},
			sessionService: &exitSessionService{run: func(ctx context.Context, bus *event.EventBus) error {
				return bus.Emit(ctx, event.Event{Type: event.EventAgentFinalAnswer, Data: event.AgentFinalAnswerData{
					Content: "ok", Done: true, Usage: &types.TokenUsage{PromptTokens: 8, CompletionTokens: 3},
				}})
			}},
		}
		ctx := context.WithValue(context.Background(), turnUsageKey{}, turnUsage{"dingtalk", "employee", "key"})
		session := &types.Session{ID: "shared-thread"}
		if stream {
			err := s.handleMessageStream(ctx, &IncomingMessage{Content: "question"}, session, nil, nil, nil, nil,
				&recordingStreamSender{}, nil, "", nil)
			require.NoError(t, err)
		} else {
			_, err := s.runQA(ctx, session, "question", nil, nil, nil, nil, "", nil)
			require.NoError(t, err)
		}
		require.Len(t, msgs.created, 2)
		for _, m := range msgs.created {
			require.Equal(t, "key", m.UsageEventKey)
			require.Equal(t, "employee", m.UsageActorUserID)
			require.Equal(t, "dingtalk", m.UsageSource)
		}
		require.NotNil(t, msgs.updated.Usage)
		require.Equal(t, 8, msgs.updated.Usage.PromptTokens)
		require.Equal(t, 3, msgs.updated.Usage.CompletionTokens)
	}
}
