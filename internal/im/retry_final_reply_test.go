package im

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type retryCardAdapter struct {
	*fullOutputAdapter
	afterCard func()
}

func (a *retryCardAdapter) FinalizeStream(ctx context.Context, msg *IncomingMessage, id, content string) error {
	err := a.fullOutputAdapter.FinalizeStream(ctx, msg, id, content)
	a.afterCard()
	return err
}

func TestIMRetryUsesCurrentMessageAndChannel(t *testing.T) {
	for _, mode := range []string{"stream", "full"} {
		for _, change := range []string{
			"card succeeds", "temporary rejection", "channel revoked", "KB revoked",
			"session reassigned", "message removed",
		} {
			t.Run(mode+"/"+change, func(t *testing.T) {
				db, messages := newIMPublicationMessages(t)
				agent := &types.CustomAgent{
					ID: "agent", TenantID: 7,
					Config: types.CustomAgentConfig{KBSelectionMode: "all"},
				}
				require.NoError(t, db.AutoMigrate(&types.CustomAgent{}, &types.KnowledgeBase{}))
				require.NoError(t, db.Create(agent).Error)
				require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7, Name: "KB"}).Error)
				session := &types.Session{ID: "session", TenantID: 7, UserID: "im_session:test"}
				require.NoError(t, db.Exec("INSERT INTO sessions(id,tenant_id,user_id) VALUES (?,?,?)",
					session.ID, session.TenantID, session.UserID).Error)
				msg := &IncomingMessage{Platform: PlatformDingtalk, UserID: "sender", Content: "question"}
				channel := &IMChannel{ID: "channel", TenantID: 7, OutputMode: mode}
				ctx := withIMIdentity(context.Background(), channel, msg)
				allowed, kbAllowed := true, true
				ctx = types.WithIMChannelScope(ctx, types.IMChannelScope{
					TenantID: 7, ChannelID: channel.ID, OwnerID: "im_session:test", AgentID: "agent",
					KnowledgeBases: map[string]uint64{"kb": 7},
					Verify: func(_ context.Context, _ uint64, kind types.ResourceType,
						_ string, _ types.ResourceAction,
					) error {
						if kind == types.GroupResourceTypeKnowledgeBase && !kbAllowed {
							return errors.New("KB revoked")
						}
						if !allowed {
							return errors.New("channel revoked")
						}
						return nil
					},
				})
				const answer = "private answer"
				svc, adapter, _, _ := newFullOutputHarness(answer)
				svc.messageService = messages
				svc.sessionService = &exitSessionService{run: func(ctx context.Context, bus *event.EventBus) error {
					proof, err := types.NewCompleteSourceLineage()
					if err != nil {
						return err
					}
					return bus.Emit(ctx, event.Event{
						Type: event.EventAgentFinalAnswer, SourceLineage: proof,
						Data: event.AgentFinalAnswerData{Content: answer, Done: true},
					})
				}}
				card := &retryCardAdapter{fullOutputAdapter: adapter, afterCard: func() {
					switch change {
					case "KB revoked":
						kbAllowed = false
					case "channel revoked":
						allowed = false
					case "session reassigned":
						require.NoError(t, db.Exec("UPDATE sessions SET user_id='other' WHERE id='session'").Error)
					case "message removed":
						require.NoError(t, db.Exec(
							"UPDATE messages SET deleted_at=CURRENT_TIMESTAMP WHERE role='assistant'",
						).Error)
					}
				}}
				if change != "card succeeds" {
					card.finalizeErr = errors.New("platform timeout")
				}
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				svc.executeQARequest(&qaRequest{
					ctx: ctx, cancel: cancel, msg: msg, session: session, agent: agent,
					channel: channel, adapter: card, userKey: "turn",
				})
				require.Equal(t, answer, card.finalContent)
				if change == "temporary rejection" {
					require.Equal(t, 1, card.plainReplies)
					require.Equal(t, answer, card.plainContent)
				} else {
					require.Zero(t, card.plainReplies)
				}
			})
		}
	}
}

func TestIMRetryRendersResourcesAfterCardFailure(t *testing.T) {
	for _, mode := range []string{"stream", "full"} {
		t.Run(mode, func(t *testing.T) {
			f := newIMResourceStorageFixture(t)
			svc, adapter, _, _ := newFullOutputHarness(f.answer)
			f.wire(svc)
			card := &retryCardAdapter{fullOutputAdapter: adapter, afterCard: func() {
				require.NoError(t, f.db.Where("id=?", "share").Delete(&types.KnowledgeBaseShare{}).Error)
				require.NoError(t, f.db.Model(&types.StorageBackend{}).Where("id=?", "oss-0").
					Update("status", types.StorageBackendStatusDisabled).Error)
			}}
			card.finalizeErr = errors.New("platform timeout")
			ctx, cancel := context.WithCancel(f.context())
			defer cancel()
			svc.executeQARequest(&qaRequest{
				ctx: ctx, cancel: cancel, tenant: f.tenant, channel: &IMChannel{OutputMode: mode}, adapter: card,
				msg:     &IncomingMessage{Platform: PlatformWeCom, UserID: "test-user", Content: "Show images"},
				session: &types.Session{ID: "session"}, userKey: "turn",
			})
			f.checkSignedImages(t, card.finalContent)
			require.Equal(t, 1, card.plainReplies)
			require.Contains(t, card.plainContent, f.refs[0])
			require.Contains(t, card.plainContent, "instance-key-1")
			require.Contains(t, card.plainContent, f.refs[2])
			require.NotContains(t, card.plainContent, "instance-key-0")
			require.NotContains(t, card.plainContent, "instance-key-2")
		})
	}
}
