package im

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	appservice "github.com/Tencent/WeKnora/internal/application/service"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestIMRealMessageProducerBothOutputPaths(t *testing.T) {
	for _, fault := range []string{"none", "unknown", "revoked"} {
		for _, stream := range []bool{false, true} {
			t.Run(map[bool]string{false: "plain", true: "stream"}[stream]+fault, func(t *testing.T) {
				db := newLifecycleTestDB(t)
				sqlDB, _ := db.DB()
				sqlDB.SetMaxOpenConns(1)
				require.NoError(t,
					db.Exec("CREATE TABLE sessions(id TEXT PRIMARY KEY,tenant_id "+
						"INTEGER,user_id TEXT,deleted_at DATETIME)").Error)
				require.NoError(t, db.AutoMigrate(&types.Message{}))
				require.NoError(t, db.Exec("ALTER TABLE messages ADD COLUMN artifacts TEXT").Error)
				ddl, err := os.ReadFile("../../migrations/sqlite/000070_message_original_materials.up.sql")
				require.NoError(t, err)
				require.NoError(t, db.Exec(string(ddl)).Error)
				for _, name := range []string{
					"000023_message_artifacts_table.up.sql",
					"000026_message_artifacts_deleted_at.up.sql",
				} {

					ddl, e := os.ReadFile("../../migrations/sqlite/" + name)
					require.NoError(t, e)
					require.NoError(t, db.Exec(string(ddl)).Error)
				}
				store := repository.NewKnowledgeInputProvenanceStore(db)
				factory := func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
					s := repository.NewKnowledgeInputProvenanceStore(tx)
					repository.ConfigureKnowledgeInputActorGroupAccess(
						s,
						appservice.NewGroupAccessService(repository.NewGroupAccessRepository(
							tx)))
					return s
				}
				repository.ConfigureKnowledgeDisplayInputWriters(store, nil, nil, factory)
				repository.ConfigureKnowledgeInputActorGroupAccess(
					store,
					appservice.NewGroupAccessService(repository.NewGroupAccessRepository(
						db)))
				messages := appservice.NewMessageService(
					repository.NewMessageRepository(db),
					repository.NewSessionRepository(db),
					nil,
					nil,
					nil,
					nil,
					nil,
					nil,
					nil)
				appservice.ConfigureMessageMaterialProvenance(messages, store)
				appservice.ConfigureMessageSourceLineage(
					messages,
					nil,
					&appservice.SessionForkService{},
					chatpipeline.NewEventManager(),
					access.NewNextcloudSourceLineageGuard(nil,
						nil,
						nil,
						nil,
						appservice.NewGroupAccessService(repository.NewGroupAccessRepository(
							db)),
						nil,
						store))
				var valid atomic.Bool
				valid.Store(true)
				ctx := withIMIdentity(context.Background(),
					7,
					"channel",
					&IncomingMessage{
						Platform: PlatformDingtalk,
						UserID:   "sender",
					})
				ctx = types.WithIMChannelScope(
					ctx,
					types.IMChannelScope{
						TenantID:  7,
						ChannelID: "channel",
						OwnerID:   "im_session:test",
						AgentID:   "agent",
						Verify: func(context.Context,
							uint64,
							types.ResourceType,
							string,
							types.ResourceAction,
						) error {
							if !valid.Load() {
								return errors.New("revoked")
							}
							return nil
						},
					})
				require.NoError(t,
					db.Exec(`INSERT INTO sessions(id,tenant_id,user_id)VALUES('session',7,'im_session:test')`).Error)
				svc := &Service{
					messageService: messages,
					streamManager:  &fullOutputStreamManager{},
					sessionService: &exitSessionService{
						run: func(ctx context.Context,
							bus *event.EventBus,
						) error {
							lineage, _ := types.NewCompleteSourceLineage()
							if fault == "unknown" {
								lineage = types.UnknownSourceLineage()
							}
							if fault == "revoked" {
								valid.Store(false)
							}
							if fault != "none" {
								_ = bus.Emit(ctx,
									event.Event{
										Type:          event.EventAgentFinalAnswer,
										SourceLineage: lineage,
										Data: event.AgentFinalAnswerData{
											Content: "fixture response",
											Done:    false,
										},
									})
								time.Sleep(600 * time.Millisecond)
							}
							_ = bus.Emit(ctx,
								event.Event{
									Type:          event.EventAgentFinalAnswer,
									SourceLineage: lineage,
									Data: event.AgentFinalAnswerData{
										Content: "fixture response",
										Done:    true,
									},
								})
							return bus.Emit(ctx,
								event.Event{
									Type:          event.EventAgentComplete,
									SourceLineage: lineage,
									Data: event.AgentCompleteData{
										FinalAnswer: "fixture response",
									},
								})
						},
					},
				}
				agent := &types.CustomAgent{
					ID:       "agent",
					TenantID: 7,
					Config: types.CustomAgentConfig{
						AgentMode:       types.AgentModeSmartReasoning,
						KBSelectionMode: "none",
					},
				}
				for turn := 0; turn < 2; turn++ {
					session := &types.Session{ID: "session", TenantID: 7, UserID: "im_session:test"}
					turnCtx, cancelTurn := context.WithCancel(ctx)
					ownedCtx, owner := svc.beginInflight(turnCtx, "key", cancelTurn)
					var runErr error
					if stream {
						adapter := &recordingStreamSender{}
						runErr = svc.handleMessageStream(
							ownedCtx,
							&IncomingMessage{
								Platform: PlatformDingtalk,
								UserID:   "sender",
								Content:  "fixture question",
							},
							session,
							agent,
							nil,
							nil,
							nil,
							adapter,
							nil,
							"key",
							nil)
						if fault != "none" {
							updates, final, _ := adapter.snapshot()
							require.Empty(t, updates)
							require.Empty(t, final)
						}
					} else {
						answer, e := svc.runQA(ownedCtx, session, "fixture question", agent, nil, nil, nil, "key", nil)
						runErr = e
						if fault == "none" {
							require.NotEmpty(t, answer)
						}
					}
					owner.mu.Lock()
					boundSession, boundMessage := owner.sessionID, owner.assistantMessageID
					owner.mu.Unlock()
					svc.finishInflight(ownedCtx, "key", owner)
					cancelTurn()
					if fault == "none" {
						require.Equal(t, session.ID, boundSession)
						require.NotEmpty(t, boundMessage)
					}
					if fault != "none" {
						require.Error(t, runErr)
						return
					}
					require.NoError(t, runErr)

					var count int64
					require.NoError(t,
						db.Model(&types.Message{}).Where(
							"role='assistant' AND is_completed=true AND content<>''",
						).Count(
							&count).Error)
					require.Equal(t, int64(turn+1), count)
				}
				var origins, receipts int64
				require.NoError(t, db.Table("message_generation_origins").Count(&origins).Error)
				require.NoError(t, db.Table("message_material_receipts").Count(&receipts).Error)
				require.Equal(t, int64(4), origins)
				require.Equal(t, origins, receipts)
			})
		}
	}
}

func TestIMCompletionCannotReusePrefixForUnknownAnswer(t *testing.T) {
	p := &imMessageProvenance{}
	lineage, _ := types.NewCompleteSourceLineage()
	p.observe(event.Event{SourceLineage: lineage, Data: event.AgentFinalAnswerData{Content: "known prefix"}})
	p.observe(event.Event{Data: event.AgentCompleteData{FinalAnswer: "unknown replacement"}})
	require.Error(t, p.lineage.RequireComplete())
}
