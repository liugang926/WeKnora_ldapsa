package service

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/agent/collaboration"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type delegatedInputSessions struct {
	collaborationSessions
	sessions *sessionService
}

func (s *delegatedInputSessions) GetSession(ctx context.Context, id string) (*types.Session, error) {
	owner := types.SessionOwnerIDFromContext(ctx)
	return s.sessions.sessionRepo.Get(ctx, types.CallerFromContext(ctx).TenantID, owner, id)
}

func TestDelegatedInputReopensProducerReceipts(t *testing.T) {
	for _, mutation := range []string{
		"none", "parent owner changes", "parent input changes", "child query changes", "wrong run",
		"stored instruction replaced", "workspace user disabled",
	} {
		t.Run(mutation, func(t *testing.T) {
			messages, _, _, db, ctx, parent := messageMaterialActualFixture(t)
			if !db.Migrator().HasColumn(&types.Session{}, "nextcloud_scoped") {
				const ddl = "ALTER TABLE sessions ADD COLUMN nextcloud_scoped BOOLEAN NOT NULL DEFAULT FALSE"
				require.NoError(t, db.Exec(ddl).Error)
			}
			require.NoError(t, db.AutoMigrate(&types.CustomAgent{}, &collaboration.Record{}))
			a := &types.CustomAgent{
				ID: "writer", TenantID: parent.TenantID, CreatedBy: parent.UserID,
				Config: types.CustomAgentConfig{AgentMode: types.AgentModeSmartReasoning},
			}
			require.NoError(t, db.Create(a).Error)
			// The production container configures message provenance after constructing services.
			store := messages.materialStore
			messages.materialStore = nil
			workers := NewAgentCollaboration(db, &collaborationAgents{agent: a}, messages,
				&collaborationUsers{active: true}, &collaborationMembers{active: true}, &collaborationTenants{})
			ConfigureMessageMaterialProvenance(messages, store)
			sessionSvc := &sessionService{
				sessionRepo: repository.NewSessionRepository(db), lineageVerifier: messages,
				sourceLineageGuard: messages.sourceLineageGuard,
				groupAccess:        NewGroupAccessService(repository.NewGroupAccessRepository(db)),
				agentService:       &agentService{db: db},
			}
			user, err := messages.CreateMessage(ctx, &types.Message{
				SessionID: parent.ID, RequestID: "parent-request",
				Role: "user", Content: "Prepare a report", IsCompleted: true,
			})
			require.NoError(t, err)
			answer, err := messages.CreateMessage(ctx, &types.Message{
				SessionID: parent.ID, RequestID: user.RequestID,
				Role: "assistant", AgentID: a.ID, AgentTenantID: a.TenantID,
			})
			require.NoError(t, err)
			scope := collaboration.Scope{
				TenantID: parent.TenantID, UserID: parent.UserID, SessionID: parent.ID, ID: answer.ID,
			}
			require.NoError(t, workers.Runtime.Store.Ensure(ctx, scope))
			config, err := workers.resolve(ctx, a.ID, types.AssistantAccessConfirm)
			require.NoError(t, err)
			_, err = workers.Runtime.Spawn(ctx, scope, "invocation", "writer", a.ID, config, "draft the report")
			require.NoError(t, err)
			require.NoError(t, workers.Runtime.Store.Change(ctx, scope, func(g *collaboration.Group) error {
				r := g.Members[0].Runs[0]
				r.State = "running"
				r.Owner = "worker"
				r.Lease = time.Now().Add(time.Minute)
				return nil
			}))
			if mutation == "stored instruction replaced" {
				require.NoError(t, workers.Runtime.Store.Change(ctx, scope, func(g *collaboration.Group) error {
					g.Members[0].Runs[0].Query = "substituted in storage"
					return nil
				}))
			}
			group, err := workers.Runtime.Store.Read(ctx, scope)
			require.NoError(t, err)
			work := collaboration.Work{Scope: scope, Member: *group.Members[0], Run: *group.Members[0].Runs[0]}
			didRun := false
			workers.Bind(&delegatedInputSessions{sessions: sessionSvc, collaborationSessions: collaborationSessions{
				run: func(workerCtx context.Context, req *types.QARequest, _ *event.EventBus) error {
					didRun = true
					check := sessionSvc.agentMCPIndependentVerifier(workerCtx, req, &types.AgentConfig{})
					require.NoError(t, check(workerCtx))
					// A resumed worker has a fresh output owner and only the persisted identities.
					checkpoint, err := workers.Runtime.Store.Read(ctx, scope)
					require.NoError(t, err)
					member := checkpoint.Members[0]
					r := member.Runs[0]
					resumed := delegatedWorkerInput{
						work:    collaboration.Work{Scope: scope, Member: *member, Run: *r},
						service: workers, native: messages, userID: r.UserMessageID, assistantID: r.MessageID,
					}
					fresh, _, _, release := BeginMessageMaterialHTTPBoundary(ctx)
					defer release()
					err = resumed.check(fresh)
					require.NoError(t, err)
					fresh = context.WithValue(fresh, delegatedWorkerInputKey{}, &resumed)
					require.NoError(t, resumed.bindChild(fresh, r.UserMessageID, r.MessageID))
					recheck := sessionSvc.agentMCPIndependentVerifier(fresh, req, &types.AgentConfig{})
					require.NoError(t, recheck(fresh))
					switch mutation {
					case "parent owner changes":
						err := db.Exec("UPDATE sessions SET user_id=? WHERE id=?", "other", parent.ID).Error
						require.NoError(t, err)
					case "workspace user disabled":
						err := db.Exec("UPDATE users SET is_active=false WHERE id=?", parent.UserID).Error
						require.NoError(t, err)
					case "parent input changes":
						require.NoError(t, db.Exec("UPDATE messages SET content='replaced' WHERE id=?", user.ID).Error)
					case "child query changes":
						err := db.Exec("UPDATE messages SET content='replaced' WHERE id=?", r.UserMessageID).Error
						require.NoError(t, err)
					case "none":
						return nil
					}
					require.Error(t, check(workerCtx))
					require.Error(t, recheck(fresh))
					return nil
				},
			}})
			if mutation == "wrong run" {
				work.Run.Query = "injected"
			}
			_, err = workers.run(ctx, work)
			if mutation == "none" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, mutation != "wrong run" && mutation != "stored instruction replaced", didRun)
		})
	}
}
