package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestAgentMCPIndependentTransportActualSessionAndMaterial(t *testing.T) {
	for _, mutation := range []string{
		"none", "scoped-session", "principal", "owner", "original-body-aba", "unbound-user",
		"other-agent", "other-agent-tenant",
		"completed-assistant", "changed-agent-config", "other-independent-session", "other-scoped-session",
	} {
		t.Run(mutation, func(t *testing.T) {
			messages, _, _, db, ctx, session := messageMaterialActualFixture(t)
			if !db.Migrator().HasColumn(&types.Session{}, "nextcloud_scoped") {
				require.NoError(
					t,
					db.Exec("ALTER TABLE sessions ADD COLUMN nextcloud_scoped BOOLEAN NOT NULL DEFAULT FALSE").Error,
				)
			}
			agent := &types.CustomAgent{ID: "ordinary-agent", TenantID: 7}
			require.NoError(t, db.AutoMigrate(&types.CustomAgent{}))
			require.NoError(t, db.Create(agent).Error)
			user, err := messages.CreateMessage(ctx, &types.Message{
				SessionID: session.ID, RequestID: "ordinary-mcp-request", Role: "user",
				Content: "current ordinary human input", IsCompleted: true,
			})
			require.NoError(t, err)
			assistant, err := messages.CreateMessage(ctx, &types.Message{
				SessionID: session.ID, RequestID: user.RequestID, Role: "assistant",
				AgentID: agent.ID, AgentTenantID: agent.TenantID,
			})
			require.NoError(t, err)
			ctx = WithMessageMaterialOutputBoundary(ctx)
			defer CloseMessageMaterialOutputBoundary(ctx)
			_, err = messages.GetMessage(ctx, session.ID, user.ID)
			require.NoError(t, err)
			require.NoError(t, messages.bindInitialMessageMaterial(ctx, assistant.ID, user.ID, user.RequestID))
			otherSessionID := session.ID + "-other"
			if mutation == "other-independent-session" || mutation == "other-scoped-session" {
				otherSession := &types.Session{TenantID: session.TenantID, UserID: session.UserID}
				require.NoError(t, db.Create(otherSession).Error)
				otherSessionID = otherSession.ID
				otherUser, err := messages.CreateMessage(ctx, &types.Message{
					SessionID: otherSessionID, RequestID: "other-independent-request", Role: "user",
					Content: "independently authored earlier conversation", IsCompleted: true,
				})
				require.NoError(t, err)
				_, err = messages.GetMessage(ctx, otherSessionID, otherUser.ID)
				require.NoError(t, err)
			}
			s := &sessionService{
				sessionRepo: repository.NewSessionRepository(db), lineageVerifier: messages,
				sourceLineageGuard: messages.sourceLineageGuard,
				groupAccess:        NewGroupAccessService(repository.NewGroupAccessRepository(db)),
				agentService:       &agentService{db: db},
			}
			req := &types.QARequest{
				Session: session, UserMessageID: user.ID, AssistantMessageID: assistant.ID,
				CustomAgent: agent,
			}
			check := s.agentMCPIndependentVerifier(ctx, req, &types.AgentConfig{})
			require.NoError(t, check(ctx), "actual ordinary session must retain MCP capability")
			checkCtx := ctx
			switch mutation {
			case "scoped-session":
				require.NoError(
					t,
					db.Model(&types.Session{}).Where("id=?", session.ID).Update("nextcloud_scoped", true).Error,
				)
			case "principal":
				checkCtx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "different-user"})
			case "other-scoped-session":
				require.NoError(t, db.Model(&types.Session{}).Where("id=?", otherSessionID).
					Update("nextcloud_scoped", true).Error)
			case "owner":
				require.NoError(
					t,
					db.Model(&types.Session{}).Where("id=?", session.ID).Update("user_id", "different-user").Error,
				)
			case "original-body-aba":
				require.NoError(
					t,
					db.Model(&types.Message{}).Where("id=?", user.ID).Update("content", "foreign text").Error,
				)
				require.NoError(
					t,
					db.Model(&types.Message{}).Where("id=?", user.ID).Update("content", user.Content).Error,
				)
			case "unbound-user":
				other := *req
				other.UserMessageID = "not-the-admitted-user"
				check = s.agentMCPIndependentVerifier(ctx, &other, &types.AgentConfig{})
			case "completed-assistant":
				require.NoError(
					t,
					db.Model(&types.Message{}).Where("id=?", assistant.ID).Update("is_completed", true).Error,
				)
			case "changed-agent-config":
				agent.Config.MCPSelectionMode = "none"
				require.NoError(t, repository.NewCustomAgentRepository(db).UpdateAgent(ctx, agent))
			case "other-agent", "other-agent-tenant":
				other := *req
				otherAgent := *agent
				if mutation == "other-agent" {
					otherAgent.ID = "different-agent"
				} else {
					otherAgent.TenantID = 8
				}
				other.CustomAgent = &otherAgent
				check = s.agentMCPIndependentVerifier(ctx, &other, &types.AgentConfig{})
			}
			if mutation == "none" || mutation == "other-independent-session" {
				require.NoError(t, check(checkCtx))
			} else {
				require.Error(t, check(checkCtx))
			}
		})
	}
}

func TestAgentMCPIndependentTransportMissingAuthorityDenied(t *testing.T) {
	s := &sessionService{}
	require.Error(t, s.agentMCPIndependentVerifier(context.Background(), nil, nil)(context.Background()))
}
