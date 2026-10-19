package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
)

// agentMCPIndependentVerifier grants only the ordinary session transport path.
// The MCP adapter separately proves its actual transport, catalog and result,
// and rejects any controlled source in the accumulated model input. Neither an
// empty lineage nor a client-supplied session flag grants this capability.
func (s *sessionService) agentMCPIndependentVerifier(
	ctx context.Context,
	req *types.QARequest,
	config *types.AgentConfig,
) func(context.Context) error {
	denied := func(context.Context) error { return access.ErrNextcloudPublicationDenied }
	if req == nil || req.Session == nil || req.CustomAgent == nil || config == nil ||
		s.sessionRepo == nil || s.sourceLineageGuard == nil {
		return denied
	}
	native, ok := s.lineageVerifier.(*messageService)
	if !ok || native.materialStore == nil {
		return denied
	}
	nativeAgent, ok := s.agentService.(*agentService)
	if !ok || nativeAgent.db == nil {
		return denied
	}
	agents := repository.NewCustomAgentRepository(nativeAgent.db)
	caller, callerOK := ctx.Value(types.CallerContextKey).(types.Caller)
	principal, principalOK := ctx.Value(types.PrincipalContextKey).(types.Principal)
	owner := types.SessionOwnerIDFromContext(ctx)
	tenant, tenantOK := types.TenantIDFromContext(ctx)
	if !callerOK || !principalOK || !principal.Valid() || caller.TenantID == 0 ||
		!tenantOK || tenant == 0 || tenant != req.Session.TenantID || owner == "" ||
		req.Session.ID == "" || req.AssistantMessageID == "" || req.UserMessageID == "" {
		return denied
	}
	sessionID, assistantID, userID := req.Session.ID, req.AssistantMessageID, req.UserMessageID
	agent, err := cloneCollection(req.CustomAgent)
	if err != nil {
		return denied
	}
	agent.EnsureDefaults()
	configBytes, err := json.Marshal(agent.Config)
	if err != nil {
		return denied
	}
	targets, err := cloneCollection(config.SearchTargets)
	if err != nil {
		return denied
	}
	control, generation, err := native.materialStore.MessageLiveControl(ctx, sessionID, assistantID)
	if err != nil || control == nil || generation == "" || control.Role != "assistant" || control.RequestID == "" ||
		control.IsCompleted ||
		control.AgentID != agent.ID ||
		control.AgentTenantID != agent.TenantID {
		return denied
	}
	requestID := control.RequestID
	oauth := types.MCPOAuthPrincipalFromContext(ctx)
	return func(checkCtx context.Context) error {
		if checkCtx == nil || checkCtx.Err() != nil {
			return access.ErrNextcloudPublicationUnavailable
		}
		actualCaller, actualCallerOK := checkCtx.Value(types.CallerContextKey).(types.Caller)
		actualPrincipal, actualPrincipalOK := checkCtx.Value(types.PrincipalContextKey).(types.Principal)
		actualTenant, actualTenantOK := types.TenantIDFromContext(checkCtx)
		if !actualCallerOK || !actualPrincipalOK || !actualTenantOK || actualCaller != caller ||
			actualPrincipal != principal || actualTenant != tenant ||
			types.SessionOwnerIDFromContext(
				checkCtx,
			) != owner || types.MCPOAuthPrincipalFromContext(checkCtx) != oauth {
			return access.ErrNextcloudPublicationDenied
		}
		if err := CheckMessageMaterialOutputBoundary(ctx); err != nil {
			return err
		}
		current, err := s.sessionRepo.Get(checkCtx, tenant, owner, sessionID)
		if err != nil || current == nil || current.ID != sessionID || current.TenantID != tenant ||
			current.UserID != owner || current.NextcloudScoped {
			return access.ErrNextcloudPublicationDenied
		}
		live, liveGeneration, err := native.materialStore.MessageLiveControl(checkCtx, sessionID, assistantID)
		if err != nil || live == nil || liveGeneration != generation || live.RequestID != requestID ||
			live.Role != "assistant" || live.IsCompleted || live.AgentID != control.AgentID || live.AgentTenantID != control.AgentTenantID {
			return access.ErrNextcloudPublicationDenied
		}
		currentAgent, err := agents.GetAgentByID(checkCtx, agent.ID, agent.TenantID)
		if errors.Is(err, repository.ErrCustomAgentNotFound) && types.IsBuiltinAgentID(agent.ID) {
			currentAgent = types.GetBuiltinAgentWithContext(checkCtx, agent.ID, agent.TenantID)
		} else if err != nil {
			return access.ErrNextcloudPublicationDenied
		}
		if currentAgent == nil || currentAgent.ID != agent.ID || currentAgent.TenantID != agent.TenantID {
			return access.ErrNextcloudPublicationDenied
		}
		currentAgent.EnsureDefaults()
		currentConfig, err := json.Marshal(currentAgent.Config)
		if err != nil || !bytes.Equal(currentConfig, configBytes) {
			return access.ErrNextcloudPublicationDenied
		}
		if principal.Type == types.PrincipalWebUser {
			if s.groupAccess == nil || principal.ID != caller.UserID || types.IsSyntheticUserID(caller.UserID) {
				return access.ErrNextcloudPublicationDenied
			}
			role, err := s.groupAccess.EffectiveTenantRole(checkCtx, caller.UserID, caller.TenantID, time.Now().UTC())
			if err != nil || !role.Member || role.ScopedOnly {
				return access.ErrNextcloudPublicationDenied
			}
		}
		parents, err := messageMaterialBoundarySnapshots(ctx)
		if err != nil {
			return err
		}
		originalUser := false
		for _, parent := range parents {
			if parent == nil || parent.Request.TenantID != tenant || parent.Request.SessionID == "" ||
				parent.SessionOwnerID != owner {
				return access.ErrNextcloudPublicationDenied
			}
			if parent.Request.SessionID != sessionID {
				other, err := s.sessionRepo.Get(checkCtx, tenant, owner, parent.Request.SessionID)
				if err != nil || other == nil || other.ID != parent.Request.SessionID || other.TenantID != tenant ||
					other.UserID != owner || other.NextcloudScoped {
					return access.ErrNextcloudPublicationDenied
				}
			}
			for _, proof := range parent.Proofs {
				if proof == nil || proof.SourceLineage.RequireComplete() != nil ||
					len(proof.SourceLineage.Sources) != 0 {
					return access.ErrNextcloudPublicationDenied
				}
				for _, field := range proof.GeneratedFields {
					if field == nil || field.SourceLineage.RequireComplete() != nil ||
						len(field.SourceLineage.Sources) != 0 {
						return access.ErrNextcloudPublicationDenied
					}
				}
				for _, message := range parent.Messages {
					if parent.Request.SessionID == sessionID && message != nil && message.ID == userID &&
						proof.MessageID == userID &&
						message.Role == "user" &&
						message.RequestID == requestID {
						originalUser = true
					}
				}
			}
		}
		if !originalUser {
			return access.ErrNextcloudPublicationDenied
		}
		if err := s.revalidateAgentExecution(checkCtx, agent, targets); err != nil {
			return err
		}
		independent, err := types.NewCompleteSourceLineage()
		if err != nil {
			return err
		}
		return s.sourceLineageGuard.Check(checkCtx, independent, access.LineageReadScope{
			Targets: targets, AgentID: agent.ID, AgentTenantID: agent.TenantID,
		})
	}
}
