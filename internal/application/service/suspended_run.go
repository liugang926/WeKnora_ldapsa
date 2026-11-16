package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/Tencent/WeKnora/internal/agent"
	"github.com/Tencent/WeKnora/internal/agent/hitl"
	agenttools "github.com/Tencent/WeKnora/internal/agent/tools"
	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
)

// SuspendedRunService keeps the assistant message of a suspended run in step
// with its prompt when the system, not the run itself, moves the prompt on:
// the reaper taking over, or the user moving on without answering.
type SuspendedRunService struct {
	RecoverAnswered func(context.Context, *types.AgentPrompt) (bool, error)
	StopDelegation  func(context.Context, *types.AgentPrompt) error
	broker          *hitl.Broker
	messages        interfaces.MessageRepository
	materialService *messageService
	durableBroker   bool
	db              *gorm.DB
	authorize       func(
		context.Context, *types.Session, *types.AgentPrompt,
		*types.Message, *agent.RunSnapshot,
	) (context.Context, error)
}

// NewSuspendedRunService builds the service.
func NewSuspendedRunService(broker *hitl.Broker, messages interfaces.MessageRepository) *SuspendedRunService {
	s := &SuspendedRunService{broker: broker, messages: messages}
	broker.SetRunHooks(s.Hooks())
	return s
}

// NewAuthorizedSuspendedRunService binds orphan-run publication to the current owner
// and the same application read boundaries used by an interactive run.
func NewAuthorizedSuspendedRunService(broker *hitl.Broker, repo interfaces.MessageRepository,
	db *gorm.DB, messages interfaces.MessageService, groups interfaces.GroupAccessService, users interfaces.UserService,
) *SuspendedRunService {
	s := NewSuspendedRunService(broker, repo)
	s.db = db
	s.durableBroker = true
	s.materialService, _ = messages.(*messageService)
	s.authorize = func(
		ctx context.Context, session *types.Session,
		prompt *types.AgentPrompt, msg *types.Message,
		snap *agent.RunSnapshot,
	) (context.Context, error) {
		service, ok := messages.(*messageService)
		if !ok || service == nil || groups == nil || users == nil || snap.SourceLineage.RequireComplete() != nil {
			return nil, ErrResourceAccessDenied
		}
		user, err := users.GetUserByID(ctx, session.UserID)
		if err != nil {
			return nil, err
		}
		if user == nil || user.ID != session.UserID || !user.IsActive {
			return nil, ErrResourceAccessDenied
		}
		role, err := groups.EffectiveTenantRole(ctx, user.ID, prompt.TenantID, time.Now())
		if err != nil {
			return nil, err
		}
		if !role.Member || !role.Role.IsValid() {
			return nil, ErrResourceAccessDenied
		}
		ctx = context.WithValue(ctx, types.UserIDContextKey, user.ID)
		ctx = context.WithValue(ctx, types.TenantIDContextKey, prompt.TenantID)
		ctx = types.WithCaller(ctx, types.Caller{TenantID: prompt.TenantID, UserID: user.ID, Role: role.Role})

		var req types.ResumableAgentRequest
		if err := json.Unmarshal(snap.Request, &req); err != nil {
			return nil, err
		}
		if req.AgentID == "" || msg.AgentID != "" && msg.AgentID != req.AgentID {
			return nil, ErrResourceAccessDenied
		}
		ctx = types.WithPrincipal(ctx, types.Principal{
			Type: types.PrincipalWebUser,
			ID:   user.ID, ActingVia: req.ActingVia,
		})
		// Shared agents keep their source workspace and their current share grant.
		agentTenant := req.AgentTenantID
		if agentTenant == 0 {
			agentTenant = prompt.TenantID
		}
		if msg.AgentTenantID != 0 && msg.AgentTenantID != agentTenant {
			return nil, ErrResourceAccessDenied
		}
		candidate := *msg
		candidate.AgentID, candidate.AgentTenantID = req.AgentID, agentTenant
		candidate.ExecutionContext.KnowledgeBaseIDs = append(
			append([]string(nil), msg.ExecutionContext.KnowledgeBaseIDs...),
			req.KnowledgeBaseIDs...,
		)
		candidate.ExecutionContext.KnowledgeIDs = append(
			append([]string(nil), msg.ExecutionContext.KnowledgeIDs...),
			req.KnowledgeIDs...,
		)
		candidate.ExecutionContext.TagScopes = append(
			append([]types.TagScope(nil), msg.ExecutionContext.TagScopes...),
			req.TagScopes...,
		)
		for _, ref := range snap.KnowledgeRefs {
			if ref == nil || ref.KnowledgeBaseID == "" {
				return nil, ErrResourceAccessDenied
			}
			candidate.ExecutionContext.KnowledgeBaseIDs = append(
				candidate.ExecutionContext.KnowledgeBaseIDs, ref.KnowledgeBaseID,
			)
		}
		for _, source := range snap.SourceLineage.Sources {
			candidate.ExecutionContext.KnowledgeBaseIDs = append(
				candidate.ExecutionContext.KnowledgeBaseIDs, source.KnowledgeBaseID,
			)
		}
		scope, err := service.messageSourceLineageScope(ctx, &candidate)
		if err != nil {
			return nil, err
		}
		guard, ok := service.sourceLineageGuard.(*access.NextcloudSourceLineageGuard)
		if !ok || guard == nil {
			return nil, ErrResourceAccessDenied
		}
		if err := guard.CheckWorkspaceRead(ctx, scope); err != nil {
			return nil, err
		}
		if !msg.ExecutionContext.ExplicitInputMaterials &&
			snap.WeKnoraACL &&
			len(snap.SourceLineage.Sources) == 0 &&
			len(snap.ImageURLs) == 0 {
			return ctx, nil
		}
		if req.UserMessageID == "" || service.materialStore == nil {
			return nil, ErrResourceAccessDenied
		}
		ids := []string{req.UserMessageID}
		for id := range snap.UserMessages {
			if id != req.UserMessageID {
				ids = append(ids, id)
			}
		}
		originals, err := service.admittedMessages(ctx, types.MessageMaterialReadRequest{
			TenantID: prompt.TenantID, SessionID: prompt.SessionID,
			MessageIDs: ids, IncludeOriginalUserFields: true,
		})
		if err != nil {
			return nil, err
		}
		if len(originals) != len(ids) {
			return nil, ErrResourceAccessDenied
		}
		for _, m := range originals {
			if m == nil || m.Role != "user" || m.SessionID != prompt.SessionID {
				return nil, ErrResourceAccessDenied
			}
			if text, ok := snap.UserMessages[m.ID]; ok && text != m.Content {
				return nil, ErrResourceAccessDenied
			}
			if m.ID == req.UserMessageID {
				if msg.RequestID != "" && m.RequestID != msg.RequestID {
					return nil, ErrResourceAccessDenied
				}
				for _, image := range snap.ImageURLs {
					if !slices.ContainsFunc(m.Images, func(stored types.MessageImage) bool {
						return stored.URL == image
					}) {
						return nil, ErrResourceAccessDenied
					}
				}
			}
		}
		return ctx, guard.CheckSourceLifecycle(ctx, snap.SourceLineage)
	}
	return s
}

// Hooks returns the message updates the broker runs as it takes prompts over.
func (s *SuspendedRunService) Hooks() hitl.RunHooks {
	return hitl.RunHooks{RecoverAnswered: s.RecoverAnswered, Suspended: s.MarkAwaitingInput, Closed: s.Close}
}

// Reap runs one reaper sweep.
func (s *SuspendedRunService) Reap(ctx context.Context, now time.Time) (int, error) {
	return s.broker.Reap(ctx, now, s.Hooks())
}

// SupersedeSession closes the session's suspended runs because the user moved
// on. Call it before the session's history is read or cut.
func (s *SuspendedRunService) SupersedeSession(ctx context.Context, sessionID string) (int, error) {
	return s.broker.SupersedeSession(ctx, sessionID, s.Hooks())
}

// MarkAwaitingInput shows a run suspended on its owner's behalf the way a
// self-suspended run looks: the saved steps, waiting on the prompt.
func (s *SuspendedRunService) MarkAwaitingInput(
	ctx context.Context, prompt *types.AgentPrompt, state *types.AgentRunSuspension,
) error {
	return s.update(ctx, prompt, state, false, func(msg *types.Message, snap *agent.RunSnapshot, _ string) {
		if snap != nil {
			msg.AgentSteps = agenttools.SanitizeAgentStepsForStorage(snap.DisplaySteps())
		}
		msg.RunState = types.MessageRunStateAwaitingInput
	})
}

// Close completes a run that will never resume. The blocked call and the
// calls after it are recorded as failed using the persisted close reason.
func (s *SuspendedRunService) Close(
	ctx context.Context, prompt *types.AgentPrompt, state *types.AgentRunSuspension, _ string,
) error {
	return s.update(ctx, prompt, state, true, func(msg *types.Message, snap *agent.RunSnapshot, storedReason string) {
		if snap != nil {
			msg.AgentSteps = agenttools.SanitizeAgentStepsForStorage(snap.ClosedSteps(storedReason))
			msg.KnowledgeReferences = mergeSnapshotReferences(msg.KnowledgeReferences, snap.KnowledgeRefs)
		}
		msg.RunState = ""
		msg.IsCompleted = true
	})
}

func (s *SuspendedRunService) update(
	ctx context.Context, prompt *types.AgentPrompt, state *types.AgentRunSuspension, closeRun bool,
	apply func(*types.Message, *agent.RunSnapshot, string),
) error {
	if prompt == nil || state == nil || prompt.ID == "" || prompt.SessionID == "" || prompt.AssistantMessageID == "" {
		return ErrResourceAccessDenied
	}
	row, err := s.broker.Store().Get(ctx, prompt.ID)
	if err != nil {
		return err
	}
	saved, err := s.broker.Suspensions().Get(ctx, prompt.AssistantMessageID)
	if err != nil {
		return err
	}
	if row == nil ||
		saved == nil ||
		row.ID != prompt.ID ||
		row.UserID != prompt.UserID ||
		row.TenantID != prompt.TenantID ||
		row.SessionID != prompt.SessionID ||
		row.AssistantMessageID != prompt.AssistantMessageID ||
		saved.PromptID != row.ID ||
		saved.TenantID != row.TenantID ||
		saved.SessionID != row.SessionID ||
		saved.AssistantMessageID != row.AssistantMessageID ||
		!bytes.Equal(saved.Snapshot, state.Snapshot) {
		return ErrResourceAccessDenied
	}
	if closeRun {
		switch row.Status {
		case types.AgentPromptFailed, types.AgentPromptCanceled, types.AgentPromptExpired, types.AgentPromptSuperseded:
		default:
			return ErrResourceAccessDenied
		}
		if s.StopDelegation != nil {
			if err := s.StopDelegation(ctx, row); err != nil {
				return err
			}
		}
	} else if row.Status != types.AgentPromptSuspended && row.Status != types.AgentPromptAnswered {
		return ErrResourceAccessDenied
	}
	msg, err := s.messages.GetMessage(ctx, row.SessionID, row.AssistantMessageID)
	if err != nil || msg == nil || msg.IsCompleted {
		return err
	}
	if msg.ID != row.AssistantMessageID ||
		msg.SessionID != row.SessionID ||
		msg.Role != "assistant" ||
		row.RequestID != "" && msg.RequestID != row.RequestID {
		return ErrResourceAccessDenied
	}
	ctx = WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(ctx)
	originalTime := msg.UpdatedAt
	snap, err := agent.DecodeRunSnapshot(saved.Snapshot)
	// A revoked grant can finish the run without publishing snapshot material.
	allowed := err == nil && row.UserID != "" && row.TenantID != 0 && s.db != nil && s.authorize != nil
	var owner types.Session
	if allowed {
		err = s.db.WithContext(ctx).
			Select("id", "tenant_id", "user_id", "owner_auth_source", "kind").
			Where("id = ? AND tenant_id = ?", row.SessionID, row.TenantID).
			Take(&owner).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		ownerPrincipal := types.Principal{Type: types.PrincipalWebUser, ID: owner.UserID}
		allowed = err == nil && owner.UserID != "" &&
			(owner.OwnerAuthSource == "" || owner.OwnerAuthSource == types.AuthSourceWeb ||
				owner.Kind == types.SessionKindAutomation) && ownerPrincipal.StorageID() == row.UserID
		if allowed {
			actor, checkErr := s.authorize(ctx, &owner, row, msg, snap)
			allowed = checkErr == nil && actor != nil && CheckMessageMaterialOutputBoundary(actor) == nil
			if allowed {
				ctx = actor
			}
		}
	}
	if !allowed {
		snap = nil
	}
	apply(msg, snap, row.CloseReason)
	msg.UpdatedAt = time.Now()
	if s.db == nil {
		return s.messages.UpdateMessage(ctx, msg)
	}
	if allowed {

		lineage := snap.SourceLineage
		if msg.SourceLineage != nil {
			lineage, err = types.UnionSourceLineage(msg.SourceLineage, lineage)
			if err != nil {
				return err
			}
		}
		msg.SourceLineage = lineage
	}
	pending := func(db *gorm.DB) *gorm.DB {
		update := db.Model(&types.Message{}).
			Where(
				"id = ? AND session_id = ? AND role = ? AND updated_at = ? AND is_completed = ?",
				msg.ID, row.SessionID, "assistant", originalTime, false,
			)
		if allowed {
			update = update.Where("EXISTS (?)",
				s.db.Model(&types.Session{}).
					Select("1").
					Where("id = ? AND tenant_id = ? AND user_id = ? AND owner_auth_source = ?",
						row.SessionID, row.TenantID, owner.UserID, owner.OwnerAuthSource,
					))
		}
		if s.durableBroker {
			prompt := s.db.Model(&types.AgentPrompt{}).Select("1").Where(
				"id = ? AND tenant_id = ? AND user_id = ? AND session_id = ? "+
					"AND assistant_message_id = ? AND status = ?",
				row.ID, row.TenantID, row.UserID, row.SessionID, row.AssistantMessageID, row.Status,
			)
			suspension := s.db.Model(&types.AgentRunSuspension{}).Select("1").Where(
				"assistant_message_id = ? AND prompt_id = ? AND tenant_id = ? AND session_id = ? AND updated_at = ?",
				saved.AssistantMessageID, saved.PromptID, saved.TenantID, saved.SessionID, saved.UpdatedAt,
			)
			update = update.Where("EXISTS (?)", prompt).Where("EXISTS (?)", suspension)
		}

		return update
	}
	if allowed {
		var finish func(error) error
		if s.materialService != nil && s.materialService.materialStore != nil {
			ctx, err = s.materialService.materialStore.WithMessageProducerInput(ctx, msg, false)
			if err != nil {
				return err
			}
			if msg.ExecutionContext.ExplicitInputMaterials || !snap.WeKnoraACL {
				ctx, finish, err = s.materialService.beginMessageMaterialCompletion(ctx, msg)
				if err != nil {
					return err
				}
			}
		}
		// Reserve the still-current run inside the material writer's transaction.
		// Updating only the timestamp here lets the repository inspect the original
		// generation, then write and seal all changed fields atomically.
		err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			result := pending(tx).UpdateColumn("updated_at", msg.UpdatedAt)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrResourceAccessDenied
			}
			repo := repository.NewMessageRepository(tx)
			if s.materialService != nil {
				repository.ConfigureMessageMaterialWriters(repo, s.materialService.materialStore)
			}
			return repo.UpdateMessage(ctx, msg)
		})
		if finish != nil {
			return finish(err)
		}
		return err
	}
	result := pending(s.db.WithContext(ctx)).Updates(map[string]interface{}{
		"updated_at": msg.UpdatedAt, "is_completed": msg.IsCompleted,
		"run_state": msg.RunState,
	})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrResourceAccessDenied
	}
	return nil
}

func mergeSnapshotReferences(existing types.References, refs []*types.SearchResult) types.References {
	if len(existing) > 0 || len(refs) == 0 {
		return existing
	}
	return types.References(refs)
}
