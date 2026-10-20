package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type historyGlueMessages struct {
	interfaces.MessageRepository
	postLinks int
}

func (r *historyGlueMessages) UpdateMessageKnowledgeID(context.Context, string, string) error {
	r.postLinks++
	return errors.New("post-link is forbidden")
}

type historyGlueSessions struct {
	interfaces.SessionRepository
	deny bool
}

func (r *historyGlueSessions) Get(ctx context.Context, tenant uint64, owner, id string) (*types.Session, error) {
	if r.deny {
		return nil, access.ErrForbidden
	}
	return r.SessionRepository.Get(ctx, tenant, owner, id)
}

type historyGlueKB struct {
	interfaces.KnowledgeBaseService
	tenant uint64
}

type historyGlueTenants struct {
	interfaces.TenantService
	tenant *types.Tenant
}

func (s historyGlueTenants) GetTenantByID(_ context.Context, id uint64) (*types.Tenant, error) {
	if id != s.tenant.ID {
		return nil, access.ErrForbidden
	}
	return s.tenant, nil
}

type historyGluePolicy struct {
	access.SourceLineageAuthorizer
	denyTarget bool
}

func (p *historyGluePolicy) Check(
	ctx context.Context,
	lineage *types.SourceLineage,
	scope access.LineageReadScope,
) error {
	if p.denyTarget {
		for _, target := range scope.Targets {
			if target.KnowledgeBaseID == "history-kb" {
				return access.ErrNextcloudPublicationDenied
			}
		}
	}
	return p.SourceLineageAuthorizer.Check(ctx, lineage, scope)
}

func (s historyGlueKB) GetKnowledgeBaseByIDOnly(ctx context.Context, id string) (*types.KnowledgeBase, error) {
	kb, err := s.KnowledgeBaseService.GetKnowledgeBaseByIDOnly(ctx, id)
	if err != nil || kb == nil {
		return kb, err
	}
	if id == "history-kb" {
		current := *kb
		current.TenantID = s.tenant
		return &current, nil
	}
	return kb, nil
}

type historyGlueWriter struct {
	interfaces.KnowledgeService
	t         *testing.T
	calls     int
	passages  int
	request   types.CommittedHistoryKnowledgeInputRequest
	writerErr error
	caller    types.Caller
	native    interfaces.CommittedHistoryKnowledgeInputWriter
	result    *types.CommittedHistoryKnowledgeInput
}

func (s *historyGlueWriter) CreateKnowledgeFromPassage(
	context.Context,
	string,
	[]string,
	string,
) (*types.Knowledge, error) {
	s.passages++
	return nil, errors.New("generic passage producer is forbidden")
}

func (s *historyGlueWriter) CreateCommittedHistoryKnowledgeInput(
	ctx context.Context,
	request types.CommittedHistoryKnowledgeInputRequest,
) (*types.CommittedHistoryKnowledgeInput, error) {
	s.calls++
	s.request = request
	require.Equal(s.t, uint64(7), types.MustTenantIDFromContext(ctx))
	require.Equal(
		s.t,
		s.caller,
		types.CallerFromContext(ctx),
	)
	require.NoError(s.t, access.RequireKBWrite(ctx, &types.KnowledgeBase{ID: "history-kb", TenantID: 7}))
	if s.writerErr != nil {
		return nil, s.writerErr
	}
	result, err := s.native.CreateCommittedHistoryKnowledgeInput(ctx, request)
	s.result = result
	return result, err
}

func TestMessageHistoryInputActualIndexAdapterUsesOnlyServerIDsAndAtomicCapability(t *testing.T) {
	for _, mode := range []string{
		"complete plain",
		"shared execution",
		"foreign caller",
		"legacy",
		"unknown",
		"controlled",
		"unfinished",
		"wrong role",
		"foreign target",
		"target revoked",
		"owner denied",
		"writer error",
		"missing capability",
		"missing material store",
		"body ABA",
	} {
		t.Run(mode, func(t *testing.T) {
			// Real message Create/Update producers install the immutable original
			// question, answer and current SQL actor. A cached completed row or a
			// returned SourceLineage field is never upgraded into an input receipt.
			// Install the actual display migration before the original-body
			// journal callbacks. Creating a new named KB is a typed name producer.
			svc, native, store, db, original, session, _ := messageBodyIntegrationFixture(
				t, false, agentPromptDisplayFixtureSchema(t),
			)
			// A new actual request retains the authenticated actor and execution
			// tenant, without borrowing the old KB's resolved resource grant.
			base := agentPromptActualRequestContext(original, t)
			require.Equal(t, types.CallerFromContext(original), types.CallerFromContext(base))
			require.Equal(t, types.MustTenantIDFromContext(original), types.MustTenantIDFromContext(base))
			principal, ok := types.PrincipalFromContext(original)
			require.True(t, ok)
			currentPrincipal, ok := types.PrincipalFromContext(base)
			require.True(t, ok)
			require.Equal(t, principal, currentPrincipal)
			require.True(t, access.HasKBGrant(original, "kb", 7, types.OrgRoleEditor))
			require.False(t, access.HasKBGrant(base, "kb", 7, types.OrgRoleEditor))
			require.False(t, access.HasKBGrant(base, "history-kb", 7, types.OrgRoleEditor))
			user, row := produceActualMessagePair(base, t, svc, session)
			caller := types.CallerFromContext(base)
			require.Equal(t, types.Caller{
				TenantID: 7, UserID: "actual-user", Role: types.TenantRoleContributor,
			}, caller)
			messages := &historyGlueMessages{MessageRepository: svc.messageRepo}
			sessions := &historyGlueSessions{SessionRepository: svc.sessionRepo}
			kb := historyGlueKB{KnowledgeBaseService: svc.kbService, tenant: 7}
			policy := &historyGluePolicy{SourceLineageAuthorizer: svc.sourceLineageGuard}
			writer := &historyGlueWriter{
				KnowledgeService: native, t: t, caller: caller, native: native,
			}
			target := &types.KnowledgeBase{
				ID: "history-kb", TenantID: 7, Type: types.KnowledgeBaseTypeDocument,
				Name: "Actual adapter history target", EmbeddingModelID: "embed",
				ChunkingConfig:   types.ChunkingConfig{ChunkSize: 512},
				IndexingStrategy: types.IndexingStrategy{VectorEnabled: true},
			}
			require.NoError(t, store.CreateKnowledgeBaseWithInput(
				repository.WithIndependentKnowledgeBaseNameInput(
					repository.WithIndependentKnowledgeInput(base), "", target.Name,
				), target,
			))
			// The actual native writer resolves the same server-owned target.
			native.kbService = svc.kbService
			svc.messageRepo, svc.sessionRepo = messages, sessions
			svc.knowService, svc.kbService, svc.sourceLineageGuard = writer, kb, policy
			switch mode {
			case "legacy":
				// Deliberately insert an old row without the native producer. It
				// must remain unreadable instead of being recertified from its body.
				legacy := *row
				legacy.ID, legacy.RequestID = "legacy-assistant", "legacy-request"
				legacy.SourceLineage = nil
				require.NoError(t, db.Create(&legacy).Error)
				row = &legacy
			case "unknown":
				row.SourceLineage = types.UnknownSourceLineage()
				require.Error(t, svc.UpdateMessage(base, row), "the native producer cannot complete unknown material")
				// Simulate an unadmitted legacy/partial DB write after producer refusal.
				require.NoError(t, db.Model(&types.Message{}).Where("id = ?", row.ID).
					Update("source_lineage", row.SourceLineage).Error)
			case "controlled":
				// An unavailable controlled producer claim remains controlled;
				// no current row or empty dependency set may authorize it.
				row.SourceLineage = ragEntryProof(t)
				require.Error(t, svc.UpdateMessage(base, row), "the unavailable source cannot complete this body")
				require.NoError(t, db.Model(&types.Message{}).Where("id = ?", row.ID).
					Update("source_lineage", row.SourceLineage).Error)
			case "unfinished":
				var err error
				row, err = svc.CreateMessage(base, &types.Message{
					SessionID: session.ID, RequestID: user.RequestID, Role: "assistant",
					ExecutionContext: user.ExecutionContext,
				})
				require.NoError(t, err)
			case "wrong role":
				row = user
			case "foreign target":
				kb.tenant = 12
				svc.kbService = kb
			case "target revoked":
				policy.denyTarget = true
			case "owner denied":
				sessions.deny = true
			case "writer error":
				writer.writerErr = access.ErrNextcloudPublicationDenied
			case "missing capability":
				svc.knowService = ragEntryKnowledge{}
			case "missing material store":
				svc.materialStore = nil
			case "body ABA":
				require.NoError(t, db.Model(&types.Message{}).Where("id = ?", row.ID).
					Update("content", "unadmitted transient body").Error)
				require.NoError(t, db.Model(&types.Message{}).Where("id = ?", row.ID).
					Update("content", row.Content).Error)
			}
			tenant := &types.Tenant{
				ID: 7,
				ChatHistoryConfig: &types.ChatHistoryConfig{
					Enabled:          true,
					EmbeddingModelID: "embed",
					KnowledgeBaseID:  "history-kb",
				},
			}
			ctx := context.WithValue(base, types.TenantInfoContextKey, tenant)
			if mode == "shared execution" {
				ctx = types.WithExecutionTenant(ctx, 12)
				ctx = context.WithValue(ctx, types.SessionTenantIDContextKey, uint64(7))
				ctx = context.WithValue(ctx, types.TenantInfoContextKey, &types.Tenant{ID: 12})
				svc.tenantService = historyGlueTenants{tenant: tenant}
			}
			if mode == "foreign caller" {
				ctx = types.WithCaller(ctx, types.Caller{TenantID: 12, UserID: caller.UserID, Role: caller.Role})
			}
			svc.IndexMessageToKB(ctx, "forged caller question", "forged caller answer", row.ID, row.SessionID)
			if mode == "complete plain" || mode == "shared execution" || mode == "writer error" {
				require.Equal(t, 1, writer.calls)
				require.Equal(
					t,
					types.CommittedHistoryKnowledgeInputRequest{
						TenantID:           7,
						KnowledgeBaseID:    "history-kb",
						SessionID:          session.ID,
						AssistantMessageID: row.ID,
					},
					writer.request,
				)
			} else {
				require.Zero(t, writer.calls)
			}
			queue := native.task.(*knowledgeInputQueue)
			if mode == "complete plain" || mode == "shared execution" {
				require.NotNil(t, writer.result)
				require.NotNil(t, writer.result.Knowledge)
				require.NotNil(t, writer.result.Proof)
				require.NoError(t, writer.result.Proof.SourceLineage.RequireComplete())
				require.Equal(t, uint64(7), writer.result.Proof.TenantID)
				require.Equal(t, target.ID, writer.result.Proof.KnowledgeBaseID)
				require.Len(t, queue.tasks, 1)
				var payload types.DocumentProcessPayload
				require.NoError(t, json.Unmarshal(queue.tasks[0].Payload(), &payload))
				require.Equal(t, writer.result.Proof, payload.OriginalInput)
				require.Equal(t, writer.result.Knowledge.ID, payload.KnowledgeID)
				require.Contains(t, payload.Passages[0], user.Content)
				require.Contains(t, payload.Passages[0], row.Content)
				require.NotContains(t, payload.Passages[0], "forged caller")
				var saved types.Message
				require.NoError(t, db.Where("id = ?", row.ID).Take(&saved).Error)
				require.Equal(t, writer.result.Knowledge.ID, saved.KnowledgeID)
			} else {
				require.Empty(t, queue.tasks)
			}
			require.Zero(t, writer.passages, "caller strings reached the generic passage producer")
			require.Zero(t, messages.postLinks, "atomic producer was followed by a separate link update")
		})
	}
}
