package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type historyGlueMessages struct {
	interfaces.MessageRepository
	row       *types.Message
	postLinks int
}

func (r *historyGlueMessages) GetMessage(context.Context, string, string) (*types.Message, error) {
	return r.row, nil
}

func (r *historyGlueMessages) UpdateMessageKnowledgeID(context.Context, string, string) error {
	r.postLinks++
	return errors.New("post-link is forbidden")
}

type historyGlueSessions struct {
	interfaces.SessionRepository
	deny bool
}

func (r *historyGlueSessions) Get(_ context.Context, tenant uint64, owner, id string) (*types.Session, error) {
	if r.deny || tenant != 7 || owner != "reader" || id != "session" {
		return nil, access.ErrForbidden
	}
	return &types.Session{ID: id, TenantID: tenant, UserID: owner}, nil
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
	consumerBatchReviewPolicy
	denyTarget bool
}

func (p *historyGluePolicy) Check(
	ctx context.Context,
	lineage *types.SourceLineage,
	scope access.LineageReadScope,
) error {
	if p.denyTarget && len(scope.Targets) > 0 {
		return access.ErrNextcloudPublicationDenied
	}
	return p.consumerBatchReviewPolicy.Check(ctx, lineage, scope)
}

func (s historyGlueKB) GetKnowledgeBaseByIDOnly(_ context.Context, id string) (*types.KnowledgeBase, error) {
	return &types.KnowledgeBase{ID: id, TenantID: s.tenant}, nil
}

type historyGlueWriter struct {
	interfaces.KnowledgeService
	t         *testing.T
	calls     int
	passages  int
	request   types.CommittedHistoryKnowledgeInputRequest
	writerErr error
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
		types.Caller{TenantID: 7, UserID: "reader", Role: types.TenantRoleViewer},
		types.CallerFromContext(ctx),
	)
	require.NoError(s.t, access.RequireKBWrite(ctx, &types.KnowledgeBase{ID: "history-kb", TenantID: 7}))
	if s.writerErr != nil {
		return nil, s.writerErr
	}
	return &types.CommittedHistoryKnowledgeInput{Knowledge: &types.Knowledge{ID: "atomic-child"}}, nil
}

func TestMessageHistoryInputActualIndexAdapterUsesOnlyServerIDsAndAtomicCapability(t *testing.T) {
	empty, _ := types.NewCompleteSourceLineage()
	for _, mode := range []string{"complete plain", "shared execution", "foreign caller", "legacy", "unknown", "controlled", "unfinished", "wrong role", "foreign target", "target revoked", "owner denied", "writer error", "missing capability"} {
		t.Run(mode, func(t *testing.T) {
			row := &types.Message{
				ID:            "assistant",
				SessionID:     "session",
				Role:          "assistant",
				RequestID:     "request",
				IsCompleted:   true,
				Content:       "actual committed answer",
				SourceLineage: empty,
			}
			messages := &historyGlueMessages{row: row}
			writer := &historyGlueWriter{t: t}
			sessions := &historyGlueSessions{}
			kb := historyGlueKB{tenant: 7}
			policy := &historyGluePolicy{}
			svc := &messageService{
				messageRepo:        messages,
				sessionRepo:        sessions,
				knowService:        writer,
				kbService:          kb,
				sourceLineageGuard: policy,
			}
			switch mode {
			case "legacy":
				row.SourceLineage = nil
			case "unknown":
				row.SourceLineage = types.UnknownSourceLineage()
			case "controlled":
				row.SourceLineage = ragEntryProof(t)
			case "unfinished":
				row.IsCompleted = false
			case "wrong role":
				row.Role = "user"
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
			}
			tenant := &types.Tenant{
				ID: 7,
				ChatHistoryConfig: &types.ChatHistoryConfig{
					Enabled:          true,
					EmbeddingModelID: "embedding",
					KnowledgeBaseID:  "history-kb",
				},
			}
			ctx := context.WithValue(ragReadContext(), types.TenantInfoContextKey, tenant)
			if mode == "shared execution" {
				ctx = types.WithExecutionTenant(ctx, 12)
				ctx = context.WithValue(ctx, types.SessionTenantIDContextKey, uint64(7))
				ctx = context.WithValue(ctx, types.TenantInfoContextKey, &types.Tenant{ID: 12})
				svc.tenantService = historyGlueTenants{tenant: tenant}
			}
			if mode == "foreign caller" {
				ctx = types.WithCaller(ctx, types.Caller{TenantID: 12, UserID: "reader", Role: types.TenantRoleViewer})
			}
			svc.IndexMessageToKB(ctx, "forged caller question", "forged caller answer", row.ID, row.SessionID)
			if mode == "complete plain" || mode == "shared execution" || mode == "writer error" {
				require.Equal(t, 1, writer.calls)
				require.Equal(
					t,
					types.CommittedHistoryKnowledgeInputRequest{
						TenantID:           7,
						KnowledgeBaseID:    "history-kb",
						SessionID:          "session",
						AssistantMessageID: "assistant",
					},
					writer.request,
				)
			} else {
				require.Zero(t, writer.calls)
			}
			require.Zero(t, writer.passages, "caller strings reached the generic passage producer")
			require.Zero(t, messages.postLinks, "atomic producer was followed by a separate link update")
		})
	}
}
