package im

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type imGroupAccessProbe struct {
	interfaces.GroupAccessService

	err          error
	calls        int
	tenantID     uint64
	resourceType types.ResourceType
	resourceID   string
	action       types.ResourceAction
	principal    types.Principal
	principalOK  bool
}

func (p *imGroupAccessProbe) Authorize(
	ctx context.Context,
	tenantID uint64,
	resourceType types.ResourceType,
	resourceID string,
	action types.ResourceAction,
) error {
	p.calls++
	p.tenantID = tenantID
	p.resourceType = resourceType
	p.resourceID = resourceID
	p.action = action
	p.principal, p.principalOK = types.PrincipalFromContext(ctx)
	return p.err
}

type imTenantServiceStub struct {
	interfaces.TenantService
}

func (*imTenantServiceStub) GetTenantByID(_ context.Context, id uint64) (*types.Tenant, error) {
	return &types.Tenant{ID: id}, nil
}

func TestAuthorizeIMAgentUse(t *testing.T) {
	const (
		tenantID = uint64(42)
		agentID  = "agent-1"
	)
	ctx := withIMIdentity(
		context.Background(),
		tenantID,
		"channel-1",
		&IncomingMessage{Platform: PlatformFeishu, UserID: "external-user-1"},
	)

	t.Run("module disabled preserves existing behavior", func(t *testing.T) {
		service := &Service{}
		if err := service.authorizeIMAgentUse(ctx, tenantID, agentID); err != nil {
			t.Fatalf("authorizeIMAgentUse() error = %v, want nil", err)
		}
	})

	t.Run("inherited policy allows IM", func(t *testing.T) {
		probe := &imGroupAccessProbe{}
		service := &Service{}
		ConfigureGroupAccess(service, probe)

		if err := service.authorizeIMAgentUse(ctx, tenantID, agentID); err != nil {
			t.Fatalf("authorizeIMAgentUse() error = %v, want nil", err)
		}
		if probe.calls != 1 {
			t.Fatalf("Authorize() calls = %d, want 1", probe.calls)
		}
		if probe.tenantID != tenantID || probe.resourceType != types.GroupResourceTypeAgent ||
			probe.resourceID != agentID || probe.action != types.ResourceActionUse {
			t.Fatalf("Authorize() args = tenant %d, type %q, id %q, action %q",
				probe.tenantID, probe.resourceType, probe.resourceID, probe.action)
		}
		if !probe.principalOK || probe.principal.Type != types.PrincipalIMUser ||
			probe.principal.ID != "42:channel-1:feishu:external-user-1" {
			t.Fatalf("Authorize() principal = %#v (ok=%v), want verified IM principal", probe.principal, probe.principalOK)
		}
	})

	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "restricted policy denies machine principal", err: errors.New("resource access denied: verifiable_user_required")},
		{name: "policy lookup failure fails closed", err: errors.New("policy store unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &imGroupAccessProbe{err: tc.err}
			service := &Service{}
			ConfigureGroupAccess(service, probe)

			err := service.authorizeIMAgentUse(ctx, tenantID, agentID)
			if !errors.Is(err, tc.err) {
				t.Fatalf("authorizeIMAgentUse() error = %v, want wrapped %v", err, tc.err)
			}
		})
	}
}

func TestHandleMessageChecksAgentAccessBeforeResolvingSession(t *testing.T) {
	denied := errors.New("resource access denied: verifiable_user_required")
	probe := &imGroupAccessProbe{err: denied}
	registry := NewCommandRegistry()
	registry.Register(newHelpCommand(registry))

	service := &Service{
		tenantService: &imTenantServiceStub{},
		groupAccess:   probe,
		cmdRegistry:   registry,
		channels: map[string]*channelState{
			"channel-1": {
				Channel: &IMChannel{
					ID:          "channel-1",
					TenantID:    42,
					AgentID:     "agent-1",
					Platform:    string(PlatformFeishu),
					SessionMode: string(SessionModeUser),
				},
				Adapter: &lifecycleTestAdapter{},
			},
		},
	}

	err := service.HandleMessage(context.Background(), &IncomingMessage{
		Platform: PlatformFeishu,
		UserID:   "external-user-1",
		ChatID:   "chat-1",
		Content:  "/help",
	}, "channel-1")
	if !errors.Is(err, denied) {
		t.Fatalf("HandleMessage() error = %v, want wrapped access denial", err)
	}
	if probe.calls != 1 {
		t.Fatalf("Authorize() calls = %d, want 1", probe.calls)
	}
	if !probe.principalOK || probe.principal.Type != types.PrincipalIMUser {
		t.Fatalf("Authorize() principal = %#v (ok=%v), want IM principal", probe.principal, probe.principalOK)
	}
}
