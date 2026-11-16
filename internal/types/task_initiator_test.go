package types

import (
	"context"
	"testing"
)

func TestTaskInitiatorRoundTrip(t *testing.T) {
	requestCtx := context.WithValue(context.Background(), UserIDContextKey, "user-1")
	requestCtx = context.WithValue(requestCtx, TenantRoleContextKey, TenantRoleAdmin)
	requestCtx = context.WithValue(requestCtx, SystemAdminContextKey, true)

	initiator := TaskInitiatorFromContext(requestCtx)
	if initiator.UserID != "user-1" || initiator.Role != TenantRoleAdmin || !initiator.SystemAdmin {
		t.Fatalf("TaskInitiatorFromContext() = %#v", initiator)
	}

	workerCtx := initiator.Apply(context.Background())
	if userID, ok := UserIDFromContext(workerCtx); !ok || userID != "user-1" {
		t.Fatalf("restored user = %q, %v", userID, ok)
	}
	if role := TenantRoleFromContext(workerCtx); role != TenantRoleAdmin {
		t.Fatalf("restored role = %q", role)
	}
	if !IsSystemAdminFromContext(workerCtx) {
		t.Fatal("system administrator flag was not restored")
	}
}

func TestTaskInitiatorOmitsSyntheticAndLegacyActors(t *testing.T) {
	synthetic := context.WithValue(context.Background(), UserIDContextKey, "system-42")
	if got := TaskInitiatorFromContext(synthetic); got.UserID != "" {
		t.Fatalf("synthetic initiator = %#v", got)
	}

	legacyCtx := (TaskInitiator{}).Apply(context.Background())
	if userID, ok := UserIDFromContext(legacyCtx); ok || userID != "" {
		t.Fatalf("legacy user = %q, %v", userID, ok)
	}
}

func TestTaskInitiatorSnapshotsAPIKeyWithoutRestoringScope(t *testing.T) {
	requestCtx := WithTenantAPIKeyScope(context.Background(), TenantAPIKeyScope{
		KeyID: 9, Name: "alice-mcp",
	})
	requestCtx = context.WithValue(requestCtx, UserIDContextKey, "system-7")

	initiator := TaskInitiatorFromContext(requestCtx)
	if initiator.UserID != "" {
		t.Fatalf("synthetic user should stay empty, got %#v", initiator)
	}
	if initiator.APIKeyID != 9 || initiator.APIKeyName != "alice-mcp" {
		t.Fatalf("TaskInitiatorFromContext() = %#v", initiator)
	}

	workerCtx := initiator.Apply(context.Background())
	if _, ok := TenantAPIKeyScopeFromContext(workerCtx); ok {
		t.Fatal("Apply must not restore TenantAPIKeyScope")
	}
	got, ok := AuditAPIKeyFromContext(workerCtx)
	if !ok || got.ID != 9 || got.Name != "alice-mcp" {
		t.Fatalf("AuditAPIKeyFromContext() = %#v, %v", got, ok)
	}
	if userID, ok := UserIDFromContext(workerCtx); ok || userID != "" {
		t.Fatalf("worker user = %q, %v", userID, ok)
	}
}

// Agent audit attribution on a queued mutation must survive without promoting
// the synthetic caller into a human or carrying its admin authority.
func TestAgentTaskInitiatorPreservesAttributionWithoutSyntheticAuthority(t *testing.T) {
	action := AuditAgentAction{SessionID: "s1", Operation: "knowledge.update", ActingVia: "automation:job-7"}
	ctx := WithAuditAgentAction(context.Background(), action)
	ctx = context.WithValue(ctx, UserIDContextKey, "system-7")
	ctx = context.WithValue(ctx, SystemAdminContextKey, true)
	initiator := TaskInitiatorFromContext(ctx)
	worker := initiator.Apply(context.Background())
	if got, ok := AuditAgentActionFromContext(worker); !ok || got != action {
		t.Fatalf("restored action = %#v, %v", got, ok)
	}
	if id, ok := UserIDFromContext(worker); ok || id != "" || IsSystemAdminFromContext(worker) {
		t.Fatal("synthetic agent action restored a user or administrator grant")
	}
}

func TestTaskInitiatorKeepsHumanActorAndAPIKey(t *testing.T) {
	requestCtx := WithTenantAPIKeyScope(context.Background(), TenantAPIKeyScope{
		KeyID: 3, Name: "bob-mcp",
	})
	requestCtx = context.WithValue(requestCtx, UserIDContextKey, "user-1")
	requestCtx = context.WithValue(requestCtx, TenantRoleContextKey, TenantRoleAdmin)

	initiator := TaskInitiatorFromContext(requestCtx)
	if initiator.UserID != "user-1" || initiator.APIKeyID != 3 || initiator.APIKeyName != "bob-mcp" {
		t.Fatalf("TaskInitiatorFromContext() = %#v", initiator)
	}

	workerCtx := initiator.Apply(context.Background())
	if userID, ok := UserIDFromContext(workerCtx); !ok || userID != "user-1" {
		t.Fatalf("restored user = %q, %v", userID, ok)
	}
	if _, ok := TenantAPIKeyScopeFromContext(workerCtx); ok {
		t.Fatal("Apply must not restore TenantAPIKeyScope")
	}
	got, ok := AuditAPIKeyFromContext(workerCtx)
	if !ok || got.Name != "bob-mcp" {
		t.Fatalf("AuditAPIKeyFromContext() = %#v, %v", got, ok)
	}
}
