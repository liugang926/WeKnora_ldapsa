package utils

import (
	"sync"
	"testing"
	"time"
)

func resetDingTalkStateSecret(t *testing.T) {
	t.Helper()
	t.Setenv("JWT_SECRET", "test-dingtalk-state-secret")
	dingTalkStateSecretOnce = sync.Once{}
	dingTalkStateSecret = ""
}

func TestSignAndVerifyDingTalkState(t *testing.T) {
	resetDingTalkStateSecret(t)

	state, err := SignDingTalkState(&DingTalkStatePayload{
		Nonce:    "nonce-abc",
		IssuedAt: time.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("SignDingTalkState: %v", err)
	}
	got, err := VerifyDingTalkState(state)
	if err != nil {
		t.Fatalf("VerifyDingTalkState: %v", err)
	}
	if got.Nonce != "nonce-abc" {
		t.Fatalf("unexpected payload: %+v", got)
	}
}

func TestVerifyDingTalkStateRejectsExpiredToken(t *testing.T) {
	resetDingTalkStateSecret(t)

	state, err := SignDingTalkState(&DingTalkStatePayload{
		Nonce:    "nonce-abc",
		IssuedAt: time.Now().Add(-15 * time.Minute).Unix(),
	})
	if err != nil {
		t.Fatalf("SignDingTalkState: %v", err)
	}
	if _, err := VerifyDingTalkState(state); err == nil {
		t.Fatal("expected expired state to be rejected")
	}
}

// TestVerifyDingTalkStateRejectsOIDCStateToken pins the cross-protocol
// isolation: the two state formats share a signing-key source but distinct
// payloads and secrets, so an OIDC state token must never verify as a
// DingTalk state (and vice versa).
func TestVerifyDingTalkStateRejectsOIDCStateToken(t *testing.T) {
	resetDingTalkStateSecret(t)

	oidcState, err := SignOIDCState(&OIDCStatePayload{
		Nonce:       "nonce-abc",
		RedirectURI: "http://localhost:5173/login",
		IssuedAt:    time.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("SignOIDCState: %v", err)
	}
	if _, err := VerifyDingTalkState(oidcState); err == nil {
		t.Fatal("expected an OIDC state token to fail DingTalk verification")
	}

	dingTalkState, err := SignDingTalkState(&DingTalkStatePayload{
		Nonce:    "nonce-abc",
		IssuedAt: time.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("SignDingTalkState: %v", err)
	}
	if _, err := VerifyOIDCState(dingTalkState); err == nil {
		t.Fatal("expected a DingTalk state token to fail OIDC verification")
	}
}

func TestVerifyDingTalkStateRejectsTamperedSignature(t *testing.T) {
	resetDingTalkStateSecret(t)

	state, err := SignDingTalkState(&DingTalkStatePayload{Nonce: "nonce-abc"})
	if err != nil {
		t.Fatalf("SignDingTalkState: %v", err)
	}
	if _, err := VerifyDingTalkState(state + "x"); err == nil {
		t.Fatal("expected tampered state to be rejected")
	}
}
