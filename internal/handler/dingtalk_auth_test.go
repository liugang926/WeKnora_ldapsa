package handler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/handler/dto"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	secutils "github.com/Tencent/WeKnora/internal/utils"
	"github.com/gin-gonic/gin"
)

// stubDingTalkUserService implements only the DingTalk surface of
// UserService; every other call panics via the nil interface embedding.
type stubDingTalkUserService struct {
	interfaces.UserService
	config  func(ctx context.Context) (*types.DingTalkConfigResponse, error)
	authURL func(ctx context.Context, callbackURL string) (*types.DingTalkAuthURLResponse, error)
	login   func(ctx context.Context, code string) (*types.OIDCCallbackResponse, error)
	inApp   func(ctx context.Context, authCode string) (*types.LoginResponse, error)
}

func (s *stubDingTalkUserService) GetDingTalkAuthConfig(
	ctx context.Context,
) (*types.DingTalkConfigResponse, error) {
	return s.config(ctx)
}

func (s *stubDingTalkUserService) BuildDingTalkAuthorizeURL(
	ctx context.Context, callbackURL string,
) (*types.DingTalkAuthURLResponse, error) {
	return s.authURL(ctx, callbackURL)
}

func (s *stubDingTalkUserService) LoginWithDingTalkCode(
	ctx context.Context, code string,
) (*types.OIDCCallbackResponse, error) {
	return s.login(ctx, code)
}

func (s *stubDingTalkUserService) LoginWithDingTalkAuthCode(
	ctx context.Context, authCode string,
) (*types.LoginResponse, error) {
	return s.inApp(ctx, authCode)
}

func newDingTalkTestHandler(enable bool, stub *stubDingTalkUserService) *DingTalkAuthHandler {
	return NewDingTalkAuthHandler(&config.Config{
		DingTalkAuth: &config.DingTalkAuthConfig{
			Enable:    enable,
			CorpID:    "ding-corp-1",
			AppKey:    "ding-app-key",
			AppSecret: "ding-app-secret",
		},
	}, stub)
}

func newDingTalkTestRouter(h *DingTalkAuthHandler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(errorCapture())
	r.GET("/auth/dingtalk/config", h.GetDingTalkConfig)
	r.GET("/auth/dingtalk/url", h.GetDingTalkAuthorizationURL)
	r.GET("/auth/dingtalk/callback", h.DingTalkRedirectCallback)
	r.POST("/auth/dingtalk/inapp", h.DingTalkInAppLogin)
	return r
}

func doDingTalkRequest(r *gin.Engine, method, target string, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	if method == http.MethodPost && strings.Contains(target, "inapp") {
		req.Header.Set("Content-Type", "application/json")
	}
	for _, cookie := range cookies {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func signedDingTalkState(t *testing.T, nonce string, issuedAt time.Time) string {
	t.Helper()
	state, err := secutils.SignDingTalkState(&secutils.DingTalkStatePayload{Nonce: nonce, IssuedAt: issuedAt.Unix()})
	if err != nil {
		t.Fatalf("SignDingTalkState: %v", err)
	}
	return state
}

func TestDingTalkConfigEndpointReturnsProviderConfig(t *testing.T) {
	stub := &stubDingTalkUserService{
		config: func(context.Context) (*types.DingTalkConfigResponse, error) {
			return &types.DingTalkConfigResponse{
				Success: true, Enabled: true, CorpID: "ding-corp-1", ProviderDisplayName: "钉钉",
			}, nil
		},
	}
	r := newDingTalkTestRouter(newDingTalkTestHandler(true, stub))
	w := doDingTalkRequest(r, http.MethodGet, "/auth/dingtalk/config")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var body types.DingTalkConfigResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if !body.Enabled || body.CorpID != "ding-corp-1" || body.ProviderDisplayName != "钉钉" {
		t.Fatalf("body = %+v", body)
	}
}

func TestDingTalkURLDisabledReturns404(t *testing.T) {
	stub := &stubDingTalkUserService{}
	r := newDingTalkTestRouter(newDingTalkTestHandler(false, stub))
	w := doDingTalkRequest(r, http.MethodGet, "/auth/dingtalk/url")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	if !strings.Contains(w.Body.String(), "dingtalk auth disabled") {
		t.Fatalf("body = %s", w.Body.String())
	}
}

func TestDingTalkURLSetsStateCookieAndReturnsURL(t *testing.T) {
	stub := &stubDingTalkUserService{
		authURL: func(_ context.Context, callbackURL string) (*types.DingTalkAuthURLResponse, error) {
			if !strings.HasSuffix(callbackURL, "/api/v1/auth/dingtalk/callback") {
				t.Errorf("callbackURL = %q, want request-origin derived callback", callbackURL)
			}
			return &types.DingTalkAuthURLResponse{
				Success:          true,
				AuthorizationURL: "https://login.dingtalk.com/oauth2/auth?state=signed-state",
				State:            "signed-state",
				Nonce:            "nonce-1",
			}, nil
		},
	}
	r := newDingTalkTestRouter(newDingTalkTestHandler(true, stub))

	req := httptest.NewRequest(http.MethodGet, "/auth/dingtalk/url", nil)
	req.Host = "weknora.example.com"
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var cookie *http.Cookie
	for _, ck := range w.Result().Cookies() {
		if ck.Name == dingTalkStateCookieName {
			cookie = ck
		}
	}
	if cookie == nil || cookie.Value != "nonce-1" {
		t.Fatalf("expected %s cookie with nonce-1, got %+v", dingTalkStateCookieName, cookie)
	}
	if !cookie.HttpOnly {
		t.Fatalf("state cookie must be HttpOnly")
	}
	if cookie.MaxAge != dingTalkStateCookieMaxAge {
		t.Fatalf("MaxAge = %d, want %d", cookie.MaxAge, dingTalkStateCookieMaxAge)
	}
	if !strings.Contains(w.Body.String(), `"authorization_url"`) {
		t.Fatalf("body = %s", w.Body.String())
	}
}

func TestDingTalkCallbackRejectsInvalidStates(t *testing.T) {
	validState := signedDingTalkState(t, "nonce-1", time.Now())
	cookieFor := func(value string) *http.Cookie {
		return &http.Cookie{Name: dingTalkStateCookieName, Value: value}
	}

	cases := []struct {
		name   string
		state  string
		cookie *http.Cookie
	}{
		{"missing state", "", cookieFor("nonce-1")},
		{"missing cookie", validState, nil},
		{"tampered state", validState + "x", cookieFor("nonce-1")},
		{"nonce mismatch", validState, cookieFor("nonce-other")},
		{"expired state", signedDingTalkState(t, "nonce-1", time.Now().Add(-15*time.Minute)), cookieFor("nonce-1")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubDingTalkUserService{
				login: func(context.Context, string) (*types.OIDCCallbackResponse, error) {
					t.Fatal("service must not be called when state verification fails")
					return nil, nil
				},
			}
			r := newDingTalkTestRouter(newDingTalkTestHandler(true, stub))
			target := "/auth/dingtalk/callback?code=abc&state=" + tc.state
			var w *httptest.ResponseRecorder
			if tc.cookie != nil {
				w = doDingTalkRequest(r, http.MethodGet, target, tc.cookie)
			} else {
				w = doDingTalkRequest(r, http.MethodGet, target)
			}
			if w.Code != http.StatusFound {
				t.Fatalf("status = %d, want 302", w.Code)
			}
			if loc := w.Header().Get("Location"); !strings.Contains(loc, "#dingtalk_error=invalid_state") {
				t.Fatalf("Location = %q, want invalid_state fragment", loc)
			}
		})
	}
}

func TestDingTalkCallbackSuccessRedirectsResultFragment(t *testing.T) {
	state := signedDingTalkState(t, "nonce-1", time.Now())
	stub := &stubDingTalkUserService{
		login: func(_ context.Context, code string) (*types.OIDCCallbackResponse, error) {
			if code != "abc" {
				t.Errorf("code = %q, want abc", code)
			}
			return &types.OIDCCallbackResponse{
				Success:      true,
				Message:      "登录成功",
				User:         &types.User{ID: "user-1", Username: "zhangsan"},
				Tenant:       &types.Tenant{ID: 7, Name: "Corp"},
				Memberships:  []types.Membership{{TenantID: 7, TenantName: "Corp", Role: types.TenantRoleViewer}},
				Token:        "jwt-access",
				RefreshToken: "jwt-refresh",
			}, nil
		},
	}
	r := newDingTalkTestRouter(newDingTalkTestHandler(true, stub))
	w := doDingTalkRequest(r, http.MethodGet, "/auth/dingtalk/callback?code=abc&state="+state,
		&http.Cookie{Name: dingTalkStateCookieName, Value: "nonce-1"})

	if w.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302", w.Code)
	}
	location := w.Header().Get("Location")
	if !strings.HasPrefix(location, "/#dingtalk_result=") {
		t.Fatalf("Location = %q, want #dingtalk_result fragment", location)
	}

	// One-time state: the cookie must be cleared on the response.
	var cleared bool
	for _, ck := range w.Result().Cookies() {
		if ck.Name == dingTalkStateCookieName && ck.MaxAge < 0 {
			cleared = true
		}
	}
	if !cleared {
		t.Fatalf("expected %s cookie to be cleared after use", dingTalkStateCookieName)
	}

	encoded := strings.TrimPrefix(location, "/#dingtalk_result=")
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode result payload: %v", err)
	}
	var payload dto.AuthOIDCCallbackResponse
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("unmarshal result payload: %v", err)
	}
	if !payload.Success || payload.Token != "jwt-access" || payload.RefreshToken != "jwt-refresh" {
		t.Fatalf("payload = %+v", payload)
	}
	if payload.User == nil || payload.User.ID != "user-1" {
		t.Fatalf("payload user = %+v", payload.User)
	}
	if payload.Tenant == nil || payload.Tenant.ID != 7 {
		t.Fatalf("payload tenant = %+v", payload.Tenant)
	}
	if len(payload.Memberships) != 1 {
		t.Fatalf("payload memberships = %+v", payload.Memberships)
	}
	if payload.IsNewUser {
		t.Fatalf("dingtalk callback must never report is_new_user")
	}
	if strings.Contains(string(raw), "app-secret") || strings.Contains(string(raw), "ding-app-key") {
		t.Fatalf("payload must not contain credentials: %s", raw)
	}
}

func TestDingTalkCallbackRejectionRedirectsReasonFragment(t *testing.T) {
	state := signedDingTalkState(t, "nonce-1", time.Now())
	stub := &stubDingTalkUserService{
		login: func(context.Context, string) (*types.OIDCCallbackResponse, error) {
			return nil, &service.DingTalkRejectionError{
				Reason:  types.DingTalkRejectNotEnterpriseMember,
				Message: "not a member",
			}
		},
	}
	r := newDingTalkTestRouter(newDingTalkTestHandler(true, stub))
	w := doDingTalkRequest(r, http.MethodGet, "/auth/dingtalk/callback?code=abc&state="+state,
		&http.Cookie{Name: dingTalkStateCookieName, Value: "nonce-1"})

	if w.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302", w.Code)
	}
	if loc := w.Header().Get("Location"); loc != "/#dingtalk_error=not_enterprise_member" {
		t.Fatalf("Location = %q, want reason fragment", loc)
	}
}

func TestDingTalkCallbackMissingCode(t *testing.T) {
	state := signedDingTalkState(t, "nonce-1", time.Now())
	stub := &stubDingTalkUserService{
		login: func(context.Context, string) (*types.OIDCCallbackResponse, error) {
			t.Fatal("service must not be called without a code")
			return nil, nil
		},
	}
	r := newDingTalkTestRouter(newDingTalkTestHandler(true, stub))
	w := doDingTalkRequest(r, http.MethodGet, "/auth/dingtalk/callback?state="+state,
		&http.Cookie{Name: dingTalkStateCookieName, Value: "nonce-1"})

	if w.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302", w.Code)
	}
	if loc := w.Header().Get("Location"); !strings.Contains(loc, "#dingtalk_error=missing_code") {
		t.Fatalf("Location = %q, want missing_code fragment", loc)
	}
}

func TestDingTalkInAppLoginSuccess(t *testing.T) {
	stub := &stubDingTalkUserService{
		inApp: func(context.Context, string) (*types.LoginResponse, error) {
			return &types.LoginResponse{
				Success:      true,
				Message:      "登录成功",
				User:         &types.User{ID: "user-1", Username: "zhangsan"},
				ActiveTenant: &types.Tenant{ID: 7, Name: "Corp"},
				Memberships:  []types.Membership{},
				Token:        "jwt-access",
				RefreshToken: "jwt-refresh",
			}, nil
		},
	}
	r := newDingTalkTestRouter(newDingTalkTestHandler(true, stub))

	req := httptest.NewRequest(http.MethodPost, "/auth/dingtalk/inapp", strings.NewReader(`{"auth_code":"inapp-1"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", w.Code, w.Body.String())
	}
	var body dto.AuthLoginResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if !body.Success || body.Token != "jwt-access" || body.User == nil || body.User.ID != "user-1" {
		t.Fatalf("body = %+v", body)
	}
	// members arrays must serialize (always-populated contract).
	if !strings.Contains(w.Body.String(), `"memberships"`) {
		t.Fatalf("body = %s", w.Body.String())
	}
}

func TestDingTalkInAppLoginStatusMapping(t *testing.T) {
	cases := []struct {
		reason string
		want   int
	}{
		{types.DingTalkRejectNotEnterpriseMember, http.StatusUnauthorized},
		{types.DingTalkRejectNoADMatch, http.StatusUnauthorized},
		{types.DingTalkRejectBindingConflict, http.StatusUnauthorized},
		{types.DingTalkRejectWrongCorp, http.StatusUnauthorized},
		{types.DingTalkRejectEmptyJobNumber, http.StatusUnauthorized},
		{types.DingTalkRejectDuplicateJobNumber, http.StatusUnauthorized},
		{types.DingTalkRejectUserInactive, http.StatusForbidden},
		{types.DingTalkRejectADOrUserDisabled, http.StatusForbidden},
		{types.DingTalkRejectDisabled, http.StatusForbidden},
		{types.DingTalkRejectUnavailable, http.StatusServiceUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.reason, func(t *testing.T) {
			stub := &stubDingTalkUserService{
				inApp: func(context.Context, string) (*types.LoginResponse, error) {
					return nil, &service.DingTalkRejectionError{Reason: tc.reason, Message: "rejected"}
				},
			}
			r := newDingTalkTestRouter(newDingTalkTestHandler(true, stub))

			req := httptest.NewRequest(http.MethodPost, "/auth/dingtalk/inapp",
				strings.NewReader(`{"auth_code":"inapp-1"}`))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if w.Code != tc.want {
				t.Fatalf("status = %d, want %d (body=%s)", w.Code, tc.want, w.Body.String())
			}
		})
	}

	t.Run("unexpected error is 503 fail-closed", func(t *testing.T) {
		stub := &stubDingTalkUserService{
			inApp: func(context.Context, string) (*types.LoginResponse, error) {
				return nil, context.DeadlineExceeded
			},
		}
		r := newDingTalkTestRouter(newDingTalkTestHandler(true, stub))
		req := httptest.NewRequest(http.MethodPost, "/auth/dingtalk/inapp",
			strings.NewReader(`{"auth_code":"inapp-1"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", w.Code)
		}
	})
}

func TestDingTalkInAppLoginValidation(t *testing.T) {
	stub := &stubDingTalkUserService{}

	t.Run("missing auth_code", func(t *testing.T) {
		r := newDingTalkTestRouter(newDingTalkTestHandler(true, stub))
		req := httptest.NewRequest(http.MethodPost, "/auth/dingtalk/inapp", strings.NewReader(`{}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", w.Code)
		}
	})

	t.Run("disabled module", func(t *testing.T) {
		r := newDingTalkTestRouter(newDingTalkTestHandler(false, stub))
		req := httptest.NewRequest(http.MethodPost, "/auth/dingtalk/inapp",
			strings.NewReader(`{"auth_code":"inapp-1"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", w.Code)
		}
	})
}
