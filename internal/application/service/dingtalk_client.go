package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/logger"
)

// Fixed DingTalk Open Platform endpoints. They are constants, not
// configuration: the module only talks to DingTalk's own API surface, so
// there is no SSRF surface to constrain.
const (
	dingTalkAuthorizeEndpoint  = "https://login.dingtalk.com/oauth2/auth"
	dingTalkUserTokenEndpoint  = "https://api.dingtalk.com/v1.0/oauth2/userAccessToken"
	dingTalkContactMeEndpoint  = "https://api.dingtalk.com/v1.0/contact/users/me"
	dingTalkCorpTokenEndpoint  = "https://api.dingtalk.com/v1.0/oauth2/accessToken"
	dingTalkUnionIDEndpoint    = "https://oapi.dingtalk.com/topapi/user/getbyunionid"
	dingTalkInAppUserEndpoint  = "https://oapi.dingtalk.com/topapi/v2/user/getuserinfo"
	dingTalkUserDetailEndpoint = "https://oapi.dingtalk.com/topapi/v2/user/get"
)

const (
	dingTalkRequestTimeout = 10 * time.Second
	// dingTalkCorpTokenMargin refreshes the enterprise access token this long
	// before its reported expiry so a login never races token rollover.
	dingTalkCorpTokenMargin = 60 * time.Second
	dingTalkResponseBodyCap = 1 << 20
)

var (
	// ErrDingTalkNotEnterpriseMember is returned when the DingTalk user
	// cannot be resolved inside the configured enterprise. For the web flow
	// this is the real-time enterprise-membership check (AC-07).
	ErrDingTalkNotEnterpriseMember = errors.New("dingtalk user is not a member of the configured enterprise")
	// ErrDingTalkAPITransient wraps transport failures and DingTalk server
	// errors so callers can fail closed (503) without blaming the user.
	ErrDingTalkAPITransient = errors.New("dingtalk api temporarily unavailable")
	// ErrDingTalkConfig is returned before any request when the module is
	// missing its AppKey/AppSecret.
	ErrDingTalkConfig = errors.New("dingtalk auth is not configured")
)

// dingTalkBusinessError marks a definitive oapi errcode!=0 answer. Unlike
// transport failures these are not retried and not transient: the caller
// resolved to a rejection, not an outage.
type dingTalkBusinessError struct {
	endpoint string
	errCode  int64
	errMsg   string
}

func (e *dingTalkBusinessError) Error() string {
	return fmt.Sprintf("dingtalk business error at %s: errcode=%d", e.endpoint, e.errCode)
}

// DingTalkCredentialError marks a definitive answer from the credential
// verification endpoint: the platform examined appKey/appSecret and rejected
// them (bad secret, unknown app key, disabled app). Code keeps DingTalk's
// own symbolic/numeric answer verbatim; Message is the platform text.
type DingTalkCredentialError struct {
	Code    string
	Message string
}

func (e *DingTalkCredentialError) Error() string {
	return fmt.Sprintf("dingtalk rejected the app credentials: code=%s message=%s", e.Code, e.Message)
}

// DingTalkClient is the narrow outbound surface of the DingTalk Open
// Platform used by the SSO flow. Production wiring uses dingTalkHTTPClient;
// tests substitute a fake.
type DingTalkClient interface {
	// BuildAuthorizeURL composes the QR/authorization page URL for the web
	// login flow.
	BuildAuthorizeURL(redirectURL, state string) string
	// ExchangeUserAccessToken swaps the one-time authorization code for a
	// user-scoped access token.
	ExchangeUserAccessToken(ctx context.Context, code string) (string, error)
	// GetUserContactMe fetches the caller's union/corp identity with the
	// user-scoped token.
	GetUserContactMe(ctx context.Context, userAccessToken string) (unionID, corpID, nick string, err error)
	// GetCorpAccessToken returns the cached enterprise access token.
	GetCorpAccessToken(ctx context.Context) (string, error)
	// GetUserIDByUnionID resolves the enterprise-scoped userid. A user who
	// left the enterprise fails here with ErrDingTalkNotEnterpriseMember.
	GetUserIDByUnionID(ctx context.Context, unionID string) (userid string, err error)
	// GetInAppUserID resolves the workbench免登 auth code to a userid. The
	// endpoint is enterprise-scoped, so failures map to
	// ErrDingTalkNotEnterpriseMember.
	GetInAppUserID(ctx context.Context, authCode string) (userid string, err error)
	// GetUserDetail loads job number / name / unionID for a userid.
	GetUserDetail(ctx context.Context, userid string) (jobNumber, name, unionID string, err error)
	// VerifyCredentials proves the appKey/appSecret pair by fetching an
	// enterprise access token from POST /v1.0/oauth2/accessToken. A
	// definitive platform rejection returns *DingTalkCredentialError;
	// transport failures return ErrDingTalkAPITransient.
	VerifyCredentials(ctx context.Context) error
}

type dingTalkHTTPClient struct {
	appKey     string
	appSecret  string
	httpClient *http.Client

	mu              sync.Mutex
	corpToken       string
	corpTokenExpiry time.Time
}

// NewDingTalkClient returns the production DingTalk Open Platform client.
// A nil configuration still returns a usable client whose calls fail with
// ErrDingTalkConfig — the service layer gates on configuration before
// making any request.
func NewDingTalkClient(cfg *config.DingTalkAuthConfig) DingTalkClient {
	c := &dingTalkHTTPClient{
		httpClient: &http.Client{Timeout: dingTalkRequestTimeout},
	}
	if cfg != nil {
		c.appKey = strings.TrimSpace(cfg.AppKey)
		c.appSecret = cfg.AppSecret
	}
	return c
}

func (c *dingTalkHTTPClient) BuildAuthorizeURL(redirectURL, state string) string {
	query := url.Values{}
	query.Set("redirect_uri", redirectURL)
	query.Set("response_type", "code")
	query.Set("client_id", c.appKey)
	query.Set("scope", "openid corpid")
	query.Set("state", state)
	query.Set("prompt", "consent")
	return dingTalkAuthorizeEndpoint + "?" + query.Encode()
}

type dingTalkUserTokenRequest struct {
	ClientID     string `json:"clientId"`
	ClientSecret string `json:"clientSecret"`
	Code         string `json:"code"`
	GrantType    string `json:"grantType"`
}

type dingTalkUserTokenResponse struct {
	AccessToken string `json:"accessToken"`
	ExpireIn    int64  `json:"expireIn"`
}

func (c *dingTalkHTTPClient) ExchangeUserAccessToken(ctx context.Context, code string) (string, error) {
	if c.appKey == "" || c.appSecret == "" {
		return "", ErrDingTalkConfig
	}
	raw, err := json.Marshal(dingTalkUserTokenRequest{
		ClientID:     c.appKey,
		ClientSecret: c.appSecret,
		Code:         code,
		GrantType:    "authorization_code",
	})
	if err != nil {
		return "", fmt.Errorf("marshal dingtalk request: %w", err)
	}
	var payload dingTalkUserTokenResponse
	if err := c.doJSON(ctx, http.MethodPost, dingTalkUserTokenEndpoint, nil, raw, &payload); err != nil {
		return "", err
	}
	if strings.TrimSpace(payload.AccessToken) == "" {
		return "", fmt.Errorf("%w: empty user access token", ErrDingTalkAPITransient)
	}
	return payload.AccessToken, nil
}

type dingTalkContactMeResponse struct {
	UnionID string `json:"unionId"`
	Nick    string `json:"nick"`
	CorpID  string `json:"corpId"`
	OpenID  string `json:"openId"`
}

func (c *dingTalkHTTPClient) GetUserContactMe(
	ctx context.Context,
	userAccessToken string,
) (string, string, string, error) {
	headers := map[string]string{"x-acs-dingtalk-access-token": userAccessToken}
	var payload dingTalkContactMeResponse
	if err := c.doJSON(ctx, http.MethodGet, dingTalkContactMeEndpoint, headers, nil, &payload); err != nil {
		return "", "", "", err
	}
	if strings.TrimSpace(payload.UnionID) == "" {
		return "", "", "", fmt.Errorf("%w: contact profile missing unionId", ErrDingTalkAPITransient)
	}
	return payload.UnionID, payload.CorpID, payload.Nick, nil
}

type dingTalkCorpTokenResponse struct {
	AccessToken string `json:"accessToken"`
	ExpireIn    int64  `json:"expireIn"`
}

// GetCorpAccessToken caches the enterprise access token in-process until
// 60 seconds before its expiry. A mutex keeps concurrent logins from
// stampeding the token endpoint.
func (c *dingTalkHTTPClient) GetCorpAccessToken(ctx context.Context) (string, error) {
	if c.appKey == "" || c.appSecret == "" {
		return "", ErrDingTalkConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.corpToken != "" && time.Now().Before(c.corpTokenExpiry) {
		return c.corpToken, nil
	}
	raw, err := json.Marshal(map[string]string{
		"appKey":    c.appKey,
		"appSecret": c.appSecret,
	})
	if err != nil {
		return "", fmt.Errorf("marshal dingtalk request: %w", err)
	}
	var payload dingTalkCorpTokenResponse
	if err := c.doJSON(ctx, http.MethodPost, dingTalkCorpTokenEndpoint, nil, raw, &payload); err != nil {
		return "", err
	}
	if strings.TrimSpace(payload.AccessToken) == "" {
		return "", fmt.Errorf("%w: empty corp access token", ErrDingTalkAPITransient)
	}
	ttl := time.Duration(payload.ExpireIn) * time.Second
	if ttl <= dingTalkCorpTokenMargin {
		ttl = dingTalkCorpTokenMargin
	}
	c.corpToken = payload.AccessToken
	c.corpTokenExpiry = time.Now().Add(ttl - dingTalkCorpTokenMargin)
	return c.corpToken, nil
}

type dingTalkOAPIEnvelope struct {
	ErrCode int64           `json:"errcode"`
	ErrMsg  string          `json:"errmsg"`
	Result  json.RawMessage `json:"result"`
}

type dingTalkUserIDResult struct {
	UserID string `json:"userid"`
}

func (c *dingTalkHTTPClient) GetUserIDByUnionID(ctx context.Context, unionID string) (string, error) {
	var result dingTalkUserIDResult
	if err := c.postOAPI(ctx, dingTalkUnionIDEndpoint, map[string]string{"unionid": unionID}, &result); err != nil {
		return "", notEnterpriseMemberOrTransient(err)
	}
	if strings.TrimSpace(result.UserID) == "" {
		return "", ErrDingTalkNotEnterpriseMember
	}
	return result.UserID, nil
}

func (c *dingTalkHTTPClient) GetInAppUserID(ctx context.Context, authCode string) (string, error) {
	var result dingTalkUserIDResult
	if err := c.postOAPI(ctx, dingTalkInAppUserEndpoint, map[string]string{"code": authCode}, &result); err != nil {
		return "", notEnterpriseMemberOrTransient(err)
	}
	if strings.TrimSpace(result.UserID) == "" {
		return "", ErrDingTalkNotEnterpriseMember
	}
	return result.UserID, nil
}

type dingTalkUserDetailResult struct {
	UserID    string `json:"userid"`
	JobNumber string `json:"job_number"`
	Name      string `json:"name"`
	UnionID   string `json:"unionid"`
}

func (c *dingTalkHTTPClient) GetUserDetail(ctx context.Context, userid string) (string, string, string, error) {
	var result dingTalkUserDetailResult
	if err := c.postOAPI(ctx, dingTalkUserDetailEndpoint, map[string]string{"userid": userid}, &result); err != nil {
		return "", "", "", notEnterpriseMemberOrTransient(err)
	}
	return result.JobNumber, result.Name, result.UnionID, nil
}

// notEnterpriseMemberOrTransient keeps the two failure families apart:
// oapi business errors are definitive rejections (the unionId/authCode/userid
// does not resolve inside this enterprise), everything else stays transient.
func notEnterpriseMemberOrTransient(err error) error {
	var biz *dingTalkBusinessError
	if errors.As(err, &biz) {
		return fmt.Errorf("%w: errcode=%d", ErrDingTalkNotEnterpriseMember, biz.errCode)
	}
	return err
}

// postOAPI performs a legacy oapi.dingtalk.com call: token in the query
// string, JSON body in, {errcode, errmsg, result} envelope out. errcode!=0
// is a business failure (returned as *dingTalkBusinessError), never retried.
func (c *dingTalkHTTPClient) postOAPI(
	ctx context.Context,
	endpoint string,
	body map[string]string,
	out any,
) error {
	corpToken, err := c.GetCorpAccessToken(ctx)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal dingtalk request: %w", err)
	}
	query := url.Values{}
	query.Set("access_token", corpToken)
	target := endpoint + "?" + query.Encode()
	var envelope dingTalkOAPIEnvelope
	if err := c.doJSON(ctx, http.MethodPost, target, nil, raw, &envelope); err != nil {
		return err
	}
	if envelope.ErrCode != 0 {
		logger.Warnf(ctx, "DingTalk API business error at %s: errcode=%d errmsg=%s",
			endpoint, envelope.ErrCode, envelope.ErrMsg)
		return &dingTalkBusinessError{endpoint: endpoint, errCode: envelope.ErrCode, errMsg: envelope.ErrMsg}
	}
	if len(envelope.Result) == 0 {
		return fmt.Errorf("dingtalk api %s returned no result", endpoint)
	}
	if err := json.Unmarshal(envelope.Result, out); err != nil {
		return fmt.Errorf("%w: decode %s result", ErrDingTalkAPITransient, endpoint)
	}
	return nil
}

// doJSON executes a DingTalk request with one transport-level retry and
// decodes the JSON response into out. The body is passed as raw bytes so
// the retry can rebuild the request without re-winding a reader. Non-2xx
// statuses are transient failures; error strings carry the (redacted)
// endpoint, never the token, secret, or code.
func (c *dingTalkHTTPClient) doJSON(
	ctx context.Context,
	method, endpoint string,
	headers map[string]string,
	body []byte,
	out any,
) error {
	execute := func() (*http.Response, error) {
		var payload io.Reader
		if body != nil {
			payload = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, endpoint, payload)
		if err != nil {
			return nil, fmt.Errorf("build dingtalk request: %w", err)
		}
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		req.Header.Set("Accept", "application/json")
		for key, value := range headers {
			req.Header.Set(key, value)
		}
		return c.httpClient.Do(req)
	}
	resp, err := execute()
	if err != nil {
		resp, err = execute()
		if err != nil {
			return fmt.Errorf("%w: %s", ErrDingTalkAPITransient, redactDingTalkURL(endpoint))
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("%w: %s status=%d", ErrDingTalkAPITransient, redactDingTalkURL(endpoint), resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, dingTalkResponseBodyCap)).Decode(out); err != nil {
		return fmt.Errorf("%w: decode %s response", ErrDingTalkAPITransient, endpoint)
	}
	return nil
}

// redactDingTalkURL strips the access_token query parameter before an
// endpoint URL can reach a log line or error string.
func redactDingTalkURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	values := parsed.Query()
	if values.Has("access_token") {
		values.Set("access_token", "[redacted]")
		parsed.RawQuery = values.Encode()
	}
	return parsed.String()
}

// dingTalkCredentialErrorBody accepts both DingTalk error dialects: the
// v1.0 surface answers {"code": "...", "message": "..."} with symbolic
// codes, the legacy oapi envelope uses {"errcode": n, "errmsg": "..."}.
type dingTalkCredentialErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	ErrCode int64  `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
}

func (b dingTalkCredentialErrorBody) credentialError() *DingTalkCredentialError {
	code := strings.TrimSpace(b.Code)
	if code == "" && b.ErrCode != 0 {
		code = fmt.Sprintf("%d", b.ErrCode)
	}
	message := strings.TrimSpace(b.Message)
	if message == "" {
		message = strings.TrimSpace(b.ErrMsg)
	}
	return &DingTalkCredentialError{Code: code, Message: message}
}

// VerifyCredentials fetches an enterprise access token with the configured
// appKey/appSecret. Unlike the login flow it must distinguish "the platform
// rejected these credentials" from "the platform is unreachable", so it
// decodes the error body of non-2xx answers instead of collapsing them into
// ErrDingTalkAPITransient. One transport-level retry, same as doJSON; the
// 10s client timeout bounds the whole call.
func (c *dingTalkHTTPClient) VerifyCredentials(ctx context.Context) error {
	if c.appKey == "" || c.appSecret == "" {
		return ErrDingTalkConfig
	}
	raw, err := json.Marshal(map[string]string{
		"appKey":    c.appKey,
		"appSecret": c.appSecret,
	})
	if err != nil {
		return fmt.Errorf("marshal dingtalk request: %w", err)
	}
	execute := func() (*http.Response, error) {
		req, err := http.NewRequestWithContext(
			ctx, http.MethodPost, dingTalkCorpTokenEndpoint, bytes.NewReader(raw))
		if err != nil {
			return nil, fmt.Errorf("build dingtalk request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		return c.httpClient.Do(req)
	}
	resp, err := execute()
	if err != nil {
		resp, err = execute()
		if err != nil {
			return fmt.Errorf("%w: %s", ErrDingTalkAPITransient, dingTalkCorpTokenEndpoint)
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var body dingTalkCredentialErrorBody
		if err := json.NewDecoder(io.LimitReader(resp.Body, dingTalkResponseBodyCap)).Decode(&body); err != nil {
			return fmt.Errorf("%w: %s status=%d",
				ErrDingTalkAPITransient, dingTalkCorpTokenEndpoint, resp.StatusCode)
		}
		credentialErr := body.credentialError()
		if credentialErr.Code == "" && credentialErr.Message == "" {
			return fmt.Errorf("%w: %s status=%d",
				ErrDingTalkAPITransient, dingTalkCorpTokenEndpoint, resp.StatusCode)
		}
		return credentialErr
	}
	var payload dingTalkCorpTokenResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, dingTalkResponseBodyCap)).Decode(&payload); err != nil {
		return fmt.Errorf("%w: decode %s response", ErrDingTalkAPITransient, dingTalkCorpTokenEndpoint)
	}
	if strings.TrimSpace(payload.AccessToken) == "" {
		return fmt.Errorf("%w: empty corp access token", ErrDingTalkAPITransient)
	}
	return nil
}
