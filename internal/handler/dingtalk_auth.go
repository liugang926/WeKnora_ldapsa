package handler

import (
	"encoding/base64"
	"encoding/json"
	stderrors "errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/config"
	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/handler/dto"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	secutils "github.com/Tencent/WeKnora/internal/utils"
)

const (
	dingTalkStateCookieName   = "dingtalk_state"
	dingTalkStateCookieMaxAge = 600
)

// DingTalkAuthHandler implements the DingTalk enterprise SSO HTTP surface.
// It deliberately mirrors the OIDC handler: signed-state cookie, 302 back
// to the SPA with a #dingtalk_result=/#dingtalk_error= fragment, and the
// same AuthLoginResponse shape for the in-app flow.
type DingTalkAuthHandler struct {
	userService interfaces.UserService
	configInfo  *config.Config
}

// NewDingTalkAuthHandler creates a new DingTalk auth handler instance.
func NewDingTalkAuthHandler(
	configInfo *config.Config,
	userService interfaces.UserService,
) *DingTalkAuthHandler {
	return &DingTalkAuthHandler{
		userService: userService,
		configInfo:  configInfo,
	}
}

func (h *DingTalkAuthHandler) dingTalkEnabled() bool {
	return h.configInfo != nil && h.configInfo.DingTalkAuth != nil && h.configInfo.DingTalkAuth.Enable
}

// dingTalkCallbackURL resolves the absolute /auth/dingtalk/callback URL.
// Lookup order: the configured redirect_url (explicit or derived from
// frontend_base_url at load time), then the request's own origin — the same
// fallback the OIDC start endpoint uses for external deep links.
func (h *DingTalkAuthHandler) dingTalkCallbackURL(c *gin.Context) string {
	if h.configInfo != nil && h.configInfo.DingTalkAuth != nil {
		if configured := strings.TrimSpace(h.configInfo.DingTalkAuth.RedirectURL); configured != "" {
			return configured
		}
	}
	scheme := "http"
	if c.Request.TLS != nil || strings.EqualFold(c.GetHeader("X-Forwarded-Proto"), "https") {
		scheme = "https"
	}
	return scheme + "://" + c.Request.Host + "/api/v1/auth/dingtalk/callback"
}

// setDingTalkStateCookie binds the state nonce to this browser so an
// attacker cannot replay their own authorization code into a victim's
// callback (same defence as the OIDC nonce cookie).
func setDingTalkStateCookie(c *gin.Context, nonce string) {
	if nonce == "" {
		return
	}
	secure := c.Request.TLS != nil || strings.EqualFold(c.GetHeader("X-Forwarded-Proto"), "https")
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(dingTalkStateCookieName, nonce, dingTalkStateCookieMaxAge, "/", "", secure, true)
}

// GetDingTalkConfig godoc
// @Summary      获取钉钉登录配置
// @Description  返回钉钉企业SSO是否启用以及provider展示名称，供前端决定是否展示钉钉登录入口
// @Tags         认证
// @Produce      json
// @Success      200  {object}  types.DingTalkConfigResponse
// @Router       /auth/dingtalk/config [get]
func (h *DingTalkAuthHandler) GetDingTalkConfig(c *gin.Context) {
	resp, err := h.userService.GetDingTalkAuthConfig(c.Request.Context())
	if err != nil {
		logger.Errorf(c.Request.Context(), "Failed to load DingTalk auth config: %v", err)
		appErr := apperrors.NewServiceUnavailableError("DingTalk auth config unavailable")
		c.Error(appErr)
		return
	}
	c.JSON(http.StatusOK, resp)
}

// GetDingTalkAuthorizationURL godoc
// @Summary      获取钉钉授权地址
// @Description  根据后端钉钉配置生成扫码/授权页跳转地址，并通过 dingtalk_state cookie 绑定 state
// @Tags         认证
// @Produce      json
// @Success      200  {object}  types.DingTalkAuthURLResponse
// @Failure      404  {object}  errors.AppError  "钉钉登录未启用"
// @Router       /auth/dingtalk/url [get]
func (h *DingTalkAuthHandler) GetDingTalkAuthorizationURL(c *gin.Context) {
	ctx := c.Request.Context()
	if !h.dingTalkEnabled() {
		c.JSON(http.StatusNotFound, gin.H{"success": false, "message": "dingtalk auth disabled"})
		return
	}
	resp, err := h.userService.BuildDingTalkAuthorizeURL(ctx, h.dingTalkCallbackURL(c))
	if err != nil {
		logger.Errorf(ctx, "Failed to generate DingTalk authorization URL: %v", err)
		appErr := apperrors.NewForbiddenError("DingTalk authorization unavailable").WithDetails(err.Error())
		c.Error(appErr)
		return
	}
	setDingTalkStateCookie(c, resp.Nonce)
	c.JSON(http.StatusOK, resp)
}

// DingTalkRedirectCallback godoc
// @Summary      钉钉登录重定向回调
// @Description  校验state后完成code交换，302回前端登录页并以 #dingtalk_result= / #dingtalk_error= 携带结果
// @Tags         认证
// @Param        code   query string false "钉钉授权码"
// @Param        state  query string false "钉钉状态"
// @Success      302
// @Router       /auth/dingtalk/callback [get]
func (h *DingTalkAuthHandler) DingTalkRedirectCallback(c *gin.Context) {
	ctx := c.Request.Context()
	frontendRedirectURI := "/"

	state := strings.TrimSpace(c.Query("state"))
	code := strings.TrimSpace(c.Query("code"))
	// A callback hit with NEITHER code NOR state is a direct navigation, not a
	// completed OAuth transaction — typically a DingTalk client opening the
	// workbench app whose homepage was misconfigured to the callback URL
	// (DingTalk clients then probe it on every open). Treat it as a plain
	// entry-point hit and land on the SPA root instead of surfacing a
	// misleading "session expired" error for an already-valid session.
	if state == "" && code == "" {
		c.Redirect(http.StatusFound, frontendRedirectURI)
		return
	}
	if err := verifyDingTalkState(state, c.Request); err != nil {
		logger.Errorf(ctx, "Failed to verify DingTalk state: %v", err)
		c.Redirect(http.StatusFound, frontendRedirectURI+"#dingtalk_error="+urlQueryEscape("invalid_state"))
		return
	}
	// One-time use: clear the binding cookie as soon as it is checked.
	c.SetCookie(dingTalkStateCookieName, "", -1, "/", "", false, true)

	if code == "" {
		c.Redirect(http.StatusFound, frontendRedirectURI+"#dingtalk_error="+urlQueryEscape("missing_code"))
		return
	}

	resp, err := h.userService.LoginWithDingTalkCode(ctx, code)
	if err != nil {
		reason := types.DingTalkRejectUnavailable
		if rejection, ok := asDingTalkRejection(err); ok {
			reason = rejection.Reason
		}
		logger.Warnf(ctx, "DingTalk redirect callback rejected login: reason=%s err=%v", reason, err)
		c.Redirect(http.StatusFound, frontendRedirectURI+"#dingtalk_error="+urlQueryEscape(reason))
		return
	}

	payload, err := encodeDingTalkCallbackPayload(resp)
	if err != nil {
		logger.Errorf(ctx, "Failed to encode DingTalk callback payload: %v", err)
		c.Redirect(http.StatusFound, frontendRedirectURI+"#dingtalk_error="+urlQueryEscape("payload_encode_failed"))
		return
	}
	c.Redirect(http.StatusFound, frontendRedirectURI+"#dingtalk_result="+urlQueryEscape(payload))
}

// verifyDingTalkState checks the HMAC signature, freshness, and the browser
// cookie binding of the state token (AC-06).
func verifyDingTalkState(raw string, req *http.Request) error {
	payload, err := secutils.VerifyDingTalkState(raw)
	if err != nil {
		return err
	}
	cookieNonce, err := req.Cookie(dingTalkStateCookieName)
	if err != nil || cookieNonce == nil || strings.TrimSpace(cookieNonce.Value) == "" {
		return apperrors.NewValidationError("dingtalk state cookie missing")
	}
	if cookieNonce.Value != payload.Nonce {
		return apperrors.NewValidationError("dingtalk state nonce mismatch")
	}
	return nil
}

// encodeDingTalkCallbackPayload mirrors the OIDC callback payload exactly
// (base64url(JSON), token/refresh_token/user/tenant/memberships plus
// is_new_user:false — DingTalk SSO never provisions accounts).
func encodeDingTalkCallbackPayload(resp *types.OIDCCallbackResponse) (string, error) {
	payload, err := json.Marshal(dto.NewAuthOIDCCallbackResponse(resp))
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

// DingTalkInAppLogin godoc
// @Summary      钉钉工作台内免登
// @Description  用钉钉客户端签发的免登 authCode 完成登录，返回与密码登录同形状的响应
// @Tags         认证
// @Accept       json
// @Produce      json
// @Param        request  body  object  true  "{\"auth_code\":\"...\"}"
// @Success      200      {object}  dto.AuthLoginResponse
// @Failure      401      {object}  errors.AppError  "非企业成员/工号不匹配等"
// @Failure      403      {object}  errors.AppError  "账号或目录身份被禁用"
// @Failure      503      {object}  errors.AppError  "钉钉接口故障（fail-closed）"
// @Router       /auth/dingtalk/inapp [post]
func (h *DingTalkAuthHandler) DingTalkInAppLogin(c *gin.Context) {
	ctx := c.Request.Context()
	if !h.dingTalkEnabled() {
		_ = c.Error(apperrors.NewForbiddenError("dingtalk auth disabled"))
		return
	}

	var request struct {
		AuthCode string `json:"auth_code"`
	}
	if err := c.ShouldBindJSON(&request); err != nil || strings.TrimSpace(request.AuthCode) == "" {
		_ = c.Error(apperrors.NewValidationError("auth_code is required"))
		return
	}

	resp, err := h.userService.LoginWithDingTalkAuthCode(ctx, strings.TrimSpace(request.AuthCode))
	if err != nil {
		writeDingTalkLoginError(c, err)
		return
	}
	c.JSON(http.StatusOK, dto.NewAuthLoginResponse(resp))
}

// writeDingTalkLoginError maps service failures onto the documented
// statuses: 401 for identity/matching rejections, 403 for disabled-class
// rejections, 503 for DingTalk-side outages and unexpected errors
// (fail-closed: no silent degradation).
func writeDingTalkLoginError(c *gin.Context, err error) {
	rejection, ok := asDingTalkRejection(err)
	if !ok {
		c.Error(apperrors.NewServiceUnavailableError(
			"DingTalk authentication is temporarily unavailable"))
		return
	}
	switch rejection.Reason {
	case types.DingTalkRejectUserInactive, types.DingTalkRejectADOrUserDisabled,
		types.DingTalkRejectDisabled:
		c.Error(apperrors.NewForbiddenError(rejection.Message))
	case types.DingTalkRejectUnavailable:
		c.Error(apperrors.NewServiceUnavailableError(rejection.Message))
	default:
		c.Error(apperrors.NewUnauthorizedError(rejection.Message))
	}
}

// asDingTalkRejection unwraps a typed login rejection, if any.
func asDingTalkRejection(err error) (*service.DingTalkRejectionError, bool) {
	var rejection *service.DingTalkRejectionError
	if stderrors.As(err, &rejection) {
		return rejection, true
	}
	return nil, false
}
