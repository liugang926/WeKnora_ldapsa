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

// dingTalkEffectiveInfo resolves the module's effective runtime state. In
// file mode this is exactly the static config check the handler always did;
// in database mode it consults the settings row through the service's cached
// loader. A resolution failure is an availability error (fail closed), never
// a silent "disabled".
func (h *DingTalkAuthHandler) dingTalkEffectiveInfo(c *gin.Context) (*types.DingTalkAuthInfo, error) {
	info, err := h.userService.DingTalkEffectiveAuthInfo(c.Request.Context())
	if err != nil {
		logger.Errorf(c.Request.Context(), "Failed to resolve DingTalk effective config: %v", err)
		return nil, apperrors.NewServiceUnavailableError("DingTalk auth config unavailable")
	}
	if info == nil {
		info = &types.DingTalkAuthInfo{}
	}
	return info, nil
}

// dingTalkCallbackURL resolves the absolute /auth/dingtalk/callback URL.
// Lookup order: the effective configured redirect_url (explicit or derived
// from frontend_base_url at load time — resolved from env in file mode, from
// the settings row in database mode), then the request's own origin — the
// same fallback the OIDC start endpoint uses for external deep links.
func dingTalkCallbackURL(c *gin.Context, info *types.DingTalkAuthInfo) string {
	if configured := strings.TrimSpace(info.RedirectURL); configured != "" {
		return configured
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
		_ = c.Error(appErr)
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
	info, err := h.dingTalkEffectiveInfo(c)
	if err != nil {
		_ = c.Error(err)
		return
	}
	if !info.Enabled {
		c.JSON(http.StatusNotFound, gin.H{"success": false, "message": "dingtalk auth disabled"})
		return
	}
	resp, err := h.userService.BuildDingTalkAuthorizeURL(ctx, dingTalkCallbackURL(c, info))
	if err != nil {
		logger.Errorf(ctx, "Failed to generate DingTalk authorization URL: %v", err)
		appErr := apperrors.NewForbiddenError("DingTalk authorization unavailable").WithDetails(err.Error())
		_ = c.Error(appErr)
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
	info, err := h.dingTalkEffectiveInfo(c)
	if err != nil {
		_ = c.Error(err)
		return
	}
	if !info.Enabled {
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
		_ = c.Error(apperrors.NewServiceUnavailableError(
			"DingTalk authentication is temporarily unavailable"))
		return
	}
	switch rejection.Reason {
	case types.DingTalkRejectUserInactive, types.DingTalkRejectADOrUserDisabled,
		types.DingTalkRejectDisabled:
		_ = c.Error(apperrors.NewForbiddenError(rejection.Message))
	case types.DingTalkRejectUnavailable:
		_ = c.Error(apperrors.NewServiceUnavailableError(rejection.Message))
	default:
		_ = c.Error(apperrors.NewUnauthorizedError(rejection.Message))
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

// RegisterAdminRoutes wires the system-admin DingTalk configuration surface.
// The caller must attach the existing authentication and SystemAdmin guards
// before invoking it, mirroring DirectoryHandler.RegisterAdminRoutes. Two
// further layers cover these endpoints by construction: the employee
// write-block middleware rejects non-admin writes under /api/v1/system, and
// the API-key gate leaves them human-only; SystemAdmin RBAC remains the
// authoritative guard.
func (h *DingTalkAuthHandler) RegisterAdminRoutes(group *gin.RouterGroup) {
	group.GET("/dingtalk/config", h.GetAdminConfig)
	group.PUT("/dingtalk/config", h.UpdateAdminConfig)
	group.POST("/dingtalk/adopt", h.AdoptAdminConfig)
	group.POST("/dingtalk/test", h.TestAdminConnection)
}

// GetAdminConfig godoc
// @Summary      获取钉钉 SSO 管理配置
// @Description  返回脱敏后的钉钉 SSO 生效配置（不含 app secret），供系统管理设置页展示
// @Tags         系统管理
// @Produce      json
// @Success      200  {object}  types.DingTalkAdminConfig
// @Failure      503  {object}  errors.AppError  "配置加载失败"
// @Router       /system/admin/dingtalk/config [get]
func (h *DingTalkAuthHandler) GetAdminConfig(c *gin.Context) {
	result, err := h.userService.GetDingTalkAdminConfig(c.Request.Context())
	if err != nil {
		dingTalkAdminHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// UpdateAdminConfig godoc
// @Summary      保存钉钉 SSO 管理配置
// @Description  保存数据库托管的钉钉 SSO 配置（app_secret 留空保持不变，clear_secret 清空回落 env），启用时必须可用
// @Tags         系统管理
// @Accept       json
// @Produce      json
// @Param        request  body  types.DingTalkAdminConfigUpdate  true  "钉钉配置"
// @Success      200      {object}  types.DingTalkAdminConfig
// @Failure      400      {object}  errors.AppError  "启用但缺少 corp_id/app_key/secret"
// @Failure      409      {object}  errors.AppError  "file 模式只读"
// @Router       /system/admin/dingtalk/config [put]
func (h *DingTalkAuthHandler) UpdateAdminConfig(c *gin.Context) {
	var request types.DingTalkAdminConfigUpdate
	if err := c.ShouldBindJSON(&request); err != nil {
		_ = c.Error(
			apperrors.NewValidationError("Invalid dingtalk configuration").
				WithDetails(err.Error()),
		)
		return
	}
	result, err := h.userService.UpdateDingTalkAdminConfig(c.Request.Context(), &request)
	if err != nil {
		dingTalkAdminHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// AdoptAdminConfig godoc
// @Summary      将环境配置导入为页面管理
// @Description  把当前 env/YAML 的钉钉配置（含密文密封的 app secret）写入设置行并切换为页面管理，无需重启；DINGTALK_AUTH_CONFIG_SOURCE=file 时拒绝（硬回退开关）
// @Tags         系统管理
// @Produce      json
// @Success      200  {object}  types.DingTalkAdminConfig
// @Failure      409  {object}  errors.AppError  "已被页面管理或 env 强制 file 模式"
// @Router       /system/admin/dingtalk/adopt [post]
func (h *DingTalkAuthHandler) AdoptAdminConfig(c *gin.Context) {
	result, err := h.userService.AdoptDingTalkEnvConfig(c.Request.Context())
	if err != nil {
		dingTalkAdminHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// TestAdminConnection godoc
// @Summary      测试钉钉凭据
// @Description  用表单候选值（未给字段回落当前生效值）调用钉钉 accessToken 接口验证凭据，不落盘
// @Tags         系统管理
// @Accept       json
// @Produce      json
// @Param        request  body  types.DingTalkAdminConfigUpdate  true  "候选配置"
// @Success      200      {object}  types.DingTalkTestResult
// @Failure      400      {object}  errors.AppError  "缺少 corp_id/app_key/secret"
// @Router       /system/admin/dingtalk/test [post]
func (h *DingTalkAuthHandler) TestAdminConnection(c *gin.Context) {
	var request types.DingTalkAdminConfigUpdate
	if err := c.ShouldBindJSON(&request); err != nil {
		_ = c.Error(
			apperrors.NewValidationError("Invalid dingtalk test configuration").
				WithDetails(err.Error()),
		)
		return
	}
	result, err := h.userService.TestDingTalkConnection(c.Request.Context(), &request)
	if err != nil {
		dingTalkAdminHTTPError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// dingTalkAdminHTTPError maps admin-surface failures onto the directory
// admin surface's status contract: validation 400, file-managed 409, and
// anything else 503 (fail closed, no secret-bearing details).
func dingTalkAdminHTTPError(c *gin.Context, err error) {
	switch {
	case stderrors.Is(err, service.ErrInvalidDingTalkConfig),
		stderrors.Is(err, service.ErrDirectoryEncryptionKey):
		_ = c.Error(apperrors.NewValidationError(err.Error()))
	case stderrors.Is(err, service.ErrDingTalkConfigFileManaged),
		stderrors.Is(err, service.ErrDingTalkConfigAlreadyManaged):
		_ = c.Error(apperrors.NewConflictError(err.Error()))
	default:
		_ = c.Error(
			apperrors.NewServiceUnavailableError("DingTalk configuration is temporarily unavailable"))
	}
}
