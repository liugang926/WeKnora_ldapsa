// DingTalk SSO helpers shared between the login page (QR entry +
// in-app autologin) and App.vue (#dingtalk_result= / #dingtalk_error=
// callback handling). Everything here is browser/DOM-only and free of
// Vue imports so the pure parsers stay unit-testable under node.

/** 官方 JSAPI CDN（钉钉容器内免登用）。禁止新增 npm 依赖，动态注入即可。 */
export const DINGTALK_JSAPI_SRC =
  'https://g.alicdn.com/dingding/dingtalk-jsapi/3.0.25/dingtalk.open.js'

/** 免登 authCode 获取的超时：钉钉容器异常时不能让用户永远停在登录页。 */
export const DINGTALK_AUTH_CODE_TIMEOUT_MS = 8000

/** 钉钉 UA 特征：钉钉桌面端 / 移动端内置浏览器均带该标识。 */
export function isDingTalkContainer(userAgent = typeof navigator !== 'undefined' ? navigator.userAgent : ''): boolean {
  return userAgent.includes('DingTalk')
}

/**
 * 解析回跳 hash 中的钉钉登录结果。
 *
 * 后端整页 302 回 `${origin}/#dingtalk_result=<base64url(JSON)>` 或
 * `#dingtalk_error=<reason_code>`，与 `#oidc_result=` 完全同构；hash 里
 * 可能同时带有其它 query 片段，因此走 URLSearchParams 而不是裸字符串
 * 截取。返回值三个字段都缺省时表示 hash 与钉钉回跳无关。
 */
export interface DingTalkCallbackHash {
  result?: string
  error?: string
}

export function parseDingTalkCallbackHash(hash: string): DingTalkCallbackHash {
  const raw = hash.startsWith('#') ? hash.slice(1) : hash
  if (!raw) return {}
  const params = new URLSearchParams(raw)
  const result = params.get('dingtalk_result') || ''
  const error = params.get('dingtalk_error') || ''
  // 未携带钉钉字段的 hash 返回空对象，交给 OIDC / 路由原逻辑处理。
  if (!result && !error) return {}
  const parsed: DingTalkCallbackHash = {}
  if (result) parsed.result = result
  if (error) parsed.error = error
  return parsed
}

/** hash 是否携带钉钉回跳结果（供路由守卫放行，让 App.vue 有机会消费）。 */
export function hasPendingDingTalkCallback(hash = typeof window !== 'undefined' ? window.location.hash : ''): boolean {
  const { result, error } = parseDingTalkCallbackHash(hash)
  return !!result || !!error
}

/**
 * 后端失败原因码 → i18n key。映射表与规格书文案一一对应；未知名走
 * fallback。key 全部静态书写，便于 i18n key 审计扫描。
 */
export const DINGTALK_ERROR_KEYS: Record<string, string> = {
  not_enterprise_member: 'auth.dingtalkErrors.not_enterprise_member',
  no_ad_match: 'auth.dingtalkErrors.no_ad_match',
  identity_link_required: 'auth.dingtalkErrors.identity_link_required',
  empty_job_number: 'auth.dingtalkErrors.empty_job_number',
  duplicate_job_number: 'auth.dingtalkErrors.duplicate_job_number',
  binding_conflict: 'auth.dingtalkErrors.binding_conflict',
  user_inactive: 'auth.dingtalkErrors.user_inactive',
  invalid_state: 'auth.dingtalkErrors.invalid_state',
  dingtalk_unavailable: 'auth.dingtalkErrors.dingtalk_unavailable',
  wrong_corp: 'auth.dingtalkErrors.wrong_corp',
}

export function resolveDingTalkErrorKey(reason?: string | null): string {
  if (!reason) return 'auth.dingtalkErrors.fallback'
  return DINGTALK_ERROR_KEYS[reason] || 'auth.dingtalkErrors.fallback'
}

/**
 * 已知原因码才返回 i18n key，未知返回 null——调用方（免登失败分支）
 * 需要区分“映射文案”与“透出后端原始 message”。
 */
export function lookupDingTalkErrorKey(reason?: string | null): string | null {
  if (!reason) return null
  return DINGTALK_ERROR_KEYS[reason] || null
}

// ---- JSAPI 动态加载 ------------------------------------------------------
//
// 仅在钉钉容器内按需注入一次；加载失败 / 超时都以 reject 收口，由调用
// 方降级到普通 AD 登录表单。不新增 npm 依赖（规格红线）。

export interface DingTalkJSAPI {
  runtime?: {
    permission?: {
      requestAuthCode?: (options: {
        corpId: string
        onSuccess?: (payload: { code?: string } | string) => void
        onFail?: (error: unknown) => void
      }) => void
    }
  }
}

declare global {
  interface Window {
    dd?: DingTalkJSAPI
  }
}

let jsapiPromise: Promise<DingTalkJSAPI> | null = null

export function loadDingTalkJSAPI(src = DINGTALK_JSAPI_SRC): Promise<DingTalkJSAPI> {
  if (typeof window === 'undefined') {
    return Promise.reject(new Error('DingTalk JSAPI is browser-only'))
  }
  if (window.dd) return Promise.resolve(window.dd)
  if (jsapiPromise) return jsapiPromise

  jsapiPromise = new Promise<DingTalkJSAPI>((resolve, reject) => {
    const existing = document.querySelector<HTMLScriptElement>(`script[src="${src}"]`)
    const script = existing || document.createElement('script')
    const onLoad = () => {
      if (window.dd) resolve(window.dd)
      else reject(new Error('DingTalk JSAPI loaded without dd global'))
    }
    script.addEventListener('load', onLoad, { once: true })
    script.addEventListener(
      'error',
      () => reject(new Error('Failed to load DingTalk JSAPI')),
      { once: true },
    )
    if (!existing) {
      script.src = src
      document.head.appendChild(script)
    } else if (window.dd) {
      // 复用已存在的 script 标签时不会再触发 load 事件。
      resolve(window.dd)
    }
  })
  return jsapiPromise
}

/**
 * 向钉钉容器索取免登 auth_code。requestAuthCode 没有 Promise 版本，
 * 且失败时可能既不回调 onSuccess 也不回调 onFail，因此统一套一层
 * 超时（默认 8s），超时按失败处理。
 */
export function requestDingTalkAuthCode(
  corpId: string,
  timeoutMs = DINGTALK_AUTH_CODE_TIMEOUT_MS,
): Promise<string> {
  return loadDingTalkJSAPI().then(
    dd =>
      new Promise<string>((resolve, reject) => {
        const requestAuthCode = dd?.runtime?.permission?.requestAuthCode
        if (typeof requestAuthCode !== 'function') {
          reject(new Error('DingTalk JSAPI requestAuthCode unavailable'))
          return
        }
        let settled = false
        const timer = setTimeout(() => {
          if (settled) return
          settled = true
          reject(new Error('DingTalk auth code request timed out'))
        }, timeoutMs)
        requestAuthCode({
          corpId,
          onSuccess: payload => {
            if (settled) return
            settled = true
            clearTimeout(timer)
            const code =
              typeof payload === 'string' ? payload : payload?.code || ''
            if (code) resolve(code)
            else reject(new Error('DingTalk auth code is empty'))
          },
          onFail: error => {
            if (settled) return
            settled = true
            clearTimeout(timer)
            reject(error instanceof Error ? error : new Error('DingTalk auth code request failed'))
          },
        })
      }),
  )
}
