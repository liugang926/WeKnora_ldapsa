// Auth-callback credential preloading (dingtalk_result / oidc_result).
//
// The SPA consumes login results from the URL hash in App.vue's onMounted.
// Route components mount (and fire their initial API burst) before that
// async handler gets a chance to persist the token, so the boot requests
// historically went out WITHOUT an Authorization header, tripped the 401
// refresh path and forceReloginRedirect() cleared the fresh session — the
// "session expired right after SSO login" loop. This module decodes the
// callback payload synchronously during bootstrap, before the router serves
// any page, so the token is already in the store when components mount.
// App.vue keeps ownership of navigation, notifications and error handling.

import { parseDingTalkCallbackHash } from './dingtalk'

export interface AuthCallbackCredentials {
  token: string
  refreshToken?: string
}

/** base64url → JSON, tolerant of stripped padding (mirrors decodeOIDCResult). */
export function decodeAuthCallbackPayload(encoded: string): Record<string, any> | null {
  try {
    const normalized = encoded.replace(/-/g, '+').replace(/_/g, '/')
    const padded = normalized + '='.repeat((4 - (normalized.length % 4)) % 4)
    // globalThis.atob exists in browsers and Node >= 16 alike.
    const binary = globalThis.atob(padded)
    const bytes = Uint8Array.from(binary, char => char.charCodeAt(0))
    return JSON.parse(new TextDecoder().decode(bytes))
  } catch {
    return null
  }
}

/** Extract login credentials from a dingtalk/oidc callback hash, if any. */
export function authCallbackCredentialsFromHash(hash: string): AuthCallbackCredentials | null {
  const raw = hash.startsWith('#') ? hash.slice(1) : hash
  if (!raw) return null
  const params = new URLSearchParams(raw)
  const dingtalk = parseDingTalkCallbackHash(hash)
  const encoded = dingtalk.result || params.get('oidc_result') || ''
  if (!encoded) return null
  const payload = decodeAuthCallbackPayload(encoded)
  if (!payload || typeof payload.token !== 'string' || payload.token === '') return null
  return {
    token: payload.token,
    refreshToken: typeof payload.refresh_token === 'string' && payload.refresh_token !== ''
      ? payload.refresh_token
      : undefined,
  }
}

/** True when the current URL carries an unconsumed auth-callback result. */
export function hasPendingAuthCallbackResult(hash = typeof window !== 'undefined' ? window.location.hash : ''): boolean {
  const raw = hash.startsWith('#') ? hash.slice(1) : hash
  if (!raw) return false
  const params = new URLSearchParams(raw)
  return Boolean(params.get('oidc_result') || params.get('dingtalk_result'))
}
