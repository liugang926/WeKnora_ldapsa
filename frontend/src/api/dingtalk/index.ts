import { get, post, put } from '@/utils/request'

/** Accept both the project's standard `{ success, data }` envelope and a direct payload. */
function unwrapDingTalkAdminResponse<T>(response: unknown): T {
  if (response && typeof response === 'object' && 'data' in response) {
    return (response as { data: T }).data
  }
  return response as T
}

export type DingTalkConfigSource = 'file' | 'database'
export type DingTalkSecretSource = 'database' | 'env' | 'none'

/**
 * Secret-free DingTalk SSO configuration returned to the administrator UI.
 * app_secret is intentionally absent; a replacement secret is accepted only
 * by DingTalkConfigUpdate and is never copied back into the form.
 */
export interface DingTalkAdminConfig {
  success: boolean
  config_source: DingTalkConfigSource | string
  enabled: boolean
  corp_id: string
  app_key: string
  agent_id: string
  display_name: string
  redirect_url: string
  has_secret: boolean
  secret_from: DingTalkSecretSource | string
  config_version: number
  updated_by: string
  updated_at?: string
}

export interface DingTalkConfigUpdate {
  enabled: boolean
  corp_id: string
  app_key: string
  agent_id: string
  display_name: string
  redirect_url: string
  /** Omit or leave empty to preserve the stored secret. */
  app_secret?: string
  /** Explicitly drop the stored secret (falls back to env). */
  clear_secret?: boolean
}

export interface DingTalkTestResult {
  ok: boolean
  error_code?: string
  message?: string
}

export async function getDingTalkAdminConfig(): Promise<DingTalkAdminConfig> {
  return unwrapDingTalkAdminResponse<DingTalkAdminConfig>(
    await get('/api/v1/system/admin/dingtalk/config'),
  )
}

export async function updateDingTalkAdminConfig(
  payload: DingTalkConfigUpdate,
): Promise<DingTalkAdminConfig> {
  return unwrapDingTalkAdminResponse<DingTalkAdminConfig>(
    await put('/api/v1/system/admin/dingtalk/config', payload),
  )
}

export async function adoptDingTalkEnvConfig(): Promise<DingTalkAdminConfig> {
  return unwrapDingTalkAdminResponse<DingTalkAdminConfig>(
    await post('/api/v1/system/admin/dingtalk/adopt', {}),
  )
}

export async function testDingTalkConnection(
  payload: Partial<DingTalkConfigUpdate>,
): Promise<DingTalkTestResult> {
  return unwrapDingTalkAdminResponse<DingTalkTestResult>(
    await post('/api/v1/system/admin/dingtalk/test', payload),
  )
}
