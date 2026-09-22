import { get, post, put } from '@/utils/request'
import { unwrapDirectoryAccessResponse } from '@/api/directory-access-response'
import { buildDirectorySearchQuery } from './query'

export { buildDirectorySearchQuery } from './query'

export type DirectoryTransport = 'ldaps' | 'starttls'
export type DirectoryConfigSource = 'database' | 'env' | 'file' | 'default' | 'mixed'
export type DirectoryRunStatus = 'queued' | 'running' | 'success' | 'failed'

export interface DirectoryServer {
  address: string
  server_name?: string
}

/**
 * Secret-free directory configuration returned to the administrator UI.
 * bind_password is intentionally absent. A replacement secret is accepted only
 * by DirectoryConfigUpdate and is never copied into the local form after save.
 */
export interface DirectoryConfig {
  enabled: boolean
  display_name: string
  servers: DirectoryServer[]
  transport: DirectoryTransport
  ca_file?: string
  base_dn: string
  user_base_dn?: string
  group_base_dn?: string
  bind_dn: string
  user_filter: string
  group_filter: string
  allowed_login_filter?: string
  login_attributes: Array<'sAMAccountName' | 'userPrincipalName' | string>
  connect_timeout_seconds: number
  query_timeout_seconds: number
  result_limit: number
  page_size: number
  sync_interval_seconds: number
  stale_after_seconds: number
  has_bind_password: boolean
  source: DirectoryConfigSource
  /** Dotted or top-level form paths controlled by deployment config. */
  read_only_fields: string[]
  /** Human-readable secret source; never contains the secret itself. */
  bind_password_source?: DirectoryConfigSource
  ca_source?: DirectoryConfigSource
}

export interface DirectoryConfigUpdate extends Omit<
  DirectoryConfig,
  'has_bind_password' | 'source' | 'read_only_fields' | 'bind_password_source' | 'ca_source'
> {
  /** Omit to preserve the current encrypted/file/env-backed secret. */
  bind_password?: string
}

export interface DirectoryHealth {
  enabled: boolean
  available: boolean
  access_paused: boolean
  syncing: boolean
  active_server?: string
  last_attempt_at?: string
  last_success_at?: string
  last_error?: string
  consecutive_failures?: number
  next_sync_at?: string
}

export interface DirectoryTestResult {
  ok: boolean
  server?: string
  latency_ms?: number
  message?: string
}

export interface DirectoryObjectSummary {
  directory_id: string
  object_guid: string
  sid?: string
  dn: string
  display_name: string
  email?: string
  account_name?: string
  user_principal_name?: string
  disabled?: boolean
}

export interface DirectoryGroupSummary extends DirectoryObjectSummary {
  direct_member_count?: number
  effective_member_count?: number
  parent_group_count?: number
}

export interface DirectorySearchResult<T> {
  items: T[]
  total: number
  truncated?: boolean
  warning?: string
}

export interface DirectoryMembershipOrigin {
  source: 'direct' | 'primary' | 'nested'
  origin_source: 'direct' | 'primary'
  depth: number
  path: DirectoryObjectSummary[]
}

export interface DirectoryGroupMember extends DirectoryObjectSummary {
  origins: DirectoryMembershipOrigin[]
}

export interface DirectoryGroupMembersResult {
  group: DirectoryObjectSummary
  items: DirectoryGroupMember[]
  total: number
  parent_groups: DirectoryObjectSummary[]
  child_groups: DirectoryObjectSummary[]
  unresolved_member_count: number
}

export async function getDirectoryGroupMembers(guid: string, query = '', limit = 20, offset = 0): Promise<DirectoryGroupMembersResult> {
  return unwrapDirectoryAccessResponse<DirectoryGroupMembersResult>(
    await get(`/api/v1/system/admin/directory/groups/${encodeURIComponent(guid)}/members?${buildDirectorySearchQuery(query, limit, offset)}`),
  )
}

export interface DirectorySyncPreview {
  users: { create: number; update: number; disable: number; unchanged: number }
  groups: { create: number; update: number; remove: number; unchanged: number }
  memberships: { add: number; remove: number; unchanged: number }
  warnings?: string[]
  complete: boolean
}

export interface DirectorySyncRun {
  id: string
  trigger: 'scheduled' | 'manual' | string
  status: DirectoryRunStatus
  started_at?: string
  finished_at?: string
  users_seen?: number
  groups_seen?: number
  memberships_seen?: number
  error?: string
  warnings?: string[]
}

export interface DirectorySyncRunsResponse {
  runs: DirectorySyncRun[]
  total: number
}

export async function getDirectoryConfig(): Promise<DirectoryConfig> {
  return unwrapDirectoryAccessResponse<DirectoryConfig>(await get('/api/v1/system/admin/directory/config'))
}

export async function updateDirectoryConfig(payload: DirectoryConfigUpdate): Promise<DirectoryConfig> {
  return unwrapDirectoryAccessResponse<DirectoryConfig>(await put('/api/v1/system/admin/directory/config', payload))
}

export async function getDirectoryStatus(): Promise<DirectoryHealth> {
  return unwrapDirectoryAccessResponse<DirectoryHealth>(await get('/api/v1/system/admin/directory/status'))
}

export async function testDirectoryConnection(
  payload: Partial<DirectoryConfigUpdate>,
): Promise<DirectoryTestResult> {
  return unwrapDirectoryAccessResponse<DirectoryTestResult>(await post('/api/v1/system/admin/directory/test', payload))
}

export async function searchDirectoryUsers(query: string, limit = 20, offset = 0): Promise<DirectorySearchResult<DirectoryObjectSummary>> {
  const qs = buildDirectorySearchQuery(query, limit, offset)
  return unwrapDirectoryAccessResponse<DirectorySearchResult<DirectoryObjectSummary>>(
    await get(`/api/v1/system/admin/directory/users?${qs}`),
  )
}

export async function searchDirectoryGroups(query: string, limit = 20, offset = 0): Promise<DirectorySearchResult<DirectoryGroupSummary>> {
  const qs = buildDirectorySearchQuery(query, limit, offset)
  return unwrapDirectoryAccessResponse<DirectorySearchResult<DirectoryGroupSummary>>(
    await get(`/api/v1/system/admin/directory/groups?${qs}`),
  )
}

export async function previewDirectorySync(): Promise<DirectorySyncPreview> {
  return unwrapDirectoryAccessResponse<DirectorySyncPreview>(
    await post('/api/v1/system/admin/directory/sync/preview', {}),
  )
}

export async function startDirectorySync(): Promise<DirectorySyncRun> {
  return unwrapDirectoryAccessResponse<DirectorySyncRun>(await post('/api/v1/system/admin/directory/sync', {}))
}

export async function listDirectorySyncRuns(limit = 20): Promise<DirectorySyncRunsResponse> {
  const safeLimit = Math.max(1, Math.min(100, Math.floor(limit) || 20))
  return unwrapDirectoryAccessResponse<DirectorySyncRunsResponse>(
    await get(`/api/v1/system/admin/directory/sync/runs?limit=${safeLimit}`),
  )
}
