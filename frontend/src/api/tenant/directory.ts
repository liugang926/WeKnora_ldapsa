import { get, post } from '@/utils/request'
import { unwrapDirectoryAccessResponse } from '@/api/directory-access-response'
import { buildDirectorySearchQuery } from '@/api/directory/query'
import type { DirectoryObjectSummary, DirectoryGroupMembersResult } from '@/api/directory'
import type { DirectoryTenantRole } from './groups'

export interface DirectoryCandidate extends DirectoryObjectSummary {
  directory_group_id?: string
  linked_user_id?: string
  status?: string
}
export interface DirectoryCatalog {
  items: DirectoryCandidate[]
  total: number
  enabled: boolean
  fresh: boolean
  last_success_at?: string
  sync_interval_seconds: number
}
export async function getTenantDirectoryCatalog(tenant: number, kind: 'users' | 'groups', query: string, limit: number, offset: number) {
  return unwrapDirectoryAccessResponse<DirectoryCatalog>(await get(`/api/v1/tenants/${tenant}/directory/catalog/${kind}?${buildDirectorySearchQuery(query, limit, offset)}`))
}
export async function getTenantDirectoryGroupMembers(tenant: number, guid: string, query: string, limit: number, offset: number) {
  return unwrapDirectoryAccessResponse<DirectoryGroupMembersResult>(await get(
    `/api/v1/tenants/${tenant}/directory/groups/${encodeURIComponent(guid)}/members?${buildDirectorySearchQuery(query, limit, offset)}`,
  ))
}
export async function addTenantDirectoryMember(tenant: number, objectGUID: string, role: DirectoryTenantRole) {
  return post(`/api/v1/tenants/${tenant}/directory/members`, { object_guid: objectGUID, role })
}
