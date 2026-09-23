import { del, get, post, put } from '@/utils/request'
import { unwrapDirectoryAccessResponse } from '@/api/directory-access-response'

export type DirectoryTenantRole = 'viewer' | 'contributor' | 'admin'

export interface TenantDirectoryGroup {
  id: string
  tenant_id: number
  directory_id: string
  directory_group_id: string
  object_guid?: string
  sid?: string
  dn: string
  display_name: string
  role: DirectoryTenantRole
  direct_member_count?: number
  effective_member_count?: number
  nested_group_count?: number
  updated_at?: string
}

export interface TenantDirectoryGroupCandidate {
  directory_id: string
  directory_group_id: string
  object_guid?: string
  sid?: string
  dn: string
  display_name: string
  direct_member_count?: number
  effective_member_count?: number
  linked: boolean
  current_role?: DirectoryTenantRole
}

export interface TenantDirectoryGroupList {
  groups: TenantDirectoryGroup[]
  total: number
}

export interface TenantDirectoryGroupSearch {
  groups: TenantDirectoryGroupCandidate[]
  total: number
  truncated?: boolean
}

function tenantGroupBase(tenantId: number): string {
  return `/api/v1/tenants/${tenantId}/directory-groups`
}

export async function listTenantDirectoryGroups(tenantId: number): Promise<TenantDirectoryGroupList> {
  return unwrapDirectoryAccessResponse<TenantDirectoryGroupList>(await get(tenantGroupBase(tenantId)))
}

export async function searchTenantDirectoryGroups(
  tenantId: number,
  query: string,
  limit = 30,
): Promise<TenantDirectoryGroupSearch> {
  const qs = new URLSearchParams({
    q: query.trim(),
    available: 'true',
    limit: String(Math.max(1, Math.min(100, Math.floor(limit) || 30))),
  })
  return unwrapDirectoryAccessResponse<TenantDirectoryGroupSearch>(await get(`${tenantGroupBase(tenantId)}?${qs}`))
}

export async function addTenantDirectoryGroup(
  tenantId: number,
  body: { directory_id: string; directory_group_id: string; role: DirectoryTenantRole },
): Promise<TenantDirectoryGroup> {
  return unwrapDirectoryAccessResponse<TenantDirectoryGroup>(await post(tenantGroupBase(tenantId), body))
}

export async function updateTenantDirectoryGroupRole(
  tenantId: number,
  grantId: string,
  role: DirectoryTenantRole,
): Promise<TenantDirectoryGroup> {
  return unwrapDirectoryAccessResponse<TenantDirectoryGroup>(
    await put(`${tenantGroupBase(tenantId)}/${encodeURIComponent(grantId)}`, { role }),
  )
}

export async function removeTenantDirectoryGroup(tenantId: number, grantId: string): Promise<void> {
  await del(`${tenantGroupBase(tenantId)}/${encodeURIComponent(grantId)}`)
}
