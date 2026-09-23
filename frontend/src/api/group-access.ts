import { get, post, put } from '@/utils/request'
import { unwrapDirectoryAccessResponse } from '@/api/directory-access-response'
import type { DirectoryTenantRole } from '@/api/tenant/groups'

export type GroupAccessResourceType = 'knowledge_base' | 'agent'
export type GroupAccessMode = 'inherit' | 'restricted'
export type GroupAccessPermission = 'read' | 'edit' | 'use'
export type GroupMembershipSource = 'direct' | 'nested' | 'primary'

export interface ResourceGroupGrant {
  id?: string
  directory_id: string
  directory_group_id: string
  display_name: string
  dn?: string
  permission: GroupAccessPermission
  workspace_role?: DirectoryTenantRole | null
  direct_member_count?: number
  effective_member_count?: number
  membership_sources?: GroupMembershipSource[]
}

export interface ResourceGroupCandidate {
  directory_id: string
  directory_group_id: string
  display_name: string
  dn?: string
  workspace_role?: DirectoryTenantRole | null
  direct_member_count?: number
  effective_member_count?: number
}

export interface ResourceGroupAccess {
  resource_type: GroupAccessResourceType
  resource_id: string
  tenant_id: number
  mode: GroupAccessMode
  grants: ResourceGroupGrant[]
  available_groups: ResourceGroupCandidate[]
  updated_at?: string
}

export interface ResourceGroupAccessUpdate {
  mode: GroupAccessMode
  grants: Array<{
    directory_id: string
    directory_group_id: string
    permission: GroupAccessPermission
  }>
}

export interface ResourceGroupAccessImpact {
  currently_allowed: number
  allowed_after: number
  losing_access: number
  gaining_access: number
  unaffected_managers: number
  warnings?: string[]
}

function resourceAccessBase(resourceType: GroupAccessResourceType, resourceId: string): string {
  return `/api/v1/group-access/${resourceType}/${encodeURIComponent(resourceId)}`
}

export async function getResourceGroupAccess(
  resourceType: GroupAccessResourceType,
  resourceId: string,
): Promise<ResourceGroupAccess> {
  return unwrapDirectoryAccessResponse<ResourceGroupAccess>(await get(resourceAccessBase(resourceType, resourceId)))
}

export async function previewResourceGroupAccess(
  resourceType: GroupAccessResourceType,
  resourceId: string,
  payload: ResourceGroupAccessUpdate,
): Promise<ResourceGroupAccessImpact> {
  return unwrapDirectoryAccessResponse<ResourceGroupAccessImpact>(
    await post(`${resourceAccessBase(resourceType, resourceId)}/preview`, payload),
  )
}

export async function updateResourceGroupAccess(
  resourceType: GroupAccessResourceType,
  resourceId: string,
  payload: ResourceGroupAccessUpdate,
): Promise<ResourceGroupAccess> {
  return unwrapDirectoryAccessResponse<ResourceGroupAccess>(
    await put(resourceAccessBase(resourceType, resourceId), payload),
  )
}
