import type {
  GroupAccessMode,
  GroupAccessPermission,
  GroupAccessResourceType,
  ResourceGroupAccessUpdate,
  ResourceGroupGrant,
} from '@/api/group-access'

export function permissionsForResource(
  resourceType: GroupAccessResourceType,
): GroupAccessPermission[] {
  return resourceType === 'knowledge_base' ? ['read', 'edit'] : ['use', 'edit']
}

export function defaultPermissionForResource(
  resourceType: GroupAccessResourceType,
): GroupAccessPermission {
  return resourceType === 'knowledge_base' ? 'read' : 'use'
}

export function hasMissingWorkspaceGroup(grant: Pick<ResourceGroupGrant, 'workspace_role'>): boolean {
  return !grant.workspace_role
}

export function normalizeMembershipSources(sources?: readonly string[]): Array<'direct' | 'nested' | 'primary'> {
  const allowed = new Set(['direct', 'nested', 'primary'])
  return Array.from(new Set(sources || []))
    .filter((source): source is 'direct' | 'nested' | 'primary' => allowed.has(source))
}

export function buildResourceGroupAccessUpdate(
  mode: GroupAccessMode,
  grants: ResourceGroupGrant[],
): ResourceGroupAccessUpdate {
  return {
    mode,
    grants: grants.map((grant) => ({
      directory_id: grant.directory_id,
      directory_group_id: grant.directory_group_id,
      permission: grant.permission,
    })),
  }
}
