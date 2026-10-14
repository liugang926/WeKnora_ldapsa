/** Server projection exists only for an enabled, restricted resource policy. */
export function hasResourceGroupEdit(resource?: { group_access_permission?: string } | null): boolean {
  return resource?.group_access_permission === 'edit'
}
