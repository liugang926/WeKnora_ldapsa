import assert from 'node:assert/strict'
import test from 'node:test'

import {
  buildResourceGroupAccessUpdate,
  defaultPermissionForResource,
  hasMissingWorkspaceGroup,
  normalizeMembershipSources,
  permissionsForResource,
} from './groupAccess.ts'

test('resource permissions use the correct read/use vocabulary', () => {
  assert.deepEqual(permissionsForResource('knowledge_base'), ['read', 'edit'])
  assert.deepEqual(permissionsForResource('agent'), ['use', 'edit'])
  assert.equal(defaultPermissionForResource('knowledge_base'), 'read')
  assert.equal(defaultPermissionForResource('agent'), 'use')
})

test('missing workspace links fail closed and membership sources are normalized', () => {
  assert.equal(hasMissingWorkspaceGroup({ workspace_role: null }), true)
  assert.equal(hasMissingWorkspaceGroup({ workspace_role: 'viewer' }), false)
  assert.deepEqual(normalizeMembershipSources(['nested', 'direct', 'nested', 'unknown']), ['nested', 'direct'])
})

test('access update strips display-only directory metadata', () => {
  assert.deepEqual(buildResourceGroupAccessUpdate('restricted', [{
    display_name: 'Finance',
    directory_id: 'corp',
    directory_group_id: 'guid-1',
    permission: 'read',
    workspace_role: 'viewer',
    effective_member_count: 42,
  }]), {
    mode: 'restricted',
    grants: [{ directory_id: 'corp', directory_group_id: 'guid-1', permission: 'read' }],
  })
})
