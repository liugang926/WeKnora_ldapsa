import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const directory = readFileSync(new URL('./directory/index.ts', import.meta.url), 'utf8')
const tenantGroups = readFileSync(new URL('./tenant/groups.ts', import.meta.url), 'utf8')
const resourceAccess = readFileSync(new URL('./group-access.ts', import.meta.url), 'utf8')

test('directory administration uses the system-admin API namespace', () => {
  assert.match(directory, /\/api\/v1\/system\/admin\/directory\/config/)
  assert.match(directory, /\/sync\/preview/)
  assert.match(directory, /\/sync\/runs/)
})

test('workspace and resource grants use their scoped API namespaces', () => {
  assert.match(tenantGroups, /\/api\/v1\/tenants\/\$\{tenantId\}\/directory-groups/)
  assert.match(resourceAccess, /\/api\/v1\/group-access\/\$\{resourceType\}\/\$\{encodeURIComponent\(resourceId\)\}/)
})
