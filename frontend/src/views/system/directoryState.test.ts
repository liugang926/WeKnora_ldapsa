import assert from 'node:assert/strict'
import test from 'node:test'

import type { DirectoryConfig } from '@/api/directory'
import {
  buildDirectoryConfigUpdate,
  directoryServersText,
  isDirectoryFieldReadOnly,
  parseDirectoryServers,
} from './directoryState.ts'

test('directory server input removes blanks and duplicate controllers in failover order', () => {
  assert.deepEqual(parseDirectoryServers('dc1:636\n\ndc2:636\ndc1:636'), [
    { address: 'dc1:636' },
    { address: 'dc2:636' },
  ])
  assert.equal(directoryServersText([{ address: 'dc1:636' }, { address: 'dc2:636' }]), 'dc1:636\ndc2:636')
})

test('deployment-managed parent paths make their child controls read-only', () => {
  assert.equal(isDirectoryFieldReadOnly(['servers', 'bind_password'], 'servers'), true)
  assert.equal(isDirectoryFieldReadOnly(['tls.ca_file'], 'tls'), true)
  assert.equal(isDirectoryFieldReadOnly(['base_dn'], 'group_base_dn'), false)
})

test('directory update never sends an unchanged secret', () => {
  const config = {
    enabled: true,
    display_name: ' Corp ',
    servers: [],
    transport: 'ldaps',
    base_dn: ' DC=corp,DC=test ',
    bind_dn: 'CN=reader,DC=corp,DC=test',
    user_filter: '(objectClass=user)',
    group_filter: '(objectClass=group)',
    login_attributes: ['sAMAccountName', 'userPrincipalName'],
    connect_timeout_seconds: 5,
    query_timeout_seconds: 10,
    result_limit: 1000,
    page_size: 500,
    sync_interval_seconds: 300,
    stale_after_seconds: 900,
    has_bind_password: true,
    source: 'database',
    read_only_fields: [],
  } satisfies DirectoryConfig
  const unchanged = buildDirectoryConfigUpdate(config, 'dc1:636', '')
  assert.equal('bind_password' in unchanged, false)
  const replaced = buildDirectoryConfigUpdate(config, 'dc1:636', 'replacement')
  assert.equal(replaced.bind_password, 'replacement')
})
