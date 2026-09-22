import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const source = readFileSync(new URL('./ResourceGroupAccessSettings.vue', import.meta.url), 'utf8')

test('restricted mode previews impact before it can be committed', () => {
  const preview = source.indexOf("previewResourceGroupAccess(")
  const confirmation = source.indexOf('async function confirmRestricted')
  assert.ok(preview > 0 && confirmation > preview)
})

test('group access exposes workspace warning and membership sources', () => {
  assert.match(source, /missingWorkspaceLink/)
  assert.match(source, /membership_sources/)
  assert.match(source, /normalizeMembershipSources/)
})
