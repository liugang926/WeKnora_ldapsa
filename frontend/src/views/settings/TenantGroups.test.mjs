import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const source = readFileSync(new URL('./TenantGroups.vue', import.meta.url), 'utf8')

test('directory group workspace roles exclude owner', () => {
  assert.match(source, /\['viewer', 'contributor', 'admin'\]/)
  assert.doesNotMatch(source, /DIRECTORY_GROUP_ROLES[^\n]*owner/)
})

test('directory group UI exposes effective and nested member counts', () => {
  assert.match(source, /effective_member_count/)
  assert.match(source, /nested_group_count/)
})
