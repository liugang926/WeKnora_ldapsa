import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'
import * as vue from 'vue'
import * as groupAccess from '../utils/groupAccess.ts'

const source = readFileSync(new URL('./ResourceGroupAccessSettings.vue', import.meta.url), 'utf8')
const script = source.match(/<script setup lang="ts">([\s\S]*?)<\/script>/)![1]

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((complete) => { resolve = complete })
  return { promise, resolve }
}

function access(resourceId = 'kb-1') {
  return { resource_id: resourceId, mode: 'inherit', grants: [], available_groups: [] }
}

function editor(api: Record<string, (...args: any[]) => any> = {}) {
  const props = { resourceType: 'knowledge_base', resourceId: 'kb-1', tenantId: 1, readOnly: false }
  const watchers: Array<() => void> = []
  const requests: any[] = []
  const defaults = {
    getResourceGroupAccess: async (kind: string, id: string) => access(id),
    previewResourceGroupAccess: async () => ({ currently_allowed: 20, allowed_after: 5, losing_access: 15 }),
    getTenantDirectoryCatalog: async () => ({ items: [], total: 0, enabled: true, fresh: true }),
    updateResourceGroupAccess: async (_kind: string, id: string, payload: any) => {
      requests.push(payload)
      return { ...access(id), mode: payload.mode, grants: payload.grants }
    },
    ...api,
  }
  const modules: Record<string, unknown> = {
    vue: { ...vue, watch: (_source: unknown, callback: () => void) => { watchers.push(callback) } },
    'tdesign-vue-next': { MessagePlugin: { success: () => {}, error: () => {} } },
    'vue-i18n': { useI18n: () => ({ t: (key: string) => key }) },
    '@/api/group-access': defaults,
    '@/api/tenant/directory': defaults,
    '@/utils/groupAccess': groupAccess,
  }
  const compiled = ts.transpileModule(script, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
  }).outputText
  const controller = new Function('require', 'defineProps', 'exports', `${compiled}
    return { mode, grants, candidates, candidateKey, impactVisible, loading, saving, error,
      impact, impactPage, impactPageSize, catalog, catalogVisible, catalogPage, catalogPageSize, catalogAppliedQuery,
      loadAccess, persist, requestModeChange, confirmRestricted, addCandidate, previewCurrentAccess, changeImpactPage,
      loadGroupCatalog, openGroupCatalog, addCatalogGroup }`
  )((name: string) => modules[name], () => props, {})
  return { ...controller, props, requests, reset: () => watchers[0]() }
}

test('groups can be prepared while access still inherits workspace roles', async () => {
  const state = editor()
  state.candidates.value = [{ directory_id: 'corp', directory_group_id: 'finance', display_name: 'Finance' }]
  state.candidateKey.value = 'corp:finance'
  state.addCandidate()
  await new Promise((resolve) => setImmediate(resolve))
  assert.equal(state.mode.value, 'inherit')
  assert.equal(state.requests[0].mode, 'inherit')
  assert.equal(state.requests[0].grants[0].directory_group_id, 'finance')
})

test('restricted confirmation applies exactly the grants in its reviewed preview', async () => {
  const state = editor()
  state.grants.value = [{ directory_id: 'corp', directory_group_id: 'finance', display_name: 'Finance', permission: 'read' }]
  await state.requestModeChange('restricted')
  assert.equal(state.impactVisible.value, true)
  state.grants.value[0].permission = 'edit'
  await state.confirmRestricted()
  assert.equal(state.requests[0].grants[0].permission, 'read')
  assert.equal(state.mode.value, 'restricted')
  assert.equal(state.impactVisible.value, false)
})

test('switching resources discards an in-flight preview and cannot confirm it on the new resource', async () => {
  const preview = deferred<any>()
  const state = editor({ previewResourceGroupAccess: () => preview.promise })
  const request = state.requestModeChange('restricted')
  state.props.resourceId = 'kb-2'
  state.reset()
  preview.resolve({ currently_allowed: 20, allowed_after: 5, losing_access: 15 })
  await request
  await state.confirmRestricted()
  assert.equal(state.impactVisible.value, false)
  assert.equal(state.requests.length, 0)
})

test('a stale save response never overwrites the newly opened resource', async () => {
  const saved = deferred<any>()
  const state = editor({ updateResourceGroupAccess: () => saved.promise })
  const request = state.persist('restricted')
  state.props.resourceId = 'kb-2'
  state.reset()
  await new Promise((resolve) => setImmediate(resolve))
  saved.resolve({ ...access('kb-1'), mode: 'restricted' })
  await request
  assert.equal(state.mode.value, 'inherit')
  assert.equal(state.saving.value, false)
})

test('latest refresh wins when directory access loads finish out of order', async () => {
  const older = deferred<any>(), newer = deferred<any>()
  let request = 0
  const state = editor({ getResourceGroupAccess: () => (++request === 1 ? older.promise : newer.promise) })
  const first = state.loadAccess(), second = state.loadAccess()
  newer.resolve({ ...access(), mode: 'restricted' })
  await second
  older.resolve(access())
  await first
  assert.equal(state.mode.value, 'restricted')
})

test('effective-access pagination keeps the reviewed grant snapshot and never changes access mode', async () => {
  const previews: any[] = []
  const state = editor({ previewResourceGroupAccess: async (...args: any[]) => {
    previews.push(args)
    return { effective_users: [], effective_users_total: 60, effective_users_limit: args[3], effective_users_offset: args[4] }
  } })
  state.grants.value = [{ directory_id: 'corp', directory_group_id: 'finance', display_name: 'Finance', permission: 'read' }]
  await state.previewCurrentAccess()
  state.grants.value[0].permission = 'edit'
  await state.changeImpactPage({ current: 2, pageSize: 20 })
  assert.equal(previews[1][4], 20)
  assert.equal(previews[1][2].grants[0].permission, 'read')
  assert.equal(state.impact.value.effective_users_offset, 20)
  await state.confirmRestricted()
  assert.equal(state.requests.length, 0)
  assert.equal(state.mode.value, 'inherit')
})

test('a failed effective-access page keeps both the displayed rows and page controls on the last successful page', async () => {
  let calls = 0
  const state = editor({ previewResourceGroupAccess: async () => {
    if (++calls > 1) throw new Error('directory unavailable')
    return { effective_users: [{ user_id: 'first-page' }], effective_users_total: 60, effective_users_limit: 20, effective_users_offset: 0 }
  } })
  await state.previewCurrentAccess()
  await state.changeImpactPage({ current: 2, pageSize: 50 })
  assert.equal(state.impactPage.value, 1)
  assert.equal(state.impactPageSize.value, 20)
  assert.equal(state.impact.value.effective_users[0].user_id, 'first-page')
})

test('catalog pagination can grant a directory group beyond the quick selector limit using its internal ID', async () => {
  const calls: any[] = []
  const group = { directory_id: 'corp', directory_group_id: 'internal-group-101', object_guid: 'ad-guid-101', display_name: 'Late group', dn: 'CN=Late group,DC=corp' }
  const state = editor({ getTenantDirectoryCatalog: async (...args: any[]) => {
    calls.push(args)
    return { items: [group], total: 120, enabled: true, fresh: true }
  } })
  state.catalogAppliedQuery.value = 'Late group'
  await state.loadGroupCatalog(6, 20)
  assert.deepEqual(calls[0], [1, 'groups', 'Late group', 20, 100])
  await state.addCatalogGroup(group)
  assert.equal(state.requests[0].mode, 'inherit')
  assert.equal(state.requests[0].grants[0].directory_group_id, 'internal-group-101')
  assert.equal(state.requests[0].grants[0].directory_id, 'corp')
})

test('resource changes discard a late group-catalog response and close its dialog', async () => {
  const response = deferred<any>()
  const state = editor({ getTenantDirectoryCatalog: () => response.promise })
  state.catalogVisible.value = true
  const request = state.loadGroupCatalog(1, 20)
  state.props.resourceId = 'kb-2'
  state.reset()
  response.resolve({ items: [{ directory_group_id: 'old-group' }], total: 1, enabled: true, fresh: true })
  await request
  assert.equal(state.catalogVisible.value, false)
  assert.equal(state.catalog.value, null)
})

test('a stale or disabled directory catalog never produces a resource grant', async () => {
  const state = editor()
  const group = { directory_id: 'corp', directory_group_id: 'group', display_name: 'Group', dn: 'CN=Group,DC=corp' }
  state.catalog.value = { items: [group], total: 1, enabled: true, fresh: false }
  await state.addCatalogGroup(group)
  state.catalog.value = { items: [group], total: 1, enabled: false, fresh: true }
  await state.addCatalogGroup(group)
  assert.equal(state.requests.length, 0)
})
