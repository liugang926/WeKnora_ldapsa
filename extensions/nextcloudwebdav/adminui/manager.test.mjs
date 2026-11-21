import assert from 'node:assert/strict'
import {readFile} from 'node:fs/promises'
import test from 'node:test'
import vm from 'node:vm'

const script = await readFile(new URL('./manager.js', import.meta.url), 'utf8')

// Run the same handlers embedded in the management page. Record requests at the
// fetch boundary so an accidental full PUT or manual sync cannot pass unnoticed.
async function editor(status, savedFolders) {
  const fields = new Map()
  const requests = []
  function field(id) {
    if (!fields.has(id)) {
      fields.set(id, {
        value: '', hidden: true, classList: {toggle() {}}, events: {},
        addEventListener(event, fn) { this.events[event] = fn },
      })
    }
    return fields.get(id)
  }
  let loggedIn = false
  const env = vm.createContext({
    document: {getElementById: field}, window: {addEventListener() {}},
    localStorage: {getItem: key => loggedIn && key === 'weknora_token' ? 'fixture-session' : ''},
    fetch: async (path, request) => {
      requests.push([path, request.method, JSON.parse(request.body)])
      return {ok: true, status: 200, json: async () => ({})}
    },
  })
  vm.runInContext(script, env)
  await Promise.resolve()
  loggedIn = true
  field('kb').value = 'kb'
  field('login').hidden = true
  env.saved = {id: 's1', knowledge_base_id: 'kb', type: 'nextcloud_webdav', status,
    sync_schedule: '', config: {resource_ids: savedFolders}}
  vm.runInContext('source = saved; selection = [...saved.config.resource_ids]; loadSources = async () => {}', env)
  return {env, field, requests}
}

test('editing an active schedule uses the partial-update route and preserves the stored identity and status', async () => {
  const {field, requests} = await editor('active', ['/技 术'])
  field('schedule').value = '0 0 1 * * *'
  await field('save').events.click()
  assert.deepEqual(requests, [['/api/v1/datasource/s1/sync-settings', 'PATCH', {sync_schedule: '0 0 1 * * *'}]])
})

test('edited folders must be saved before resuming or syncing; saving never starts a run', async () => {
  const {env, field, requests} = await editor('paused', ['/旧'])
  vm.runInContext("selection = ['/新 & 资料']", env)
  await field('toggle').events.click()
  await field('sync').events.click()
  assert.equal(requests.length, 0)
  await field('save').events.click()
  assert.deepEqual(requests, [['/api/v1/datasource/s1/sync-settings', 'PATCH', {sync_schedule: '', resource_ids: ['/新 & 资料']}]])
})
