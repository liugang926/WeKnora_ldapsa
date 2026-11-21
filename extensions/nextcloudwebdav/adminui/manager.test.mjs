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

async function openLink(search, sourceRows, kbStatus = 200) {
  const fields = new Map()
  const calls = []
  function element() {
    return {value: '', options: [], hidden: true, classList: {toggle() {}},
      addEventListener() {}, append() {}, querySelector() { return null },
      add(option) { if (!this.options.length) this.value = option.value; this.options.push(option) },
      replaceChildren(...options) { this.options = []; for (const option of options) this.add(option) }}
  }
  function field(id) {
    if (!fields.has(id)) fields.set(id, element())
    return fields.get(id)
  }
  const env = vm.createContext({
    document: {getElementById: field, createElement: element}, window: {addEventListener() {}},
    location: {search}, URLSearchParams, localStorage: {getItem: () => 'fixture'},
    Option: class { constructor(text, value) { Object.assign(this, {text, value, dataset: {}}) } },
    fetch: async (path, request) => {
      calls.push([request.method, path])
      if (path === '/api/v1/knowledge-bases/kbA') {
        return {ok: kbStatus === 200, status: kbStatus, json: async () => ({data: {id: 'kbA', name: 'A'}})}
      }
      assert.equal(path, '/api/v1/datasource?kb_id=kbA')
      return {ok: true, status: 200, json: async () => sourceRows}
    },
  })
  await vm.runInContext(script, env)
  return {calls, field}
}

test('KB picker links open the requested KB and exact WebDAV source using only reads', async () => {
  const rows = [{id: 's1', type: 'nextcloud_webdav', knowledge_base_id: 'kbA', status: 'paused'}]
  const create = await openLink('?kb_id=kbA', rows)
  assert.equal(create.field('kb').value, 'kbA')
  assert.equal(create.field('kb').disabled, true)
  assert.equal(create.field('create').hidden, false)
  const edit = await openLink('?kb_id=kbA&source_id=s1', rows)
  assert.equal(edit.field('source').value, 's1')
  assert.equal(edit.field('create').hidden, true)
  assert.equal(edit.field('settings').hidden, false)
  assert.equal(edit.field('controls').disabled, false)
  assert.deepEqual(edit.calls, [['GET', '/api/v1/knowledge-bases/kbA'], ['GET', '/api/v1/datasource?kb_id=kbA']])
})

test('an inaccessible KB or a missing, foreign or legacy source cannot fall back to creating a source', async () => {
  for (const rows of [[], [{id: 's1', type: 'nextcloud_webdav', knowledge_base_id: 'kbB'}],
    [{id: 's1', type: 'nextcloud', knowledge_base_id: 'kbA'}]]) {
    const {field} = await openLink('?kb_id=kbA&source_id=s1', rows)
    assert.equal(field('controls').disabled, true)
    assert.match(field('notice').textContent, /找不到/)
  }
  const denied = await openLink('?kb_id=kbA', [], 403)
  assert.equal(denied.field('controls').disabled, true)
  assert.equal(denied.calls.length, 1)
})
