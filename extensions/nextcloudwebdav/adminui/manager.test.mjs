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
    URL, document: {getElementById: field}, window: {addEventListener() {}},
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


async function connectionForm() {
  const form = await editor('paused', ['/'])
  form.field('name').value = 'Nextcloud'
  form.field('url').value = 'https://cloud.example/nc'
  form.field('user').value = 'service'
  form.field('password').value = 'fixture-app-secret'
  return form
}

test('connection test, creation and credential replacement report their actual WebDAV and WeKnora errors', async () => {
  const id = 'a38d02c9-2222-4166-9bab-e7ac62cef217'
  const unauthorized = 'invalid credentials: Nextcloud PROPFIND returned HTTP 401'
  const missing = 'resource not found in source system: Nextcloud PROPFIND returned HTTP 404'
  for (const [action, status, body, expected] of [
    ['test', 400, {error: unauthorized}, /测试连接失败.*Nextcloud 拒绝认证（401）/],
    ['test', 400, {error: missing}, /测试连接失败.*WebDAV 路径（404）/],
    ['create', 400, {error: missing}, /保存暂停的数据源失败.*WebDAV 路径（404）/],
    ['replace', 400, {error: {code: 400, message: 'failed to update credentials: ' + unauthorized}}, /更换连接凭证失败.*Nextcloud 拒绝认证（401）/],
    ['test', 400, {error: 'invalid configuration: Nextcloud server address is not permitted'}, /出站地址检查/],
    ['test', 401, {error: unauthorized}, /登录已过期/],
  ]) {
    const {env, field} = await connectionForm()
    env.fetch = async () => ({ok: false, status, json: async () => body, headers: {get: () => id}})
    await field(action).events.click()
    assert.match(field('notice').textContent, expected)
    assert.equal(field('login').hidden, status !== 401)
    if (status === 400) assert.ok(field('notice').textContent.includes(id))
  }
})

test('unrecognized error text, malformed bodies and unsafe request identifiers stay private', async () => {
  const secret = 'fixture-app-secret'
  const malformed = async () => { throw new SyntaxError('<html>' + secret) }
  for (const [ok, json] of [
    [false, async () => ({error: secret, details: secret})],
    [false, async () => ({error: {message: 'invalid credentials: Nextcloud PROPFIND returned HTTP 401 ' + secret}})],
    [false, malformed], [true, malformed],
  ]) {
    const {env, field} = await connectionForm()
    env.fetch = async () => ({ok, status: ok ? 200 : 400, json, headers: {get: () => secret}})
    await field('test').events.click()
    assert.match(field('notice').textContent, /服务端日志/)
    assert.ok(!field('notice').textContent.includes(secret))
    assert.ok(!field('notice').textContent.includes('拒绝认证'))
  }
})

test('only a connected response confirms credentials, and full DAV URLs are rejected before sending', async () => {
  const {env, field} = await connectionForm()
  const urls = []
  let result = {valid: false}
  env.fetch = async (path, request) => {
    assert.equal(path, '/api/v1/datasource/validate-credentials')
    urls.push(JSON.parse(request.body).credentials.base_url)
    return {ok: true, status: 200, json: async () => result}
  }
  await field('test').events.click()
  assert.match(field('notice').textContent, /未获得服务端确认/)
  result = {status: 'connected'}
  await field('test').events.click()
  assert.equal(field('notice').textContent, '已验证连接。')
  for (const suffix of ['/remote.php/dav/files/service/', '/remote.php/webdav']) {
    field('url').value = 'https://cloud.example/nc' + suffix
    await field('test').events.click()
    assert.match(field('notice').textContent, /实例地址.*去掉/)
  }
  assert.deepEqual(urls, ['https://cloud.example/nc', 'https://cloud.example/nc'])
})

test('creation requires a returned source ID before clearing the form and loading the source', async () => {
  const {env, field} = await connectionForm()
  const loaded = []
  env.captureLoad = id => loaded.push(id)
  vm.runInContext('loadSources = captureLoad', env)
  let body
  env.fetch = async () => ({ok: true, status: 201, json: async () => body})
  for (const response of [
    {}, {id: ' '}, {success: false, error: {message: 'fixture-app-secret'}, data: {id: 's2'}},
  ]) {
    body = response
    await field('create').events.click()
    const notice = field('notice').textContent
    assert.match(notice, response.success === false ? /保存暂停的数据源失败/ : /避免重复创建/)
    assert.ok(!notice.includes('fixture-app-secret'))
    assert.equal(field('password').value, 'fixture-app-secret')
    assert.deepEqual(loaded, [])
  }
  body = {id: 's2', type: 'nextcloud_webdav', knowledge_base_id: 'kb', status: 'paused'}
  await field('create').events.click()
  assert.match(field('notice').textContent, /已保存暂停的数据源/)
  assert.equal(field('password').value, '')
  assert.deepEqual(loaded, ['s2'])
})

test('credential replacement requires an affirmative configured flag before dismissing the form', async () => {
  const {env, field} = await connectionForm()
  field('connection').hidden = false
  let body
  env.fetch = async () => ({ok: true, status: 200, json: async () => body})
  const configured = {fields: {credentials: {configured: true}}}
  for (const response of [
    {}, {success: true, data: {fields: {credentials: {configured: false}}}},
    {success: false, error: 'fixture-app-secret', data: configured},
  ]) {
    body = response
    await field('replace').events.click()
    const notice = field('notice').textContent
    assert.match(notice, response.success === false ? /更换连接凭证失败/ : /未确认新凭证/)
    assert.ok(!notice.includes('fixture-app-secret'))
    assert.equal(field('password').value, 'fixture-app-secret')
    assert.equal(field('connection').hidden, false)
  }
  body = {success: true, data: configured}
  await field('replace').events.click()
  assert.match(field('notice').textContent, /新凭证已保存/)
  assert.equal(field('password').value, '')
  assert.equal(field('connection').hidden, true)
})
