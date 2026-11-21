const TYPE = 'nextcloud_webdav'
const dsAPI = '/api/v1/datasource'
const $ = id => document.getElementById(id)
const tenant = () => localStorage.getItem('weknora_selected_tenant_id') || ''
const session = () => localStorage.getItem('weknora_token') || ''
let sources = []
let source = null
let selection = []
let folder = null
let busy = false
let initialized = false

function message(text, error = false) {
  $('notice').textContent = text
  $('notice').classList.toggle('error', error)
}

async function api(path, method = 'GET', body) {
  const token = session()
  if (!token) throw new Error('请先登录 WeKnora。')
  const headers = { Authorization: 'Bearer ' + token, 'Accept-Language': 'zh-CN' }
  if (tenant()) headers['X-Tenant-ID'] = tenant()
  if (body !== undefined) headers['Content-Type'] = 'application/json'
  const response = await fetch(path, {
    method, headers, body: JSON.stringify(body), mode: 'same-origin', redirect: 'error', cache: 'no-store',
  })
  if (!response.ok) {
    if (response.status === 401) {
      $('login').hidden = false
      throw new Error('登录已过期，请重新登录并刷新。')
    }
    if (response.status === 409) throw new Error('设置已发生变化，请刷新后重新核对。')
    if (response.status === 403) throw new Error('当前账户没有权限执行此操作。')
    throw new Error('请求失败（HTTP ' + response.status + '），请检查连接凭证、知识库和目录设置。')
  }
  if (response.status === 204) return null
  const result = await response.json()
  return result?.data ?? result
}

async function run(task) {
  if (busy) return
  busy = true
  $('controls').disabled = true
  message('')
  try { await task() }
  catch (error) { message(error.message || '请求失败，请重试。', true) }
  finally {
    busy = false
    $('controls').disabled = !initialized || !session() || !$('kb').value || !$('login').hidden
  }
}

function on(id, event, task) {
  $(id).addEventListener(event, () => run(task))
}

function sourcePath(suffix = '') {
  if (!source || source.knowledge_base_id !== $('kb').value || source.type !== TYPE) throw new Error('请先选择数据源。')
  return dsAPI + '/' + encodeURIComponent(source.id) + suffix
}

function credentials() {
  const base = $('url').value.trim()
  const username = $('user').value.trim()
  const password = $('password').value
  let url
  try { url = new URL(base) } catch { /* Show a field-specific error below. */ }
  if (!url || url.protocol !== 'https:' || url.username || url.password || url.search || url.hash || base.includes('\\')) {
    throw new Error('请填写 HTTPS 实例地址，地址中不能包含凭证、查询参数或片段。')
  }
  if (!username || !password) throw new Error('请填写专用服务账号和该账号的应用密码。')
  return { base_url: base, username, app_password: password }
}

function clearConnection() {
  for (const id of ['url', 'user', 'password']) $(id).value = ''
}

function foldersChanged() {
  const saved = source.config?.resource_ids || []
  return saved.length !== selection.length || selection.some(path => !saved.includes(path))
}

function settingsChanged() {
  return foldersChanged() || $('schedule').value !== (source.sync_schedule || '')
}

function updateActions() {
  const pending = settingsChanged()
  const paused = source.status === 'paused'
  const canRunSaved = !pending && !!source.config?.resource_ids?.length
  $('pending').hidden = !pending
  $('save').disabled = !pending || !selection.length
  $('toggle').disabled = paused && !canRunSaved
  $('sync').disabled = paused || !canRunSaved
}

function renderSelection() {
  const list = $('selection')
  list.replaceChildren()
  for (const path of selection) {
    list.append(item(path, action('移除', () => {
      selection = selection.filter(value => value !== path)
      renderSelection()
    })))
  }
  if (!selection.length) list.append(item('尚未选择目录'))
  updateActions()
}

function item(title, ...actions) {
  const row = document.createElement('li')
  const label = document.createElement('span')
  label.textContent = title
  row.append(label, ...actions)
  return row
}

function action(label, fn) {
  const button = document.createElement('button')
  button.type = 'button'
  button.textContent = label
  button.addEventListener('click', () => run(fn))
  return button
}

function includesPath(parent, child) {
  return parent === '/' || parent === child || child.startsWith(parent + '/')
}

function selectPath(path) {
  if (!selection.some(selected => includesPath(selected, path))) {
    selection = selection.filter(selected => !includesPath(path, selected))
    selection.push(path)
  }
  renderSelection()
}

async function browse(path = null) {
  const query = path === null ? '' : '?' + new URLSearchParams({ parent_id: path })
  const rows = await api(sourcePath('/resources') + query)
  folder = path
  $('browser').hidden = false
  $('path').textContent = path ?? '账号文件根目录'
  $('up').disabled = path === null
  $('folders').replaceChildren()
  for (const row of rows || []) {
    const id = row.external_id
    const actions = [action('选择', () => selectPath(id))]
    if (row.has_children) actions.unshift(action('打开', () => browse(id)))
    $('folders').append(item(row.name || id, ...actions))
  }
  if (!rows?.length) $('folders').append(item('此处没有可选子目录'))
}

function showSource() {
  source = sources.find(row => row.id === $('source').value) || null
  clearConnection()
  selection = [...(source?.config?.resource_ids || [])]
  folder = null
  $('settings').hidden = !source
  $('history').hidden = !source
  $('connection').hidden = !!source
  $('browser').hidden = true
  $('newName').hidden = !!source
  $('create').hidden = !!source
  $('replace').hidden = !source
  $('cancel').hidden = !source
  $('entries').replaceChildren()
  $('name').value = 'Nextcloud'
  if (!source) return
  const paused = source.status === 'paused'
  $('state').textContent = ({ active: '已启用', paused: '已暂停', error: '异常' })[source.status] || source.status
  $('toggle').textContent = paused ? '启用数据源' : '暂停数据源'
  $('rotate').disabled = !paused
  $('rotate').title = paused ? '' : '暂停数据源后可更换凭证'
  const schedule = $('schedule')
  schedule.querySelector('[data-current]')?.remove()
  const value = source.sync_schedule || ''
  if (![...schedule.options].some(option => option.value === value)) {
    const custom = new Option(value, value)
    custom.dataset.current = 'true'
    schedule.add(custom)
  }
  schedule.value = value
  renderSelection()
}

async function loadSources(id = '') {
  sources = []
  $('source').replaceChildren(new Option('新增数据源', ''))
  showSource()
  const list = await api(dsAPI + '?' + new URLSearchParams({ kb_id: $('kb').value }))
  sources = (list || []).filter(row => row.type === TYPE && row.knowledge_base_id === $('kb').value)
  for (const row of sources) $('source').add(new Option(row.name, row.id))
  $('source').value = id
  showSource()
}

// An already typed secret belongs to the previous endpoint and account.
for (const id of ['url', 'user']) {
  $(id).addEventListener('input', () => { $('password').value = '' })
}

on('kb', 'change', () => loadSources())
on('source', 'change', showSource)
on('test', 'click', async () => {
  await api(dsAPI + '/validate-credentials', 'POST', { type: TYPE, credentials: credentials() })
  message('已验证连接。')
})
on('create', 'click', async () => {
  const name = $('name').value.trim()
  if (!name) throw new Error('请填写数据源名称。')
  const created = await api(dsAPI, 'POST', {
    type: TYPE, name, knowledge_base_id: $('kb').value,
    config: { type: TYPE, credentials: credentials(), resource_ids: [], settings: {} },
    status: 'paused', sync_mode: 'incremental', sync_schedule: '', conflict_strategy: 'overwrite', sync_deletions: false,
  })
  clearConnection()
  await loadSources(created.id)
  message('已保存暂停的数据源。请选择需要同步的目录。')
})
on('browse', 'click', () => browse())
on('up', 'click', () => browse(folder === '/' ? null : folder.slice(0, folder.lastIndexOf('/')) || '/'))
on('schedule', 'change', updateActions)
on('save', 'click', async () => {
  if (!selection.length || selection.length > 50) throw new Error('请选择 1 到 50 个目录。')
  if (!settingsChanged()) return
  const id = source.id
  // Saving the schedule also binds the editor's authorization to a changed folder selection.
  const patch = { sync_schedule: $('schedule').value }
  if (foldersChanged()) patch.resource_ids = selection
  await api(sourcePath('/sync-settings'), 'PATCH', patch)
  await loadSources(id)
  message('目录和定时计划已保存。')
})
on('toggle', 'click', async () => {
  const id = source.id
  const resume = source.status === 'paused'
  if (resume && (settingsChanged() || !source.config?.resource_ids?.length)) throw new Error('请先保存目录和计划。')
  await api(sourcePath(resume ? '/resume' : '/pause'), 'POST', {})
  await loadSources(id)
  message(resume ? '数据源已启用。' : '数据源已暂停。')
})
on('sync', 'click', async () => {
  if (source.status === 'paused' || settingsChanged() || !source.config?.resource_ids?.length) throw new Error('请先保存设置并启用数据源。')
  await api(sourcePath('/sync'), 'POST', {})
  message('同步请求已提交。')
})
on('rotate', 'click', () => {
  clearConnection()
  $('connection').hidden = false
  $('url').focus()
})
on('cancel', 'click', () => { clearConnection(); $('connection').hidden = true })
on('replace', 'click', async () => {
  if (source.status !== 'paused') throw new Error('请先暂停数据源，再更换凭证。')
  await api(sourcePath('/credentials'), 'PUT', { credentials: credentials() })
  clearConnection()
  $('connection').hidden = true
  $('browser').hidden = true
  message('新凭证已保存。请核对目录后再启用。')
})
on('logs', 'click', async () => {
  const records = await api(sourcePath('/logs') + '?limit=20')
  const labels = { running: '运行中', success: '成功', partial: '部分成功', failed: '失败', canceled: '已取消' }
  $('entries').replaceChildren()
  for (const row of records || []) {
    const time = new Date(row.started_at).toLocaleString()
    $('entries').append(item(time + ' · ' + (labels[row.status] || row.status)
      + ' · 新增 ' + row.items_created + ' · 更新 ' + row.items_updated + ' · 跳过 ' + row.items_skipped + ' · 失败 ' + row.items_failed))
  }
  if (!records?.length) $('entries').append(item('暂无同步记录'))
})

window.addEventListener('storage', event => {
  if (!event.key || ['weknora_selected_tenant_id', 'weknora_user'].includes(event.key) || (event.key === 'weknora_token' && !event.newValue)) location.reload()
})
run(async () => {
  if (!session()) { $('login').hidden = false; return }
  const query = new URLSearchParams(location.search)
  const kbId = query.get('kb_id')
  const sourceId = query.get('source_id')
  if (sourceId && !kbId) throw new Error('数据源链接缺少目标知识库。')
  if (kbId) {
    const kb = await api('/api/v1/knowledge-bases/' + encodeURIComponent(kbId))
    if (kb?.id !== kbId) throw new Error('找不到指定的知识库。')
    $('kb').add(new Option(kb.name, kb.id))
    $('kb').disabled = true
    await loadSources(sourceId || '')
    if (sourceId && !source) throw new Error('当前知识库中找不到指定的 Nextcloud WebDAV 数据源。')
  } else {
    const rows = await api('/api/v1/knowledge-bases')
    for (const kb of rows || []) $('kb').add(new Option(kb.name, kb.id))
    if (!$('kb').value) { message('当前工作区没有可用知识库。'); return }
    await loadSources()
  }
  initialized = true
})
