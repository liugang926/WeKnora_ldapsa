import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'
import * as vue from 'vue'
import { safeNextcloudAskPath } from '../../utils/nextcloudAskLink.ts'

const source = readFileSync(new URL('./SourceKnowledge.vue', import.meta.url), 'utf8')
const script = source.match(/<script setup lang="ts">([\s\S]*?)<\/script>/)![1]
const compiled = ts.transpileModule(script, {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
}).outputText
const link = '/platform/nextcloud-ask?instance_id=instance&binding_id=binding&file_id=77&source_etag=etag-1'
const target = { knowledge_id: 'file-1', knowledge_base_id: 'kb-1', title: 'Published file', source_etag: 'etag-1' }

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>(complete => { resolve = complete })
  return { promise, resolve }
}

function page(options: { ask?: string; api?: Record<string, (...args: any[]) => any> } = {}) {
  const route = { query: { ask: options.ask } }
  const streams: any[] = []
  let sessionsCreated = 0
  let stops = 0
  const auth = {
    refreshFromAuthMe: async () => true,
    memberships: [{ tenant_id: 7, tenant_name: 'Source', scoped_only: true }],
    effectiveTenantId: 7, selectedTenantId: 7, selectedTenantName: 'Source', currentTenantScopedOnly: true,
    setSelectedTenant: () => {}, logout: () => {},
  }
  const api = {
    listKnowledgeBases: async () => ({ success: true, data: [{ id: 'kb-1', name: 'Published source' }] }),
    createSessions: async () => { sessionsCreated++; return { success: true, data: { id: 'session-1' } } },
    getMessageList: async () => ({ success: true, data: [] }),
    get: async () => ({ success: true, data: { ...target } }),
    logout: async () => {}, updateMyPreferences: async () => {},
    ...options.api,
  }
  const modules: Record<string, unknown> = {
    vue: { ...vue, onMounted: () => {}, onUnmounted: () => {}, watch: () => {} },
    'vue-router': { useRoute: () => route, useRouter: () => ({ replace: async () => {} }) },
    '@/stores/auth': { useAuthStore: () => auth },
    '@/api/knowledge-base': api, '@/api/chat': api, '@/api/auth': api, '@/utils/request': api,
    '@/utils/nextcloudAskLink': { safeNextcloudAskPath },
    '@/api/chat/streame': { useStream: () => ({
      startStream: async (params: any) => { streams.push(params) },
      stopStream: () => { stops++ }, onChunk: () => {}, isStreaming: vue.ref(false), error: vue.ref(null),
    }) },
  }
  const controller = new Function('require', 'exports', `${compiled}
    return { load, ask, history, clearConversation, knowledgeBases, selectedKB, question, fileTarget,
      messages, answer, notice, sessionID, busy }`
  )((name: string) => modules[name], {})
  return { ...controller, streams, route, auth, sessionsCreated: () => sessionsCreated, stops: () => stops }
}

test('a Files deep link asks only the freshly authorized file with external tools disabled', async () => {
  const state = page({ ask: link })
  await state.load()
  state.question.value = 'What is in this file?'
  await state.ask()
  assert.equal(state.streams.length, 1)
  assert.deepEqual(state.streams[0].knowledge_base_ids, ['kb-1'])
  assert.deepEqual(state.streams[0].knowledge_ids, ['file-1'])
  assert.equal(state.streams[0].agent_enabled, false)
  assert.equal(state.streams[0].web_search_enabled, false)
  assert.equal(state.streams[0].local_browser_enabled, false)
})

test('a changed published file cannot create a chat or send its question', async () => {
  let lookups = 0
  const state = page({ ask: link, api: { get: async () => ({ success: true,
    data: { ...target, source_etag: ++lookups === 1 ? 'etag-1' : 'etag-2' },
  }) } })
  await state.load()
  state.question.value = 'Question about the old version'
  await state.ask()
  assert.equal(state.sessionsCreated(), 0)
  assert.equal(state.streams.length, 0)
  assert.ok(state.notice.value)
})

test('a Files target outside the current authorized KB list remains unavailable', async () => {
  const state = page({ ask: link, api: { listKnowledgeBases: async () => ({ success: true, data: [] }) } })
  await state.load()
  assert.equal(state.fileTarget.value, null)
  assert.equal(state.selectedKB.value, '')
  assert.ok(state.notice.value)
})

test('failed history authorization immediately hides displayed answer bodies and stops the stream', async () => {
  const state = page({ api: { getMessageList: async () => { throw new Error('revoked') } } })
  state.sessionID.value = 'session-1'
  state.messages.value = [{ id: 'old', role: 'assistant', content: 'Previously visible' }]
  state.answer.value = 'Partial answer'
  await state.history()
  assert.deepEqual(state.messages.value, [])
  assert.equal(state.answer.value, '')
  assert.equal(state.sessionID.value, '')
  assert.ok(state.stops() > 0)
  assert.ok(state.notice.value)
})

test('clearing a conversation discards a pending session creation before it starts streaming', async () => {
  const created = deferred<any>()
  const state = page({ api: { createSessions: () => created.promise } })
  await state.load()
  state.question.value = 'Question'
  const pending = state.ask()
  await new Promise(resolve => setImmediate(resolve))
  state.clearConversation()
  created.resolve({ success: true, data: { id: 'late-session' } })
  await pending
  assert.equal(state.streams.length, 0)
  assert.equal(state.sessionID.value, '')
})

test('a late knowledge scope response cannot replace a newer authorized list', async () => {
  const older = deferred<any>()
  let calls = 0
  const state = page({ api: { listKnowledgeBases: () => ++calls === 1 ? older.promise
    : Promise.resolve({ success: true, data: [{ id: 'kb-new', name: 'Current scope' }] }) } })
  const first = state.load()
  await new Promise(resolve => setImmediate(resolve))
  await state.load()
  older.resolve({ success: true, data: [{ id: 'kb-old', name: 'Old scope' }] })
  await first
  assert.deepEqual(state.knowledgeBases.value, [{ id: 'kb-new', name: 'Current scope' }])
  assert.equal(state.selectedKB.value, 'kb-new')
})
