import assert from 'node:assert/strict'
import { setImmediate as settle } from 'node:timers/promises'
import { after, mock, test, type TestContext } from 'node:test'
import { effectScope, ref } from 'vue'
import type { MessageSearchGroupItem, MessageSearchRequest, MessageSearchResponse } from '../../api/chat-history.ts'

interface Deferred<T> {
  promise: Promise<T>
  resolve: (value: T) => void
  reject: (reason: Error) => void
}
function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void
  let reject!: (reason: Error) => void
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}
interface SearchCall { request: MessageSearchRequest; result: Deferred<MessageSearchResponse> }
let calls: SearchCall[] = []

// Keep the real chat-history API wrapper and stable-key helper. Only the
// transport, unrelated knowledge request and Pinia neighbours are replaced.
// File URLs match the targets resolved by tsx's @/ path aliases.
const moduleMocks = [
  mock.module(new URL('../../utils/request.ts', import.meta.url).href, {
    namedExports: {
      get: () => { throw new Error('Unexpected GET in message search state test') },
      put: () => { throw new Error('Unexpected PUT in message search state test') },
      post: (url: string, request: MessageSearchRequest) => {
        assert.equal(url, '/api/v1/messages/search')
        const result = deferred<MessageSearchResponse>()
        calls.push({ request: structuredClone(request), result })
        return result.promise
      },
    },
  }),
  mock.module(new URL('../../api/knowledge-base/index.ts', import.meta.url).href, {
    namedExports: { knowledgeSemanticSearch: async () => ({ success: true, data: [] }) },
  }),
  mock.module(new URL('../../stores/organization.ts', import.meta.url).href, {
    namedExports: { useOrganizationStore: () => ({ sharedKnowledgeBases: [], sharedAgents: [] }) },
  }),
  mock.module(new URL('../../stores/chatResources.ts', import.meta.url).href, {
    namedExports: {
      useChatResourcesStore: () => ({
        rawKnowledgeBases: [{ id: 'owned-kb', name: 'Owned knowledge base' }],
        agents: [],
        ensureKnowledgeBases: async () => {},
        ensureAgents: async () => {},
      }),
    },
  }),
  mock.module(new URL('../../stores/menu.ts', import.meta.url).href, {
    namedExports: { useMenuStore: () => ({ menuArr: [] }) },
  }),
]
after(() => { for (const handle of moduleMocks) handle.restore() })

// Import the actual production composable only after its neighbours are mocked.
const { useCmdkSearch } = await import('./useSearch.ts')

function item(id: string, body = id): MessageSearchGroupItem {
  return {
    request_id: id, session_id: 'owned-session', session_title: 'Owned conversation',
    query_content: `Question ${id}`, answer_content: body,
    score: 1, match_type: 'keyword', created_at: '2026-10-14T00:00:00Z',
  }
}
function page(items: MessageSearchGroupItem[], hasMore = false, nextCursor = ''): MessageSearchResponse {
  return { success: true, data: { items, total: items.length, has_more: hasMore, next_cursor: nextCursor } }
}
function fixture(t: TestContext) {
  calls = []
  t.mock.timers.enable({ apis: ['setTimeout'] })
  // The effect-scope harness deliberately has no mounted component. Vue's
  // lifecycle warnings are expected; all refs/computed/watch are actual Vue.
  t.mock.method(console, 'warn', () => {})
  t.mock.method(console, 'error', () => {})
  const enabled = ref(true)
  const identity = ref('user-a:tenant-7')
  const effects = effectScope()
  const state = effects.run(() => useCmdkSearch({
    debounceMs: 0, enabled: () => enabled.value, contextKey: () => identity.value,
    agentsEnabled: () => false,
  }))!
  t.after(() => { state.clearResults(); effects.stop() })
  return { state, enabled, identity, t }
}
type Fixture = ReturnType<typeof fixture>
const bodies = (f: Fixture) => f.state.messageGroups.value.flatMap(group => group.items.map(message => message.answer_content))
async function start(f: Fixture, query = 'owned query') {
  f.state.query.value = query
  f.t.mock.timers.tick(1)
  await settle()
  assert.equal(calls.length, 1)
  assert.equal(calls[0].request.query, query)
  assert.equal(calls[0].request.mode, 'hybrid')
  assert.equal(calls[0].request.cursor, undefined)
}
async function first(f: Fixture, response: MessageSearchResponse) {
  await start(f)
  calls[0].result.resolve(response)
  await settle()
  assert.equal(f.state.loading.value, false)
}

test('next and previous pages clear bodies immediately and re-fetch authorized snapshots', async t => {
  const f = fixture(t)
  await first(f, page([item('first', 'old first body')], true, 'cursor-1'))
  assert.deepEqual(bodies(f), ['old first body'])
  const forward = f.state.continueMessageSearch()
  assert.deepEqual(bodies(f), [])
  assert.equal(f.state.totalMessages.value, 0)
  assert.equal(f.state.messageLoadingMore.value, true)
  assert.equal(calls[1].request.cursor, 'cursor-1')
  calls[1].result.resolve(page([item('second', 'second body')]))
  await forward
  assert.deepEqual(bodies(f), ['second body'])
  assert.equal(f.state.messageHasPrevious.value, true)
  const backward = f.state.previousMessagePage()
  assert.deepEqual(bodies(f), [])
  assert.equal(calls.length, 3, 'previous page requires another server call')
  assert.equal(calls[2].request.cursor, undefined)
  calls[2].result.resolve(page([item('first', 'fresh first body')], true, 'fresh-cursor-1'))
  await backward
  assert.deepEqual(bodies(f), ['fresh first body'])
  assert.equal(f.state.messageHasPrevious.value, false)
  assert.equal(f.state.totalMessages.value, 1)
})

test('empty first page with has_more remains incomplete and can find an earlier conversation', async t => {
  const f = fixture(t)
  await first(f, page([], true, 'older-cursor'))
  assert.equal(f.state.totalMessages.value, 0)
  assert.equal(f.state.messageHasMore.value, true)
  assert.equal(f.state.searchIncomplete.value, true)
  const forward = f.state.continueMessageSearch()
  assert.equal(calls[1].request.cursor, 'older-cursor')
  calls[1].result.resolve(page([item('old-conversation', 'found beyond first candidate page')]))
  await forward
  assert.deepEqual(bodies(f), ['found beyond first candidate page'])
  assert.equal(f.state.searchIncomplete.value, false)
})

test('continuation failure removes prior content, exposes incomplete state and retries the same cursor', async t => {
  const f = fixture(t)
  await first(f, page([item('first', 'body that must not survive failure')], true, 'retry-cursor'))
  const forward = f.state.continueMessageSearch()
  assert.deepEqual(bodies(f), [])
  calls[1].result.reject(new Error('Owned unavailable authority fixture'))
  await forward
  assert.deepEqual(bodies(f), [])
  assert.equal(f.state.messageSearchError.value, true)
  assert.equal(f.state.searchIncomplete.value, true, 'failure cannot certify complete zero results')
  const retry = f.state.continueMessageSearch()
  assert.equal(calls[2].request.cursor, 'retry-cursor')
  calls[2].result.resolve(page([]))
  await retry
  assert.equal(f.state.messageSearchError.value, false)
  assert.equal(f.state.messageHasMore.value, false)
  assert.equal(f.state.messageHasPrevious.value, true, 'empty later page retains navigation, not a whole-history zero claim')
  assert.deepEqual(bodies(f), [])
})

test('standalone identities remain distinct and a page exposes more than four results', async t => {
  const f = fixture(t)
  const solo = (id: string, body: string) => ({ ...item('', body), message_id: id })
  await first(f, page([
    solo('solo-1', 'solo one'), solo('solo-2', 'solo two'),
    item('solo-1', 'request namespace'), item('pair-2'), item('pair-3'), item('pair-4'),
    solo('solo-2', 'fresh solo two'),
  ], true, 'identity-cursor'))
  assert.equal(f.state.totalMessages.value, 6)
  assert.deepEqual(bodies(f), ['solo one', 'fresh solo two', 'request namespace', 'pair-2', 'pair-3', 'pair-4'])
  const forward = f.state.continueMessageSearch()
  calls[1].result.resolve(page([{ ...item('', 'identity-less body') }]))
  await forward
  assert.deepEqual(bodies(f), [])
  assert.equal(f.state.messageSearchError.value, true, 'missing identity cannot silently merge standalone messages')
})

for (const change of ['query', 'close', 'tenant'] as const) {
  test(`${change} change clears state synchronously and rejects a late response`, async t => {
    const f = fixture(t)
    await first(f, page([item('first', 'old authorized body')], true, 'old-cursor'))
    const oldPage = f.state.continueMessageSearch()
    const oldCall = calls[1]
    if (change === 'query') f.state.query.value = 'new query'
    else if (change === 'close') f.enabled.value = false
    else f.identity.value = 'user-a:tenant-8'
    assert.deepEqual(bodies(f), [], 'clear before debounce or old response')
    assert.equal(f.state.messageHasMore.value, false)
    assert.equal(f.state.messageHasPrevious.value, false)
    assert.equal(f.state.messageLoadingMore.value, false)
    if (change !== 'close') {
      t.mock.timers.tick(1)
      await settle()
      assert.equal(calls.length, 3)
      assert.equal(calls[2].request.cursor, undefined, 'new context starts at a fresh page')
      assert.equal(calls[2].request.query, change === 'query' ? 'new query' : 'owned query')
      calls[2].result.resolve(page([item('fresh', 'current context body')], true, 'current-cursor'))
      await settle()
      assert.deepEqual(bodies(f), ['current context body'])
    }
    oldCall.result.resolve(page([item('late', 'stale response body')], true, 'stale-cursor'))
    await oldPage
    await settle()
    if (change === 'close') {
      assert.deepEqual(bodies(f), [])
      assert.equal(f.state.hasSearched.value, false)
      t.mock.timers.tick(1000)
      await f.state.continueMessageSearch()
      assert.equal(calls.length, 2, 'closed palette issues no continuation')
    } else {
      assert.deepEqual(bodies(f), ['current context body'])
      const next = f.state.continueMessageSearch()
      assert.equal(calls[3].request.cursor, 'current-cursor', 'late response cannot replace current navigation')
      calls[3].result.resolve(page([]))
      await next
    }
  })
}

test('a non-advancing continuation is an explicit error and causes no automatic request loop', async t => {
  const f = fixture(t)
  await first(f, page([item('first')], true, 'same-cursor'))
  const forward = f.state.continueMessageSearch()
  calls[1].result.resolve(page([item('untrusted-next')], true, 'same-cursor'))
  await forward
  assert.deepEqual(bodies(f), [])
  assert.equal(f.state.messageSearchError.value, true)
  assert.equal(f.state.searchIncomplete.value, true)
  t.mock.timers.tick(1000)
  await settle()
  assert.equal(calls.length, 2)
})
