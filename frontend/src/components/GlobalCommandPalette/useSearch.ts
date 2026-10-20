import { ref, watch, onMounted, onUnmounted, computed } from 'vue'
import { knowledgeSemanticSearch } from '@/api/knowledge-base'
import { searchMessages, messageSearchGroupKey, type MessageSearchGroupItem } from '@/api/chat-history'
import { useOrganizationStore } from '@/stores/organization'
import { useChatResourcesStore } from '@/stores/chatResources'
import { useMenuStore } from '@/stores/menu'

export interface CmdkKb {
  id: string
  name: string
  type?: string
}

export interface CmdkAgent {
  id: string
  name: string
  description?: string
  avatar?: string
  isBuiltin: boolean
  source: 'own' | 'shared'
  orgName?: string
}

export interface CmdkSessionItem {
  id: string
  title: string
}

export interface CmdkChunk {
  id: string
  chunk_index: number
  knowledge_id: string
  knowledge_base_id: string
  knowledge_title: string
  kb_name: string
  content: string
  matched_content?: string
  match_type: 'vector' | 'keyword' | string
  score: number
}

export interface CmdkFileGroup {
  knowledgeId: string
  kbId: string
  title: string
  kbName: string
  chunks: CmdkChunk[]
}

export interface CmdkMsgGroup {
  sessionId: string
  sessionTitle: string
  items: MessageSearchGroupItem[]
}

/**
 * useCmdkSearch — debounced, cancellable search that fans out to
 *   • /api/v1/knowledge-search (chunks / files)
 *   • /api/v1/messages/search  (chat history)
 * and produces three client-side groups: kbs / files / messages.
 *
 * Cancellation is implemented with a monotonically-increasing request id so
 * that stale responses are dropped (AbortController is not used to keep the
 * backend API untouched).
 *
 * `lockedKbIds` is a getter returning the current scope. When it returns a
 * non-empty array, chunk search is limited to those KBs and message search
 * is skipped. Changes to the getter's return value re-trigger the search.
 */
export function useCmdkSearch(options: {
  /** When set, scopes knowledge search to these KBs. Re-evaluated on each search. */
  lockedKbIds?: () => string[]
  /** How many knowledge chunks to keep after grouping; default no-cap. */
  chunkLimit?: number
  /** Debounce delay in ms. */
  debounceMs?: number
  /** 当前部署是否提供智能体路由；不提供时不发起预加载请求。 */
  agentsEnabled?: () => boolean
  /** Disable requests immediately when the palette closes. */
  enabled?: () => boolean
  /** User and active tenant identity; changing it invalidates every page. */
  contextKey?: () => string
}) {
  const debounceMs = options.debounceMs ?? 350
  const query = ref('')
  const loading = ref(false)
  const hasSearched = ref(false)
  const messageHasMore = ref(false)
  const messageLoadingMore = ref(false)
  const messagePageIndex = ref(0)
  const messageHasPrevious = computed(() => messagePageIndex.value > 0)
  const messageSearchError = ref(false)
  const knowledgeSearchError = ref(false)
  const searchIncomplete = computed(() => messageHasMore.value || messageSearchError.value || knowledgeSearchError.value)
  const contextKey = () => options.contextKey?.() || ''

  // All KBs the current user can see (own + shared). Loaded lazily & cached.
  const knowledgeBases = ref<CmdkKb[]>([])
  const kbsLoaded = ref(false)
  let kbsLoadingPromise: Promise<void> | null = null
  let kbsLoadingContext = ''

  const fileGroups = ref<CmdkFileGroup[]>([])
  const messageGroups = ref<CmdkMsgGroup[]>([])
  const totalChunks = ref(0)
  const totalMessages = ref(0)

  // Agents the current user can access (own + builtin + shared via orgs).
  const agents = ref<CmdkAgent[]>([])
  const agentsLoaded = ref(false)
  let agentsLoadingPromise: Promise<void> | null = null
  let agentsLoadingContext = ''

  const orgStore = useOrganizationStore()
  const menuStore = useMenuStore()

  const ensureKbs = async (): Promise<void> => {
    if (kbsLoaded.value) return
    const identity = contextKey()
    if (kbsLoadingPromise && kbsLoadingContext === identity) return kbsLoadingPromise
    kbsLoadingContext = identity
    kbsLoadingPromise = (async () => {
      try {
        const chatResources = useChatResourcesStore()
        await chatResources.ensureKnowledgeBases()
        if (identity !== contextKey()) return
        const own: CmdkKb[] = chatResources.rawKnowledgeBases.map((kb: any) => ({
          id: String(kb.id),
          name: kb.name || '',
          type: kb.type,
        }))
        const ownIds = new Set(own.map(k => k.id))
        const sharedList: CmdkKb[] = (orgStore.sharedKnowledgeBases || [])
          .filter((s: any) => s?.knowledge_base != null)
          .map((s: any) => ({
            id: String(s.knowledge_base.id),
            name: s.knowledge_base.name || '',
            type: s.knowledge_base.type,
          }))
          .filter((k: CmdkKb) => !ownIds.has(k.id))
        knowledgeBases.value = [...own, ...sharedList]
        kbsLoaded.value = true
      } catch (e) {
        if (identity === contextKey() && options.enabled?.() !== false) knowledgeSearchError.value = true
        console.error('[cmdk] failed to load knowledge bases', e)
      } finally {
        if (identity === kbsLoadingContext) kbsLoadingPromise = null
      }
    })()
    return kbsLoadingPromise
  }

  // Derived: KB name lookup
  const getKbName = (kbId: string): string => {
    if (!kbId) return ''
    const kb = knowledgeBases.value.find(k => k.id === kbId)
    return kb?.name || ''
  }

  // KB name hits (client-side, works even before a remote search returns).
  const kbMatches = computed<CmdkKb[]>(() => {
    const q = query.value.trim().toLowerCase()
    if (!q) return []
    return knowledgeBases.value
      .filter(k => k.name.toLowerCase().includes(q))
      .slice(0, 4)
  })

  // Agents (own + shared). Lazily loaded & cached; no backend search endpoint
  // exists so we always filter client-side.
  const ensureAgents = async (): Promise<void> => {
    if (options.agentsEnabled?.() === false) return
    if (agentsLoaded.value) return
    const identity = contextKey()
    if (agentsLoadingPromise && agentsLoadingContext === identity) return agentsLoadingPromise
    agentsLoadingContext = identity
    agentsLoadingPromise = (async () => {
      try {
        const chatResources = useChatResourcesStore()
        await chatResources.ensureAgents()
        if (identity !== contextKey()) return
        const own: CmdkAgent[] = chatResources.agents.map((a: any) => ({
          id: String(a.id),
          name: a.name || '',
          description: a.description,
          avatar: a.avatar,
          isBuiltin: !!a.is_builtin,
          source: 'own',
        }))
        const ownIds = new Set(own.map(a => a.id))
        const sharedList: CmdkAgent[] = (orgStore.sharedAgents || [])
          .filter((s: any) => s?.agent != null)
          .map((s: any) => ({
            id: String(s.agent.id),
            name: s.agent.name || '',
            description: s.agent.description,
            avatar: s.agent.avatar,
            isBuiltin: !!s.agent.is_builtin,
            source: 'shared' as const,
            orgName: s.org_name,
          }))
          .filter((a: CmdkAgent) => !ownIds.has(a.id))
        agents.value = [...own, ...sharedList]
        agentsLoaded.value = true
      } catch (e) {
        console.error('[cmdk] failed to load agents', e)
      } finally {
        if (identity === agentsLoadingContext) agentsLoadingPromise = null
      }
    })()
    return agentsLoadingPromise
  }

  const agentMatches = computed<CmdkAgent[]>(() => {
    if (options.agentsEnabled?.() === false) return []
    const q = query.value.trim().toLowerCase()
    if (!q) return []
    return agents.value
      .filter(a => {
        const name = (a.name || '').toLowerCase()
        const desc = (a.description || '').toLowerCase()
        return name.includes(q) || desc.includes(q)
      })
      .slice(0, 5)
  })

  // Sessions: no dedicated list API; we re-use whatever menuStore already has
  // cached (the sidebar already paginates them). Good enough for "jump to a
  // session I've seen recently" — not a substitute for message-content search.
  const sessionList = computed<CmdkSessionItem[]>(() => {
    const chatMenu = (menuStore.menuArr as any[]).find((m: any) => m.path === 'creatChat')
    const children = (chatMenu?.children as any[]) || []
    return children.map((c: any) => ({
      id: String(c.id || ''),
      title: c.title || '',
    }))
  })

  const sessionMatches = computed<CmdkSessionItem[]>(() => {
    const q = query.value.trim().toLowerCase()
    if (!q) return []
    // If a session already appears in the message-content group, suppress it
    // here — showing the same session title twice (once with a message hit,
    // once by title match) is noise. The message-group entry is strictly
    // more informative (it shows which message matched), so it wins.
    const coveredBySession = new Set(messageGroups.value.map(g => g.sessionId))
    return sessionList.value
      .filter(s => s.title && s.title.toLowerCase().includes(q))
      .filter(s => !coveredBySession.has(s.id))
      .slice(0, 5)
  })

  // Every query, scope, identity or close change invalidates old responses
  // before the next debounce timer can fire. Continuation remains user-driven.
  let requestSeq = 0
  let debounceTimer: ReturnType<typeof setTimeout> | null = null
  // Store only navigation controls. Previous bodies are never cached.
  let messagePageCursors = ['']

  const clearResults = () => {
    requestSeq++
    if (debounceTimer) clearTimeout(debounceTimer)
    debounceTimer = null
    loading.value = false
    messageLoadingMore.value = false
    fileGroups.value = []
    messageGroups.value = []
    totalChunks.value = 0
    totalMessages.value = 0
    hasSearched.value = false
    messageHasMore.value = false
    messageSearchError.value = false
    knowledgeSearchError.value = false
    messagePageIndex.value = 0
    messagePageCursors = ['']
  }

  const readMessagePage = async (q: string, seq: number, pageIndex = 0) => {
    const cursor = messagePageCursors[pageIndex] || ''
    // A navigation or retry action must fetch current authority again. Clear
    // prior body output immediately, including when the new request fails.
    messageGroups.value = []
    totalMessages.value = 0
    messagePageIndex.value = pageIndex
    messageHasMore.value = false
    messageSearchError.value = false
    try {
      const response = await searchMessages({ query: q, mode: 'hybrid', limit: 30, ...(cursor ? { cursor } : {}) })
      if (seq !== requestSeq) return
      const page = response.data
      if (!response.success || !page || !Array.isArray(page.items)) throw new Error('Invalid message search response')
      const hasMore = page.has_more === true
      const nextCursor = page.next_cursor || ''
      if (hasMore && (!nextCursor || nextCursor === cursor || messagePageCursors.slice(0, pageIndex + 1).includes(nextCursor))) {
        throw new Error('Message search cursor did not advance')
      }

      // Deduplicate only within this fresh, authorized page. Previous pages
      // retain their cursor, never their bodies; returning to one re-fetches it.
      const merged = new Map<string, MessageSearchGroupItem>()
      for (const item of page.items) merged.set(messageSearchGroupKey(item), item)
      const groups = new Map<string, CmdkMsgGroup>()
      for (const item of merged.values()) {
        const sid = item.session_id
        if (!groups.has(sid)) groups.set(sid, { sessionId: sid, sessionTitle: item.session_title || '', items: [] })
        groups.get(sid)!.items.push(item)
      }
      messageGroups.value = Array.from(groups.values())
      totalMessages.value = merged.size
      messagePageCursors = messagePageCursors.slice(0, pageIndex + 1)
      if (hasMore) messagePageCursors.push(nextCursor)
      messageHasMore.value = hasMore
    } catch (e) {
      if (seq !== requestSeq) return
      console.error('[cmdk] message search failed', e)
      // Preserve the continuation point for an explicit retry. A failure
      // never certifies an empty or completed search.
      messageSearchError.value = true
    }
  }

  const runSearch = async (q: string, seq: number) => {
    loading.value = true
    hasSearched.value = true
    const locked = options.lockedKbIds?.() || []
    const scoped = locked.length > 0
    let kbIds: string[]
    if (scoped) {
      kbIds = locked
    } else {
      // Start requests after debounce, when tenant selection has also updated
      // its request headers and shared resource stores.
      void ensureAgents()
      await ensureKbs()
      if (seq !== requestSeq) return
      kbIds = knowledgeBases.value.map(k => k.id)
    }

    const knowledgePromise = kbIds.length > 0
      ? knowledgeSemanticSearch({ query: q, knowledge_base_ids: kbIds })
          .then((res: any) => {
            if (!res?.success || !Array.isArray(res.data)) throw new Error('Invalid knowledge search response')
            return res.data
          })
          .catch((e) => {
            if (seq === requestSeq) knowledgeSearchError.value = true
            console.error('[cmdk] knowledge search failed', e)
            return []
          })
      : Promise.resolve([])
    const [chunks] = await Promise.all([knowledgePromise, scoped ? Promise.resolve() : readMessagePage(q, seq)])
    if (seq !== requestSeq) return

    const fmap = new Map<string, CmdkFileGroup>()
    for (const item of chunks as any[]) {
      const kid = item.knowledge_id || 'unknown'
      if (!fmap.has(kid)) {
        fmap.set(kid, {
          knowledgeId: kid,
          kbId: item.knowledge_base_id || '',
          title: item.knowledge_title || item.knowledge_filename || kid,
          kbName: getKbName(item.knowledge_base_id),
          chunks: [],
        })
      }
      fmap.get(kid)!.chunks.push({
        id: item.id,
        chunk_index: item.chunk_index,
        knowledge_id: kid,
        knowledge_base_id: item.knowledge_base_id || '',
        knowledge_title: item.knowledge_title || '',
        kb_name: getKbName(item.knowledge_base_id),
        content: item.content || '',
        matched_content: item.matched_content,
        match_type: item.match_type || 'vector',
        score: item.score || 0,
      })
    }
    fileGroups.value = Array.from(fmap.values())
    totalChunks.value = (chunks as any[]).length
    loading.value = false
  }

  const continueMessageSearch = async () => {
    if (loading.value || messageLoadingMore.value || options.enabled?.() === false || (options.lockedKbIds?.() || []).length) return
    if (!messageHasMore.value && !messageSearchError.value) return
    const q = query.value.trim()
    if (!q) return
    const seq = requestSeq
    messageLoadingMore.value = true
    try {
      await readMessagePage(q, seq, messageSearchError.value ? messagePageIndex.value : messagePageIndex.value + 1)
    } finally {
      if (seq === requestSeq) messageLoadingMore.value = false
    }
  }

  const previousMessagePage = async () => {
    if (!messageHasPrevious.value || loading.value || messageLoadingMore.value || options.enabled?.() === false || (options.lockedKbIds?.() || []).length) return
    const q = query.value.trim()
    if (!q) return
    const seq = requestSeq
    messageLoadingMore.value = true
    try {
      await readMessagePage(q, seq, messagePageIndex.value - 1)
    } finally {
      if (seq === requestSeq) messageLoadingMore.value = false
    }
  }

  const retrySearch = () => {
    const q = query.value.trim()
    clearResults()
    if (q && options.enabled?.() !== false) void runSearch(q, requestSeq)
  }

  const lockedSignature = () => (options.lockedKbIds?.() || []).slice().sort().join(',')
  watch([query, lockedSignature, contextKey, () => options.enabled?.() !== false], ([val, , identity], old) => {
    clearResults()
    if (old && identity !== old[2]) {
      // Name matches and preloads belong to the same identity as remote pages.
      knowledgeBases.value = []
      agents.value = []
      kbsLoaded.value = false
      agentsLoaded.value = false
    }
    const trimmed = (val as string).trim()
    if (!trimmed || options.enabled?.() === false) return
    const seq = requestSeq
    debounceTimer = setTimeout(() => {
      debounceTimer = null
      if (seq === requestSeq) void runSearch(trimmed, seq)
    }, debounceMs)
  }, { flush: 'sync' })

  onUnmounted(clearResults)

  onMounted(() => {
    // Preload KB and agent lists so name matches show up instantly on first
    // keystroke. Sessions come from the menuStore (already populated by the
    // sidebar) so no extra fetch needed here.
    ensureKbs()
    if (options.agentsEnabled?.() !== false) {
      ensureAgents()
    }
  })

  return {
    query,
    loading,
    hasSearched,
    messageHasMore,
    messageHasPrevious,
    messageLoadingMore,
    messageSearchError,
    knowledgeSearchError,
    searchIncomplete,
    continueMessageSearch,
    previousMessagePage,
    retrySearch,
    knowledgeBases,
    fileGroups,
    messageGroups,
    kbMatches,
    agents,
    agentMatches,
    sessionMatches,
    totalChunks,
    totalMessages,
    clearResults,
    ensureKbs,
    ensureAgents,
  }
}
