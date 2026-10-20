<template>
  <main class="source-knowledge">
    <header>
      <h1>我的来源知识库</h1>
      <p>只显示当前获准读取的知识库。每次提问和读取回答都会重新检查来源权限。</p>
      <label>工作空间
        <select :value="auth.effectiveTenantId || ''" :disabled="busy || isStreaming" @change="switchTenant">
          <option v-for="scope in auth.memberships" :key="scope.tenant_id" :value="scope.tenant_id">
            {{ scope.tenant_name || `#${scope.tenant_id}` }}
          </option>
        </select>
      </label>
      <button type="button" @click="leave">退出登录</button>
    </header>
    <section aria-labelledby="source-kb-title">
      <h2 id="source-kb-title">{{ fileTarget ? `提问文件：${fileTarget.title}` : '选择知识库' }}</h2>
      <p v-if="busy" role="status">正在验证访问权限…</p>
      <p v-if="notice" role="alert">{{ notice }}</p>
      <p v-if="!busy && !knowledgeBases.length">当前没有可读取的知识库。</p>
      <label v-if="knowledgeBases.length">知识库
        <select v-model="selectedKB" :disabled="isStreaming || !!fileTarget" @change="clearConversation">
          <option value="" disabled>请选择</option>
          <option v-for="kb in knowledgeBases" :key="kb.id" :value="kb.id">{{ kb.name }}</option>
        </select>
      </label>
      <form v-if="selectedKB" @submit.prevent="ask">
        <label for="source-question">你的问题</label>
        <textarea id="source-question" v-model.trim="question" rows="4" maxlength="4000" required :disabled="busy || isStreaming" />
        <button type="submit" :disabled="busy || isStreaming || !question">提问</button>
        <button v-if="isStreaming" type="button" @click="stopStream">停止回答</button>
        <button v-else type="button" @click="clearConversation">新会话</button>
      </form>
      <div class="conversation" aria-live="polite">
        <article v-for="message in messages" :key="message.id">
          <strong>{{ message.role === 'user' ? '我的问题' : '回答' }}</strong>
          <p>{{ message.content }}</p>
        </article>
        <article v-if="answer"><strong>回答</strong><p>{{ answer }}</p></article>
      </div>
    </section>
  </main>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuthStore } from '@/stores/auth'
import { listKnowledgeBases } from '@/api/knowledge-base'
import { createSessions, getMessageList } from '@/api/chat'
import { logout as logoutApi, updateMyPreferences } from '@/api/auth'
import { get } from '@/utils/request'
import { useStream } from '@/api/chat/streame'
import { safeNextcloudAskPath } from '@/utils/nextcloudAskLink'

type KB = { id: string; name: string }
type Message = { id: string; role: string; content: string }
type FileTarget = { knowledge_id: string; knowledge_base_id: string; title: string; source_etag: string }
const auth = useAuthStore()
const route = useRoute()
const router = useRouter()
const knowledgeBases = ref<KB[]>([])
const selectedKB = ref('')
const question = ref('')
const notice = ref('')
const busy = ref(false)
const sessionID = ref('')
const answer = ref('')
const messages = ref<Message[]>([])
const fileTarget = ref<FileTarget | null>(null)
const { startStream, stopStream, onChunk, isStreaming, error } = useStream()
let generation = 0
let poll: ReturnType<typeof setInterval> | undefined
let checking = false

function clearConversation() {
  generation++
  stopStream()
  sessionID.value = ''; answer.value = ''; messages.value = []; question.value = ''; busy.value = false
}
function unavailable() {
  clearConversation()
  notice.value = '无法验证当前来源权限。内容已隐藏，请刷新或返回 Nextcloud。'
}
async function resolveFile(): Promise<FileTarget | null> {
  if (!route.query.ask) return null
  const safe = safeNextcloudAskPath(route.query.ask)
  if (!safe) throw new Error('invalid source link')
  const query = new URL(safe, 'https://weknora.invalid').search
  const response = await get<{ success: boolean; data: FileTarget }>(`/api/v1/integrations/nextcloud/ask-target${query}`)
  if (!response.success || !response.data?.knowledge_id) throw new Error('source unavailable')
  return response.data
}
async function load() {
  clearConversation(); const expected = generation
  busy.value = true; notice.value = ''; knowledgeBases.value = []; fileTarget.value = null; selectedKB.value = ''
  try {
    if (!await auth.refreshFromAuthMe()) throw new Error('scope unavailable')
    if (expected !== generation) return
    const response = await listKnowledgeBases()
    if (!response?.success || !Array.isArray(response.data)) throw new Error('KB scope unavailable')
    const bases = response.data.map((kb: KB) => ({ id: kb.id, name: kb.name }))
    const file = await resolveFile()
    if (expected !== generation) return
    const target = file?.knowledge_base_id
    if (target && !bases.some((kb: KB) => kb.id === target)) throw new Error('KB no longer readable')
    knowledgeBases.value = bases; fileTarget.value = file
    selectedKB.value = target || bases[0]?.id || ''
  } catch { if (expected === generation) unavailable() }
  finally { if (expected === generation) busy.value = false }
}
async function switchTenant(event: Event) {
  const id = Number((event.target as HTMLSelectElement).value)
  const scope = auth.memberships.find(m => m.tenant_id === id)
  if (!scope) return
  clearConversation(); busy.value = true
  const previous = auth.selectedTenantId
  const previousName = auth.selectedTenantName
  try {
    auth.setSelectedTenant(id, scope.tenant_name || null)
    if (!await auth.refreshFromAuthMe()) throw new Error('scope unavailable')
    await updateMyPreferences({ last_active_tenant_id: id })
    await router.replace(auth.currentTenantScopedOnly ? '/source-knowledge' : '/platform/knowledge-bases')
    if (auth.currentTenantScopedOnly) await load()
  } catch { auth.setSelectedTenant(previous, previousName); unavailable() }
  finally { busy.value = false }
}
async function history() {
  if (!sessionID.value || checking) return
  checking = true
  const expected = generation
  try {
    const response = await getMessageList({ session_id: sessionID.value, limit: 100, created_at: '' })
    if (!response?.success || !Array.isArray(response.data)) throw new Error('history unavailable')
    if (expected === generation && !isStreaming.value) {
      messages.value = response.data.filter((m: Message) => m.role === 'user' || m.role === 'assistant')
        .map((m: Message) => ({ id: m.id, role: m.role, content: m.content }))
      answer.value = ''
    }
  } catch { if (expected === generation) unavailable() }
  finally { checking = false }
}
async function ask() {
  if (!selectedKB.value || !question.value || isStreaming.value || busy.value) return
  busy.value = true; notice.value = ''; answer.value = ''
  const expected = generation
  const selected = selectedKB.value
  const query = question.value
  try {
    const fresh = await resolveFile()
    if (expected !== generation) return
    if (fileTarget.value && (!fresh || fresh.knowledge_id !== fileTarget.value.knowledge_id ||
        fresh.source_etag !== fileTarget.value.source_etag)) throw new Error('source changed')
    if (!sessionID.value) {
      const created = await createSessions({})
      if (expected !== generation) return
      if (!created?.success || !created.data?.id) throw new Error('session unavailable')
      sessionID.value = created.data.id
    }
    question.value = ''; busy.value = false
    await startStream({ method: 'POST', url: '/api/v1/knowledge-chat', session_id: sessionID.value,
      query, knowledge_base_ids: [selected], knowledge_ids: fresh ? [fresh.knowledge_id] : undefined,
      agent_enabled: false, web_search_enabled: false, local_browser_enabled: false })
    if (error.value) throw new Error('source unavailable')
    await history()
  } catch { if (expected === generation) unavailable() }
  finally { if (expected === generation) busy.value = false }
}
onChunk((chunk) => {
  if (chunk.response_type === 'error') { unavailable(); return }
  if (chunk.response_type === 'answer' && typeof chunk.content === 'string') answer.value += chunk.content
})
watch(error, value => { if (value) unavailable() })
watch(() => route.query.ask, () => { void load() })
async function leave() { clearConversation(); try { await logoutApi() } finally { auth.logout(); await router.replace('/login') } }
onMounted(async () => { await load(); poll = setInterval(history, 5000) })
onUnmounted(() => { if (poll) clearInterval(poll); clearConversation() })
</script>

<style scoped>
.source-knowledge { box-sizing: border-box; width: min(100%, 64rem); margin: auto; padding: 2rem; color: var(--td-text-color-primary); }
.source-knowledge *, .source-knowledge *::before, .source-knowledge *::after { box-sizing: border-box; }
header, section { background: var(--td-bg-color-container); border-radius: var(--app-radius-xl); padding: 1.5rem; margin-bottom: 1rem; }
h1 { font-size: 1.7rem; } h2 { font-size: 1.2rem; margin-bottom: 1rem; }
label { display: block; margin: .75rem 0; } select, textarea { display: block; width: 100%; padding: .65rem; font: inherit; border: 1px solid var(--td-border-level-2-color); border-radius: var(--app-radius-sm); background: var(--td-bg-color-container); color: inherit; }
button { padding: .65rem 1rem; margin: .5rem .5rem .5rem 0; background: var(--td-brand-color); color: white; border: 0; border-radius: var(--app-radius-sm); cursor: pointer; } button:disabled { opacity: .5; cursor: default; }
.conversation article { border-top: 1px solid var(--td-border-level-1-color); padding: 1rem 0; } .conversation p { white-space: pre-wrap; overflow-wrap: anywhere; }
@media (max-width: 600px) { .source-knowledge { padding: .75rem; } header, section { padding: 1rem; } }
</style>
