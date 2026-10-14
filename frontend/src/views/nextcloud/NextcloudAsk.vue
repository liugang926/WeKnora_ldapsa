<template>
  <main class="nextcloud-ask">
    <section class="nextcloud-ask__card" aria-labelledby="nextcloud-ask-title">
      <h1 id="nextcloud-ask-title">在知识库中提问</h1>
      <p v-if="loading" role="status">正在确认文件版本和你的访问权限…</p>
      <p v-else-if="error" role="alert">{{ error }}</p>
      <template v-else-if="target">
        <p class="nextcloud-ask__source">当前文件：{{ target.title || '已发布文件' }}</p>
        <p>回答将只检索这个文件的当前已发布版本。你的 WeKnora 与 Nextcloud 权限会在提问时再次检查。</p>
        <form @submit.prevent="submit">
          <label for="nextcloud-ask-question">你的问题</label>
          <textarea id="nextcloud-ask-question" v-model.trim="question" rows="5"
            maxlength="4000" :disabled="submitting" required />
          <button type="submit" :disabled="submitting || !question.trim()">
            {{ submitting ? '正在开始问答…' : '提问此文件' }}
          </button>
        </form>
      </template>
    </section>
  </main>
</template>

<script setup lang="ts">
import { ref, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { get } from '@/utils/request'
import { createSessions } from '@/api/chat'
import { BUILTIN_QUICK_ANSWER_ID } from '@/api/agent'
import { useSettingsStore } from '@/stores/settings'
import { useMenuStore } from '@/stores/menu'
import { safeNextcloudAskPath } from '@/utils/nextcloudAskLink'

type AskTarget = { knowledge_id: string; knowledge_base_id: string; title: string; source_etag: string }

const route = useRoute()
const router = useRouter()
const settings = useSettingsStore()
const menu = useMenuStore()
const target = ref<AskTarget | null>(null)
const loading = ref(true)
const submitting = ref(false)
const question = ref('')
const error = ref('')

async function resolveTarget(): Promise<AskTarget> {
  const path = safeNextcloudAskPath(route.fullPath)
  if (!path) throw new Error('invalid_link')
  const query = new URL(path, 'https://weknora.invalid').search
  const response = await get<{ success: boolean; data: AskTarget }>(
    `/api/v1/integrations/nextcloud/ask-target${query}`)
  const value = response?.data
  if (!response?.success || !value || !value.knowledge_id || !value.knowledge_base_id) {
    throw new Error('target_unavailable')
  }
  return value
}

async function load() {
  loading.value = true
  target.value = null
  error.value = ''
  try {
    target.value = await resolveTarget()
  } catch {
    error.value = '无法确认这个文件仍已发布且你有权读取。请返回 Nextcloud 刷新文件状态。'
  } finally {
    loading.value = false
  }
}

async function submit() {
  if (!target.value || !question.value.trim() || submitting.value) return
  submitting.value = true
  error.value = ''
  try {
    const fresh = await resolveTarget()
    if (fresh.knowledge_id !== target.value.knowledge_id ||
        fresh.knowledge_base_id !== target.value.knowledge_base_id ||
        fresh.source_etag !== target.value.source_etag) {
      throw new Error('target_changed')
    }
    const created = await createSessions({})
    const sessionID = created?.data?.id
    if (!created?.success || typeof sessionID !== 'string' || !sessionID) {
      throw new Error('session_unavailable')
    }

    // The regular chat page sends its first question from this store. Reset
    // previously selected KBs, files, tags, tools and web search before it
    // constructs the request; the server checks the exact file again.
    settings.selectAgent(BUILTIN_QUICK_ANSWER_ID)
    settings.addFile(fresh.knowledge_id)
    settings.toggleWebSearch(false)
    settings.toggleLocalBrowser(false)
    const now = new Date().toISOString()
    menu.updataMenuChildren({
      title: '新会话', path: `chat/${sessionID}`, id: sessionID,
      isMore: false, isNoTitle: true, created_at: now, updated_at: now,
    })
    menu.changeIsFirstSession(true)
    menu.changeFirstQuery(question.value.trim(), [], '', [], [])
    await router.push(`/platform/chat/${encodeURIComponent(sessionID)}`)
  } catch {
    error.value = '提问未开始。请确认文件仍已发布且你拥有访问权限，然后重试。'
  } finally {
    submitting.value = false
  }
}

onMounted(load)
watch(() => route.fullPath, load)
</script>

<style scoped>
.nextcloud-ask { display: grid; place-items: center; min-height: 70vh; padding: 2rem; }
.nextcloud-ask__card { width: min(100%, 42rem); padding: 2rem; border-radius: 1rem; background: var(--td-bg-color-container); box-shadow: 0 0.4rem 2rem #0001; }
.nextcloud-ask__card h1 { font-size: 1.5rem; margin: 0 0 1rem; }
.nextcloud-ask__source { font-weight: 600; overflow-wrap: anywhere; }
.nextcloud-ask__card form { display: grid; gap: .75rem; margin-top: 1.5rem; }
.nextcloud-ask__card textarea { width: 100%; padding: .75rem; border: 1px solid #999; border-radius: .5rem; resize: vertical; font: inherit; }
.nextcloud-ask__card button { justify-self: start; padding: .65rem 1.25rem; border: 0; border-radius: .5rem; background: #167d57; color: white; cursor: pointer; }
.nextcloud-ask__card button:disabled { opacity: .5; cursor: not-allowed; }
</style>
