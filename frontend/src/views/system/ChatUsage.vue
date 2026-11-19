<template>
  <section class="chat-usage">
    <header class="usage-header">
      <div>
        <h2>{{ t('chatUsage.title') }}</h2>
        <p>{{ t('chatUsage.scope') }}</p>
      </div>
      <t-button size="small" variant="text" :disabled="loading" @click="reload">
        <template #icon><t-icon name="refresh" /></template>{{ t('common.refresh') }}
      </t-button>
    </header>

    <template v-if="!selected">
      <h3>{{ t('chatUsage.global') }}</h3>
      <dl v-if="users?.summary" class="summary">
        <div v-for="metric in questionMetrics" :key="metric.key">
          <dt>{{ t(metric.label) }}</dt><dd>{{ number(users.summary[metric.key]) }}</dd>
        </div>
        <div><dt>{{ t('chatUsage.people') }}</dt><dd>{{ number(users.summary.user_count) }}</dd></div>
        <div><dt>{{ t('chatUsage.sessions') }}</dt><dd>{{ number(users.summary.session_count) }}</dd></div>
        <div><dt>{{ t('chatUsage.tokens') }}</dt><dd>{{ token(users.summary, 'total_tokens') }}</dd></div>
        <div><dt>{{ t('chatUsage.missing') }}</dt><dd>{{ number(users.summary.unreported_turns) }}</dd></div>
      </dl>
      <section v-if="unattributed && !query" class="unattributed">
        <h3>{{ t('chatUsage.unattributed') }}</h3>
        <dl class="summary">
          <div><dt>{{ t('chatUsage.questions') }}</dt><dd>{{ number(unattributed.question_count) }}</dd></div>
          <div><dt>{{ t('chatUsage.sessions') }}</dt><dd>{{ number(unattributed.session_count) }}</dd></div>
          <div><dt>{{ t('chatUsage.input') }}</dt><dd>{{ token(unattributed, 'prompt_tokens') }}</dd></div>
          <div><dt>{{ t('chatUsage.output') }}</dt><dd>{{ token(unattributed, 'completion_tokens') }}</dd></div>
          <div><dt>{{ t('chatUsage.tokens') }}</dt><dd>{{ token(unattributed, 'total_tokens') }}</dd></div>
          <div><dt>{{ t('chatUsage.missing') }}</dt><dd>{{ number(unattributed.unreported_turns) }}</dd></div>
        </dl>
      </section>
      <form class="toolbar" @submit.prevent="applySearch">
        <t-input v-model="draft" clearable :aria-label="t('chatUsage.search')" :placeholder="t('chatUsage.search')"
          @clear="clearSearch" />
        <t-button type="submit" variant="outline">{{ t('chatUsage.find') }}</t-button>
        <t-button type="button" variant="outline" :loading="exporting" :disabled="loading || failed || (!total && (query !== '' || !unattributed))" @click="exportPeople">
          {{ t('chatUsage.exportCsv') }}
        </t-button>
      </form>
      <t-alert v-if="exportFailed" theme="error" :message="t('chatUsage.exportFailed')" />
    </template>
    <div v-else class="detail-header">
      <t-button size="small" variant="text" @click="selected = null">
        <template #icon><t-icon name="chevron-left" /></template>{{ t('chatUsage.peopleList') }}
      </t-button>
      <h3>{{ selected.display_name || selected.username || selected.user_id }}</h3>
      <small v-if="selected.display_name && selected.username">{{ selected.username }}</small>
    </div>

    <p class="usage-note">{{ t('chatUsage.coverage') }}</p>
    <t-alert v-if="failed" theme="error" :message="t('chatUsage.failed')">
      <template #operation><t-button size="small" @click="reload">{{ t('common.refresh') }}</t-button></template>
    </t-alert>
    <div v-else-if="loading" class="state" role="status"><t-loading :text="t('common.loading')" /></div>
    <div v-else-if="!rows.length" class="state"><t-empty :description="t('common.noData')" /></div>
    <template v-else>
      <div class="table-scroll">
        <table>
          <caption class="sr-only">{{ selected ? t('chatUsage.sessionDetails') : t('chatUsage.peopleList') }}</caption>
          <thead><tr>
            <th scope="col">{{ selected ? t('chatUsage.started') : t('chatUsage.person') }}</th>
            <th v-if="selected" scope="col">{{ t('chatUsage.workspace') }}</th>
            <th v-if="selected" scope="col">{{ t('chatUsage.source') }}</th>
            <th v-for="metric in visibleQuestionMetrics" :key="metric.key" scope="col" class="numeric">{{ t(metric.label) }}</th>
            <th v-if="!selected" scope="col" class="numeric">{{ t('chatUsage.sessions') }}</th>
            <th scope="col" class="numeric">{{ t('chatUsage.input') }}</th>
            <th scope="col" class="numeric">{{ t('chatUsage.output') }}</th>
            <th scope="col" class="numeric">{{ t('chatUsage.tokens') }}</th>
            <th scope="col" class="numeric">{{ t('chatUsage.missing') }}</th>
          </tr></thead>
          <tbody>
            <tr v-for="row in rows" :key="'session_id' in row ? row.session_id : row.user_id">
              <template v-if="'session_id' in row">
                <th scope="row">
                  <time :datetime="row.started_at">{{ date(row.started_at) }}</time>
                  <small>ID: {{ row.session_id }}</small>
                </th>
                <td>{{ row.workspace_name || '—' }}<small>ID: {{ row.workspace_id }}</small></td>
                <td>{{ row.source === 'dingtalk' ? t('chatUsage.dingtalk') : row.source === 'web' ? t('chatUsage.web') : '—' }}</td>
              </template>
              <template v-else>
                <th scope="row">
                  <button class="person" type="button" @click="openUser(row)">{{ row.display_name || row.username || row.user_id }}</button>
                  <small v-if="row.display_name && row.username">{{ row.username }}</small>
                </th>
              </template>
              <td v-for="metric in visibleQuestionMetrics" :key="metric.key" class="numeric">{{ number(row[metric.key]) }}</td>
              <td v-if="'user_id' in row" class="numeric">{{ number(row.session_count) }}</td>
              <td class="numeric">{{ token(row, 'prompt_tokens') }}</td>
              <td class="numeric">{{ token(row, 'completion_tokens') }}</td>
              <td class="numeric">{{ token(row, 'total_tokens') }}</td>
              <td class="numeric">{{ number(row.unreported_turns) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </template>
    <t-pagination v-if="!failed && !loading && total > CHAT_USAGE_PAGE_SIZE" v-model="page"
      :total="total" :page-size="CHAT_USAGE_PAGE_SIZE" :show-page-size="false" size="small" />
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { CHAT_USAGE_PAGE_SIZE, getChatSessions, getChatUsers, type ChatUsage, type ChatUserUsage } from '@/api/system/chatUsage'
import { csvHeaderKeys, hasUsage, peopleCsv, reportedTokens } from './chatUsageCsv'
import { useChatUsagePage } from './useChatUsagePage'

const { t, locale } = useI18n()
const draft = ref('')
const query = ref('')
const userPage = ref(1)
const sessionPage = ref(1)
const selected = ref<ChatUserUsage | null>(null)

const userResult = useChatUsagePage(signal => getChatUsers(userPage.value, query.value, signal))
const detailResult = useChatUsagePage(signal => selected.value
  ? getChatSessions(selected.value.user_id, sessionPage.value, signal) : undefined)
const users = userResult.data
const unattributed = computed(() => hasUsage(users.value?.unattributed_dingtalk) ? users.value?.unattributed_dingtalk : undefined)
const questionMetrics = [
  { key: 'question_count', label: 'chatUsage.questions' },
  { key: 'web_question_count', label: 'chatUsage.webQuestions' },
  { key: 'dingtalk_question_count', label: 'chatUsage.dingtalkQuestions' },
] as const
const visibleQuestionMetrics = computed(() => selected.value ? questionMetrics.slice(0, 1) : questionMetrics)
const current = computed(() => selected.value ? detailResult : userResult)
const rows = computed(() => current.value.data.value?.items ?? [])
const total = computed(() => current.value.data.value?.total ?? 0)
const failed = computed(() => current.value.failed.value)
const loading = computed(() => current.value.loading.value)
const page = computed({
  get: () => selected.value ? sessionPage.value : userPage.value,
  set: n => { (selected.value ? sessionPage : userPage).value = n },
})
function reload() { current.value.reload() }
function openUser(user: ChatUserUsage) { sessionPage.value = 1; selected.value = user }
function applySearch() { query.value = draft.value.trim(); userPage.value = 1 }
function clearSearch() { draft.value = ''; applySearch() }

const formatter = computed(() => new Intl.NumberFormat(locale.value))
const dateFormatter = computed(() => new Intl.DateTimeFormat(locale.value, { dateStyle: 'medium', timeStyle: 'short' }))
const number = (n: number) => formatter.value.format(n)
const date = (s: string) => {
  const d = new Date(s)
  return Number.isNaN(d.getTime()) ? '—' : dateFormatter.value.format(d)
}
function token(row: ChatUsage, key: 'prompt_tokens' | 'completion_tokens' | 'total_tokens') {
  const value = reportedTokens(row, key)
  return value === null ? '—' : number(value)
}

const exporting = ref(false)
const exportFailed = ref(false)
let controller: AbortController | undefined
function cancelExport() {
  controller?.abort()
  controller = undefined
  exporting.value = false
  exportFailed.value = false
}
watch([query, selected], cancelExport)
onBeforeUnmount(cancelExport)

async function exportPeople() {
  if (exporting.value) return
  const request = new AbortController()
  controller = request
  const filter = query.value
  exporting.value = true
  exportFailed.value = false
  try {
    const headers = csvHeaderKeys.map(key => t('chatUsage.' + key))
    const csv = await peopleCsv(headers, page => getChatUsers(page, filter, request.signal, 100),
      filter ? undefined : t('chatUsage.unattributed'))
    if (request.signal.aborted) return
    const url = URL.createObjectURL(new Blob([csv], { type: 'text/csv;charset=utf-8' }))
    const link = document.createElement('a')
    link.href = url
    link.download = 'chat-usage-' + new Date().toISOString().slice(0, 10) + '.csv'
    link.click()
    setTimeout(() => URL.revokeObjectURL(url), 1000)
  } catch {
    if (!request.signal.aborted) exportFailed.value = true
  } finally {
    if (controller === request) {
      exporting.value = false
      controller = undefined
    }
  }
}
</script>

<style scoped>
.chat-usage { min-width: 0; color: var(--td-text-color-primary); }
.usage-header, .toolbar { display: flex; align-items: center; gap: 12px; }
.usage-header { justify-content: space-between; align-items: flex-start; }
h2 { font-size: var(--app-text-3xl); margin: 0; }
.usage-header p, .usage-note, small, dt { color: var(--td-text-color-secondary); }
.usage-header p, .usage-note { font-size: var(--app-text-md); margin: 8px 0 18px; }
.summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; margin: 0 0 20px; }
.summary div { padding: 12px; border: 1px solid var(--td-component-border); border-radius: var(--app-radius-md); }
dt, small { font-size: var(--app-text-sm); }
dd { margin: 4px 0 0; font-size: var(--app-text-2xl); font-weight: 600; overflow-wrap: anywhere; }
.toolbar { max-width: 620px; flex-wrap: wrap; }
.toolbar :deep(.t-input__wrap) { flex: 1 1 180px; }
.detail-header h3 { margin: 12px 0 0; font-size: var(--app-text-xl); }
.table-scroll { overflow-x: auto; border: 1px solid var(--td-component-border); border-radius: var(--app-radius-md); }
table { border-collapse: collapse; width: 100%; font-size: var(--app-text-md); }
td, th { text-align: start; padding: 10px; vertical-align: top; }
thead th { font-weight: 500; color: var(--td-text-color-secondary); background: var(--td-bg-color-secondarycontainer); }
tbody th { font-weight: normal; }
tbody tr + tr { border-top: 1px solid var(--td-component-border); }
.numeric, dd { font-variant-numeric: tabular-nums; }
.numeric { text-align: end; }
.person { padding: 0; border: 0; cursor: pointer; font: inherit; color: var(--td-brand-color); background: none; text-align: start; overflow-wrap: anywhere; }
.person:hover { text-decoration: underline; }
small { display: block; overflow-wrap: anywhere; }
.state { padding: 28px 0; text-align: center; }
.sr-only { position: absolute; width: 1px; height: 1px; overflow: hidden; clip-path: inset(50%); }
:deep(.t-pagination) { margin-top: 14px; }
</style>
