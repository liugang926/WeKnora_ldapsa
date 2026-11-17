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
      <dl v-if="users?.summary" class="summary">
        <div><dt>{{ t('chatUsage.people') }}</dt><dd>{{ number(users.summary.user_count) }}</dd></div>
        <div><dt>{{ t('chatUsage.sessions') }}</dt><dd>{{ number(users.summary.session_count) }}</dd></div>
        <div><dt>{{ t('chatUsage.tokens') }}</dt><dd>{{ token(users.summary, 'total_tokens') }}</dd></div>
        <div><dt>{{ t('chatUsage.missing') }}</dt><dd>{{ number(users.summary.unreported_turns) }}</dd></div>
      </dl>
      <form class="toolbar" @submit.prevent="applySearch">
        <t-input v-model="draft" clearable :aria-label="t('chatUsage.search')" :placeholder="t('chatUsage.search')"
          @clear="clearSearch" />
        <t-button type="submit" variant="outline">{{ t('chatUsage.find') }}</t-button>
      </form>
    </template>
    <div v-else class="detail-header">
      <t-button size="small" variant="text" @click="selected = null">
        <template #icon><t-icon name="chevron-left" /></template>{{ t('chatUsage.peopleList') }}
      </t-button>
      <h3>{{ selected.username || selected.user_id }}</h3>
      <small v-if="selected.username && selected.user_id !== selected.username">{{ selected.user_id }}</small>
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
            <th scope="col" :class="{ numeric: !selected }">{{ selected ? t('chatUsage.workspace') : t('chatUsage.sessions') }}</th>
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
              </template>
              <template v-else>
                <th scope="row">
                  <button class="person" type="button" @click="openUser(row)">{{ row.username || row.user_id }}</button>
                  <small v-if="row.username && row.user_id !== row.username">{{ row.user_id }}</small>
                </th>
                <td class="numeric">{{ number(row.session_count) }}</td>
              </template>
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
import { computed, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { CHAT_USAGE_PAGE_SIZE, getChatSessions, getChatUsers, type ChatUsage, type ChatUserUsage } from '@/api/system/chatUsage'
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
  if (!row.reported_turns) return '—'
  if (key !== 'total_tokens' && !row.prompt_tokens && !row.completion_tokens && row.total_tokens > 0) return '—'
  return number(row[key])
}
</script>

<style scoped>
.chat-usage { min-width: 0; color: var(--td-text-color-primary); }
.usage-header, .toolbar { display: flex; align-items: center; gap: 12px; }
.usage-header { justify-content: space-between; align-items: flex-start; }
h2 { font-size: 20px; margin: 0; }
.usage-header p, .usage-note, small, dt { color: var(--td-text-color-secondary); }
.usage-header p, .usage-note { font-size: 13px; margin: 8px 0 18px; }
.summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; margin: 0 0 20px; }
.summary div { padding: 12px; border: 1px solid var(--td-component-border); border-radius: 8px; }
dt, small { font-size: 12px; }
dd { margin: 4px 0 0; font-size: 19px; font-weight: 600; overflow-wrap: anywhere; }
.toolbar { max-width: 440px; }
.detail-header h3 { margin: 12px 0 0; font-size: 16px; }
.table-scroll { overflow-x: auto; border: 1px solid var(--td-component-border); border-radius: 8px; }
table { border-collapse: collapse; width: 100%; font-size: 13px; }
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
