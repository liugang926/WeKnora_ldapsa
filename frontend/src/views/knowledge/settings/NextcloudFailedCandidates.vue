<script setup lang="ts">
import { ref, watch } from 'vue'
import { MessagePlugin } from 'tdesign-vue-next'
import { useI18n } from 'vue-i18n'
import {
  getNextcloudFailedCandidateRetry,
  listNextcloudFailedCandidates,
  retryNextcloudFailedCandidate,
  type NextcloudFailedCandidate,
  type NextcloudFailedCandidateRetryStatus,
} from '@/api/datasource'
import { submitFailedCandidateRetry } from './failedCandidateRetryFlow'
import { FailedCandidateRequestGate } from './failedCandidateRequestGate'

const props = defineProps<{ dataSourceId: string; dataSourceName: string }>()
const visible = defineModel<boolean>('visible', { default: false })
const { t } = useI18n()

const candidates = ref<NextcloudFailedCandidate[]>([])
const operationId = ref('')
const nextCursor = ref('')
const selectedFileId = ref('')
const exactStatus = ref<NextcloudFailedCandidateRetryStatus | null>(null)
const loading = ref(false)
const loadingMore = ref(false)
const loadingStatus = ref(false)
const saving = ref(false)
const loadError = ref(false)
const requestGate = new FailedCandidateRequestGate()

function clearSelection() {
  exactStatus.value = null
  selectedFileId.value = ''
  loadingStatus.value = false
}

async function loadPage(reset = true) {
  if (!props.dataSourceId || (!reset && (!nextCursor.value || loadingMore.value))) return
  const current = requestGate.beginList(reset)
  if (reset) {
    clearSelection()
    candidates.value = []
    operationId.value = ''
    nextCursor.value = ''
    loading.value = true
  } else {
    loadingMore.value = true
  }
  loadError.value = false
  try {
    const page = await listNextcloudFailedCandidates(props.dataSourceId, reset ? '' : nextCursor.value)
    if (!requestGate.isCurrentList(current) || !visible.value) return
    operationId.value = page.operation_id
    candidates.value = reset ? page.candidates : [...candidates.value, ...page.candidates]
    nextCursor.value = page.next_cursor
  } catch {
    if (requestGate.isCurrentList(current)) loadError.value = true
  } finally {
    if (requestGate.isCurrentList(current)) {
      loading.value = false
      loadingMore.value = false
    }
  }
}

async function selectCandidate(fileId: string) {
  if (!operationId.value || saving.value) return
  selectedFileId.value = fileId
  exactStatus.value = null
  const current = requestGate.beginDetail()
  loadingStatus.value = true
  try {
    const response = await getNextcloudFailedCandidateRetry(operationId.value, fileId)
    if (requestGate.isCurrentDetail(current) && visible.value && selectedFileId.value === fileId &&
      response.retry.file_id === fileId) {
      exactStatus.value = response.retry
    }
  } catch (error: any) {
    if (!requestGate.isCurrentDetail(current)) return
    if (error?.status === 409) {
      await loadPage(true)
    } else {
      MessagePlugin.error(t('datasource.failedCandidates.loadFailed'))
    }
  } finally {
    if (requestGate.isCurrentDetail(current)) loadingStatus.value = false
  }
}

async function retrySelected() {
  const snapshot = exactStatus.value
  const fileId = selectedFileId.value
  const pairing = operationId.value
  const sourceId = props.dataSourceId
  const context = requestGate.currentContext()
  const isCurrent = () => visible.value && props.dataSourceId === sourceId &&
    requestGate.isCurrentContext(context)
  if (!snapshot || snapshot.file_id !== fileId || !pairing || saving.value) return
  saving.value = true
  try {
    const result = await submitFailedCandidateRetry(snapshot,
      () => { if (isCurrent()) exactStatus.value = null },
      (current) => retryNextcloudFailedCandidate(pairing, fileId, current),
      () => loadPage(true),
      isCurrent,
    )
    if (!isCurrent()) return
    if (result === 'changed') {
      MessagePlugin.warning(t('datasource.failedCandidates.changed'))
      if (candidates.value.some(candidate => candidate.file_id === fileId)) {
        saving.value = false
        await selectCandidate(fileId)
      }
    } else {
      MessagePlugin.success(t('datasource.failedCandidates.scheduled'))
    }
  } catch {
    if (isCurrent()) MessagePlugin.error(t('datasource.failedCandidates.retryFailed'))
  } finally {
    if (isCurrent()) saving.value = false
  }
}

function reason(code: string) {
  const known = ['parse_failed', 'candidate_retry_exhausted', 'sync_stale_manual_review']
  return known.includes(code)
    ? t(`datasource.failedCandidates.reason.${code}`)
    : t('datasource.failedCandidates.reason.other')
}

function state(stateCode: string) {
  const known = ['retry', 'manual', 'leased']
  return known.includes(stateCode)
    ? t(`datasource.failedCandidates.state.${stateCode}`)
    : t('datasource.failedCandidates.state.other')
}

function time(value: string | undefined) {
  if (!value) return ''
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? '' : date.toLocaleString()
}

watch([visible, () => props.dataSourceId], ([isVisible]) => {
  requestGate.invalidate()
  saving.value = false
  clearSelection()
  candidates.value = []
  operationId.value = ''
  nextCursor.value = ''
  if (isVisible) void loadPage(true)
})
</script>

<template>
  <t-drawer v-model:visible="visible" size="520px" destroy-on-close>
    <template #header>
      <span>{{ t('datasource.failedCandidates.title') }} · {{ dataSourceName }}</span>
    </template>
    <div class="failed-candidates">
      <p class="failed-candidates__hint">{{ t('datasource.failedCandidates.hint') }}</p>
      <div class="failed-candidates__toolbar">
        <t-button size="small" variant="outline" :loading="loading" :disabled="saving" @click="loadPage(true)">
          {{ t('datasource.failedCandidates.refresh') }}
        </t-button>
      </div>
      <div v-if="loadError" role="alert" class="failed-candidates__error">
        {{ t('datasource.failedCandidates.loadFailed') }}
      </div>
      <t-loading v-if="loading" size="small" />
      <t-empty v-else-if="!loadError && candidates.length === 0 && !nextCursor"
        :description="t('datasource.failedCandidates.empty')" />
      <div v-else class="failed-candidates__list">
        <button v-for="candidate in candidates" :key="candidate.file_id" type="button"
          class="failed-candidates__item"
          :class="{ 'failed-candidates__item--selected': selectedFileId === candidate.file_id }"
          :aria-pressed="selectedFileId === candidate.file_id"
          :disabled="saving"
          @click="selectCandidate(candidate.file_id)">
          <span class="failed-candidates__file">{{ t('datasource.failedCandidates.fileId', { id: candidate.file_id }) }}</span>
          <span>{{ state(candidate.state) }} · {{ reason(candidate.last_error_code) }}</span>
        </button>
        <t-button v-if="nextCursor" block variant="outline" :loading="loadingMore" :disabled="saving"
          @click="loadPage(false)">{{ t('datasource.failedCandidates.more') }}</t-button>
      </div>
      <div v-if="selectedFileId" class="failed-candidates__detail">
        <t-loading v-if="loadingStatus" size="small" />
        <template v-else-if="exactStatus">
          <h3>{{ t('datasource.failedCandidates.fileId', { id: exactStatus.file_id }) }}</h3>
          <p>{{ state(exactStatus.state) }} · {{ reason(exactStatus.last_error_code) }}</p>
          <p>{{ t('datasource.failedCandidates.attempts', { count: exactStatus.attempt_count }) }}</p>
          <p v-if="time(exactStatus.next_attempt_at)">{{ t('datasource.failedCandidates.nextAttempt', { time: time(exactStatus.next_attempt_at) }) }}</p>
          <t-button theme="primary" :loading="saving" @click="retrySelected">
            {{ t('datasource.failedCandidates.retry') }}
          </t-button>
        </template>
      </div>
    </div>
  </t-drawer>
</template>

<style scoped>
.failed-candidates { display: grid; gap: 12px; }
.failed-candidates__hint { margin: 0; color: var(--td-text-color-secondary); line-height: 1.5; }
.failed-candidates__toolbar { display: flex; justify-content: flex-end; }
.failed-candidates__error { color: var(--td-error-color); }
.failed-candidates__list { display: grid; gap: 8px; }
.failed-candidates__item { display: grid; gap: 4px; width: 100%; padding: 10px; border: 1px solid var(--td-component-stroke); border-radius: 6px; background: var(--td-bg-color-container); text-align: left; color: var(--td-text-color-secondary); cursor: pointer; }
.failed-candidates__item--selected { border-color: var(--td-brand-color); }
.failed-candidates__item:focus-visible { outline: 2px solid var(--td-brand-color); outline-offset: 2px; }
.failed-candidates__file { color: var(--td-text-color-primary); font-weight: 600; }
.failed-candidates__detail { padding: 12px; border: 1px solid var(--td-component-stroke); border-radius: 6px; }
.failed-candidates__detail h3 { margin: 0 0 8px; }
.failed-candidates__detail p { margin: 0 0 8px; }
</style>
