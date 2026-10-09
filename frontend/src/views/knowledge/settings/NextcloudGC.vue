<script setup lang="ts">
import { ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { listNextcloudGC, previewNextcloudGC, collectNextcloudGC, retryNextcloudGC, type NextcloudGCStatus, type NextcloudGCPreview } from '@/api/datasource'
const props = defineProps<{kbId: string}>()
const {t} = useI18n()
const jobs = ref<NextcloudGCStatus[]>([])
const preview = ref<NextcloudGCPreview | null>(null)
const loading = ref(false)
const error = ref(false)
let generation = 0
const size = (n: number) => `${(n / 1024 / 1024).toFixed(2)} MiB`
const date = (s: string) => new Date(s).toLocaleString()
const label = (group: string, key: string) => t(`datasource.gc.${group}.${key}`, t('datasource.gc.unknown'))
async function load() {
  const current = ++generation
  loading.value = true; error.value = false; preview.value = null; jobs.value = []
  try { const result = await listNextcloudGC(props.kbId); if (current === generation) jobs.value = result }
  catch { if (current === generation) error.value = true }
  finally { if (current === generation) loading.value = false }
}
async function inspect(job: NextcloudGCStatus) {
  const current = generation
  loading.value = true; error.value = false; preview.value = null
  try { const result = await previewNextcloudGC(job.id); if (current === generation && result.status.knowledge_base_id === props.kbId) preview.value = result }
  catch { if (current === generation) error.value = true }
  finally { if (current === generation) loading.value = false }
}
async function act(kind: 'collect' | 'retry') {
  if (!preview.value || loading.value) return
  const id = preview.value.status.id, current = generation
  loading.value = true; error.value = false
  try {
    if (kind === 'collect') {
      const result = await collectNextcloudGC(id)
      if (current === generation) {
        preview.value = result
        jobs.value = jobs.value.map(job => job.id === id ? result.status : job)
      }
    } else { await retryNextcloudGC(id); if (current === generation) await load() }
  } catch { if (current === generation) error.value = true }
  finally { if (current === generation) loading.value = false }
}
watch(() => props.kbId, load, {immediate: true})
</script>
<template>
  <section class="gc" :aria-busy="loading">
    <h3>{{ t('datasource.gc.title') }}</h3>
    <p>{{ t('datasource.gc.description') }}</p>
    <p v-if="error" role="alert">{{ t('datasource.gc.unavailable') }}</p>
    <p v-if="!loading && !jobs.length">{{ t('datasource.gc.empty') }}</p>
    <ul class="jobs">
      <li v-for="job in jobs" :key="job.id">
        <strong>{{ label('reason', job.reason) }}</strong> · {{ label('state', job.state) }}
        <p>{{ t('datasource.gc.derivedExpiry') }}: {{ date(job.not_before) }} · {{ t('datasource.gc.originalExpiry') }}: {{ date(job.original_not_before) }}</p>
        <p>{{ t('datasource.gc.estimated') }}: {{ size(job.estimated_bytes) }} · {{ t('datasource.gc.confirmed') }}: {{ size(job.confirmed_released_bytes) }}</p>
        <p v-if="job.last_error_code">{{ t('datasource.gc.failure') }}: {{ job.last_error_code }}</p>
        <t-button variant="outline" :disabled="loading" @click="inspect(job)">{{ t('datasource.gc.preview') }}</t-button>
      </li>
    </ul>
    <div v-if="preview" class="preview" role="region" :aria-label="t('datasource.gc.preview')">
      <h4>{{ t('datasource.gc.preview') }}</h4>
      <p>{{ t('datasource.gc.safety') }}</p>
      <table>
        <thead><tr><th>{{ t('datasource.gc.category') }}</th><th>{{ t('datasource.gc.status') }}</th><th>{{ t('datasource.gc.count') }}</th><th>{{ t('datasource.gc.estimated') }}</th><th>{{ t('datasource.gc.confirmed') }}</th></tr></thead>
        <tbody><tr v-for="item in preview.items" :key="`${item.kind}:${item.state}`"><td>{{ label('kind', item.kind) }}</td><td>{{ label('state', item.state) }}</td><td>{{ item.count }}</td><td>{{ size(item.estimated_bytes) }}</td><td>{{ size(item.confirmed_released_bytes) }}</td></tr></tbody>
      </table>
      <t-button :disabled="loading || !preview.can_request_collection" @click="act('collect')">{{ t('datasource.gc.collect') }}</t-button>
      <t-button variant="outline" :disabled="loading || !['blocked','retry'].includes(preview.status.state)" @click="act('retry')">{{ t('datasource.gc.retry') }}</t-button>
    </div>
    <t-button variant="outline" :disabled="loading" @click="load">{{ t('datasource.gc.refresh') }}</t-button>
  </section>
</template>
<style scoped>
.gc { padding:16px; margin-bottom:16px; border:1px solid var(--td-component-border); border-radius:8px; color:var(--td-text-color-primary); background:var(--td-bg-color-container); }
.jobs { list-style:none; padding:0; }.jobs li { padding:12px 0; border-bottom:1px solid var(--td-component-border); }
.preview { overflow:auto; margin:16px 0; }table { width:100%; text-align:left; border-collapse:collapse; margin-bottom:12px; }th,td { padding:8px; border-bottom:1px solid var(--td-component-border); }button { margin:4px; }
</style>
