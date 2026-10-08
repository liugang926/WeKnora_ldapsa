<script setup lang="ts">
import { ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { getNextcloudCapacity, setNextcloudCapacity, type NextcloudCapacityStatus } from '@/api/datasource'
const props = defineProps<{ kbId: string }>()
const { t } = useI18n()
const status = ref<NextcloudCapacityStatus | null>(null)
const loading = ref(false)
const error = ref(false)
const quotaMiB = ref(0)
const warning = ref(80)
const pause = ref(90)
let generation = 0
function bytes(value: number) { return `${(value / 1024 / 1024).toFixed(1)} MiB` }
async function load() {
  const current = ++generation
  const id = props.kbId
  loading.value = true
  error.value = false
  status.value = null
  try {
    const value = await getNextcloudCapacity(id)
    if (current !== generation || id !== props.kbId) return
    status.value = value
    quotaMiB.value = value.policy.RetentionQuotaBytes / 1024 / 1024
    warning.value = value.policy.WarningPercent
    pause.value = value.policy.PausePercent
  } catch { if (current === generation) error.value = true }
  finally { if (current === generation) loading.value = false }
}
async function save() {
  if (loading.value || quotaMiB.value < 0 || warning.value < 1 || warning.value >= pause.value || pause.value > 100) return
  const id = props.kbId
  loading.value = true
  try {
    await setNextcloudCapacity(id, {retention_quota_bytes: Math.floor(quotaMiB.value * 1024 * 1024), warning_percent: warning.value, pause_percent: pause.value})
    if (id === props.kbId) await load()
  } catch { if (id === props.kbId) error.value = true }
  finally { if (id === props.kbId) loading.value = false }
}
watch(() => props.kbId, load, { immediate: true })
</script>
<template>
  <section class="capacity" :aria-busy="loading">
    <h3>{{ t('datasource.capacity.title') }}</h3>
    <p>{{ t('datasource.capacity.description') }}</p>
    <p v-if="error" role="alert">{{ t('datasource.capacity.unavailable') }}</p>
    <template v-if="status">
      <p role="status">{{ t(`datasource.capacity.${status.state}`) }}</p>
      <dl>
        <dt>{{ t('datasource.capacity.physical') }}</dt><dd>{{ bytes(status.physical_available_bytes) }} / {{ bytes(status.physical_total_bytes) }}</dd>
        <dt>{{ t('datasource.capacity.retained') }}</dt><dd>{{ bytes(status.logical_retained_bytes) }}</dd>
        <dt>{{ t('datasource.capacity.reserved') }}</dt><dd>{{ bytes(status.reserved_bytes) }}</dd>
      </dl>
      <div class="policy">
        <label>{{ t('datasource.capacity.quota') }} <t-input-number v-model="quotaMiB" :min="0" :disabled="loading" /></label>
        <label>{{ t('datasource.capacity.warningLabel') }} <t-input-number v-model="warning" :min="1" :max="99" :disabled="loading" /></label>
        <label>{{ t('datasource.capacity.pause') }} <t-input-number v-model="pause" :min="2" :max="100" :disabled="loading" /></label>
        <t-button :loading="loading" :disabled="warning >= pause || quotaMiB < 0" @click="save">{{ t('datasource.capacity.save') }}</t-button>
      </div>
    </template>
    <t-button variant="outline" :disabled="loading" @click="load">{{ t('datasource.capacity.refresh') }}</t-button>
  </section>
</template>
<style scoped>
.capacity { padding: 16px; margin-bottom: 16px; border: 1px solid var(--td-component-border); border-radius: 8px; color: var(--td-text-color-primary); background: var(--td-bg-color-container); }
dl { display: grid; grid-template-columns: minmax(150px, 1fr) 2fr; gap: 8px; }
dd { margin: 0; }
.policy { display: flex; gap: 12px; flex-wrap: wrap; align-items: end; margin: 16px 0; }
.policy label { display: flex; gap: 6px; flex-direction: column; }
</style>
