<script setup lang="ts">
import { ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { get } from '@/utils/request'

const props = defineProps<{ sourceId: string; scopeChanged: boolean; scheduleChanged: boolean }>()
// This model carries the editor's explicit request, independently of the saved grant.
const requested = defineModel<boolean>({ required: true })
const { t } = useI18n()
type Status = 'authorized' | 'required' | 'revoked'
const status = ref<Status | 'loading' | 'unavailable'>('loading')
const reload = ref(0)

watch([() => props.sourceId, reload], async ([id], _, onCleanup) => {
  let current = true
  onCleanup(() => { current = false })
  status.value = 'loading'
  try {
    const result = await get<{ status: Status }>(
      `/api/v1/group-access/data-sources/${encodeURIComponent(id)}/schedule-authorization`,
    )
    if (current) {
      const saved = result?.status
      status.value = saved === 'authorized' || saved === 'required' || saved === 'revoked' ? saved : 'unavailable'
      if (status.value === 'unavailable' || (status.value === 'authorized' && !props.scopeChanged)) {
        requested.value = false
      }
    }
  } catch {
    if (current) {
      status.value = 'unavailable'
      requested.value = false
    }
  }
}, { immediate: true })
</script>

<template>
  <div aria-live="polite">
    <t-checkbox v-if="scheduleChanged" :checked="true" disabled>
      {{ t('datasource.dingTalkGrantOnSave') }}
    </t-checkbox>
    <span v-else-if="status === 'loading'">{{ t('common.loading') }}</span>
    <template v-else-if="status === 'unavailable'">
      {{ t('datasource.dingTalkGrantUnavailable') }}
      <t-button variant="text" size="small" @click="reload++">{{ t('common.retry') }}</t-button>
    </template>
    <t-checkbox v-else-if="status === 'authorized' && !scopeChanged" :checked="true" disabled>
      {{ t('datasource.dingTalkGrantActive') }}
    </t-checkbox>
    <template v-else>
      <span v-if="status === 'revoked'">{{ t('datasource.dingTalkGrantRevoked') }}</span>
      <t-checkbox v-model="requested">{{ t('datasource.authorizeDingTalkSchedule') }}</t-checkbox>
    </template>
  </div>
</template>
