<template>
  <div class="group-access">
    <div class="group-access__header">
      <div>
        <h3>{{ $t('groupAccess.title') }}</h3>
        <p>{{ $t('groupAccess.description') }}</p>
      </div>
      <t-button variant="outline" :loading="loading" @click="loadAccess">
        <template #icon><t-icon name="refresh" /></template>
        {{ $t('common.refresh') }}
      </t-button>
    </div>

    <t-alert v-if="error" theme="error" :message="error" />
    <t-loading v-if="loading" />
    <template v-else>
      <div class="mode-cards">
        <button type="button" :class="['mode-card', { active: mode === 'inherit' }]" :disabled="readOnly || saving"
          @click="requestModeChange('inherit')">
          <t-icon name="usergroup" />
          <span><strong>{{ $t('groupAccess.inherit') }}</strong><small>{{ $t('groupAccess.inheritHint') }}</small></span>
        </button>
        <button type="button" :class="['mode-card', { active: mode === 'restricted' }]" :disabled="readOnly || saving"
          @click="requestModeChange('restricted')">
          <t-icon name="lock-on" />
          <span><strong>{{ $t('groupAccess.restricted') }}</strong><small>{{ $t('groupAccess.restrictedHint') }}</small></span>
        </button>
      </div>
      <t-alert v-if="mode === 'restricted'" theme="warning" :message="$t('groupAccess.managerBypass')" />

      <div v-if="mode === 'restricted'" class="grant-panel">
        <div class="grant-panel__toolbar">
          <h4>{{ $t('groupAccess.groupsTitle') }}</h4>
          <div v-if="!readOnly" class="add-group">
            <t-select v-model="candidateKey" :placeholder="$t('groupAccess.selectGroup')" filterable clearable>
              <t-option v-for="candidate in unselectedCandidates" :key="groupKey(candidate)" :value="groupKey(candidate)"
                :label="candidate.display_name" />
            </t-select>
            <t-button variant="outline" :disabled="!candidateKey" @click="addCandidate">{{ $t('groupAccess.addGroup') }}</t-button>
          </div>
        </div>

        <t-empty v-if="grants.length === 0" :description="$t('groupAccess.empty')" />
        <div v-else class="grant-list">
          <article v-for="(grant, index) in grants" :key="groupKey(grant)" class="grant-row">
            <div class="grant-main">
              <strong>{{ grant.display_name }}</strong>
              <span v-if="grant.dn" class="grant-dn">{{ grant.dn }}</span>
              <div class="grant-meta">
                <t-tag v-for="source in normalizedSources(grant)" :key="source" size="small" variant="light"
                  :theme="source === 'nested' ? 'warning' : 'default'">
                  {{ $t(`groupAccess.sources.${source}`) }}
                </t-tag>
                <span>{{ $t('groupAccess.effectiveMembers', { count: grant.effective_member_count ?? 0 }) }}</span>
              </div>
              <t-alert v-if="hasMissingWorkspaceGroup(grant)" class="workspace-warning" theme="warning"
                :message="$t('groupAccess.missingWorkspaceLink')" />
            </div>
            <t-select v-model="grant.permission" class="permission-select" size="small" :disabled="readOnly"
              @change="saveCurrent">
              <t-option v-for="permission in permissions" :key="permission" :value="permission"
                :label="$t(`groupAccess.permissions.${permission}`)" />
            </t-select>
            <t-button v-if="!readOnly" theme="danger" variant="text" size="small" @click="removeGrant(index)">
              {{ $t('common.remove') }}
            </t-button>
          </article>
        </div>
      </div>
    </template>

    <t-dialog v-model:visible="impactVisible" :header="$t('groupAccess.previewTitle')"
      :confirm-btn="{ content: $t('groupAccess.confirmRestricted'), loading: saving }" :cancel-btn="$t('common.cancel')"
      @confirm="confirmRestricted">
      <p>{{ $t('groupAccess.previewBody') }}</p>
      <div v-if="impact" class="impact-grid">
        <div><strong>{{ impact.currently_allowed }}</strong><span>{{ $t('groupAccess.currentlyAllowed') }}</span></div>
        <div><strong>{{ impact.allowed_after }}</strong><span>{{ $t('groupAccess.allowedAfter') }}</span></div>
        <div><strong>{{ impact.losing_access }}</strong><span>{{ $t('groupAccess.losing') }}</span></div>
        <div><strong>{{ impact.gaining_access }}</strong><span>{{ $t('groupAccess.gaining') }}</span></div>
        <div><strong>{{ impact.unaffected_managers }}</strong><span>{{ $t('groupAccess.unaffected') }}</span></div>
      </div>
      <t-alert v-for="warning in impact?.warnings || []" :key="warning" theme="warning" :message="warning" />
    </t-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { MessagePlugin } from 'tdesign-vue-next'
import { useI18n } from 'vue-i18n'
import {
  getResourceGroupAccess,
  previewResourceGroupAccess,
  updateResourceGroupAccess,
  type GroupAccessMode,
  type GroupAccessResourceType,
  type ResourceGroupAccessImpact,
  type ResourceGroupCandidate,
  type ResourceGroupGrant,
} from '@/api/group-access'
import {
  buildResourceGroupAccessUpdate,
  defaultPermissionForResource,
  hasMissingWorkspaceGroup,
  normalizeMembershipSources,
  permissionsForResource,
} from '@/utils/groupAccess'

const props = defineProps<{
  resourceType: GroupAccessResourceType
  resourceId: string
  tenantId: number
  readOnly?: boolean
}>()
const { t } = useI18n()
const loading = ref(false)
const saving = ref(false)
const error = ref('')
const mode = ref<GroupAccessMode>('inherit')
const grants = ref<ResourceGroupGrant[]>([])
const candidates = ref<ResourceGroupCandidate[]>([])
const candidateKey = ref('')
const impact = ref<ResourceGroupAccessImpact | null>(null)
const impactVisible = ref(false)
const permissions = computed(() => permissionsForResource(props.resourceType))
const unselectedCandidates = computed(() => {
  const selected = new Set(grants.value.map(groupKey))
  return candidates.value.filter((candidate) => !selected.has(groupKey(candidate)))
})

function groupKey(group: Pick<ResourceGroupGrant, 'directory_id' | 'directory_group_id'>) {
  return `${group.directory_id}:${group.directory_group_id}`
}

function normalizedSources(grant: ResourceGroupGrant) {
  return normalizeMembershipSources(grant.membership_sources)
}

async function loadAccess() {
  if (!props.resourceId) return
  loading.value = true
  error.value = ''
  try {
    const response = await getResourceGroupAccess(props.resourceType, props.resourceId)
    mode.value = response.mode
    grants.value = response.grants ?? []
    candidates.value = response.available_groups ?? []
  } catch (cause: any) {
    error.value = cause?.message || t('groupAccess.loadFailed')
  } finally {
    loading.value = false
  }
}

async function persist(nextMode = mode.value) {
  saving.value = true
  try {
    const response = await updateResourceGroupAccess(
      props.resourceType,
      props.resourceId,
      buildResourceGroupAccessUpdate(nextMode, grants.value),
    )
    mode.value = response.mode
    grants.value = response.grants ?? grants.value
    candidates.value = response.available_groups ?? candidates.value
    MessagePlugin.success(t('groupAccess.saved'))
  } catch (cause: any) {
    MessagePlugin.error(cause?.message || t('groupAccess.saveFailed'))
    await loadAccess()
  } finally {
    saving.value = false
  }
}

async function requestModeChange(nextMode: GroupAccessMode) {
  if (props.readOnly || nextMode === mode.value) return
  if (nextMode === 'inherit') {
    await persist('inherit')
    return
  }
  try {
    // Restricted mode is never committed until the server-calculated impact is shown and confirmed.
    impact.value = await previewResourceGroupAccess(
      props.resourceType,
      props.resourceId,
      buildResourceGroupAccessUpdate('restricted', grants.value),
    )
    impactVisible.value = true
  } catch (cause: any) {
    MessagePlugin.error(cause?.message || t('groupAccess.previewFailed'))
  }
}

async function confirmRestricted() {
  await persist('restricted')
  impactVisible.value = false
}

function addCandidate() {
  const candidate = unselectedCandidates.value.find((item) => groupKey(item) === candidateKey.value)
  if (!candidate) return
  grants.value.push({ ...candidate, permission: defaultPermissionForResource(props.resourceType), membership_sources: [] })
  candidateKey.value = ''
  void saveCurrent()
}

function removeGrant(index: number) {
  grants.value.splice(index, 1)
  void saveCurrent()
}

async function saveCurrent() {
  if (!props.readOnly) await persist()
}

watch(() => [props.resourceType, props.resourceId], () => void loadAccess(), { immediate: true })
</script>

<style scoped>
.group-access { display: flex; flex-direction: column; gap: 16px; }
.group-access__header, .grant-panel__toolbar, .add-group, .grant-row, .grant-meta { display: flex; align-items: center; gap: 12px; }
.group-access__header, .grant-panel__toolbar { justify-content: space-between; }
.group-access__header h3, .grant-panel__toolbar h4 { margin: 0 0 5px; }
.group-access__header p { margin: 0; color: var(--td-text-color-secondary); }
.mode-cards { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
.mode-card { display: flex; gap: 12px; padding: 16px; border: 1px solid var(--td-component-border); border-radius: 8px; background: transparent; text-align: left; cursor: pointer; }
.mode-card.active { border-color: var(--td-brand-color); background: var(--td-brand-color-light); }
.mode-card span { display: flex; flex-direction: column; gap: 4px; }
.mode-card small, .grant-dn, .grant-meta { color: var(--td-text-color-secondary); }
.grant-panel { display: flex; flex-direction: column; gap: 14px; }
.add-group { width: min(520px, 60%); }
.add-group :deep(.t-select) { flex: 1; }
.grant-list { border: 1px solid var(--td-component-border); border-radius: 8px; overflow: hidden; }
.grant-row { padding: 14px 16px; border-bottom: 1px solid var(--td-component-border); align-items: flex-start; }
.grant-row:last-child { border-bottom: 0; }
.grant-main { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 6px; }
.grant-dn { font-size: var(--app-text-sm); overflow-wrap: anywhere; }
.permission-select { width: 140px; }
.workspace-warning { margin-top: 4px; }
.impact-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin: 16px 0; }
.impact-grid div { display: flex; flex-direction: column; padding: 12px; border-radius: 6px; background: var(--td-bg-color-container-hover); }
.impact-grid strong { font-size: var(--app-text-3xl); }
.impact-grid span { font-size: var(--app-text-sm); color: var(--td-text-color-secondary); }
@media (max-width: 760px) { .mode-cards { grid-template-columns: 1fr; } .grant-row { flex-wrap: wrap; } .add-group { width: 100%; } }
</style>
