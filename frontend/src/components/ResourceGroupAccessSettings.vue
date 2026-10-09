<template>
  <div class="group-access">
    <div class="group-access__header">
      <div>
        <h3>{{ $t('groupAccess.title') }}</h3>
        <p>{{ $t('groupAccess.description') }}</p>
      </div>
      <div class="header-actions">
        <t-button variant="outline" :loading="previewing" :disabled="loading || saving || !!error" @click="previewCurrentAccess">
          {{ $t('groupAccess.previewEffective') }}
        </t-button>
        <t-button variant="outline" :loading="loading" :disabled="saving || previewing" @click="loadAccess">
          <template #icon><t-icon name="refresh" /></template>
          {{ $t('common.refresh') }}
        </t-button>
      </div>
    </div>

    <t-alert v-if="error" theme="error" :message="error" />
    <t-loading v-if="loading" />
    <template v-else-if="!error">
      <div class="mode-cards">
        <button type="button" :class="['mode-card', { active: mode === 'inherit' }]" :disabled="readOnly || saving || previewing"
          @click="requestModeChange('inherit')">
          <t-icon name="usergroup" />
          <span><strong>{{ $t('groupAccess.inherit') }}</strong><small>{{ $t('groupAccess.inheritHint') }}</small></span>
        </button>
        <button type="button" :class="['mode-card', { active: mode === 'restricted' }]" :disabled="readOnly || saving || previewing"
          @click="requestModeChange('restricted')">
          <t-icon name="lock-on" />
          <span><strong>{{ $t('groupAccess.restricted') }}</strong><small>{{ $t('groupAccess.restrictedHint') }}</small></span>
        </button>
      </div>
      <t-alert v-if="mode === 'restricted'" theme="warning" :message="$t('groupAccess.managerBypass')" />

      <div class="grant-panel">
        <t-alert v-if="mode === 'inherit'" theme="info" :message="$t('groupAccess.inheritGroupsHint')" />
        <div class="grant-panel__toolbar">
          <h4>{{ $t('groupAccess.groupsTitle') }}</h4>
          <div v-if="!readOnly" class="add-group">
            <t-select v-model="candidateKey" :placeholder="$t('groupAccess.selectGroup')" :disabled="saving || previewing" filterable clearable>
              <t-option v-for="candidate in unselectedCandidates" :key="groupKey(candidate)" :value="groupKey(candidate)"
                :label="candidate.display_name" />
            </t-select>
            <t-button variant="outline" :disabled="!candidateKey || saving || previewing" @click="addCandidate">{{ $t('groupAccess.addGroup') }}</t-button>
            <t-button variant="text" :disabled="saving || previewing" @click="openGroupCatalog">{{ $t('groupAccess.searchAllGroups') }}</t-button>
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
            <t-select v-model="grant.permission" class="permission-select" size="small" :disabled="readOnly || saving || previewing"
              @change="saveCurrent">
              <t-option v-for="permission in permissions" :key="permission" :value="permission"
                :label="$t(`groupAccess.permissions.${permission}`)" />
            </t-select>
            <t-button v-if="!readOnly" :disabled="saving || previewing" theme="danger" variant="text" size="small" @click="removeGrant(index)">
              {{ $t('common.remove') }}
            </t-button>
          </article>
        </div>
      </div>
    </template>

    <t-dialog v-model:visible="impactVisible" :header="$t('groupAccess.previewTitle')" width="min(1080px, 96vw)"
      :confirm-btn="pendingModeChange ? { content: $t('groupAccess.confirmRestricted'), loading: saving, disabled: previewing } : null"
      :cancel-btn="pendingModeChange ? $t('common.cancel') : $t('common.close')"
      @confirm="confirmRestricted">
      <p>{{ $t(pendingModeChange ? 'groupAccess.previewBody' : 'groupAccess.previewEffectiveHint') }}</p>
      <div v-if="impact" class="impact-grid">
        <div><strong>{{ impact.currently_allowed }}</strong><span>{{ $t('groupAccess.currentlyAllowed') }}</span></div>
        <div><strong>{{ impact.allowed_after }}</strong><span>{{ $t('groupAccess.allowedAfter') }}</span></div>
        <div><strong>{{ impact.losing_access }}</strong><span>{{ $t('groupAccess.losing') }}</span></div>
        <div><strong>{{ impact.gaining_access }}</strong><span>{{ $t('groupAccess.gaining') }}</span></div>
        <div><strong>{{ impact.unaffected_managers }}</strong><span>{{ $t('groupAccess.unaffected') }}</span></div>
      </div>
      <t-alert v-for="warning in impact?.warnings || []" :key="warning" theme="warning" :message="warning" />
      <template v-if="impact?.effective_users">
        <h4>{{ $t('groupAccess.effectiveUsers') }}</h4>
        <t-table row-key="user_id" :data="impact.effective_users" :columns="effectiveUserColumns" :loading="previewing" size="small">
          <template #workspace_role="{ row }">
            {{ row.workspace_role === 'owner' ? $t('groupAccess.owner') : $t(`directoryGroups.roles.${row.workspace_role}`) }}
          </template>
          <template #currently_allowed="{ row }">{{ $t(row.currently_allowed ? 'groupAccess.allowed' : 'groupAccess.denied') }}</template>
          <template #permission_after="{ row }">
            <t-tag :theme="row.allowed_after ? 'success' : 'danger'" size="small">
              {{ row.allowed_after && ['owner', 'admin'].includes(row.workspace_role)
                ? $t('groupAccess.managementPermission') : $t(`groupAccess.permissions.${row.permission_after}`) }}
            </t-tag>
          </template>
          <template #group_matches="{ row }">
            <div v-for="match in row.group_matches" :key="`${match.directory_id}:${match.directory_group_id}:${match.membership_source}`" class="effective-match">
              <strong>{{ match.group_display_name || match.directory_group_id }}</strong>
              <t-tag size="small" variant="light">{{ $t(`groupAccess.sources.${match.membership_source}`) }}</t-tag>
              <span v-if="match.membership_depth > 0">{{ $t('directoryAdmin.browse.depth', { count: match.membership_depth }) }}</span>
              <span>{{ $t(`groupAccess.permissions.${match.permission}`) }}</span>
            </div>
            <span v-if="!row.group_matches?.length">—</span>
          </template>
        </t-table>
        <t-pagination :current="impactPage" :page-size="impactPageSize" :total="impact.effective_users_total || 0"
          :page-size-options="[20, 50, 100]" :disabled="previewing" @change="changeImpactPage" />
      </template>
    </t-dialog>

    <t-dialog v-model:visible="catalogVisible" :header="$t('groupAccess.searchAllGroups')" width="min(900px, 96vw)"
      :confirm-btn="null" :cancel-btn="$t('common.close')">
      <div class="catalog-search">
        <t-input v-model="catalogQuery" clearable :placeholder="$t('directoryGroups.searchPlaceholder')" @enter="searchGroupCatalog" />
        <t-button :loading="catalogLoading" @click="searchGroupCatalog">{{ $t('directoryAdmin.browse.searchButton') }}</t-button>
      </div>
      <t-alert v-if="catalogError" theme="error" :message="catalogError" />
      <t-alert v-if="catalog && (!catalog.enabled || !catalog.fresh)" theme="warning"
        :message="$t(catalog.enabled ? 'directoryAdmin.catalog.stale' : 'directoryAdmin.catalog.disabled')" />
      <t-table row-key="directory_group_id" :data="catalog?.items || []" :columns="catalogColumns" :loading="catalogLoading" size="small">
        <template #select="{ row }">
          <t-button variant="text" theme="primary" :loading="saving"
            :disabled="readOnly || catalogLoading || !!catalogError || !catalog?.enabled || !catalog.fresh || !row.directory_group_id || row.disabled || catalogGroupSelected(row)"
            @click="addCatalogGroup(row)">
            {{ $t(catalogGroupSelected(row) ? 'groupAccess.alreadyGranted' : 'groupAccess.addGroup') }}
          </t-button>
        </template>
      </t-table>
      <t-pagination :current="catalogPage" :page-size="catalogPageSize" :total="catalog?.total || 0"
        :page-size-options="[20, 50, 100]" :disabled="catalogLoading || saving" @change="changeCatalogPage" />
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
import { getTenantDirectoryCatalog, type DirectoryCandidate, type DirectoryCatalog } from '@/api/tenant/directory'

const props = defineProps<{
  resourceType: GroupAccessResourceType
  resourceId: string
  tenantId: number
  readOnly?: boolean
}>()
const { t } = useI18n()
const loading = ref(false)
const saving = ref(false)
const previewing = ref(false)
const error = ref('')
const mode = ref<GroupAccessMode>('inherit')
const grants = ref<ResourceGroupGrant[]>([])
const candidates = ref<ResourceGroupCandidate[]>([])
const candidateKey = ref('')
const impact = ref<ResourceGroupAccessImpact | null>(null)
const impactVisible = ref(false)
const pendingModeChange = ref(false)
const impactPage = ref(1)
const impactPageSize = ref(20)
const catalogVisible = ref(false)
const catalogLoading = ref(false)
const catalogError = ref('')
const catalogQuery = ref('')
const catalogAppliedQuery = ref('')
const catalogPage = ref(1)
const catalogPageSize = ref(20)
const catalog = ref<DirectoryCatalog | null>(null)
let catalogRequest = 0
let generation = 0
let loadGeneration = 0
let previewPayload: ReturnType<typeof buildResourceGroupAccessUpdate> | null = null
const permissions = computed(() => permissionsForResource(props.resourceType))
const effectiveUserColumns = computed(() => [
  { colKey: 'user_id', title: t('groupAccess.userId'), minWidth: 150 },
  { colKey: 'workspace_role', title: t('directoryGroups.role'), width: 110 },
  { colKey: 'currently_allowed', title: t('groupAccess.currentAccess'), width: 95 },
  { colKey: 'permission_after', title: t('groupAccess.effectivePermission'), width: 130 },
  { colKey: 'group_matches', title: t('groupAccess.matchedGroups'), minWidth: 280 },
])
const catalogColumns = computed(() => [
  { colKey: 'display_name', title: t('directoryGroups.group'), minWidth: 140 },
  { colKey: 'dn', title: 'DN', minWidth: 300 },
  { colKey: 'select', title: t('groupAccess.addGroup'), width: 130 },
])
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
  impactVisible.value = false
  pendingModeChange.value = false
  previewPayload = null
  const resourceGeneration = generation
  const request = ++loadGeneration
  const resourceType = props.resourceType
  const resourceId = props.resourceId
  loading.value = true
  error.value = ''
  try {
    const response = await getResourceGroupAccess(resourceType, resourceId)
    if (resourceGeneration !== generation || request !== loadGeneration) return
    mode.value = response.mode
    grants.value = response.grants ?? []
    candidates.value = response.available_groups ?? []
  } catch (cause: any) {
    if (resourceGeneration !== generation || request !== loadGeneration) return
    error.value = cause?.message || t('groupAccess.loadFailed')
  } finally {
    if (resourceGeneration === generation && request === loadGeneration) loading.value = false
  }
}

async function persist(nextMode = mode.value, payload = buildResourceGroupAccessUpdate(nextMode, grants.value)) {
  if (props.readOnly || saving.value) return false
  const resourceGeneration = generation
  const resourceType = props.resourceType
  const resourceId = props.resourceId
  saving.value = true
  try {
    const response = await updateResourceGroupAccess(
      resourceType,
      resourceId,
      payload,
    )
    if (resourceGeneration !== generation) return false
    mode.value = response.mode
    grants.value = response.grants ?? grants.value
    candidates.value = response.available_groups ?? candidates.value
    MessagePlugin.success(t('groupAccess.saved'))
    return true
  } catch (cause: any) {
    if (resourceGeneration !== generation) return false
    MessagePlugin.error(cause?.message || t('groupAccess.saveFailed'))
    await loadAccess()
    return false
  } finally {
    if (resourceGeneration === generation) saving.value = false
  }
}

async function requestModeChange(nextMode: GroupAccessMode) {
  if (props.readOnly || saving.value || previewing.value || nextMode === mode.value) return
  if (nextMode === 'inherit') {
    await persist('inherit')
    return
  }
  await openImpactPreview(buildResourceGroupAccessUpdate('restricted', grants.value), true)
}

async function previewCurrentAccess() {
  if (loading.value || saving.value || previewing.value || error.value) return
  await openImpactPreview(buildResourceGroupAccessUpdate(mode.value, grants.value), false)
}

async function openImpactPreview(payload: ReturnType<typeof buildResourceGroupAccessUpdate>, changesMode: boolean) {
  const resourceGeneration = generation
  const resourceType = props.resourceType
  const resourceId = props.resourceId
  previewing.value = true
  try {
    // Restricted mode is never committed until the server-calculated impact is shown and confirmed.
    const response = await previewResourceGroupAccess(resourceType, resourceId, payload, impactPageSize.value, 0)
    if (resourceGeneration !== generation) return
    impact.value = response
    previewPayload = payload
    pendingModeChange.value = changesMode
    impactPage.value = 1
    impactVisible.value = true
  } catch (cause: any) {
    if (resourceGeneration !== generation) return
    MessagePlugin.error(cause?.message || t('groupAccess.previewFailed'))
  } finally {
    if (resourceGeneration === generation) previewing.value = false
  }
}

async function changeImpactPage(info: { current: number; pageSize: number }) {
  if (!previewPayload || previewing.value || !impactVisible.value) return
  const resourceGeneration = generation
  previewing.value = true
  try {
    const response = await previewResourceGroupAccess(props.resourceType, props.resourceId, previewPayload,
      info.pageSize, (info.current - 1) * info.pageSize)
    if (resourceGeneration === generation) {
      impact.value = response
      impactPage.value = info.current
      impactPageSize.value = info.pageSize
    }
  } catch (cause: any) {
    if (resourceGeneration === generation) MessagePlugin.error(cause?.message || t('groupAccess.previewFailed'))
  } finally {
    if (resourceGeneration === generation) previewing.value = false
  }
}

function catalogGroupSelected(group: DirectoryCandidate) {
  return grants.value.some((grant) => grant.directory_id === group.directory_id && grant.directory_group_id === group.directory_group_id)
}

function openGroupCatalog() {
  if (props.readOnly || saving.value || previewing.value) return
  catalogQuery.value = ''
  catalogAppliedQuery.value = ''
  catalog.value = null
  catalogPage.value = 1
  catalogVisible.value = true
  void loadGroupCatalog(1, catalogPageSize.value)
}

function searchGroupCatalog() {
  catalogAppliedQuery.value = catalogQuery.value.trim()
  void loadGroupCatalog(1, catalogPageSize.value)
}

async function loadGroupCatalog(page: number, pageSize: number) {
  const resourceGeneration = generation
  const request = ++catalogRequest
  catalogLoading.value = true
  catalogError.value = ''
  try {
    const response = await getTenantDirectoryCatalog(props.tenantId, 'groups', catalogAppliedQuery.value, pageSize, (page - 1) * pageSize)
    if (resourceGeneration !== generation || request !== catalogRequest) return
    catalog.value = response
    catalogPage.value = page
    catalogPageSize.value = pageSize
  } catch (cause: any) {
    if (resourceGeneration === generation && request === catalogRequest) catalogError.value = cause?.message || t('directoryGroups.searchFailed')
  } finally {
    if (resourceGeneration === generation && request === catalogRequest) catalogLoading.value = false
  }
}

function changeCatalogPage(info: { current: number; pageSize: number }) {
  if (!catalogLoading.value && !saving.value) void loadGroupCatalog(info.current, info.pageSize)
}

async function addCatalogGroup(group: DirectoryCandidate) {
  if (props.readOnly || saving.value || previewing.value || catalogLoading.value || catalogError.value || !catalog.value?.enabled || !catalog.value.fresh ||
    !group.directory_group_id || group.disabled || catalogGroupSelected(group)) return
  const resourceGeneration = generation
  grants.value.push({
    directory_id: group.directory_id, directory_group_id: group.directory_group_id,
    display_name: group.display_name, dn: group.dn,
    permission: defaultPermissionForResource(props.resourceType), membership_sources: [],
  })
  if (await persist() && resourceGeneration === generation) catalogVisible.value = false
}

async function confirmRestricted() {
  if (!previewPayload || !impactVisible.value || !pendingModeChange.value || previewing.value || props.readOnly) return
  const resourceGeneration = generation
  if (await persist('restricted', previewPayload) && resourceGeneration === generation) {
    impactVisible.value = false
    previewPayload = null
  }
}

function addCandidate() {
  if (props.readOnly || saving.value || previewing.value) return
  const candidate = unselectedCandidates.value.find((item) => groupKey(item) === candidateKey.value)
  if (!candidate) return
  grants.value.push({ ...candidate, permission: defaultPermissionForResource(props.resourceType), membership_sources: [] })
  candidateKey.value = ''
  void saveCurrent()
}

function removeGrant(index: number) {
  if (props.readOnly || saving.value || previewing.value) return
  grants.value.splice(index, 1)
  void saveCurrent()
}

async function saveCurrent() {
  if (!props.readOnly) await persist()
}

watch(() => [props.resourceType, props.resourceId, props.tenantId], () => {
  generation++
  loadGeneration++
  saving.value = false
  previewing.value = false
  impactVisible.value = false
  pendingModeChange.value = false
  impact.value = null
  previewPayload = null
  mode.value = 'inherit'
  grants.value = []
  candidates.value = []
  candidateKey.value = ''
  catalogVisible.value = false
  catalogLoading.value = false
  catalog.value = null
  catalogRequest++
  void loadAccess()
}, { immediate: true })
</script>

<style scoped>
.group-access { display: flex; flex-direction: column; gap: 16px; }
.group-access__header, .header-actions, .grant-panel__toolbar, .add-group, .grant-row, .grant-meta { display: flex; align-items: center; gap: 12px; }
.group-access__header, .grant-panel__toolbar { justify-content: space-between; }
.group-access__header h3, .grant-panel__toolbar h4 { margin: 0 0 5px; }
.group-access__header p { margin: 0; color: var(--td-text-color-secondary); }
.mode-cards { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
.mode-card { display: flex; gap: 12px; padding: 16px; border: 1px solid var(--td-component-border); border-radius: 8px; background: transparent; text-align: left; cursor: pointer; }
.mode-card.active { border-color: var(--td-brand-color); background: var(--td-brand-color-light); }
.mode-card span { display: flex; flex-direction: column; gap: 4px; }
.mode-card small, .grant-dn, .grant-meta { color: var(--td-text-color-secondary); }
.grant-panel { display: flex; flex-direction: column; gap: 14px; }
.add-group { width: min(600px, 75%); flex-wrap: wrap; }
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
.effective-match { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; margin: 5px 0; }
.effective-match span { color: var(--td-text-color-secondary); }
.catalog-search { display: flex; gap: 10px; margin-bottom: 14px; }
:deep(.t-pagination) { margin-top: 14px; }
@media (max-width: 760px) { .mode-cards { grid-template-columns: 1fr; } .grant-row { flex-wrap: wrap; } .add-group { width: 100%; } }
</style>
