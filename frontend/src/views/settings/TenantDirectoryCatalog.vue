<template>
  <section :class="['catalog', { 'catalog--compact': compact }]">
    <h3 v-if="!compact">{{ t('directoryAdmin.catalog.title') }}</h3>
    <p v-if="!compact">{{ t('directoryAdmin.catalog.hint') }}</p>
    <t-alert v-if="error" theme="error" :message="error" />
    <t-alert v-if="data && !data.enabled" theme="info" :message="t('directoryAdmin.catalog.disabled')" />
    <template v-if="data?.enabled">
      <p>{{ t('directoryAdmin.catalog.sync', { minutes: Math.round(data.sync_interval_seconds / 60), time: syncTime }) }}</p>
      <t-alert v-if="!data.fresh" theme="warning" :message="t('directoryAdmin.catalog.stale')" />
      <div class="toolbar">
        <t-radio-group v-model="kind" @change="reset"><t-radio-button value="users">{{ t('directoryAdmin.diagnostics.users') }}</t-radio-button><t-radio-button value="groups">{{ t('directoryAdmin.diagnostics.groups') }}</t-radio-button></t-radio-group>
        <t-input v-model="query" clearable :placeholder="t('directoryAdmin.browse.search')" @enter="reset" @clear="clear" />
        <t-button :loading="loading" @click="reset">{{ t('directoryAdmin.browse.searchButton') }}</t-button>
      </div>
      <t-table row-key="object_guid" :columns="columns" :data="data.items" :loading="loading" :size="compact ? 'small' : 'medium'">
        <template #email="{ row }">{{ row.email || '—' }}</template>
        <template #status="{ row }">{{ row.disabled ? t('directoryAdmin.diagnostics.disabled') : t('directoryAdmin.catalog.synced') }}</template>
        <template #action="{ row }">
          <div class="catalog-actions">
            <t-button v-if="kind === 'groups'" variant="text" @click="preview(row)">{{ t('directoryAdmin.browse.details') }}</t-button>
            <t-button variant="text" theme="primary" :disabled="row.disabled || !data.fresh || (kind === 'users' && !canAddUser) || isLinked(row)" @click="select(row)">
              {{ isLinked(row) ? t('directoryAdmin.catalog.linked') : t('directoryAdmin.catalog.select') }}
            </t-button>
          </div>
        </template>
      </t-table>
      <t-pagination v-model="page" v-model:page-size="pageSize" :total="data.total" :page-size-options="compact ? [5,20,50,100] : [20,50,100]" :size="compact ? 'small' : 'medium'" @change="paginate" />
    </template>
    <t-dialog v-model:visible="confirmVisible" :header="t('directoryAdmin.catalog.select')" :confirm-btn="{ content: t('common.confirm'), loading: saving }" :cancel-btn="t('common.cancel')" @confirm="add">
      <p>{{ selected?.display_name }} · {{ selected?.account_name }}</p>
      <p>{{ kind === 'groups' ? t('directoryAdmin.catalog.groupHint') : t('directoryAdmin.catalog.userHint') }}</p>
      <t-button v-if="kind === 'groups' && selected" variant="text" @click="preview(selected)">{{ t('directoryAdmin.browse.details') }}</t-button>
      <t-select v-model="role" :aria-label="t('directoryGroups.role')">
        <t-option v-for="value in roles" :key="value" :value="value" :label="t(`directoryGroups.roles.${value}`)" />
      </t-select>
    </t-dialog>
    <t-drawer v-model:visible="previewVisible" :header="previewTitle" size="min(1100px, 96vw)" :footer="false" :close-btn="true">
      <t-alert v-if="previewError" theme="error" :message="previewError" />
      <template v-if="previewData">
        <p>{{ previewData.group.dn }}</p>
        <div class="relations">
          <strong>{{ t('directoryAdmin.browse.parents') }}</strong>
          <span v-if="!previewData.parent_groups.length">—</span>
          <t-button v-for="group in previewData.parent_groups" :key="group.object_guid" variant="text" @click="preview(group)">{{ group.display_name || group.account_name }}</t-button>
        </div>
        <div class="relations">
          <strong>{{ t('directoryAdmin.browse.children') }}</strong>
          <span v-if="!previewData.child_groups.length">—</span>
          <t-button v-for="group in previewData.child_groups" :key="group.object_guid" variant="text" @click="preview(group)">{{ group.display_name || group.account_name }}</t-button>
        </div>
      </template>
      <p>{{ t('directoryAdmin.browse.originHint') }}</p>
      <div class="toolbar">
        <t-input v-model="memberQuery" clearable :placeholder="t('directoryAdmin.browse.memberSearch')" @enter="resetMembers" @clear="clearMembers" />
        <t-button :loading="previewLoading" @click="resetMembers">{{ t('directoryAdmin.browse.searchButton') }}</t-button>
      </div>
      <p>{{ t('directoryAdmin.browse.total', { count: previewData?.total || 0 }) }}</p>
      <t-table row-key="object_guid" :columns="memberColumns" :data="previewData?.items || []" :loading="previewLoading" :empty="t('directoryAdmin.diagnostics.empty')">
        <template #email="{ row }">{{ row.email || '—' }}</template>
        <template #status="{ row }">{{ row.disabled ? t('directoryAdmin.diagnostics.disabled') : t('directoryAdmin.browse.enabled') }}</template>
        <template #origins="{ row }">
          <div v-for="(origin, index) in row.origins" :key="index" class="origin">
            <t-tag size="small">{{ t(`groupAccess.sources.${origin.source}`) }}</t-tag>
            <span v-if="origin.source === 'nested'">{{ t(`groupAccess.sources.${origin.origin_source}`) }} · {{ t('directoryAdmin.browse.depth', { count: origin.depth }) }}</span>
            <span>{{ origin.path.map((group: DirectoryObjectSummary) => group.display_name || group.account_name).join(' → ') }}</span>
          </div>
        </template>
      </t-table>
      <t-pagination v-model="memberPage" v-model:page-size="memberPageSize" :total="previewData?.total || 0" :page-size-options="[20,50,100]" @change="paginateMembers" />
    </t-drawer>
  </section>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { MessagePlugin } from 'tdesign-vue-next'
import { getTenantDirectoryCatalog, getTenantDirectoryGroupMembers, addTenantDirectoryMember, type DirectoryCandidate, type DirectoryCatalog } from '@/api/tenant/directory'
import type { DirectoryGroupMembersResult, DirectoryObjectSummary } from '@/api/directory'
import { addTenantDirectoryGroup, listTenantDirectoryGroups, type DirectoryTenantRole } from '@/api/tenant/groups'
const props = defineProps<{ tenantId: number; canAddUser: boolean; compact?: boolean }>()
const emit = defineEmits<{ changed: []; available: [enabled: boolean] }>()
const { t } = useI18n()
const kind = ref<'users'|'groups'>('users')
const query = ref(''), applied = ref(''), error = ref('')
const page = ref(1), pageSize = ref(props.compact ? 5 : 20)
const data = ref<DirectoryCatalog | null>(null)
const loading = ref(false), saving = ref(false), confirmVisible = ref(false)
const selected = ref<DirectoryCandidate | null>(null)
const selectedTenant = ref(0)
const role = ref<DirectoryTenantRole>('viewer')
const roles: DirectoryTenantRole[] = ['viewer','contributor','admin']
const previewVisible = ref(false), previewLoading = ref(false), previewError = ref('')
const previewGroup = ref<DirectoryObjectSummary | null>(null)
const previewData = ref<DirectoryGroupMembersResult | null>(null)
const memberQuery = ref(''), appliedMemberQuery = ref('')
const memberPage = ref(1), memberPageSize = ref(20)
let previewGeneration = 0
const previewTitle = computed(() => `${t('directoryAdmin.browse.details')} · ${previewGroup.value?.display_name || previewGroup.value?.account_name || ''}`)
const memberColumns = computed(() => [
  {colKey:'display_name', title:t('directoryAdmin.browse.name'), minWidth:140},
  {colKey:'account_name', title:t('directoryAdmin.browse.account'), minWidth:140},
  {colKey:'email', title:t('directoryAdmin.browse.email'), minWidth:160},
  {colKey:'status', title:t('directoryAdmin.browse.status'), width:90},
  {colKey:'origins', title:t('directoryAdmin.browse.origins'), minWidth:300},
])
const linkedGroups = ref<string[]>([])
let generation = 0
const syncTime = computed(() => data.value?.last_success_at ? new Date(data.value.last_success_at).toLocaleString() : '—')
const columns = computed(() => [
  {colKey:'display_name', title:t(`directoryAdmin.browse.${kind.value === 'users' ? 'name' : 'groupName'}`), minWidth:140},
  {colKey:'account_name', title:t('directoryAdmin.browse.account'), minWidth:140},
  {colKey:'email', title:t('directoryAdmin.browse.email'), minWidth:140},
  {colKey:'status', title:t('directoryAdmin.browse.status'), width:90},
  {colKey:'action', title:t('directoryAdmin.catalog.select'), width:240},
])
function isLinked(row: DirectoryCandidate) { return kind.value === 'groups' && linkedGroups.value.includes(row.directory_group_id || '') }
async function load() {
  const request = ++generation
  if (!props.tenantId) return
  loading.value = true; error.value = ''
  try {
    const [catalog, linked] = await Promise.all([
      getTenantDirectoryCatalog(props.tenantId, kind.value, applied.value, pageSize.value, (page.value-1)*pageSize.value),
      listTenantDirectoryGroups(props.tenantId),
    ])
    if (request !== generation) return
    data.value = catalog; linkedGroups.value = linked.groups.map(group => group.directory_group_id)
    emit('available', catalog.enabled)
  } catch (e: any) { if (request === generation) { error.value = e?.message || t('directoryAdmin.diagnostics.searchFailed'); data.value = null } }
  finally { if (request === generation) loading.value = false }
}
function reset() { page.value=1; applied.value=query.value.trim(); confirmVisible.value=false; void load() }
function clear() { query.value=''; reset() }
function paginate(info: { current: number; pageSize: number }) { page.value=info.current; pageSize.value=info.pageSize; void load() }
function select(row: DirectoryCandidate) { selected.value=row; selectedTenant.value=props.tenantId; role.value='viewer'; confirmVisible.value=true }
function preview(group: DirectoryObjectSummary) {
  previewGroup.value=group; previewData.value=null; previewError.value=''; memberQuery.value=''; appliedMemberQuery.value=''
  memberPage.value=1; previewVisible.value=true; void loadMembers()
}
async function loadMembers() {
  if (!previewGroup.value) return
  const request=++previewGeneration, tenant=props.tenantId, guid=previewGroup.value.object_guid
  previewLoading.value=true; previewError.value=''
  try {
    const result=await getTenantDirectoryGroupMembers(tenant,guid,appliedMemberQuery.value,memberPageSize.value,(memberPage.value-1)*memberPageSize.value)
    if (request===previewGeneration && tenant===props.tenantId) previewData.value=result
  } catch(e:any) {
    if (request===previewGeneration) { previewData.value=null; previewError.value=e?.message || t('directoryAdmin.diagnostics.searchFailed') }
  } finally { if (request===previewGeneration) previewLoading.value=false }
}
function resetMembers() { appliedMemberQuery.value=memberQuery.value.trim(); memberPage.value=1; void loadMembers() }
function clearMembers() { memberQuery.value=''; resetMembers() }
function paginateMembers(info:{current:number;pageSize:number}) { memberPage.value=info.current; memberPageSize.value=info.pageSize; void loadMembers() }
async function add() {
  if (!selected.value || selectedTenant.value !== props.tenantId) return
  saving.value=true
  try {
    if (kind.value === 'users') await addTenantDirectoryMember(props.tenantId, selected.value.object_guid, role.value)
    else await addTenantDirectoryGroup(props.tenantId, { directory_id:selected.value.directory_id, directory_group_id:selected.value.directory_group_id!, role:role.value })
    confirmVisible.value=false; emit('changed'); await load(); MessagePlugin.success(t('directoryAdmin.catalog.added'))
  } catch(e: any) { MessagePlugin.error(e?.message || t('directoryGroups.addFailed')) }
  finally { saving.value=false }
}
watch(() => props.tenantId, () => { previewGeneration++; previewVisible.value=false; previewData.value=null; emit('available', false); data.value=null; query.value=''; reset() }, {immediate:true})
</script>

<style scoped>
.catalog { margin: 24px 0; padding-top: 24px; border-top: 1px solid var(--td-component-border); }
.catalog--compact { margin: 0; padding: 0; border: 0; }
.catalog p { color: var(--td-text-color-secondary); margin: 10px 0; }
.catalog--compact p { margin: 8px 0; }
.toolbar { display:flex; align-items:center; gap:12px; margin:16px 0; flex-wrap:wrap; }
.catalog--compact .toolbar { margin: 12px 0; }
.catalog-actions { display:flex; align-items:center; gap:4px; white-space:nowrap; }
.catalog-actions :deep(.t-button) { flex:none; }
.toolbar :deep(.t-input__wrap) { flex:1; min-width:200px; }
:deep(.t-pagination) { margin-top:16px; }
.relations, .origin { display:flex; flex-wrap:wrap; align-items:center; gap:8px; margin:8px 0; }
</style>
