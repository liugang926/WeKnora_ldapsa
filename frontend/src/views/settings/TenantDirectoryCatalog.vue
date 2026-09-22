<template>
  <section class="catalog">
    <h3>{{ t('directoryAdmin.catalog.title') }}</h3>
    <p>{{ t('directoryAdmin.catalog.hint') }}</p>
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
      <t-table row-key="object_guid" :columns="columns" :data="data.items" :loading="loading">
        <template #email="{ row }">{{ row.email || '—' }}</template>
        <template #status="{ row }">{{ row.disabled ? t('directoryAdmin.diagnostics.disabled') : t('directoryAdmin.catalog.synced') }}</template>
        <template #action="{ row }">
          <t-button variant="text" theme="primary" :disabled="row.disabled || !data.fresh || (kind === 'users' && !canAddUser) || isLinked(row)" @click="select(row)">
            {{ isLinked(row) ? t('directoryAdmin.catalog.linked') : t('directoryAdmin.catalog.select') }}
          </t-button>
        </template>
      </t-table>
      <t-pagination v-model="page" v-model:page-size="pageSize" :total="data.total" :page-size-options="[20,50,100]" @change="paginate" />
    </template>
    <t-dialog v-model:visible="confirmVisible" :header="t('directoryAdmin.catalog.select')" :confirm-btn="{ content: t('common.confirm'), loading: saving }" :cancel-btn="t('common.cancel')" @confirm="add">
      <p>{{ selected?.display_name }} · {{ selected?.account_name }}</p>
      <p>{{ kind === 'groups' ? t('directoryAdmin.catalog.groupHint') : t('directoryAdmin.catalog.userHint') }}</p>
      <t-select v-model="role" :aria-label="t('directoryGroups.role')">
        <t-option v-for="value in roles" :key="value" :value="value" :label="t(`directoryGroups.roles.${value}`)" />
      </t-select>
    </t-dialog>
  </section>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { MessagePlugin } from 'tdesign-vue-next'
import { getTenantDirectoryCatalog, addTenantDirectoryMember, type DirectoryCandidate, type DirectoryCatalog } from '@/api/tenant/directory'
import { addTenantDirectoryGroup, listTenantDirectoryGroups, type DirectoryTenantRole } from '@/api/tenant/groups'
const props = defineProps<{ tenantId: number; canAddUser: boolean }>()
const emit = defineEmits<{ changed: [] }>()
const { t } = useI18n()
const kind = ref<'users'|'groups'>('users')
const query = ref(''), applied = ref(''), error = ref('')
const page = ref(1), pageSize = ref(20)
const data = ref<DirectoryCatalog | null>(null)
const loading = ref(false), saving = ref(false), confirmVisible = ref(false)
const selected = ref<DirectoryCandidate | null>(null)
const selectedTenant = ref(0)
const role = ref<DirectoryTenantRole>('viewer')
const roles: DirectoryTenantRole[] = ['viewer','contributor','admin']
const linkedGroups = ref<string[]>([])
let generation = 0
const syncTime = computed(() => data.value?.last_success_at ? new Date(data.value.last_success_at).toLocaleString() : '—')
const columns = computed(() => [
  {colKey:'display_name', title:t(`directoryAdmin.browse.${kind.value === 'users' ? 'name' : 'groupName'}`), minWidth:140},
  {colKey:'account_name', title:t('directoryAdmin.browse.account'), minWidth:140},
  {colKey:'email', title:t('directoryAdmin.browse.email'), minWidth:140},
  {colKey:'status', title:t('directoryAdmin.browse.status'), width:90},
  {colKey:'action', title:t('directoryAdmin.catalog.select'), width:145},
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
  } catch (e: any) { if (request === generation) { error.value = e?.message || t('directoryAdmin.diagnostics.searchFailed'); data.value = null } }
  finally { if (request === generation) loading.value = false }
}
function reset() { page.value=1; applied.value=query.value.trim(); confirmVisible.value=false; void load() }
function clear() { query.value=''; reset() }
function paginate(info: { current: number; pageSize: number }) { page.value=info.current; pageSize.value=info.pageSize; void load() }
function select(row: DirectoryCandidate) { selected.value=row; selectedTenant.value=props.tenantId; role.value='viewer'; confirmVisible.value=true }
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
watch(() => props.tenantId, () => { data.value=null; query.value=''; reset() }, {immediate:true})
</script>

<style scoped>
.catalog { margin: 24px 0; padding-top: 24px; border-top: 1px solid var(--td-component-border); }
.catalog p { color: var(--td-text-color-secondary); margin: 10px 0; }
.toolbar { display:flex; align-items:center; gap:12px; margin:16px 0; flex-wrap:wrap; }
.toolbar :deep(.t-input__wrap) { flex:1; min-width:200px; }
:deep(.t-pagination) { margin-top:16px; }
</style>
