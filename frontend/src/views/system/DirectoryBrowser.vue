<template>
  <section class="directory-browser">
    <div class="search-bar">
      <t-radio-group v-model="kind" @change="resetSearch">
        <t-radio-button value="users">{{ t('directoryAdmin.diagnostics.users') }}</t-radio-button>
        <t-radio-button value="groups">{{ t('directoryAdmin.diagnostics.groups') }}</t-radio-button>
      </t-radio-group>
      <t-input v-model="query" clearable :aria-label="t('directoryAdmin.browse.search')"
        :placeholder="t('directoryAdmin.browse.search')" @enter="resetSearch" @clear="clearSearch" />
      <t-button :loading="loading" @click="resetSearch">{{ t('directoryAdmin.browse.searchButton') }}</t-button>
    </div>
    <t-alert v-if="error" theme="error" :message="error" />
    <p class="hint">{{ t('directoryAdmin.browse.total', { count: total }) }}</p>
    <t-table row-key="object_guid" :data="items" :columns="columns" :loading="loading" :empty="t('directoryAdmin.diagnostics.empty')">
      <template #display_name="{ row }">
        <t-button v-if="kind === 'groups'" variant="text" theme="primary" @click="openGroup(row)">{{ row.display_name || row.account_name }}</t-button>
        <span v-else>{{ row.display_name || '—' }}</span>
      </template>
      <template #account_name="{ row }">{{ row.account_name || '—' }}</template>
      <template #email="{ row }">{{ row.email || '—' }}</template>
      <template #user_principal_name="{ row }">{{ row.user_principal_name || '—' }}</template>
      <template #disabled="{ row }">{{ row.disabled ? t('directoryAdmin.diagnostics.disabled') : t('directoryAdmin.browse.enabled') }}</template>
      <template #direct_member_count="{ row }">{{ row.direct_member_count || 0 }}</template>
      <template #effective_member_count="{ row }">{{ row.effective_member_count || 0 }}</template>
      <template #actions="{ row }"><t-button variant="text" @click="openGroup(row)">{{ t('directoryAdmin.browse.details') }}</t-button></template>
    </t-table>
    <t-pagination v-model="page" v-model:page-size="pageSize" :total="total" :page-size-options="[20, 50, 100]" @change="changePage" />

    <t-drawer v-model:visible="detailVisible" :header="detailTitle" size="min(1100px, 96vw)" :footer="false" :close-btn="true">
      <t-alert v-if="memberError" theme="error" :message="memberError" />
      <template v-if="detail">
        <p class="dn">{{ detail.group.dn }}</p>
        <div v-for="relation in relations" :key="relation.key" class="relations">
          <strong>{{ relation.label }}</strong>
          <span v-if="!relation.groups.length">—</span>
          <t-button v-for="group in relation.groups" :key="group.object_guid" variant="text" theme="primary" @click="openGroup(group)">
            {{ group.display_name || group.account_name }}
          </t-button>
        </div>
        <t-alert v-if="detail.unresolved_member_count" theme="warning"
          :message="t('directoryAdmin.browse.unresolved', { count: detail.unresolved_member_count })" />
      </template>
      <p class="hint">{{ t('directoryAdmin.browse.originHint') }}</p>
      <div class="search-bar">
        <t-input v-model="memberQuery" clearable :aria-label="t('directoryAdmin.browse.memberSearch')"
          :placeholder="t('directoryAdmin.browse.memberSearch')" @enter="resetMembers" @clear="clearMembers" />
        <t-button :loading="memberLoading" @click="resetMembers">{{ t('directoryAdmin.browse.searchButton') }}</t-button>
      </div>
      <p class="hint">{{ t('directoryAdmin.browse.total', { count: detail?.total || 0 }) }}</p>
      <t-table row-key="object_guid" :data="detail?.items || []" :columns="memberColumns" :loading="memberLoading" :empty="t('directoryAdmin.diagnostics.empty')">
        <template #email="{ row }">{{ row.email || '—' }}</template>
        <template #disabled="{ row }">{{ row.disabled ? t('directoryAdmin.diagnostics.disabled') : t('directoryAdmin.browse.enabled') }}</template>
        <template #origins="{ row }">
          <div v-for="(origin, index) in row.origins" :key="index" class="origin">
            <t-tag size="small">{{ t(`groupAccess.sources.${origin.source}`) }}</t-tag>
            <span v-if="origin.source === 'nested'">{{ t(`groupAccess.sources.${origin.origin_source}`) }} · {{ t('directoryAdmin.browse.depth', { count: origin.depth }) }}</span>
            <span>{{ origin.path.map((group: DirectoryObjectSummary) => group.display_name || group.account_name).join(' → ') }}</span>
          </div>
        </template>
      </t-table>
      <t-pagination v-model="memberPage" v-model:page-size="memberPageSize" :total="detail?.total || 0" :page-size-options="[20, 50, 100]" @change="changeMemberPage" />
    </t-drawer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { getDirectoryGroupMembers, searchDirectoryGroups, searchDirectoryUsers,
  type DirectoryGroupMembersResult, type DirectoryGroupSummary, type DirectoryObjectSummary } from '@/api/directory'

const { t } = useI18n()
const kind = ref<'users' | 'groups'>('users')
const query = ref('')
const appliedQuery = ref('')
const page = ref(1)
const pageSize = ref(20)
const total = ref(0)
const items = ref<DirectoryGroupSummary[]>([])
const loading = ref(false)
const error = ref('')
let searchRequest = 0
const detailVisible = ref(false)
const selected = ref<DirectoryObjectSummary | null>(null)
const detail = ref<DirectoryGroupMembersResult | null>(null)
const memberQuery = ref('')
const appliedMemberQuery = ref('')
const memberPage = ref(1)
const memberPageSize = ref(20)
const memberLoading = ref(false)
const memberError = ref('')
let memberRequest = 0
const detailTitle = computed(() => `${t('directoryAdmin.browse.details')} · ${selected.value?.display_name || selected.value?.account_name || ''}`)
const relations = computed(() => [
  { key: 'parents', label: t('directoryAdmin.browse.parents'), groups: detail.value?.parent_groups || [] },
  { key: 'children', label: t('directoryAdmin.browse.children'), groups: detail.value?.child_groups || [] },
])
const identityColumns = computed(() => [
  { colKey: 'display_name', title: t('directoryAdmin.browse.name'), minWidth: 140 },
  { colKey: 'account_name', title: t('directoryAdmin.browse.account'), minWidth: 140 },
  { colKey: 'email', title: t('directoryAdmin.browse.email'), minWidth: 170 },
])
const columns = computed(() => [
  ...identityColumns.value.map(column => column.colKey === 'display_name' && kind.value === 'groups'
    ? { ...column, title: t('directoryAdmin.browse.groupName') } : column),
  ...(kind.value === 'users' ? [
    { colKey: 'user_principal_name', title: 'UPN', minWidth: 190 },
    { colKey: 'disabled', title: t('directoryAdmin.browse.status'), width: 90 },
  ] : [
    { colKey: 'direct_member_count', title: t('directoryAdmin.browse.direct'), width: 115 },
    { colKey: 'effective_member_count', title: t('directoryAdmin.browse.effective'), width: 115 },
    { colKey: 'actions', title: t('directoryAdmin.browse.details'), width: 145 },
  ]),
])
const memberColumns = computed(() => [...identityColumns.value,
  { colKey: 'disabled', title: t('directoryAdmin.browse.status'), width: 80 },
  { colKey: 'origins', title: t('directoryAdmin.browse.origins'), minWidth: 300 },
])

async function load() {
  const request = ++searchRequest
  loading.value = true
  error.value = ''
  items.value = []
  try {
    const search = kind.value === 'users' ? searchDirectoryUsers : searchDirectoryGroups
    const result = await search(appliedQuery.value, pageSize.value, (page.value - 1) * pageSize.value)
    if (request !== searchRequest) return
    items.value = result.items
    total.value = result.total
  } catch (e: any) {
    if (request === searchRequest) { error.value = e?.message || t('directoryAdmin.diagnostics.searchFailed'); total.value = 0 }
  } finally { if (request === searchRequest) loading.value = false }
}
function resetSearch() { appliedQuery.value = query.value.trim(); page.value = 1; void load() }
function clearSearch() { query.value = ''; resetSearch() }
function changePage(info: { current: number; pageSize: number }) {
  page.value = info.current; pageSize.value = info.pageSize; void load()
}
function openGroup(group: DirectoryObjectSummary) {
  selected.value = group; detail.value = null; memberQuery.value = ''; appliedMemberQuery.value = ''
  memberPage.value = 1; detailVisible.value = true; void loadMembers()
}
async function loadMembers() {
  if (!selected.value) return
  const request = ++memberRequest
  memberLoading.value = true; memberError.value = ''
  try {
    const result = await getDirectoryGroupMembers(selected.value.object_guid, appliedMemberQuery.value, memberPageSize.value, (memberPage.value - 1) * memberPageSize.value)
    if (request === memberRequest) detail.value = result
  } catch (e: any) {
    if (request === memberRequest) { detail.value = null; memberError.value = e?.message || t('directoryAdmin.diagnostics.searchFailed') }
  } finally { if (request === memberRequest) memberLoading.value = false }
}
function resetMembers() { appliedMemberQuery.value = memberQuery.value.trim(); memberPage.value = 1; void loadMembers() }
function clearMembers() { memberQuery.value = ''; resetMembers() }
function changeMemberPage(info: { current: number; pageSize: number }) {
  memberPage.value = info.current; memberPageSize.value = info.pageSize; void loadMembers()
}
onMounted(load)
</script>

<style scoped lang="less">
.directory-browser { min-width: 0; }
.search-bar { display: flex; gap: 12px; flex-wrap: wrap; margin: 16px 0; align-items: center; }
.search-bar :deep(.t-input__wrap) { flex: 1; min-width: 240px; }
.hint, .dn { color: var(--td-text-color-secondary); margin: 12px 0; overflow-wrap: anywhere; }
.relations { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin: 12px 0; }
.origin { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; margin: 6px 0; overflow-wrap: anywhere; }
:deep(.t-pagination) { margin-top: 16px; }
</style>
