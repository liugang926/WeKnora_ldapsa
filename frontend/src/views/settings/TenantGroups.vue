<template>
  <section class="directory-groups">
    <div class="directory-groups__header">
      <div>
        <h3>{{ $t('directoryGroups.title') }}</h3>
        <p>{{ $t('directoryGroups.description') }}</p>
      </div>
      <t-button v-if="canManage" theme="primary" variant="outline" @click="openAdd">
        <template #icon><t-icon name="add" /></template>
        {{ $t('directoryGroups.add') }}
      </t-button>
    </div>

    <t-alert v-if="error" theme="error" :message="error" />
    <t-loading v-if="loading" />
    <t-empty v-else-if="groups.length === 0" :description="$t('directoryGroups.empty')" />
    <t-table v-else row-key="id" :data="groups" :columns="columns" size="medium" hover>
      <template #group="{ row }">
        <div class="group-cell">
          <strong>{{ row.display_name }}</strong>
          <span>{{ row.dn }}</span>
        </div>
      </template>
      <template #members="{ row }">
        <div class="member-counts">
          <t-tag size="small" variant="light">{{ $t('directoryGroups.direct') }} {{ row.direct_member_count ?? 0 }}</t-tag>
          <t-tag size="small" variant="light">{{ $t('directoryGroups.effective') }} {{ row.effective_member_count ?? 0 }}</t-tag>
          <t-tag v-if="row.nested_group_count" size="small" variant="light">{{ $t('directoryGroups.nested') }} {{ row.nested_group_count }}</t-tag>
        </div>
      </template>
      <template #role="{ row }">
        <t-select v-if="canManage" :model-value="row.role" size="small" @change="(value: DirectoryTenantRole) => changeRole(row, value)">
          <t-option v-for="option in roleOptions" :key="option.value" :value="option.value" :label="option.label" />
        </t-select>
        <t-tag v-else size="small">{{ $t(`directoryGroups.roles.${row.role}`) }}</t-tag>
      </template>
      <template #actions="{ row }">
        <t-popconfirm v-if="canManage" :content="$t('directoryGroups.removeConfirm', { name: row.display_name })"
          :confirm-btn="{ content: $t('common.confirm'), theme: 'danger' }" :cancel-btn="$t('common.cancel')"
          @confirm="removeGroup(row)">
          <t-button theme="danger" variant="text" size="small">{{ $t('common.remove') }}</t-button>
        </t-popconfirm>
      </template>
    </t-table>

    <t-dialog v-model:visible="addVisible" :header="$t('directoryGroups.add')" :confirm-btn="{ content: $t('common.add'), loading: adding }"
      :cancel-btn="$t('common.cancel')" width="560px" @confirm="addSelected">
      <t-input v-model="query" :placeholder="$t('directoryGroups.searchPlaceholder')" clearable @enter="searchGroups">
        <template #suffix-icon><t-icon name="search" @click="searchGroups" /></template>
      </t-input>
      <div class="candidate-list">
        <t-loading v-if="searching" />
        <t-empty v-else-if="candidates.length === 0" :description="$t('directoryGroups.candidateEmpty')" />
        <button v-for="candidate in candidates" v-else :key="candidate.directory_group_id" type="button"
          :class="['candidate', { selected: selected?.directory_group_id === candidate.directory_group_id }]"
          @click="selected = candidate">
          <strong>{{ candidate.display_name }}</strong>
          <span>{{ candidate.dn }}</span>
        </button>
      </div>
      <t-form label-align="top">
        <t-form-item :label="$t('directoryGroups.role')">
          <t-select v-model="selectedRole">
            <t-option v-for="option in roleOptions" :key="option.value" :value="option.value" :label="option.label" />
          </t-select>
        </t-form-item>
      </t-form>
    </t-dialog>
  </section>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { MessagePlugin } from 'tdesign-vue-next'
import { useI18n } from 'vue-i18n'
import {
  addTenantDirectoryGroup,
  listTenantDirectoryGroups,
  removeTenantDirectoryGroup,
  searchTenantDirectoryGroups,
  updateTenantDirectoryGroupRole,
  type DirectoryTenantRole,
  type TenantDirectoryGroup,
  type TenantDirectoryGroupCandidate,
} from '@/api/tenant/groups'

const props = defineProps<{ tenantId: number; canManage: boolean }>()
const { t } = useI18n()
const loading = ref(false)
const error = ref('')
const groups = ref<TenantDirectoryGroup[]>([])
const addVisible = ref(false)
const adding = ref(false)
const searching = ref(false)
const query = ref('')
const candidates = ref<TenantDirectoryGroupCandidate[]>([])
const selected = ref<TenantDirectoryGroupCandidate | null>(null)
const selectedRole = ref<DirectoryTenantRole>('viewer')

// Directory groups intentionally cannot confer Owner or system-admin privileges.
const DIRECTORY_GROUP_ROLES: DirectoryTenantRole[] = ['viewer', 'contributor', 'admin']
const roleOptions = computed(() => DIRECTORY_GROUP_ROLES.map((value) => ({ value, label: t(`directoryGroups.roles.${value}`) })))
const columns = computed(() => [
  { colKey: 'group', title: t('directoryGroups.group'), minWidth: 260 },
  { colKey: 'members', title: t('directoryGroups.members'), width: 260 },
  { colKey: 'role', title: t('directoryGroups.role'), width: 160 },
  { colKey: 'actions', title: t('directoryGroups.actions'), width: 100 },
])

async function loadGroups() {
  if (!props.tenantId) return
  loading.value = true
  error.value = ''
  try {
    const response = await listTenantDirectoryGroups(props.tenantId)
    groups.value = response.groups ?? []
  } catch (cause: any) {
    error.value = cause?.message || t('directoryGroups.loadFailed')
  } finally {
    loading.value = false
  }
}

async function searchGroups() {
  searching.value = true
  try {
    const response = await searchTenantDirectoryGroups(props.tenantId, query.value)
    candidates.value = response.groups ?? []
  } catch (cause: any) {
    MessagePlugin.error(cause?.message || t('directoryGroups.searchFailed'))
  } finally {
    searching.value = false
  }
}

function openAdd() {
  selected.value = null
  selectedRole.value = 'viewer'
  query.value = ''
  addVisible.value = true
  void searchGroups()
}

async function addSelected() {
  if (!selected.value) {
    MessagePlugin.warning(t('directoryGroups.selectRequired'))
    return
  }
  adding.value = true
  try {
    await addTenantDirectoryGroup(props.tenantId, {
      directory_id: selected.value.directory_id,
      directory_group_id: selected.value.directory_group_id,
      role: selectedRole.value,
    })
    addVisible.value = false
    await loadGroups()
    MessagePlugin.success(t('directoryGroups.added'))
  } catch (cause: any) {
    MessagePlugin.error(cause?.message || t('directoryGroups.addFailed'))
  } finally {
    adding.value = false
  }
}

async function changeRole(group: TenantDirectoryGroup, role: DirectoryTenantRole) {
  try {
    await updateTenantDirectoryGroupRole(props.tenantId, group.id, role)
    group.role = role
    MessagePlugin.success(t('directoryGroups.updated'))
  } catch (cause: any) {
    MessagePlugin.error(cause?.message || t('directoryGroups.updateFailed'))
    await loadGroups()
  }
}

async function removeGroup(group: TenantDirectoryGroup) {
  try {
    await removeTenantDirectoryGroup(props.tenantId, group.id)
    groups.value = groups.value.filter((item) => item.id !== group.id)
    MessagePlugin.success(t('directoryGroups.removed'))
  } catch (cause: any) {
    MessagePlugin.error(cause?.message || t('directoryGroups.removeFailed'))
  }
}

watch(() => props.tenantId, () => void loadGroups(), { immediate: true })
</script>

<style scoped>
.directory-groups { margin-top: 28px; padding-top: 24px; border-top: 1px solid var(--td-component-border); }
.directory-groups__header { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; margin-bottom: 16px; }
.directory-groups__header h3 { margin: 0 0 6px; font-size: var(--app-text-xl); }
.directory-groups__header p { margin: 0; color: var(--td-text-color-secondary); }
.group-cell, .candidate { display: flex; flex-direction: column; gap: 3px; }
.group-cell span, .candidate span { color: var(--td-text-color-placeholder); font-size: var(--app-text-sm); overflow-wrap: anywhere; }
.member-counts { display: flex; flex-wrap: wrap; gap: 6px; }
.candidate-list { display: flex; flex-direction: column; gap: 6px; max-height: 260px; overflow: auto; margin: 14px 0; }
.candidate { width: 100%; padding: 10px 12px; border: 1px solid var(--td-component-border); border-radius: 6px; background: transparent; text-align: left; cursor: pointer; }
.candidate:hover, .candidate.selected { border-color: var(--td-brand-color); background: var(--td-brand-color-light); }
</style>
