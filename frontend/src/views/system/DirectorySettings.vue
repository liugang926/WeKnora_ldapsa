<template>
  <div class="directory-settings">
    <header class="section-header directory-header">
      <div>
        <div class="directory-title-row">
          <h2>{{ t('directoryAdmin.title') }}</h2>
          <t-tag :theme="statusTheme" variant="light" size="small">{{ statusLabel }}</t-tag>
          <t-tag v-if="config?.source && config.source !== 'database'" theme="primary" variant="light" size="small">
            {{ t(`directoryAdmin.source.${config.source}`) }}
          </t-tag>
        </div>
        <p class="section-description">{{ t('directoryAdmin.description') }}</p>
      </div>
      <t-button variant="text" size="small" :loading="loading" @click="loadAll">
        <template #icon><t-icon name="refresh" /></template>
        {{ t('directoryAdmin.refresh') }}
      </t-button>
    </header>

    <t-alert v-if="health?.access_paused" theme="error" class="directory-alert"
      :message="t('directoryAdmin.status.accessPaused')" />
    <t-alert v-else-if="health?.last_error" theme="warning" class="directory-alert"
      :message="t('directoryAdmin.status.lastError', { error: health.last_error })" />

    <div class="directory-status-grid">
      <div class="status-card">
        <span class="status-label">{{ t('directoryAdmin.status.activeServer') }}</span>
        <strong>{{ health?.active_server || '—' }}</strong>
      </div>
      <div class="status-card">
        <span class="status-label">{{ t('directoryAdmin.status.lastSuccess') }}</span>
        <strong>{{ formatDate(health?.last_success_at) }}</strong>
      </div>
      <div class="status-card">
        <span class="status-label">{{ t('directoryAdmin.status.nextSync') }}</span>
        <strong>{{ formatDate(health?.next_sync_at) }}</strong>
      </div>
      <div class="status-card">
        <span class="status-label">{{ t('directoryAdmin.status.failures') }}</span>
        <strong>{{ health?.consecutive_failures ?? 0 }}</strong>
      </div>
    </div>

    <t-tabs v-model="activeTab" class="directory-tabs">
      <t-tab-panel value="configuration" :label="t('directoryAdmin.tabs.configuration')" />
      <t-tab-panel value="diagnostics" :label="t('directoryAdmin.tabs.diagnostics')" />
      <t-tab-panel value="sync" :label="t('directoryAdmin.tabs.sync')" />
      <t-tab-panel value="runs" :label="t('directoryAdmin.tabs.runs')" />
    </t-tabs>

    <div v-if="loading && !config" class="loading-state">
      <t-loading :text="t('directoryAdmin.loading')" />
    </div>

    <template v-else-if="config">
      <div v-show="activeTab === 'configuration'" class="directory-panel">
        <t-alert v-if="config.read_only_fields.length" theme="info" class="directory-alert"
          :message="t('directoryAdmin.readOnlyHint')" />

        <section class="settings-group">
          <div class="setting-row">
            <div class="setting-info">
              <label>{{ t('directoryAdmin.fields.enabled') }}</label>
              <p class="desc">{{ t('directoryAdmin.fields.enabledHint') }}</p>
            </div>
            <div class="setting-control">
              <t-switch v-model="config.enabled" :disabled="fieldReadOnly('enabled')" />
            </div>
          </div>
          <div class="setting-row">
            <div class="setting-info"><label>{{ t('directoryAdmin.fields.displayName') }}</label></div>
            <div class="setting-control">
              <t-input v-model="config.display_name" :disabled="fieldReadOnly('display_name')" />
            </div>
          </div>
          <div class="setting-row setting-row--top">
            <div class="setting-info">
              <label>{{ t('directoryAdmin.fields.servers') }}</label>
              <p class="desc">{{ t('directoryAdmin.fields.serversHint') }}</p>
            </div>
            <div class="setting-control">
              <t-textarea v-model="serverText" :disabled="fieldReadOnly('servers')" :autosize="{ minRows: 2, maxRows: 5 }"
                placeholder="dc1.example.com:636&#10;dc2.example.com:636" />
            </div>
          </div>
          <div class="setting-row">
            <div class="setting-info"><label>{{ t('directoryAdmin.fields.transport') }}</label></div>
            <div class="setting-control">
              <t-radio-group v-model="config.transport" :disabled="fieldReadOnly('transport')">
                <t-radio-button value="ldaps">LDAPS</t-radio-button>
                <t-radio-button value="starttls">StartTLS</t-radio-button>
              </t-radio-group>
            </div>
          </div>
          <div class="setting-row">
            <div class="setting-info">
              <label>{{ t('directoryAdmin.fields.caFile') }}</label>
              <p class="desc">{{ t('directoryAdmin.fields.caHint') }}</p>
            </div>
            <div class="setting-control">
              <t-input v-model="config.ca_file" :disabled="fieldReadOnly('ca_file')" placeholder="/run/secrets/ad-ca.pem" />
            </div>
          </div>
        </section>

        <section class="settings-group">
          <h3 class="group-title">{{ t('directoryAdmin.sections.search') }}</h3>
          <div v-for="field in dnFields" :key="field.key" class="setting-row">
            <div class="setting-info">
              <label>{{ t(`directoryAdmin.fields.${field.label}`) }}</label>
              <p v-if="field.hint" class="desc">{{ t(`directoryAdmin.fields.${field.hint}`) }}</p>
            </div>
            <div class="setting-control">
              <t-input v-model="config[field.key]" :disabled="fieldReadOnly(field.key)" />
            </div>
          </div>
          <div class="setting-row">
            <div class="setting-info"><label>{{ t('directoryAdmin.fields.loginAttributes') }}</label></div>
            <div class="setting-control">
              <t-select v-model="config.login_attributes" multiple :disabled="fieldReadOnly('login_attributes')">
                <t-option value="sAMAccountName" label="sAMAccountName" />
                <t-option value="userPrincipalName" label="userPrincipalName (UPN)" />
              </t-select>
            </div>
          </div>
        </section>

        <section class="settings-group">
          <h3 class="group-title">{{ t('directoryAdmin.sections.serviceAccount') }}</h3>
          <div class="setting-row">
            <div class="setting-info"><label>{{ t('directoryAdmin.fields.bindDn') }}</label></div>
            <div class="setting-control">
              <t-input v-model="config.bind_dn" :disabled="fieldReadOnly('bind_dn')" autocomplete="off" />
            </div>
          </div>
          <div class="setting-row">
            <div class="setting-info">
              <label>{{ t('directoryAdmin.fields.bindPassword') }}</label>
              <p class="desc">{{ passwordHint }}</p>
            </div>
            <div class="setting-control">
              <t-input v-model="replacementPassword" type="password" autocomplete="new-password"
                :disabled="fieldReadOnly('bind_password')" :placeholder="t('directoryAdmin.fields.passwordPlaceholder')" />
            </div>
          </div>
        </section>

        <section class="settings-group">
          <h3 class="group-title">{{ t('directoryAdmin.sections.limits') }}</h3>
          <div class="numeric-grid">
            <label v-for="field in numericFields" :key="field.key" class="numeric-field">
              <span>{{ t(`directoryAdmin.fields.${field.label}`) }}</span>
              <t-input-number v-model="config[field.key]" :min="field.min" :max="field.max"
                :disabled="fieldReadOnly(field.key)" theme="normal" />
            </label>
          </div>
        </section>

        <div class="panel-actions">
          <t-button variant="outline" :loading="testing" @click="runConnectionTest">
            {{ t('directoryAdmin.actions.test') }}
          </t-button>
          <t-button theme="primary" :loading="saving" @click="saveConfig">
            {{ t('common.save') }}
          </t-button>
        </div>
      </div>

      <div v-show="activeTab === 'diagnostics'" class="directory-panel">
        <section class="diagnostic-actions">
          <div>
            <h3>{{ t('directoryAdmin.diagnostics.title') }}</h3>
            <p>{{ t('directoryAdmin.diagnostics.description') }}</p>
          </div>
          <t-button variant="outline" :loading="testing" @click="runConnectionTest">
            {{ t('directoryAdmin.actions.test') }}
          </t-button>
        </section>
        <t-alert v-if="testResult" :theme="testResult.ok ? 'success' : 'error'"
          :message="testResult.message || (testResult.ok ? t('directoryAdmin.messages.testSuccess') : t('directoryAdmin.messages.testFailed'))" />

        <DirectoryBrowser v-if="activeTab === 'diagnostics' && config?.enabled" />
      </div>

      <div v-show="activeTab === 'sync'" class="directory-panel">
        <section class="sync-actions">
          <div>
            <h3>{{ t('directoryAdmin.sync.title') }}</h3>
            <p>{{ t('directoryAdmin.sync.description') }}</p>
          </div>
          <div class="sync-buttons">
            <t-button variant="outline" :loading="previewing" @click="loadPreview">{{ t('directoryAdmin.actions.preview') }}</t-button>
            <t-button theme="primary" :loading="syncing || health?.syncing" @click="runSync">{{ t('directoryAdmin.actions.syncNow') }}</t-button>
          </div>
        </section>
        <template v-if="preview">
          <t-alert v-if="!preview.complete" theme="error" :message="t('directoryAdmin.sync.incomplete')" />
          <div class="preview-grid">
            <div class="preview-card">
              <strong>{{ t('directoryAdmin.sync.users') }}</strong>
              <span>+{{ preview.users.create }} / ~{{ preview.users.update }} / −{{ preview.users.disable }}</span>
            </div>
            <div class="preview-card">
              <strong>{{ t('directoryAdmin.sync.groups') }}</strong>
              <span>+{{ preview.groups.create }} / ~{{ preview.groups.update }} / −{{ preview.groups.remove }}</span>
            </div>
            <div class="preview-card">
              <strong>{{ t('directoryAdmin.sync.memberships') }}</strong>
              <span>+{{ preview.memberships.add }} / −{{ preview.memberships.remove }}</span>
            </div>
          </div>
          <ul v-if="preview.warnings?.length" class="warning-list">
            <li v-for="warning in preview.warnings" :key="warning">{{ warning }}</li>
          </ul>
        </template>
        <t-empty v-else :description="t('directoryAdmin.sync.emptyPreview')" />
      </div>

      <div v-show="activeTab === 'runs'" class="directory-panel">
        <div class="run-header">
          <h3>{{ t('directoryAdmin.runs.title') }}</h3>
          <t-button variant="text" size="small" @click="loadRuns">{{ t('directoryAdmin.refresh') }}</t-button>
        </div>
        <t-table v-if="runs.length" row-key="id" :data="runs" :columns="runColumns" size="medium" hover>
          <template #status="{ row }">
            <t-tag :theme="runTheme(row.status)" variant="light" size="small">
              {{ t(`directoryAdmin.runs.status.${row.status}`) }}
            </t-tag>
          </template>
          <template #started_at="{ row }">{{ formatDate(row.started_at) }}</template>
          <template #summary="{ row }">{{ runSummary(row) }}</template>
          <template #error="{ row }"><span class="run-error">{{ row.error || '—' }}</span></template>
        </t-table>
        <t-empty v-else :description="t('directoryAdmin.runs.empty')" />
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { MessagePlugin } from 'tdesign-vue-next'
import { useI18n } from 'vue-i18n'
import DirectoryBrowser from './DirectoryBrowser.vue'
import {
  getDirectoryConfig,
  getDirectoryStatus,
  listDirectorySyncRuns,
  previewDirectorySync,
  startDirectorySync,
  testDirectoryConnection,
  updateDirectoryConfig,
  type DirectoryConfig,
  type DirectoryHealth,
  type DirectoryRunStatus,
  type DirectorySyncPreview,
  type DirectorySyncRun,
  type DirectoryTestResult,
} from '@/api/directory'
import {
  buildDirectoryConfigUpdate,
  directoryServersText,
  isDirectoryFieldReadOnly,
} from './directoryState'

const { t, locale } = useI18n()
const activeTab = ref('configuration')
const loading = ref(false)
const saving = ref(false)
const testing = ref(false)
const previewing = ref(false)
const syncing = ref(false)
const config = ref<DirectoryConfig | null>(null)
const health = ref<DirectoryHealth | null>(null)
const serverText = ref('')
const replacementPassword = ref('')
const testResult = ref<DirectoryTestResult | null>(null)
const preview = ref<DirectorySyncPreview | null>(null)
const runs = ref<DirectorySyncRun[]>([])

type StringConfigKey = 'base_dn' | 'user_base_dn' | 'group_base_dn' | 'user_filter' | 'group_filter' | 'allowed_login_filter'
const dnFields: Array<{ key: StringConfigKey; label: string; hint?: string }> = [
  { key: 'base_dn', label: 'baseDn' },
  { key: 'user_base_dn', label: 'userBaseDn' },
  { key: 'group_base_dn', label: 'groupBaseDn' },
  { key: 'user_filter', label: 'userFilter', hint: 'filterHint' },
  { key: 'group_filter', label: 'groupFilter', hint: 'filterHint' },
  { key: 'allowed_login_filter', label: 'allowedLoginFilter', hint: 'allowedLoginFilterHint' },
]

type NumericConfigKey = 'connect_timeout_seconds' | 'query_timeout_seconds' | 'result_limit' | 'page_size' | 'sync_interval_seconds' | 'stale_after_seconds'
const numericFields: Array<{ key: NumericConfigKey; label: string; min: number; max: number }> = [
  { key: 'connect_timeout_seconds', label: 'connectTimeout', min: 1, max: 120 },
  { key: 'query_timeout_seconds', label: 'queryTimeout', min: 1, max: 300 },
  { key: 'result_limit', label: 'resultLimit', min: 1, max: 100000 },
  { key: 'page_size', label: 'pageSize', min: 1, max: 5000 },
  { key: 'sync_interval_seconds', label: 'syncInterval', min: 60, max: 86400 },
  { key: 'stale_after_seconds', label: 'staleAfter', min: 60, max: 604800 },
]

const statusTheme = computed<'success' | 'warning' | 'danger' | 'default'>(() => {
  if (!config.value?.enabled) return 'default'
  if (health.value?.access_paused) return 'danger'
  if (health.value?.available) return 'success'
  return 'warning'
})

const statusLabel = computed(() => {
  if (!config.value?.enabled) return t('directoryAdmin.status.disabled')
  if (health.value?.access_paused) return t('directoryAdmin.status.paused')
  if (health.value?.available) return t('directoryAdmin.status.healthy')
  return t('directoryAdmin.status.unavailable')
})

const passwordHint = computed(() => {
  if (fieldReadOnly('bind_password')) {
    const source = config.value?.bind_password_source || config.value?.source || 'file'
    return t('directoryAdmin.fields.passwordManaged', { source: t(`directoryAdmin.source.${source}`) })
  }
  return config.value?.has_bind_password
    ? t('directoryAdmin.fields.passwordStored')
    : t('directoryAdmin.fields.passwordMissing')
})

const runColumns = computed(() => [
  { colKey: 'started_at', title: t('directoryAdmin.runs.startedAt'), width: 170 },
  { colKey: 'trigger', title: t('directoryAdmin.runs.trigger'), width: 100 },
  { colKey: 'status', title: t('directoryAdmin.runs.statusLabel'), width: 100 },
  { colKey: 'summary', title: t('directoryAdmin.runs.summary'), minWidth: 180 },
  { colKey: 'error', title: t('directoryAdmin.runs.error'), minWidth: 180, ellipsis: true },
])

function fieldReadOnly(field: string): boolean {
  return isDirectoryFieldReadOnly(config.value?.read_only_fields || [], field)
}

function formatDate(value?: string): string {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return new Intl.DateTimeFormat(locale.value || 'zh-CN', {
    year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit',
  }).format(date)
}

function runTheme(status: DirectoryRunStatus): 'success' | 'warning' | 'danger' | 'default' {
  if (status === 'success') return 'success'
  if (status === 'failed') return 'danger'
  if (status === 'running') return 'warning'
  return 'default'
}

function runSummary(run: DirectorySyncRun): string {
  return t('directoryAdmin.runs.summaryValue', {
    users: run.users_seen ?? 0,
    groups: run.groups_seen ?? 0,
    memberships: run.memberships_seen ?? 0,
  })
}

async function loadConfig() {
  const value = await getDirectoryConfig()
  config.value = value
  serverText.value = directoryServersText(value.servers)
  replacementPassword.value = ''
}

async function loadStatus() {
  health.value = await getDirectoryStatus()
}

async function loadRuns() {
  const result = await listDirectorySyncRuns()
  runs.value = result.runs || []
}

async function loadAll() {
  loading.value = true
  try {
    await Promise.all([loadConfig(), loadStatus(), loadRuns()])
  } catch (error: any) {
    MessagePlugin.error(error?.message || t('directoryAdmin.messages.loadFailed'))
  } finally {
    loading.value = false
  }
}

function draftPayload() {
  if (!config.value) return null
  return buildDirectoryConfigUpdate(config.value, serverText.value, replacementPassword.value)
}

async function saveConfig() {
  const payload = draftPayload()
  if (!payload) return
  if (!payload.servers.length || !payload.base_dn || !payload.bind_dn) {
    MessagePlugin.warning(t('directoryAdmin.messages.required'))
    return
  }
  saving.value = true
  try {
    const value = await updateDirectoryConfig(payload)
    config.value = value
    serverText.value = directoryServersText(value.servers)
    replacementPassword.value = ''
    MessagePlugin.success(t('directoryAdmin.messages.saved'))
    await loadStatus()
  } catch (error: any) {
    MessagePlugin.error(error?.message || t('directoryAdmin.messages.saveFailed'))
  } finally {
    saving.value = false
  }
}

async function runConnectionTest() {
  const payload = draftPayload()
  if (!payload) return
  testing.value = true
  testResult.value = null
  try {
    testResult.value = await testDirectoryConnection(payload)
    const resultMessage = testResult.value.message
      || t(testResult.value.ok ? 'directoryAdmin.messages.testSuccess' : 'directoryAdmin.messages.testFailed')
    if (testResult.value.ok) MessagePlugin.success(resultMessage)
    else MessagePlugin.error(resultMessage)
  } catch (error: any) {
    const errorMessage = error?.message || t('directoryAdmin.messages.testFailed')
    testResult.value = { ok: false, message: errorMessage }
    MessagePlugin.error(errorMessage)
  } finally {
    testing.value = false
  }
}


async function loadPreview() {
  previewing.value = true
  try {
    preview.value = await previewDirectorySync()
  } catch (error: any) {
    MessagePlugin.error(error?.message || t('directoryAdmin.sync.previewFailed'))
  } finally {
    previewing.value = false
  }
}

async function runSync() {
  syncing.value = true
  try {
    await startDirectorySync()
    MessagePlugin.success(t('directoryAdmin.sync.started'))
    await Promise.all([loadStatus(), loadRuns()])
  } catch (error: any) {
    MessagePlugin.error(error?.message || t('directoryAdmin.sync.startFailed'))
  } finally {
    syncing.value = false
  }
}

onMounted(loadAll)
</script>

<style scoped lang="less">
@import (reference) '@/components/css/settings-section.less';

.directory-settings { width: 100%; }
.section-header { .settings-section-header(); }
.directory-header { display: flex; justify-content: space-between; gap: 20px; }
.directory-title-row { display: flex; align-items: center; gap: 8px; }
.directory-title-row h2 { margin: 0; }
.directory-alert { margin: 12px 0; }
.directory-status-grid, .preview-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; margin: 14px 0; }
.status-card, .preview-card { border: 1px solid var(--td-component-stroke); border-radius: var(--app-radius-md); padding: 12px; background: var(--td-bg-color-container); display: flex; flex-direction: column; gap: 6px; min-width: 0; }
.status-label { color: var(--td-text-color-secondary); font-size: var(--app-text-sm); }
.status-card strong { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.directory-tabs { margin-top: 8px; }
.directory-panel { padding: 16px 2px 6px; }
.loading-state { min-height: 240px; display: flex; align-items: center; justify-content: center; }
.settings-group { border: 1px solid var(--td-component-stroke); border-radius: var(--app-radius-lg); margin-bottom: 14px; overflow: hidden; }
.group-title { margin: 0; padding: 12px 16px; font-size: var(--app-text-base); background: var(--td-bg-color-secondarycontainer); }
.setting-row { display: grid; grid-template-columns: minmax(180px, 42%) minmax(260px, 1fr); gap: 20px; padding: 14px 16px; border-top: 1px solid var(--td-component-stroke); align-items: center; }
.setting-row:first-child { border-top: 0; }
.setting-row--top { align-items: start; }
.setting-info label { font-weight: 500; color: var(--td-text-color-primary); }
.setting-info .desc { margin: 4px 0 0; color: var(--td-text-color-secondary); font-size: var(--app-text-sm); line-height: 1.45; }
.setting-control { min-width: 0; }
.numeric-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; padding: 16px; }
.numeric-field { display: flex; flex-direction: column; gap: 7px; color: var(--td-text-color-secondary); font-size: var(--app-text-sm); }
.panel-actions, .sync-buttons { display: flex; justify-content: flex-end; gap: 10px; }
.diagnostic-actions, .sync-actions, .run-header { display: flex; align-items: center; justify-content: space-between; gap: 20px; margin-bottom: 16px; }
.diagnostic-actions h3, .sync-actions h3, .run-header h3 { margin: 0 0 4px; }
.diagnostic-actions p, .sync-actions p { margin: 0; color: var(--td-text-color-secondary); }
.directory-search-bar { display: grid; grid-template-columns: auto 1fr auto; gap: 10px; margin: 18px 0 12px; }
.directory-results { border: 1px solid var(--td-component-stroke); border-radius: var(--app-radius-md); overflow: hidden; }
.directory-result-row { display: flex; justify-content: space-between; gap: 20px; padding: 12px 14px; border-top: 1px solid var(--td-component-stroke); }
.directory-result-row:first-child { border-top: 0; }
.directory-result-row > div:first-child { min-width: 0; display: flex; flex-direction: column; gap: 3px; }
.directory-result-row span { color: var(--td-text-color-secondary); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.result-identifiers { display: flex; align-items: center; gap: 8px; }
.preview-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
.warning-list { margin: 14px 0; color: var(--td-warning-color); }
.run-error { color: var(--td-error-color); }

@media (max-width: 900px) {
  .directory-status-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .setting-row { grid-template-columns: 1fr; gap: 8px; }
}
</style>
