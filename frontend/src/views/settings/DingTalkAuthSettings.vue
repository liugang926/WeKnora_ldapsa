<template>
  <div class="dingtalk-settings">
    <header class="section-header dingtalk-header">
      <div>
        <div class="dingtalk-title-row">
          <h2>{{ t('dingtalkAdmin.title') }}</h2>
          <t-tag :theme="config?.enabled ? 'success' : 'default'" variant="light" size="small">
            {{ config?.enabled ? t('dingtalkAdmin.status.enabled') : t('dingtalkAdmin.status.disabled') }}
          </t-tag>
          <t-tag theme="primary" variant="light" size="small">
            {{ t(`dingtalkAdmin.source.${config?.config_source || 'file'}`) }}
          </t-tag>
          <t-tag v-if="config" :theme="secretTheme" variant="light" size="small">
            {{ t(`dingtalkAdmin.secretSource.${config.secret_from}`) }}
          </t-tag>
        </div>
        <p class="section-description">{{ t('dingtalkAdmin.description') }}</p>
      </div>
      <t-button variant="text" size="small" :loading="loading" @click="loadConfig">
        <template #icon><t-icon name="refresh" /></template>
        {{ t('dingtalkAdmin.refresh') }}
      </t-button>
    </header>

    <t-alert v-if="readOnly" theme="info" class="dingtalk-alert"
      :message="t('dingtalkAdmin.readOnlyHint')" />

    <div v-if="loading && !config" class="loading-state">
      <t-loading :text="t('dingtalkAdmin.loading')" />
    </div>

    <template v-else-if="config">
      <section class="settings-group">
        <div class="setting-row">
          <div class="setting-info">
            <label>{{ t('dingtalkAdmin.fields.enabled') }}</label>
            <p class="desc">{{ t('dingtalkAdmin.fields.enabledHint') }}</p>
          </div>
          <div class="setting-control">
            <t-switch v-model="config.enabled" :disabled="readOnly" />
          </div>
        </div>
        <div class="setting-row">
          <div class="setting-info">
            <label>{{ t('dingtalkAdmin.fields.corpId') }}</label>
            <p class="desc">{{ t('dingtalkAdmin.fields.corpIdHint') }}</p>
          </div>
          <div class="setting-control">
            <t-input v-model="config.corp_id" :disabled="readOnly" autocomplete="off" />
          </div>
        </div>
        <div class="setting-row">
          <div class="setting-info"><label>{{ t('dingtalkAdmin.fields.appKey') }}</label></div>
          <div class="setting-control">
            <t-input v-model="config.app_key" :disabled="readOnly" autocomplete="off" />
          </div>
        </div>
        <div class="setting-row">
          <div class="setting-info">
            <label>{{ t('dingtalkAdmin.fields.appSecret') }}</label>
            <p class="desc">{{ secretHint }}</p>
          </div>
          <div class="setting-control secret-control">
            <t-input v-model="replacementSecret" type="password" autocomplete="new-password"
              :disabled="readOnly" :placeholder="t('dingtalkAdmin.fields.secretPlaceholder')" />
            <t-checkbox v-if="!readOnly && config.has_secret && config.secret_from === 'database'"
              v-model="clearSecret" class="clear-secret">
              {{ t('dingtalkAdmin.fields.clearSecret') }}
            </t-checkbox>
          </div>
        </div>
        <div class="setting-row">
          <div class="setting-info">
            <label>{{ t('dingtalkAdmin.fields.agentId') }}</label>
            <p class="desc">{{ t('dingtalkAdmin.fields.agentIdHint') }}</p>
          </div>
          <div class="setting-control">
            <t-input v-model="config.agent_id" :disabled="readOnly" autocomplete="off" />
          </div>
        </div>
        <div class="setting-row">
          <div class="setting-info">
            <label>{{ t('dingtalkAdmin.fields.displayName') }}</label>
            <p class="desc">{{ t('dingtalkAdmin.fields.displayNameHint') }}</p>
          </div>
          <div class="setting-control">
            <t-input v-model="config.display_name" :disabled="readOnly" />
          </div>
        </div>
      </section>

      <section class="settings-group">
        <h3 class="group-title">{{ t('dingtalkAdmin.sections.redirect') }}</h3>
        <div class="setting-row setting-row--top">
          <div class="setting-info">
            <label>{{ t('dingtalkAdmin.fields.redirectUrl') }}</label>
            <p class="desc">{{ t('dingtalkAdmin.fields.redirectUrlHint') }}</p>
          </div>
          <div class="setting-control redirect-control">
            <t-input :value="config.redirect_url || '—'" readonly>
              <template #suffix>
                <t-button variant="text" size="small" :disabled="!config.redirect_url" @click="copyRedirect">
                  {{ copied ? t('dingtalkAdmin.copied') : t('dingtalkAdmin.copy') }}
                </t-button>
              </template>
            </t-input>
          </div>
        </div>
      </section>

      <div v-if="testResult" class="dingtalk-alert">
        <t-alert :theme="testResult.ok ? 'success' : 'error'" :message="testMessage" />
      </div>

      <div class="panel-actions">
        <t-button variant="outline" :loading="testing" @click="runConnectionTest">
          {{ t('dingtalkAdmin.actions.test') }}
        </t-button>
        <t-button theme="primary" :disabled="readOnly" :loading="saving" @click="saveConfig">
          {{ t('common.save') }}
        </t-button>
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { MessagePlugin } from 'tdesign-vue-next'
import { useI18n } from 'vue-i18n'
import {
  getDingTalkAdminConfig,
  testDingTalkConnection,
  updateDingTalkAdminConfig,
  type DingTalkAdminConfig,
  type DingTalkConfigUpdate,
  type DingTalkTestResult,
} from '@/api/dingtalk'

const { t } = useI18n()
const loading = ref(false)
const saving = ref(false)
const testing = ref(false)
const copied = ref(false)
const config = ref<DingTalkAdminConfig | null>(null)
const replacementSecret = ref('')
const clearSecret = ref(false)
const testResult = ref<DingTalkTestResult | null>(null)

// File/env-managed deployments keep the settings read-only here, matching the
// directory module's read_only_fields contract.
const readOnly = computed(() => config.value?.config_source !== 'database')

const secretTheme = computed<'success' | 'warning' | 'danger'>(() => {
  if (!config.value?.has_secret) return 'danger'
  return config.value.secret_from === 'database' ? 'success' : 'warning'
})

const secretHint = computed(() => {
  if (readOnly.value) {
    return t('dingtalkAdmin.fields.secretManaged', {
      source: t(`dingtalkAdmin.source.${config.value?.config_source || 'file'}`),
    })
  }
  if (config.value?.secret_from === 'database' && config.value.has_secret) {
    return t('dingtalkAdmin.fields.secretStored')
  }
  if (config.value?.secret_from === 'env' && config.value.has_secret) {
    return t('dingtalkAdmin.fields.secretFromEnv')
  }
  return t('dingtalkAdmin.fields.secretMissing')
})

const testMessage = computed(() => {
  if (!testResult.value) return ''
  const result = testResult.value
  if (result.ok) return result.message || t('dingtalkAdmin.messages.testSuccess')
  const detail = result.message || t('dingtalkAdmin.messages.testFailed')
  return result.error_code
    ? t('dingtalkAdmin.messages.testRejected', { code: result.error_code, detail })
    : detail
})

async function loadConfig() {
  loading.value = true
  try {
    config.value = await getDingTalkAdminConfig()
    replacementSecret.value = ''
    clearSecret.value = false
    testResult.value = null
  } catch (error: any) {
    MessagePlugin.error(error?.message || t('dingtalkAdmin.messages.loadFailed'))
  } finally {
    loading.value = false
  }
}

// The test button sends the form's current candidate values (including a
// just-typed secret) without saving first — the directory module's
// interaction: unsaved edits stay testable, saving stays explicit.
function draftPayload(): DingTalkConfigUpdate | null {
  const current = config.value
  if (!current) return null
  return {
    enabled: current.enabled,
    corp_id: current.corp_id.trim(),
    app_key: current.app_key.trim(),
    agent_id: current.agent_id.trim(),
    display_name: current.display_name.trim(),
    redirect_url: current.redirect_url.trim(),
    app_secret: replacementSecret.value.trim() || undefined,
    clear_secret: clearSecret.value || undefined,
  }
}

async function saveConfig() {
  const payload = draftPayload()
  if (!payload) return
  if (payload.enabled && (!payload.corp_id.trim() || !payload.app_key.trim())) {
    MessagePlugin.warning(t('dingtalkAdmin.messages.required'))
    return
  }
  saving.value = true
  try {
    config.value = await updateDingTalkAdminConfig(payload)
    replacementSecret.value = ''
    clearSecret.value = false
    MessagePlugin.success(t('dingtalkAdmin.messages.saved'))
  } catch (error: any) {
    MessagePlugin.error(error?.message || t('dingtalkAdmin.messages.saveFailed'))
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
    testResult.value = await testDingTalkConnection(payload)
    if (testResult.value.ok) MessagePlugin.success(testMessage.value)
    else MessagePlugin.error(testMessage.value)
  } catch (error: any) {
    const message = error?.message || t('dingtalkAdmin.messages.testFailed')
    testResult.value = { ok: false, message }
    MessagePlugin.error(message)
  } finally {
    testing.value = false
  }
}

async function copyRedirect() {
  if (!config.value?.redirect_url) return
  try {
    await navigator.clipboard.writeText(config.value.redirect_url)
    copied.value = true
    setTimeout(() => { copied.value = false }, 2000)
  } catch {
    MessagePlugin.error(t('dingtalkAdmin.messages.copyFailed'))
  }
}

onMounted(loadConfig)
</script>

<style scoped lang="less">
@import (reference) '@/components/css/settings-section.less';

.dingtalk-settings { width: 100%; }
.section-header { .settings-section-header(); }
.dingtalk-header { display: flex; justify-content: space-between; gap: 20px; }
.dingtalk-title-row { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.dingtalk-title-row h2 { margin: 0; }
.dingtalk-alert { margin: 12px 0; }
.loading-state { min-height: 240px; display: flex; align-items: center; justify-content: center; }
.settings-group { border: 1px solid var(--td-component-stroke); border-radius: var(--app-radius-lg); margin-bottom: 14px; overflow: hidden; }
.group-title { margin: 0; padding: 12px 16px; font-size: var(--app-text-base); background: var(--td-bg-color-secondarycontainer); }
.setting-row { display: grid; grid-template-columns: minmax(180px, 42%) minmax(260px, 1fr); gap: 20px; padding: 14px 16px; border-top: 1px solid var(--td-component-stroke); align-items: center; }
.setting-row:first-child { border-top: 0; }
.setting-row--top { align-items: start; }
.setting-info label { font-weight: 500; color: var(--td-text-color-primary); }
.setting-info .desc { margin: 4px 0 0; color: var(--td-text-color-secondary); font-size: var(--app-text-sm); line-height: 1.45; }
.setting-control { min-width: 0; }
.secret-control { display: flex; flex-direction: column; gap: 8px; }
.clear-secret { align-self: flex-end; }
.panel-actions { display: flex; justify-content: flex-end; gap: 10px; }

@media (max-width: 900px) {
  .setting-row { grid-template-columns: 1fr; gap: 8px; }
}
</style>
