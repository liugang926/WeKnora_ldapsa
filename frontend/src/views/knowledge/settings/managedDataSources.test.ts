import assert from 'node:assert/strict'
import test from 'node:test'
import { fixture, flush } from '../../../test-utils/vueFixture'
import { managerHref } from './managedDataSources'

test('KB settings exposes and edits only the installed WebDAV source, in the selected KB', async t => {
  const f = fixture(t)
  const kbId = 'kb & A'
  const row = { id: 'source/1', type: 'nextcloud_webdav', knowledge_base_id: kbId }
  const types = [{ type: 'nextcloud' }]
  const navigate = t.mock.fn()
  const errors = t.mock.fn()
  Object.assign(f.window, { location: { assign: navigate } })
  const { vm } = f.component(new URL('./DataSourceSettings.vue', import.meta.url), {
    'vue-i18n': { useI18n: () => ({ t: (key: string) => key }) },
    'tdesign-vue-next': { MessagePlugin: { error: errors } },
    '@/stores/auth': { useAuthStore: () => ({ hasRole: () => true }) },
    '@/composables/useConnectorLabel': { useConnectorLabel: () => ({ connectorLabel: String }) },
    '@/utils/cronHumanize': {},
    '@/api/datasource': { getConnectorTypes: async () => types, listDataSources: async () => [] },
    './managedDataSources': { managerHref },
  }, { kbId })
  vm.openCreate()
  await flush()
  assert.equal(vm.editorVisible, true)
  assert.equal(vm.managerLinks.length, 0)
  await vm.openEdit(row)
  assert.equal(navigate.mock.callCount(), 0)
  assert.equal(errors.mock.callCount(), 1)

  types.push({ type: 'nextcloud_webdav' })
  vm.openCreate()
  await flush()
  assert.equal(vm.managerLinks.length, 1)
  const href = '/api/v1/extensions/nextcloud-webdav/?kb_id=kb+%26+A'
  assert.equal(vm.managerLinks[0].href, href)
  await vm.openEdit(row)
  assert.equal(navigate.mock.calls[0].arguments[0], href + '&source_id=source%2F1')
  await vm.openEdit({ ...row, knowledge_base_id: 'other' })
  assert.equal(navigate.mock.callCount(), 1)
  await vm.openEdit({ ...row, type: 'dingtalk' })
  assert.equal(vm.editingDs.type, 'dingtalk')
})
