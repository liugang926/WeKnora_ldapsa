import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import { compileScript, parse } from '@vue/compiler-sfc'
import { renderToString } from '@vue/server-renderer'
import ts from 'typescript'
import * as vue from 'vue'
import { permissionCanEditKB, permissionCanManageKB } from '../../utils/kbPermission.ts'
import { hasResourceGroupEdit } from '../../utils/resourceGroupPermission.ts'

const kbSource = readFileSync(new URL('./KnowledgeBase.vue', import.meta.url), 'utf8')
const agentSource = readFileSync(new URL('../agent/AgentList.vue', import.meta.url), 'utf8')
function scriptOf(source: string) { return parse(source).descriptor.scriptSetup!.content }
function selectedDeclarations(source: string, names: string[]) {
  const file = ts.createSourceFile('permissions.ts', scriptOf(source), ts.ScriptTarget.Latest, true, ts.ScriptKind.TS)
  return file.statements.filter((statement) => {
    if (ts.isVariableStatement(statement)) return statement.declarationList.declarations.some((entry) => names.includes(entry.name.getText(file)))
    return ts.isFunctionDeclaration(statement) && !!statement.name && names.includes(statement.name.text)
  }).map((statement) => statement.getText(file)).join('\n')
}
function kbAccess(groupPermission?: string, role = 'viewer', shared = false, sharePermission = '', creator = false) {
  const names = ['canLegacyEdit', 'hasGroupEdit', 'canEdit', 'canManage', 'canEditSettings', 'canMutateKnowledge', 'canBatchEditKnowledge', 'canDownloadKnowledge']
  const source = selectedDeclarations(kbSource, names)
  return new Function('computed', 'kbInfo', 'effectiveKBPermission', 'isViaShare', 'isOwner', 'authStore', 'orgStore', 'kbId',
    'permissionCanEditKB', 'permissionCanManageKB', 'hasResourceGroupEdit', `${source}; return { ${names.map(name => `${name}: ${name}.value`).join(',')} }`
  )(vue.computed, vue.ref({ group_access_permission: groupPermission }), vue.ref(sharePermission), vue.ref(shared), vue.ref(creator),
    { hasRole: (floor: string) => ['viewer', 'contributor', 'admin', 'owner'].indexOf(role) >= ['viewer', 'contributor', 'admin', 'owner'].indexOf(floor) },
    { canEditKB: () => false, canManageKB: () => false }, vue.ref('kb'), permissionCanEditKB, permissionCanManageKB, hasResourceGroupEdit)
}

async function renderFile(file: string, props: Record<string, unknown>) {
  const filename = new URL(file, import.meta.url)
  const descriptor = parse(readFileSync(filename, 'utf8'), { filename: filename.pathname }).descriptor
  const source = compileScript(descriptor, { id: 'group-edit-visibility', inlineTemplate: true }).content
  const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText
  const exports: any = {}
  new Function('require', 'exports', compiled)((name: string) => {
    if (name === 'vue') return vue
    if (name === 'vue-i18n') return { useI18n: () => ({ t: (key: string) => key }) }
    return { default: { render: () => null } }
  }, exports)
  const app = vue.createSSRApp(exports.default, props)
  app.config.globalProperties.$t = (key: string) => key
  app.config.warnHandler = () => {}
  const wrapper = { setup: (_props: unknown, { slots }: any) => () => vue.h('div', slots.default?.()) }
  for (const name of ['t-icon', 't-tooltip', 't-popconfirm', 't-button', 't-popup']) app.component(name, wrapper)
  return renderToString(app)
}

test('a group Viewer editor can edit and configure a KB without delete, move or management privileges', () => {
  const access = kbAccess('edit')
  assert.equal(access.canEdit, true)
  assert.equal(access.canEditSettings, true)
  assert.equal(access.canBatchEditKnowledge, true)
  assert.equal(access.canDownloadKnowledge, true)
  assert.equal(access.canLegacyEdit, false)
  assert.equal(access.canManage, false)
  assert.equal(access.canMutateKnowledge, false)
})

test('read grants, absent projections and read-only cross-space shares keep their original editing rules', () => {
  for (const projection of [undefined, 'read']) {
    assert.equal(kbAccess(projection).canEdit, false)
    assert.equal(kbAccess(projection).canDownloadKnowledge, false)
  }
  assert.equal(kbAccess(undefined, 'admin').canManage, true)
  assert.equal(kbAccess(undefined, 'admin', true, 'viewer').canEdit, false)
  assert.equal(kbAccess(undefined, 'contributor').canDownloadKnowledge, true)
  const downgradedCreator = kbAccess('read', 'contributor', false, '', true)
  assert.equal(downgradedCreator.canEdit, false)
  assert.equal(downgradedCreator.canManage, false)
  assert.equal(downgradedCreator.canEditSettings, false)
  assert.equal(downgradedCreator.canDownloadKnowledge, false)
  assert.equal(kbAccess(undefined, 'contributor', false, '', true).canManage, true)
})

test('the actual document action menu shows group-editor edit/reparse/folder actions and hides delete and cross-KB move', async () => {
  const access = kbAccess('edit')
  const html = await renderFile('./components/DocumentActionMenu.vue', {
    item: { id: 'doc', type: 'manual', parse_status: 'success' }, canDownload: access.canDownloadKnowledge,
    canMutateKnowledge: access.canMutateKnowledge, canDelete: access.canLegacyEdit, canBatchEdit: access.canBatchEditKnowledge, traceVisible: false,
  })
  for (const label of ['knowledgeBase.editDocument', 'knowledgeBase.rebuildDocument', 'knowledgeBase.moveToFolder.action', 'menu.batchManage']) assert.ok(html.includes(label), label)
  assert.ok(!html.includes('knowledgeBase.deleteDocument'))
  assert.ok(!html.includes('knowledgeBase.moveDocument'))
  assert.ok(html.includes('common.download'))
})

test('the actual batch bar shows group-editor reparse and tags while keeping destructive batch deletion hidden', async () => {
  const access = kbAccess('edit')
  const html = await renderFile('./components/DocumentBatchBar.vue', { count: 1, canEdit: access.canBatchEditKnowledge, canMutate: access.canMutateKnowledge, canDownload: access.canDownloadKnowledge })
  assert.ok(html.includes('knowledgeBase.rebuildDocument'))
  assert.ok(html.includes('knowledgeBase.batchTag'))
  assert.ok(!html.includes('knowledgeBase.batchDelete'))
  assert.ok(html.includes('knowledgeBase.batchDownload'))
})

test('Agent list editing uses group edit without upgrading its delete/share management predicate', () => {
  const source = ts.transpileModule(selectedDeclarations(agentSource, ['canManageAgent', 'canEditAgent']), {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.None },
  }).outputText
  const access = new Function('authStore', 'hasResourceGroupEdit', `${source};return { canManageAgent, canEditAgent }`)(
    { user: { id: 'viewer' }, hasRole: () => false }, hasResourceGroupEdit,
  )
  const agent = { created_by: 'someone-else', is_builtin: false, group_access_permission: 'edit' }
  assert.equal(access.canEditAgent(agent), true)
  assert.equal(access.canManageAgent(agent), false)
  assert.equal(access.canEditAgent({ ...agent, group_access_permission: 'use' }), false)
  assert.equal(access.canEditAgent({ ...agent, is_builtin: true }), false)
  assert.equal(access.canEditAgent({ ...agent, created_by: 'viewer', group_access_permission: 'use' }), false)
  assert.equal(access.canManageAgent({ ...agent, created_by: 'viewer' }), false)
})
