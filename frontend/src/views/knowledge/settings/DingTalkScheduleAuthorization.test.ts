import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import { runInNewContext } from 'node:vm'
import { parse as parseTemplate } from '@vue/compiler-dom'
import { compileScript, parse } from '@vue/compiler-sfc'
import ts from 'typescript'
import * as Vue from 'vue'

const flush = async () => { await new Promise(resolve => setImmediate(resolve)); await Vue.nextTick() }

function load(name: string, imports: Record<string, any>) {
  let source = readFileSync(new URL(name, import.meta.url), 'utf8')
  if (name.includes('EditorDialog')) {
    // Render the real authorization control with its actual parent bindings and save handler.
    const { template } = parse(source).descriptor
    function find(node: any): string | undefined {
      return node.tag === 'DingTalkScheduleAuthorization' ? node.loc.source :
        node.children?.map(find).find(Boolean)
    }
    const control = find(parseTemplate(template!.content))
    assert.ok(control)
    source = source.replace(template!.content, control)
      .replace('</script>', 'defineExpose({ handleSubmit, form, selectedResourceIds, replaceCredentialsMode })\n</script>')
  }
  const { descriptor } = parse(source, { filename: name })
  const script = compileScript(descriptor, { id: name, inlineTemplate: true })
  const code = ts.transpileModule(script.content, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  }).outputText
  const exports: any = {}
  runInNewContext(code, {
    exports, console, require: (id: string) => id === 'vue' ? Vue : imports[id] || {},
  })
  return exports.default
}

async function editor(read: (path: string) => Promise<{ status: string }>, save: (body: any) => void, resourceIds: string[] = []) {
  const imports = {
    'vue-i18n': { useI18n: () => ({ t: (key: string) => key }) },
    'tdesign-vue-next': { MessagePlugin: { warning() {}, success() {} } },
    '@/utils/request': { get: read },
    '@/api/datasource': {
      async updateDataSource(_id: string, body: any) { save(body) },
      async putDataSourceCredentials() {},
    },
  }
  const control = load('./DingTalkScheduleAuthorization.vue', imports)
  const dialog = load('./DataSourceEditorDialog.vue', {
    ...imports, './DingTalkScheduleAuthorization.vue': { __esModule: true, default: control },
  })
  const source = Vue.reactive({
    id: 'id-a', name: 'DingTalk', type: 'dingtalk', status: 'active',
    config: { resource_ids: resourceIds, settings: {} }, sync_schedule: '0 0 8 * * *',
  })
  const visible = Vue.ref(false)
  const instance = Vue.ref<any>()
  const renderer = Vue.createRenderer<any, any>({
    createElement: () => ({}), createText: () => ({}), createComment: () => ({}),
    insert(node, parent, anchor) {
      const children = parent.children ||= []
      const index = anchor ? children.indexOf(anchor) : -1
      children.splice(index === -1 ? children.length : index, 0, node)
      node.parent = parent
    },
    remove(node) { node.parent.children.splice(node.parent.children.indexOf(node), 1) },
    parentNode: node => node.parent,
    nextSibling: node => node.parent?.children[node.parent.children.indexOf(node) + 1] || null,
    setElementText() {}, setText() {}, patchProp() {},
  })
  const app = renderer.createApp({ render: () => Vue.h(dialog, {
    visible: visible.value, 'onUpdate:visible': (v: boolean) => { visible.value = v },
    kbId: 'kb', dataSource: source, ref: instance,
  }) })
  let box: { checked: boolean; disabled: boolean; select: () => void } | undefined
  let retry: (() => void) | undefined
  app.component('t-checkbox', Vue.defineComponent({
    props: ['checked', 'disabled', 'modelValue'], emits: ['update:modelValue'],
    setup(props, { emit }) {
      Vue.onBeforeUnmount(() => { box = undefined })
      return () => {
        box = {
          checked: Boolean(props.checked ?? props.modelValue),
          disabled: props.disabled !== undefined && props.disabled !== false,
          select: () => emit('update:modelValue', true),
        }
        return Vue.h('span')
      }
    },
  }))
  app.component('t-button', Vue.defineComponent({
    setup(_, { attrs }) {
      return () => { retry = attrs.onClick as () => void; return Vue.h('button') }
    },
  }))
  app.mount({})
  visible.value = true
  await flush()
  return { box: () => box, retry: () => retry!(), submit: () => instance.value.handleSubmit(),
    source, visible, vm: instance.value, close: () => app.unmount() }
}

test('DingTalk save and reopen reads the grant and ordinary edits preserve its signer', async t => {
  let status = 'required'
  const reads: string[] = []
  const writes: any[] = []
  const f = await editor(async path => { reads.push(path); return { status } }, body => {
    writes.push(body)
    if (body.save_schedule === true) status = 'authorized'
  })
  t.after(f.close)
  assert.equal(f.box()?.checked, false)
  f.box()!.select()
  await f.submit()
  assert.equal(writes[0].save_schedule, true)
  await flush()
  assert.equal(f.visible.value, false)
  f.visible.value = true
  await flush()
  assert.equal(f.box()?.checked, true)
  assert.equal(f.box()?.disabled, true)
  assert.deepEqual(reads, Array(2).fill('/api/v1/group-access/data-sources/id-a/schedule-authorization'))
  f.vm.form.name = 'Renamed'
  await f.submit()
  assert.equal(writes[1].save_schedule, undefined)
  assert.equal(writes[1].sync_schedule, undefined)
})

test('a stale grant read cannot replace a new source’s failure or retry result', async t => {
  let finish!: (status: { status: string }) => void
  let fail = true
  const f = await editor(async path => {
    if (path.includes('/id-a/')) return await new Promise(resolve => { finish = resolve })
    if (fail) throw new Error('unavailable')
    return { status: 'revoked' }
  }, () => {})
  t.after(f.close)
  assert.equal(f.box(), undefined)
  f.visible.value = false
  await flush()
  f.source.id = 'id-b'
  f.visible.value = true
  await flush()
  finish({ status: 'authorized' })
  await flush()
  assert.equal(f.box(), undefined)
  fail = false
  f.retry()
  await flush()
  assert.equal(f.box()?.checked, false)
  assert.equal(f.box()?.disabled, false)
})

test('changing then restoring a scope preserves its existing grant, regardless of selection order', async t => {
  const writes: any[] = []
  const f = await editor(async () => ({ status: 'authorized' }), body => writes.push(body), ['a', 'b'])
  t.after(f.close)
  f.vm.selectedResourceIds = ['c', 'b']
  await flush()
  f.box()!.select()
  f.vm.selectedResourceIds = ['b', 'c']
  await flush()
  assert.equal(f.box()?.checked, true)
  assert.equal(f.box()?.disabled, false)
  f.vm.selectedResourceIds = ['c']
  await flush()
  assert.equal(f.box()?.checked, false)
  f.box()!.select()
  f.vm.selectedResourceIds = ['b', 'a']
  await flush()
  assert.equal(f.box()?.checked, true)
  assert.equal(f.box()?.disabled, true)
  f.vm.form.name = 'Renamed'
  await f.submit()
  assert.equal(writes[0].save_schedule, undefined)
  assert.equal(writes[0].sync_schedule, undefined)
})

test('explicit consent for replacement credentials survives the successful credential PUT', async t => {
  const writes: any[] = []
  const f = await editor(async () => ({ status: 'authorized' }), body => writes.push(body))
  t.after(f.close)
  f.vm.replaceCredentialsMode = true
  f.vm.form.config.credentials = { app_secret: 'new secret' }
  await flush()
  f.box()!.select()
  await f.submit()
  assert.equal(f.vm.replaceCredentialsMode, false)
  assert.equal(writes[0].save_schedule, true)
})
