import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { fileURLToPath } from 'node:url'
import { runInNewContext } from 'node:vm'
import test from 'node:test'
import { compileScript, parse } from '@vue/compiler-sfc'
import ts from 'typescript'
import { createRenderer, h, nextTick, reactive, ref } from 'vue'

const require = createRequire(import.meta.url)
const filename = fileURLToPath(new URL('./DataSourceEditorDialog.vue', import.meta.url))
const { descriptor } = parse(readFileSync(filename, 'utf8'), { filename })
const script = compileScript(descriptor, { id: 'datasource-schedule-test' }).content
  .replace('__expose();', '')
  .replace('return __returned__', '__expose(__returned__); return __returned__')
const compiled = ts.transpileModule(script, {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
}).outputText

async function fixture(schedule: string | null = '0 0 */6 * * *') {
  const saves: Array<{ method: string; payload: any }> = []
  const api = {
    async updateDataSource(_id: string, payload: any) {
      saves.push({ method: 'update', payload: JSON.parse(JSON.stringify(payload)) })
    },
    async createDataSource(payload: any) {
      saves.push({ method: 'create', payload: JSON.parse(JSON.stringify(payload)) })
      return { id: 'created-source' }
    },
    async triggerSync() {},
  }
  const props = reactive({
    visible: false, kbId: 'kb-one',
    dataSource: schedule === null ? null : {
      id: 'source-one', name: 'GitLab', type: 'gitlab',
      credentials: { credentials: { configured: true } },
      config: { resource_ids: [], settings: { projects: [{ project_id: '123', paths: [] }] } },
      sync_schedule: schedule, sync_mode: 'incremental',
      conflict_strategy: 'overwrite', sync_deletions: true,
    },
  })
  const exports: any = {}
  runInNewContext(compiled, {
    exports,
    require(name: string) {
      if (name === 'vue') return require('vue')
      if (name === 'vue-i18n') return { useI18n: () => ({ t: (key: string) => key }) }
      if (name === 'tdesign-vue-next') return { MessagePlugin: { warning() {}, success() {}, error() {} } }
      if (name === '@/api/datasource') return api
      return { default: {} }
    },
    URL, console,
  })
  const component = exports.default
  component.render = () => null
  const renderer = createRenderer<any, any>({
    createElement: () => ({}), createText: () => ({}), createComment: () => ({}),
    insert() {}, remove() {}, setElementText() {}, setText() {}, patchProp() {},
    parentNode: () => null, nextSibling: () => null,
  })
  const instance = ref<any>()
  const app = renderer.createApp({ render: () => h(component, { ...props, ref: instance }) })
  app.mount({})
  props.visible = true
  await nextTick()
  return { vm: instance.value, saves, close: () => app.unmount() }
}

for (const schedule of ['0 */30 * * * *', '0 0 * * * *', '0 0 */6 * * *', '0 0 */12 * * *', '0 0 2 * * *']) {
  test(`selecting preset ${schedule} saves the chosen schedule`, async () => {
    const f = await fixture('0 45 9 * * *')
    try {
      f.vm.onScheduleChange(schedule)
      assert.equal(f.vm.form.sync_schedule, schedule)
      await f.vm.handleSubmit()
      assert.equal(f.saves.length, 1)
      assert.equal(f.saves[0].payload.sync_schedule, schedule)
    } finally { f.close() }
  })
}

test('custom daily time can be changed and switched back to a normal preset', async () => {
  const f = await fixture()
  try {
    f.vm.onScheduleChange('__daily_custom__')
    assert.equal(f.vm.form.sync_schedule, '0 0 1 * * *')
    f.vm.onDailyTimeChange('23:59')
    assert.equal(f.vm.form.sync_schedule, '0 59 23 * * *')
    f.vm.onScheduleChange('0 */30 * * * *')
    assert.equal(f.vm.form.sync_schedule, '0 */30 * * * *')
    f.vm.onScheduleChange('__daily_custom__')
    assert.equal(f.vm.form.sync_schedule, '0 59 23 * * *')
    f.vm.onDailyTimeChange('00:00')
    await f.vm.handleSubmit()
    assert.equal(f.saves[0].payload.sync_schedule, '0 0 0 * * *')
  } finally { f.close() }
})

test('editing a saved daily schedule hydrates its time and preserves its payload', async () => {
  const f = await fixture('0 7 8 * * *')
  try {
    assert.equal(f.vm.dailyCustomTime, '08:07')
    f.vm.onScheduleChange('0 0 */12 * * *')
    f.vm.onScheduleChange('__daily_custom__')
    assert.equal(f.vm.form.sync_schedule, '0 7 8 * * *')
    await f.vm.handleSubmit()
    assert.equal(f.saves[0].payload.sync_schedule, '0 7 8 * * *')
  } finally { f.close() }
})

test('a new source saves the selected schedule in its create payload', async () => {
  const f = await fixture(null)
  try {
    f.vm.onScheduleChange('0 */30 * * * *')
    await f.vm.handleSubmit()
    assert.equal(f.saves.length, 1)
    assert.equal(f.saves[0].method, 'create')
    assert.equal(f.saves[0].payload.sync_schedule, '0 */30 * * * *')
  } finally { f.close() }
})
