import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import ts from 'typescript'
import * as vue from 'vue'

const source = readFileSync(new URL('./useDirectoryFeature.ts', import.meta.url), 'utf8')
const compiled = ts.transpileModule(source, {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
}).outputText

function feature(load: () => Promise<any>, initiallyVisible = false) {
  const exports: any = {}
  new Function('require', 'exports', compiled)(
    (name: string) => name === 'vue' ? vue : { getAuthConfig: load }, exports,
  )
  const visible = vue.ref(initiallyVisible)
  const scope = vue.effectScope()
  const state = scope.run(() => exports.useDirectoryFeature(() => visible.value))!
  return { ...state, visible, scope }
}

test('directory controls default to disabled and appear only for a successful enabled capability', async () => {
  let calls = 0
  const state = feature(async () => { calls++; return { success: true, ldap_enabled: true } })
  assert.equal(state.directoryEnabled.value, false)
  assert.equal(calls, 0)
  state.visible.value = true
  await vue.nextTick()
  await new Promise((resolve) => setImmediate(resolve))
  assert.equal(state.directoryEnabled.value, true)
  state.scope.stop()
})

test('closing an editor invalidates an in-flight capability lookup', async () => {
  let resolve!: (value: any) => void
  const state = feature(() => new Promise((done) => { resolve = done }), true)
  state.visible.value = false
  await vue.nextTick()
  resolve({ success: true, ldap_enabled: true })
  await new Promise((done) => setImmediate(done))
  assert.equal(state.directoryEnabled.value, false)
  state.scope.stop()
})

test('capability failures keep directory controls hidden', async () => {
  const state = feature(async () => ({ success: false, ldap_enabled: true }), true)
  await new Promise((done) => setImmediate(done))
  assert.equal(state.directoryEnabled.value, false)
  state.scope.stop()
})
