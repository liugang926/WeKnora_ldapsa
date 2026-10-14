import test from 'node:test'
import assert from 'node:assert/strict'
import { resolveStorageInitialSelection } from './kbStorageInitialSelection'

test('a locked legacy KB displays the fallback without emitting a new binding', () => {
  assert.deepEqual(resolveStorageInitialSelection('', 'current-default', ['current-default'], true), {
    displayID: 'current-default', persistID: null,
  })
})

test('a locked KB with an existing binding keeps it without emitting a change', () => {
  assert.deepEqual(resolveStorageInitialSelection('original', 'current-default', ['current-default'], true), {
    displayID: 'original', persistID: null,
  })
})

test('an empty KB can adopt the configured default or first active backend', () => {
  assert.deepEqual(resolveStorageInitialSelection('', 'current-default', ['first'], false), {
    displayID: 'current-default', persistID: 'current-default',
  })
  assert.deepEqual(resolveStorageInitialSelection('', '', ['first'], false), {
    displayID: 'first', persistID: 'first',
  })
})

test('no active backend produces no binding', () => {
  assert.deepEqual(resolveStorageInitialSelection('', '', [], false), {
    displayID: '', persistID: null,
  })
})
