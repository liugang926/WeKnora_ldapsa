import assert from 'node:assert/strict'
import test from 'node:test'
import { evaluationErrorMessage } from './evaluationError'

test('evaluation shows structured HTTP and network errors without coercing objects', () => {
  assert.equal(evaluationErrorMessage({ status: 500, message: 'source lineage is unknown', error: { code: 1007 } }), 'source lineage is unknown')
  assert.equal(evaluationErrorMessage({ error: { message: 'backend unavailable' } }), 'backend unavailable')
  assert.equal(evaluationErrorMessage(new Error('Network Error')), 'Network Error')
  assert.equal(evaluationErrorMessage({ message: 'Network Error' }), 'Network Error')
  assert.equal(evaluationErrorMessage('request cancelled'), 'request cancelled')
  for (const value of [null, undefined, {}, { message: {} }, { error: {} }, '']) {
    assert.ok(evaluationErrorMessage(value).length > 0)
    assert.ok(!evaluationErrorMessage(value).includes('[object Object]'))
  }
})
