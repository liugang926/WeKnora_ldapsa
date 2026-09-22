import assert from 'node:assert/strict'
import test from 'node:test'
import { unwrapDirectoryAccessResponse } from './directory-access-response.ts'

test('directory APIs accept standard envelopes and direct payloads', () => {
  assert.deepEqual(unwrapDirectoryAccessResponse({ success: true, data: { enabled: true } }), { enabled: true })
  assert.deepEqual(unwrapDirectoryAccessResponse({ enabled: false }), { enabled: false })
})
