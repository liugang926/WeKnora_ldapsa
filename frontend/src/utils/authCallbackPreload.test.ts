import test from 'node:test'
import assert from 'node:assert/strict'
import { Buffer } from 'node:buffer'

import {
  authCallbackCredentialsFromHash,
  decodeAuthCallbackPayload,
  hasPendingAuthCallbackResult,
} from './authCallbackPreload'

function encodePayload(obj: Record<string, unknown>): string {
  return Buffer.from(JSON.stringify(obj)).toString('base64url')
}

test('decodeAuthCallbackPayload decodes base64url JSON with and without padding', () => {
  const payload = { success: true, token: 'tok', refresh_token: 'ref' }
  const encoded = encodePayload(payload)
  assert.deepEqual(decodeAuthCallbackPayload(encoded), payload)
  // URLSearchParams already strips '='; the decoder must tolerate it.
  const stripped = encoded.replace(/=+$/, '')
  assert.deepEqual(decodeAuthCallbackPayload(stripped), payload)
})

test('decodeAuthCallbackPayload returns null on garbage input', () => {
  assert.equal(decodeAuthCallbackPayload('%%%not-base64%%%'), null)
  assert.equal(decodeAuthCallbackPayload('bm90LWpzb24'), null)
})

test('authCallbackCredentialsFromHash extracts dingtalk credentials', () => {
  const encoded = encodePayload({ token: 'dt-token', refresh_token: 'dt-refresh' })
  const creds = authCallbackCredentialsFromHash(`#dingtalk_result=${encoded}`)
  assert.deepEqual(creds, { token: 'dt-token', refreshToken: 'dt-refresh' })
})

test('authCallbackCredentialsFromHash extracts oidc credentials', () => {
  const encoded = encodePayload({ token: 'oidc-token' })
  const creds = authCallbackCredentialsFromHash(`foo=bar&oidc_result=${encoded}`)
  assert.deepEqual(creds, { token: 'oidc-token', refreshToken: undefined })
})

test('authCallbackCredentialsFromHash ignores error hashes and tokenless payloads', () => {
  assert.equal(authCallbackCredentialsFromHash('#dingtalk_error=invalid_state'), null)
  assert.equal(authCallbackCredentialsFromHash('#oidc_error=login_failed'), null)
  assert.equal(
    authCallbackCredentialsFromHash(`#dingtalk_result=${encodePayload({ success: false })}`),
    null,
  )
  assert.equal(authCallbackCredentialsFromHash(''), null)
  assert.equal(authCallbackCredentialsFromHash('#/platform/creatChat'), null)
})

test('hasPendingAuthCallbackResult detects result hashes only', () => {
  assert.equal(hasPendingAuthCallbackResult('#dingtalk_result=abc'), true)
  assert.equal(hasPendingAuthCallbackResult('#oidc_result=abc'), true)
  assert.equal(hasPendingAuthCallbackResult('#dingtalk_error=x'), false)
  assert.equal(hasPendingAuthCallbackResult('#/login'), false)
  assert.equal(hasPendingAuthCallbackResult(''), false)
})
