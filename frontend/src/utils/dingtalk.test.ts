import assert from 'node:assert/strict'
import test from 'node:test'

import {
  isDingTalkContainer,
  parseDingTalkCallbackHash,
  hasPendingDingTalkCallback,
  resolveDingTalkErrorKey,
  lookupDingTalkErrorKey,
} from './dingtalk.ts'

test('parses dingtalk result and error hashes, ignoring unrelated hashes', () => {
  assert.deepEqual(parseDingTalkCallbackHash('#dingtalk_result=abc123'), { result: 'abc123' })
  assert.deepEqual(parseDingTalkCallbackHash('#dingtalk_error=no_ad_match'), { error: 'no_ad_match' })
  assert.deepEqual(parseDingTalkCallbackHash('dingtalk_result=no-hash-prefix'), { result: 'no-hash-prefix' })
  // 无关 hash（OIDC 回跳 / 普通路由锚点）必须返回空，交给原逻辑处理。
  assert.deepEqual(parseDingTalkCallbackHash('#oidc_result=xyz'), {})
  assert.deepEqual(parseDingTalkCallbackHash('#/platform/knowledge-bases'), {})
  assert.deepEqual(parseDingTalkCallbackHash(''), {})
})

test('pending-callback detection matches both success and failure hashes', () => {
  assert.equal(hasPendingDingTalkCallback('#dingtalk_result=abc'), true)
  assert.equal(hasPendingDingTalkCallback('#dingtalk_error=wrong_corp'), true)
  assert.equal(hasPendingDingTalkCallback('#oidc_result=abc'), false)
  assert.equal(hasPendingDingTalkCallback(''), false)
})

test('every backend failure reason maps to a dedicated i18n key', () => {
  const reasons = [
    'not_enterprise_member',
    'no_ad_match',
    'empty_job_number',
    'duplicate_job_number',
    'binding_conflict',
    'user_inactive',
    'invalid_state',
    'dingtalk_unavailable',
    'wrong_corp',
  ]
  for (const reason of reasons) {
    assert.equal(resolveDingTalkErrorKey(reason), `auth.dingtalkErrors.${reason}`)
    assert.equal(lookupDingTalkErrorKey(reason), `auth.dingtalkErrors.${reason}`)
  }
})

test('unknown or missing reasons fall back to the generic message', () => {
  assert.equal(resolveDingTalkErrorKey('something_new'), 'auth.dingtalkErrors.fallback')
  assert.equal(resolveDingTalkErrorKey(''), 'auth.dingtalkErrors.fallback')
  assert.equal(resolveDingTalkErrorKey(null), 'auth.dingtalkErrors.fallback')
  assert.equal(lookupDingTalkErrorKey('something_new'), null)
  assert.equal(lookupDingTalkErrorKey(undefined), null)
})

test('container detection keys off the DingTalk UA marker', () => {
  assert.equal(isDingTalkContainer('Mozilla/5.0 (iPhone; ...) DingTalk/7.6.0'), true)
  assert.equal(isDingTalkContainer('Mozilla/5.0 (Macintosh) Chrome/126.0'), false)
  assert.equal(isDingTalkContainer(''), false)
})
