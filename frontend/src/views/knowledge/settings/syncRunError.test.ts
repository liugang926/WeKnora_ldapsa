import assert from 'node:assert/strict'
import test from 'node:test'
import type { DataSource, SyncLog } from '@/api/datasource'
import zh from '../../../i18n/locales/zh-CN'
import { dataSourceError, syncRunError } from './syncRunError'

const required = 'dingtalk_schedule_authorization_required'
const revoked = 'dingtalk_schedule_authorization_revoked'
const t = (key: string) => Object.entries(zh.datasource.syncError)
  .find(([code]) => key === `datasource.syncError.${code}`)?.[1] || key

function source(error: string, latestError: string, status: SyncLog['status'] = 'failed') {
  return {
    type: 'dingtalk', status: status === 'success' ? 'active' : 'error', error_message: error,
    latest_sync_log: { status, error_message: latestError },
  } as DataSource
}

test('schedule authorization errors explain how to restore scheduling on cards and in history', () => {
  for (const code of [required, revoked]) {
    const explanation = syncRunError(code, t)
    assert.match(explanation, /重新保存定时设置/)
    assert.equal(dataSourceError(source('', code), t), explanation)
    assert.equal(dataSourceError(source(code, code), t), explanation)
  }
  assert.notEqual(syncRunError(required, t), syncRunError(revoked, t))
})

test('ordinary failures stay unchanged and later runs do not inherit an old schedule warning', () => {
  const ordinaryError = 'DingTalk synchronization could not complete; retry and check the server log'
  assert.equal(syncRunError(ordinaryError, t), ordinaryError)
  assert.equal(dataSourceError(source(ordinaryError, ordinaryError), t), ordinaryError)
  assert.equal(dataSourceError(source(required, ordinaryError), t), '')
  assert.equal(dataSourceError(source(revoked, '', 'success'), t), '')
  assert.equal(dataSourceError(source(revoked, '', 'running'), t), '')
  assert.equal(syncRunError(required + ' (retry)', t), required + ' (retry)')
})
