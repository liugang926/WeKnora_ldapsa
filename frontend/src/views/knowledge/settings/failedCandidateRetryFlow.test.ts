import test from 'node:test'
import assert from 'node:assert/strict'
import { submitFailedCandidateRetry } from './failedCandidateRetryFlow'
import { FailedCandidateRequestGate } from './failedCandidateRequestGate'
import type { NextcloudFailedCandidateRetryStatus } from '@/api/datasource'

const status = {
  file_id: '77', source_etag: 'etag-77', candidate_id: 'candidate-77', state: 'manual',
  attempt_count: 3, first_staged_at: '', next_attempt_at: '', last_error_code: 'candidate_retry_exhausted',
} satisfies NextcloudFailedCandidateRetryStatus

test('409 clears the exact snapshot before POST and reloads the list', async () => {
  const calls: string[] = []
  let snapshot: NextcloudFailedCandidateRetryStatus | null = status
  const result = await submitFailedCandidateRetry(status,
    () => { snapshot = null; calls.push('clear') },
    async (submitted) => {
      assert.equal(snapshot, null)
      assert.equal(submitted.source_etag, 'etag-77')
      assert.equal(submitted.candidate_id, 'candidate-77')
      calls.push('post')
      throw { status: 409 }
    },
    async () => { calls.push('reload') },
  )
  assert.equal(result, 'changed')
  assert.deepEqual(calls, ['clear', 'post', 'reload'])
})

test('successful retry also clears the generation and reloads', async () => {
  const calls: string[] = []
  const result = await submitFailedCandidateRetry(status,
    () => { calls.push('clear') },
    async () => { calls.push('post') },
    async () => { calls.push('reload') },
  )
  assert.equal(result, 'scheduled')
  assert.deepEqual(calls, ['clear', 'post', 'reload'])
})

test('late 409 from a previous drawer source cannot refresh the new source', async () => {
  const gate = new FailedCandidateRequestGate()
  const context = gate.currentContext()
  let finishPost!: () => void
  const waiting = new Promise<void>(resolve => { finishPost = resolve })
  const calls: string[] = []
  const result = submitFailedCandidateRetry(status,
    () => { calls.push('clear-old') },
    async () => { calls.push('post-old'); await waiting; throw { status: 409 } },
    async () => { calls.push('reload-new') },
    () => gate.isCurrentContext(context),
  )
  gate.invalidate() // The drawer closed or switched data sources while POST was pending.
  finishPost()
  assert.equal(await result, 'changed')
  assert.deepEqual(calls, ['clear-old', 'post-old'])
})
