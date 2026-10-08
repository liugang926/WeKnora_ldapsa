import test from 'node:test'
import assert from 'node:assert/strict'
import { FailedCandidateRequestGate } from './failedCandidateRequestGate'

test('load more and selected-file GET can finish in either order', () => {
  const gate = new FailedCandidateRequestGate()
  const detail = gate.beginDetail()
  const more = gate.beginList(false)
  assert.equal(gate.isCurrentDetail(detail), true)
  assert.equal(gate.isCurrentList(more), true)
  const nextDetail = gate.beginDetail()
  assert.equal(gate.isCurrentList(more), true)
  assert.equal(gate.isCurrentDetail(detail), false)
  assert.equal(gate.isCurrentDetail(nextDetail), true)
})

test('refresh and close invalidate old exact-file responses', () => {
  const gate = new FailedCandidateRequestGate()
  const detail = gate.beginDetail()
  gate.beginList(true)
  assert.equal(gate.isCurrentDetail(detail), false)
  const list = gate.beginList(false)
  gate.invalidate()
  assert.equal(gate.isCurrentList(list), false)
})
