import assert from 'node:assert/strict'
import test from 'node:test'
import { defaultReview, manualCoverage, manualRate, validReview, type ReviewCase, type ReviewRun } from './evaluationReview'

const answered: ReviewCase = { question_id: 1, reference_answer: '虚构参考答案' }
const unanswered: ReviewCase = { question_id: 2, reference_answer: '' }

test('review labels are required for applicable case types', () => {
  assert.deepEqual(defaultReview(answered), { faithfulness: '', citation_accuracy: '', abstention: 'not_applicable' })
  assert.deepEqual(defaultReview(unanswered), { faithfulness: 'not_applicable', citation_accuracy: 'not_applicable', abstention: '' })
  assert.equal(validReview(answered, defaultReview(answered)), false)
  assert.equal(validReview(answered, { faithfulness: 'pass', citation_accuracy: 'fail', abstention: 'not_applicable' }), true)
  assert.equal(validReview(answered, { faithfulness: 'pass', citation_accuracy: 'not_applicable', abstention: 'not_applicable' }), false)
  assert.equal(validReview(unanswered, { faithfulness: 'not_applicable', citation_accuracy: 'not_applicable', abstention: 'pass' }), true)
  assert.equal(validReview(unanswered, { faithfulness: 'pass', citation_accuracy: 'pass', abstention: 'pass' }), false)
})

test('human comparison rates stay empty until every applicable question is reviewed', () => {
  const run: ReviewRun = { task: { total: 2 }, cases: [answered, unanswered] }
  assert.equal(manualCoverage(run), '0/2')
  assert.equal(manualRate(run, 'faithfulness'), undefined)
  run.cases![0] = { ...answered, review: {
    faithfulness: 'pass', citation_accuracy: 'fail', abstention: 'not_applicable',
    reviewed_by: 'reviewer', reviewed_at: '2026-09-30T00:00:00Z',
  } }
  assert.equal(manualCoverage(run), '1/2')
  assert.equal(manualRate(run, 'faithfulness'), 1)
  assert.equal(manualRate(run, 'citation_accuracy'), 0)
  assert.equal(manualRate(run, 'abstention'), undefined)
  run.cases![1] = { ...unanswered, review: {
    faithfulness: 'not_applicable', citation_accuracy: 'not_applicable', abstention: 'fail',
    reviewed_by: 'reviewer', reviewed_at: '2026-09-30T00:00:00Z',
  } }
  assert.equal(manualRate(run, 'abstention'), 0)
  run.task.total = 3
  assert.equal(manualRate(run, 'faithfulness'), undefined, 'missing result must not become a complete score')
})
