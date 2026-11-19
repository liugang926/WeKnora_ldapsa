import assert from 'node:assert/strict'
import test from 'node:test'
import { csvHeaderKeys, peopleCsv } from './chatUsageCsv'

const person = { user_id: '1', username: '@unsafe', display_name: '王,"一"', session_count: 1,
  question_count: 5, web_question_count: 3, dingtalk_question_count: 2,
  prompt_tokens: 0, completion_tokens: 0, total_tokens: 37, reported_turns: 1, unreported_turns: 1 }
test('exports all pages with UTF-8 BOM, literal names/accounts and unavailable counts left blank', async () => {
  const csv = await peopleCsv(['姓名'], async page => ({
    page, total: 3, page_size: 1, items: [{ ...person, user_id: String(page), ...(page === 2 ?
      { reported_turns: 0, display_name: ' =SUM(7)', username: '2' } : page === 3 ?
      { prompt_tokens: 37, completion_tokens: 0 } : {}) }],
  }))
  assert.deepEqual(csv.split('\r\n'), ['\uFEFF"姓名"', '"王,""一""","\'@unsafe","1","5","3","2","1","","","37","1"',
    '"\' =SUM(7)","2","2","5","3","2","1","","","","1"', '"王,""一""","\'@unsafe","3","5","3","2","1","37","0","37","1"', ''])
})
test('rejects incomplete or shifting pages instead of handing back a partial export', async () => {
  const first = { page: 1, page_size: 1, total: 2, items: [person] }
  for (const second of [{ ...first, page: 2 }, { ...first, page: 2, items: [] }, { ...first, page: 2, total: 3 }]) {
    await assert.rejects(peopleCsv([], async n => n === 1 ? first : second))
  }
  await assert.rejects(peopleCsv([], async n => { if (n === 1) return first; throw new Error('offline') }))
})

test('unfiltered export appends the unattributed bucket once, even when no accounts match', async () => {
  const unknown = { ...person, question_count: 3, web_question_count: 0, dingtalk_question_count: 3 }
  const pages = (size: number) => async (page: number) => ({
    page, page_size: 1, total: size, items: size ? [{ ...person, user_id: String(page) }] : [], unattributed_dingtalk: unknown,
  })
  const headers = [...csvHeaderKeys]
  for (const n of [0, 2]) {
    const csv = await peopleCsv(headers, pages(n), '未归属')
    const rows = csv.trimEnd().split('\r\n')
    assert.equal(rows.length, 2 + n)
    assert.equal(rows.at(-1), '"未归属","","","3","0","3","1","","","37","1"')
    assert.ok(!(await peopleCsv(headers, pages(n))).includes('未归属'))
  }
})
