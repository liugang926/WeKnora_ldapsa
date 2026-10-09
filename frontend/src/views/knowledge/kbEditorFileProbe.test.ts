import test from 'node:test'
import assert from 'node:assert/strict'
import { loadKBEditorReadiness } from './kbEditorFileProbe'

test('a rejected file probe preserves KB details and locks file-sensitive edits', async () => {
  const details = { data: { id: 'kb-1', name: 'Source settings' } }
  const forbidden = Object.assign(new Error('publication unavailable'), { status: 403 })
  const loaded = await loadKBEditorReadiness(
    Promise.resolve(details), Promise.reject(forbidden),
  )
  assert.equal(loaded.kbInfo, details)
  assert.equal(loaded.hasKnowledgeFiles, true)
})

test('a successful empty count unlocks file-sensitive edits', async () => {
  const loaded = await loadKBEditorReadiness(Promise.resolve({ data: { id: 'kb-1' } }),
    Promise.resolve({ total: 0 }))
  assert.equal(loaded.hasKnowledgeFiles, false)
})

test('present or malformed counts keep file-sensitive edits locked', async () => {
  for (const result of [{ total: 1 }, { total: Number.NaN }, {}]) {
    const loaded = await loadKBEditorReadiness(Promise.resolve({ data: { id: 'kb-1' } }),
      Promise.resolve(result))
    assert.equal(loaded.hasKnowledgeFiles, true)
  }
})

test('a KB details failure still rejects the editor load', async () => {
  await assert.rejects(loadKBEditorReadiness(Promise.reject(new Error('KB unavailable')),
    Promise.resolve({ total: 0 })), /KB unavailable/)
})
