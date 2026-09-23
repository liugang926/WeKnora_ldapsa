import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const source = readFileSync(new URL('./DirectorySettings.vue', import.meta.url), 'utf8')
const browserSource = readFileSync(new URL('./DirectoryBrowser.vue', import.meta.url), 'utf8')

test('directory settings never hydrate or resend the stored bind password', () => {
  assert.match(source, /const replacementPassword = ref\(''\)/)
  assert.match(source, /replacementPassword\.value = ''/)
  assert.doesNotMatch(source, /config\.value\.bind_password/)
})

test('deployment-managed fields use the server read-only metadata', () => {
  assert.match(source, /config\.value\?\.read_only_fields/)
  assert.match(source, /fieldReadOnly\('bind_password'\)/)
  assert.match(source, /:disabled="fieldReadOnly\('servers'\)"/)
})

test('directory administration exposes test, search, preview, sync and run history', () => {
  for (const action of ['runConnectionTest', 'loadPreview', 'runSync', 'loadRuns']) {
    assert.match(source, new RegExp(action))
  }
  assert.match(source, /<DirectoryBrowser/)
  assert.match(browserSource, /resetSearch/)
  assert.match(browserSource, /searchDirectoryUsers/)
  assert.match(browserSource, /searchDirectoryGroups/)
})
