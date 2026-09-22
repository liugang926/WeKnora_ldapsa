import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const settings = readFileSync(new URL('./Settings.vue', import.meta.url), 'utf8')
const access = readFileSync(new URL('../../config/settingsAccess.ts', import.meta.url), 'utf8')
const router = readFileSync(new URL('../../router/index.ts', import.meta.url), 'utf8')

test('directory administration is system-admin-only and routable', () => {
  assert.match(access, /SYSTEM_ADMIN_SETTINGS_SECTIONS[\s\S]*'directory'/)
  assert.match(settings, /currentSection === 'directory'/)
  assert.match(settings, /<DirectorySettings/)
  assert.match(router, /system\/directory/)
})
