import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const login = readFileSync(new URL('./Login.vue', import.meta.url), 'utf8')
const api = readFileSync(new URL('../../api/auth/index.ts', import.meta.url), 'utf8')
const request = readFileSync(new URL('../../utils/request.ts', import.meta.url), 'utf8')

test('LDAP login uses its public endpoint and an identifier', () => {
  assert.match(api, /\/api\/v1\/auth\/ldap\/login/)
  assert.match(api, /identifier: string/)
  assert.match(request, /\/auth\/ldap\/login/)
})

test('login selector is driven by public auth config', () => {
  assert.match(login, /response\.ldap_enabled/)
  assert.match(login, /loginMode === 'ldap'/)
  assert.match(login, /ldapLogin\(\{ identifier:/)
})
