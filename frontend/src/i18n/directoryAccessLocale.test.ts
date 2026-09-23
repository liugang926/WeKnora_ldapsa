import assert from 'node:assert/strict'
import test from 'node:test'
import enUS from './locales/en-US'
import zhCN from './locales/zh-CN'
import jaJP from './locales/ja-JP'
import koKR from './locales/ko-KR'
import ruRU from './locales/ru-RU'

test('all five locales expose the directory and group-access namespaces', () => {
  for (const locale of [enUS, zhCN, jaJP, koKR, ruRU]) {
    assert.equal(typeof locale.directoryAdmin.login.identifier, 'string')
    assert.deepEqual(Object.keys(locale.directoryGroups.roles), ['viewer', 'contributor', 'admin'])
    assert.equal(typeof locale.groupAccess.sources.nested, 'string')
  }
})
