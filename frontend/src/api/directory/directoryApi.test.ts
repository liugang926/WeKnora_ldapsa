import assert from 'node:assert/strict'
import test from 'node:test'

test('directory pagination preserves names and empty searches', () => {
  assert.equal(buildDirectorySearchQuery('', 20, 20), 'limit=20&offset=20')
  const params = new URLSearchParams(buildDirectorySearchQuery(' 张三 ', 50, 100))
  assert.equal(params.get('q'), '张三')
  assert.equal(params.get('offset'), '100')
  assert.equal(buildDirectorySearchQuery('', 20, -1), 'limit=20')
})

import { buildDirectorySearchQuery } from './query.ts'

test('directory search query trims input and bounds result size', () => {
  assert.equal(buildDirectorySearchQuery(' alice ', 500), 'q=alice&limit=100')
  assert.equal(buildDirectorySearchQuery(' ', 0), 'limit=20')
})
