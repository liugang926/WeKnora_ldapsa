import assert from 'node:assert/strict'
import test from 'node:test'

import { buildDirectorySearchQuery } from './query.ts'

test('directory search query trims input and bounds result size', () => {
  assert.equal(buildDirectorySearchQuery(' alice ', 500), 'q=alice&limit=100')
  assert.equal(buildDirectorySearchQuery(' ', 0), 'limit=20')
})
