import assert from 'node:assert/strict'
import test from 'node:test'
import { nextcloudOriginalUrl } from './nextcloudOriginalUrl.ts'

test('uses only the matching human Files route, never a machine content URL', () => {
  const metadata = {
    nextcloud_file_id: '77',
    nextcloud_human_url: 'https://files.example/index.php/f/77',
    url: 'https://files.example/index.php/apps/integration_weknora/api/v1/bindings/b/files/77/content',
  }
  assert.equal(nextcloudOriginalUrl(metadata), metadata.nextcloud_human_url)
  for (const humanURL of [
    'javascript:alert(1)', 'https://files.example/index.php/f/78',
    'https://user:password@files.example/index.php/f/77',
    'https://files.example/index.php/f/77?token=secret',
    metadata.url,
  ]) {
    assert.equal(nextcloudOriginalUrl({ ...metadata, nextcloud_human_url: humanURL }), '')
  }
})
