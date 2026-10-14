import assert from 'node:assert/strict'
import test from 'node:test'
import { safeNextcloudAskPath } from './nextcloudAskLink.ts'

const valid = '/platform/nextcloud-ask?instance_id=nc123&binding_id=engineering&file_id=77&source_etag=v1.2%3Afoo'

test('accepts only a bounded local Nextcloud ask route', () => {
  assert.equal(safeNextcloudAskPath(valid), valid)
  for (const candidate of [
    'https://attacker.example/platform/nextcloud-ask?instance_id=nc123&binding_id=engineering&file_id=77&source_etag=v1',
    '//attacker.example/platform/nextcloud-ask?instance_id=nc123&binding_id=engineering&file_id=77&source_etag=v1',
    valid + '&knowledge_id=other',
    valid + '&file_id=78',
    valid + '#fragment',
    valid.replace('file_id=77', 'file_id=0'),
    valid.replace('binding_id=engineering', 'binding_id=%2Fother'),
  ]) {
    assert.equal(safeNextcloudAskPath(candidate), null, candidate)
  }
})
