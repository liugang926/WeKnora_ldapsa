import hashlib,pathlib,tempfile,unittest
from update import cached_asset_valid,read_checksums
class ChecksumCacheTests(unittest.TestCase):
 def test_only_matching_content_is_reusable(self):
  with tempfile.TemporaryDirectory() as directory:
   p=pathlib.Path(directory)/'deployment.tar.gz';p.write_bytes(b'release')
   good=hashlib.sha256(b'release').hexdigest()
   self.assertTrue(cached_asset_valid(p,good))
   p.write_bytes(b'corrupt')
   self.assertFalse(cached_asset_valid(p,good))
   self.assertFalse(cached_asset_valid(p,None))
   self.assertFalse(cached_asset_valid(p.with_name('missing'),good))
 def test_checksum_records_reject_invalid_digests_and_paths(self):
  with tempfile.TemporaryDirectory() as directory:
   p=pathlib.Path(directory)/'SHA256SUMS'
   for value in ['x  deployment.tar.gz\n','a'*64+'  ../deployment.tar.gz\n']:
    p.write_text(value)
    with self.assertRaises(RuntimeError):read_checksums(p)
   p.write_text('a'*64+'  deployment.tar.gz\n')
   self.assertEqual(read_checksums(p),{'deployment.tar.gz':'a'*64})
