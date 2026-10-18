import io, pathlib, tempfile, unittest
from unittest.mock import patch
from update import download_asset
class MetadataDownloadTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=pathlib.Path(self.tmp.name)/'manifest.json'
    def test_metadata_is_always_refetched(self):
        self.path.write_bytes(b'old!')
        with patch('update.get',return_value=io.BytesIO(b'new!')) as get:
            download_asset('https://example.com/asset',self.path,4)
        get.assert_called_once();self.assertEqual(self.path.read_bytes(),b'new!')
    def test_read_is_bounded_to_expected_plus_one(self):
        response=io.BytesIO(b'valid')
        with patch('update.get',return_value=response),patch.object(response,'read',wraps=response.read) as read:
            download_asset('https://example.com/asset',self.path,5)
            read.assert_called_once_with(6)
    def test_short_or_oversized_metadata_does_not_replace_previous_file(self):
        for content in [b'short',b'too-long']:
            self.path.write_bytes(b'previous')
            with patch('update.get',side_effect=lambda _:io.BytesIO(content)),patch('update.time.sleep'):
                with self.assertRaisesRegex(RuntimeError,'Metadata download failed'):
                    download_asset('https://example.com/asset',self.path,7)
            self.assertEqual(self.path.read_bytes(),b'previous')
    def test_network_failure_is_bounded_and_preserves_existing_metadata(self):
        self.path.write_bytes(b'previous')
        with patch('update.get',side_effect=OSError('network')) as get,patch('update.time.sleep'):
            with self.assertRaises(RuntimeError): download_asset('https://example.com/asset',self.path,7)
            self.assertEqual(get.call_count,3)
        self.assertEqual(self.path.read_bytes(),b'previous')
    def test_unbounded_metadata_size_is_rejected_without_network(self):
        with patch('update.get') as get:
            with self.assertRaises(ValueError): download_asset('https://example.com/asset',self.path,16*1024*1024+1)
            get.assert_not_called()
