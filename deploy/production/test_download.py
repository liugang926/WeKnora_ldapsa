import pathlib
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from update import download_asset


class DownloadAssetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.destination = pathlib.Path(self.tmp.name) / 'images.tar.gz.part00'
        self.url = 'https://github.com/example/repo/releases/download/prod-1/asset'

    def output_path(self, command):
        self.assertEqual(command[0], 'curl')
        self.assertEqual(command[-1], self.url)
        output = pathlib.Path(command[command.index('--output') + 1])
        self.assertEqual(output, self.destination)
        return output

    def test_complete_resumable_part_is_reused_without_curl(self):
        self.destination.write_bytes(b'complete')
        with patch('update.subprocess.run') as run:
            download_asset(self.url, self.destination, 8, resume=True)
        run.assert_not_called()
        self.assertEqual(self.destination.read_bytes(), b'complete')

    def test_partial_part_resumes_from_existing_file(self):
        self.destination.write_bytes(b'part')

        def complete(command, *, check):
            self.assertTrue(check)
            output = self.output_path(command)
            self.assertEqual(command[command.index('--continue-at') + 1], '-')
            self.assertEqual(output.read_bytes(), b'part')
            with output.open('ab') as stream:
                stream.write(b'ial')

        with patch('update.subprocess.run', side_effect=complete) as run:
            download_asset(self.url, self.destination, 7, resume=True)
        run.assert_called_once()
        self.assertEqual(self.destination.read_bytes(), b'partial')

    def test_oversized_part_is_removed_before_fresh_download(self):
        self.destination.write_bytes(b'oversized')

        def replace(command, *, check):
            self.assertTrue(check)
            output = self.output_path(command)
            self.assertNotIn('--continue-at', command)
            self.assertFalse(output.exists(), 'Oversized content must be discarded before curl')
            output.write_bytes(b'fresh')

        with patch('update.subprocess.run', side_effect=replace) as run:
            download_asset(self.url, self.destination, 5, resume=True)
        run.assert_called_once()
        self.assertEqual(self.destination.read_bytes(), b'fresh')

    def test_non_resumable_metadata_is_fetched_again_even_when_size_matches(self):
        self.destination = self.destination.with_name('manifest.json')
        self.destination.write_bytes(b'old!')
        payloads = iter([b'new1', b'new2'])

        def replace(command, *, check):
            self.assertTrue(check)
            output = self.output_path(command)
            self.assertNotIn('--continue-at', command)
            output.write_bytes(next(payloads))

        with patch('update.subprocess.run', side_effect=replace) as run:
            download_asset(self.url, self.destination, 4)
            self.assertEqual(self.destination.read_bytes(), b'new1')
            download_asset(self.url, self.destination, 4)
        self.assertEqual(run.call_count, 2)
        self.assertEqual(self.destination.read_bytes(), b'new2')

    def test_successful_curl_with_wrong_final_size_is_rejected(self):
        for content in [b'short', b'too-long']:
            with self.subTest(content=content):
                def wrong_size(command, *, check):
                    self.assertTrue(check)
                    self.output_path(command).write_bytes(content)

                with patch('update.subprocess.run', side_effect=wrong_size) as run:
                    with self.assertRaisesRegex(RuntimeError, 'Incomplete asset: images.tar.gz.part00'):
                        download_asset(self.url, self.destination, 7)
                run.assert_called_once()

    def test_curl_failure_propagates_and_preserves_partial_download(self):
        self.destination.write_bytes(b'part')
        failure = subprocess.CalledProcessError(28, ['curl'])
        with patch('update.subprocess.run', side_effect=failure):
            with self.assertRaises(subprocess.CalledProcessError):
                download_asset(self.url, self.destination, 7, resume=True)
        self.assertEqual(self.destination.read_bytes(), b'part')


if __name__ == '__main__':
    unittest.main()
