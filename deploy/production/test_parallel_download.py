import pathlib
import re
import socket
import tempfile
import threading
import unittest
import urllib.error
from unittest.mock import patch

from parallel_download import DownloadError, download_parallel


class Response:
    def __init__(self, body, start, end, total, status=206, content_range=None, read_hook=None):
        self.status = status
        self.headers = {'Content-Range': content_range or f'bytes {start}-{end}/{total}'}
        self.body = body
        self.read_hook = read_hook
        self.read_limits = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self, limit):
        self.read_limits.append(limit)
        if self.read_hook:
            self.read_hook()
        return self.body[:limit]


class ParallelDownloadTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.destination = pathlib.Path(self.tmp.name) / 'images.tar.gz.part00'
        self.url = 'https://example.invalid/immutable-asset?signature=do-not-log'
        self.data = b'abcdefghijklmn'
        self.requests = []
        self.responses = []
        self.lock = threading.Lock()
        sleep_patch = patch('parallel_download.time.sleep')
        self.sleep = sleep_patch.start()
        self.addCleanup(sleep_patch.stop)

    def network(self, request, *, timeout):
        self.assertEqual(request.full_url, self.url)
        self.assertEqual(request.get_header('Accept-encoding'), 'identity')
        self.assertEqual(timeout, 60)
        match = re.fullmatch(r'bytes=(\d+)-(\d+)', request.get_header('Range'))
        self.assertIsNotNone(match)
        start, end = map(int, match.groups())
        self.assertGreaterEqual(start, 0)
        self.assertLess(end, len(self.data))
        self.assertLessEqual(end - start + 1, 4)
        with self.lock:
            self.requests.append((start, end))
        response = Response(self.data[start:end + 1], start, end, len(self.data))
        with self.lock:
            self.responses.append(response)
        return response

    def test_multiple_chunks_download_concurrently_and_assemble_in_order(self):
        self.data = b'abcdefghijkl'
        barrier = threading.Barrier(3)
        active = 0
        maximum = 0

        def read_hook():
            nonlocal active, maximum
            with self.lock:
                active += 1
                maximum = max(maximum, active)
            try:
                barrier.wait(timeout=5)
            finally:
                with self.lock:
                    active -= 1

        def network(request, *, timeout):
            response = self.network(request, timeout=timeout)
            response.read_hook = read_hook
            return response

        with patch('parallel_download.urllib.request.urlopen', side_effect=network):
            download_parallel(self.url, self.destination, len(self.data), workers=3, chunk_size=4)
        self.assertEqual(maximum, 3)
        self.assertCountEqual(self.requests, [(0, 3), (4, 7), (8, 11)])
        self.assertEqual(self.destination.read_bytes(), self.data)
        self.assertFalse(self.destination.with_name(self.destination.name + '.chunks').exists())
        self.assertTrue(all(response.read_limits == [5] for response in self.responses))

    def test_existing_partial_imports_only_complete_chunks(self):
        self.destination.write_bytes(b'abcdef')
        with patch('parallel_download.urllib.request.urlopen', side_effect=self.network):
            download_parallel(self.url, self.destination, len(self.data), workers=3, chunk_size=4)
        self.assertCountEqual(self.requests, [(4, 7), (8, 11), (12, 13)])
        self.assertEqual(self.destination.read_bytes(), self.data)
        self.assertCountEqual([response.read_limits for response in self.responses], [[5], [5], [3]])

    def test_invalid_status_or_content_range_is_rejected_before_body_read(self):
        self.data = b'abcd'
        for status, header in [(200, 'bytes 0-3/4'), (206, 'bytes 1-3/4'),
                               (206, 'bytes 0-3/999'), (206, 'bytes 0-4/4')]:
            with self.subTest(status=status, header=header):
                responses = []

                def invalid(request, *, timeout):
                    response = Response(self.data, 0, 3, 4, status=status, content_range=header)
                    responses.append(response)
                    return response

                with patch('parallel_download.urllib.request.urlopen', side_effect=invalid) as network:
                    with self.assertRaises(DownloadError):
                        download_parallel(self.url, self.destination, 4, workers=1, chunk_size=4)
                self.assertEqual(network.call_count, 3)
                self.assertTrue(all(not response.read_limits for response in responses))
                self.assertFalse(self.destination.exists())
                self.assertFalse(list(pathlib.Path(self.tmp.name).rglob('*.part')))

    def test_short_or_oversized_body_is_rejected_with_bounded_reads(self):
        self.data = b'abcd'
        for body in [b'abc', b'abcdefghi']:
            with self.subTest(body=body):
                responses = []

                def invalid(request, *, timeout):
                    response = Response(body, 0, 3, 4)
                    responses.append(response)
                    return response

                with patch('parallel_download.urllib.request.urlopen', side_effect=invalid) as network:
                    with self.assertRaisesRegex(DownloadError, 'wrong size'):
                        download_parallel(self.url, self.destination, 4, workers=1, chunk_size=4)
                self.assertEqual(network.call_count, 3)
                self.assertTrue(all(response.read_limits == [5] for response in responses))
                self.assertFalse(self.destination.exists())
                self.assertFalse(list(pathlib.Path(self.tmp.name).rglob('*.part')))

    def test_failure_retains_partial_and_complete_chunks_for_retry(self):
        # Seed one full block and an incomplete second block. A successful
        # first missing block must survive a later block's failed retries.
        self.destination.write_bytes(b'abcdef')

        def fail_last_chunk(request, *, timeout):
            response = self.network(request, timeout=timeout)
            if request.get_header('Range') == 'bytes=12-13':
                response.status = 200
            return response

        with patch('parallel_download.urllib.request.urlopen', side_effect=fail_last_chunk):
            with self.assertRaises(DownloadError):
                download_parallel(self.url, self.destination, len(self.data), workers=1, chunk_size=4)
        self.assertEqual(self.destination.read_bytes(), b'abcdef')
        cached = sorted(pathlib.Path(self.tmp.name).rglob('*.part'))
        self.assertEqual([path.read_bytes() for path in cached], [b'abcd', b'efgh', b'ijkl'])
        self.assertEqual(self.requests.count((12, 13)), 3)
        self.requests.clear()

        with patch('parallel_download.urllib.request.urlopen', side_effect=self.network):
            download_parallel(self.url, self.destination, len(self.data), workers=3, chunk_size=4)
        self.assertEqual(self.requests, [(12, 13)])
        self.assertEqual(self.destination.read_bytes(), self.data)
        self.assertFalse(list(pathlib.Path(self.tmp.name).rglob('.chunk-*')))
        self.assertFalse(list(pathlib.Path(self.tmp.name).glob('*.assembling-*')))

    def test_network_failure_is_sanitized_without_url_or_token(self):
        failure = urllib.error.URLError(self.url)
        with patch('parallel_download.urllib.request.urlopen', side_effect=failure):
            with self.assertRaises(DownloadError) as caught:
                download_parallel(self.url, self.destination, 4, workers=1, chunk_size=4)
        self.assertNotIn(self.url, str(caught.exception))
        self.assertNotIn('do-not-log', str(caught.exception))
        self.assertTrue(caught.exception.__suppress_context__)

    def test_failed_middle_chunk_does_not_stop_later_chunks_or_discard_cache(self):
        self.destination.write_bytes(b'a')

        def fail_middle(request, *, timeout):
            response = self.network(request, timeout=timeout)
            if request.get_header('Range') == 'bytes=4-7':
                response.status = 503
            return response

        with patch('parallel_download.urllib.request.urlopen', side_effect=fail_middle):
            with self.assertRaisesRegex(DownloadError, '1 chunks failed: Chunk 1.*HTTP 503'):
                download_parallel(self.url, self.destination, len(self.data), workers=1, chunk_size=4)
        self.assertEqual(self.destination.read_bytes(), b'a')
        self.assertEqual(self.requests, [(0, 3), (4, 7), (4, 7), (4, 7), (8, 11), (12, 13)])
        cached = sorted(pathlib.Path(self.tmp.name).rglob('*.part'))
        self.assertEqual([path.read_bytes() for path in cached], [b'abcd', b'ijkl', b'mn'])
        self.requests.clear()

        with patch('parallel_download.urllib.request.urlopen', side_effect=self.network):
            download_parallel(self.url, self.destination, len(self.data), workers=3, chunk_size=4)
        self.assertEqual(self.requests, [(4, 7)])
        self.assertEqual(self.destination.read_bytes(), self.data)

    def test_network_error_categories_never_expose_exception_text(self):
        cases = [
            (urllib.error.HTTPError(self.url, 502, self.url, None, None), 'HTTP 502'),
            (socket.timeout(self.url), 'request timeout'),
            (urllib.error.URLError(socket.timeout(self.url)), 'URLError timeout'),
            (urllib.error.URLError(PermissionError(self.url)), 'URLError reason type: PermissionError'),
            (urllib.error.URLError(self.url), 'URLError reason type: str'),
            (PermissionError(self.url), 'OSError type: PermissionError'),
            (RuntimeError(self.url), 'exception type: RuntimeError'),
        ]
        for failure, category in cases:
            with self.subTest(category=category):
                with patch('parallel_download.urllib.request.urlopen', side_effect=failure) as network:
                    with self.assertRaises(DownloadError) as caught:
                        download_parallel(self.url, self.destination, 4, workers=1, chunk_size=4)
                self.assertEqual(network.call_count, 3)
                self.assertIn(category, str(caught.exception))
                self.assertNotIn(self.url, str(caught.exception))
                self.assertNotIn('do-not-log', str(caught.exception))
                self.assertTrue(caught.exception.__suppress_context__)

    def test_existing_complete_destination_needs_no_requests(self):
        self.destination.write_bytes(self.data)
        with patch('parallel_download.urllib.request.urlopen') as network:
            download_parallel(self.url, self.destination, len(self.data), chunk_size=4)
        network.assert_not_called()

    def test_invalid_bounds_are_rejected_without_requests(self):
        for options in [dict(expected_size=-1), dict(expected_size=4, workers=0),
                        dict(expected_size=4, chunk_size=0)]:
            with self.subTest(options=options):
                with patch('parallel_download.urllib.request.urlopen') as network:
                    with self.assertRaises(ValueError):
                        download_parallel(self.url, self.destination, **options)
                network.assert_not_called()


if __name__ == '__main__':
    unittest.main()
