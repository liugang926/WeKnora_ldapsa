"""Download immutable release assets using bounded, resumable HTTP ranges."""

import concurrent.futures
import hashlib
import os
import pathlib
import shutil
import tempfile
import time
import urllib.request


_ATTEMPTS = 3
_REQUEST_TIMEOUT = 30


class DownloadError(RuntimeError):
    """A sanitized download failure; messages never include remote URLs."""


def _write_chunk(path, content):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(prefix='.chunk-', dir=path.parent, delete=False) as output:
            temporary = pathlib.Path(output.name)
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _valid_chunk(path, size):
    return path.is_file() and path.stat().st_size == size


def _fetch_chunk(url, path, index, start, end, total):
    size = end - start + 1
    if _valid_chunk(path, size):
        return
    reason = 'network or local I/O failure'
    for attempt in range(_ATTEMPTS):
        try:
            request = urllib.request.Request(url, headers={
                'User-Agent': 'weknora-production-updater',
                'Range': f'bytes={start}-{end}',
                'Accept-Encoding': 'identity',
            })
            with urllib.request.urlopen(request, timeout=_REQUEST_TIMEOUT) as response:
                if response.status != 206:
                    raise DownloadError('server did not return HTTP 206')
                if response.headers.get('Content-Range') != f'bytes {start}-{end}/{total}':
                    raise DownloadError('unexpected Content-Range')
                # A server ignoring the range must never make us consume an
                # unbounded body. Read one extra byte to detect oversized data.
                content = response.read(size + 1)
                if len(content) != size:
                    raise DownloadError('response body has the wrong size')
            _write_chunk(path, content)
            return
        except DownloadError as error:
            reason = str(error)
        except Exception:
            # HTTPError/URLError messages and tracebacks can contain signed
            # redirects. Surface only a fixed reason without exception chaining.
            reason = 'network or local I/O failure'
        if attempt + 1 < _ATTEMPTS:
            time.sleep(0.5 * (attempt + 1))
    raise DownloadError(f'Chunk {index} failed after {_ATTEMPTS} attempts: {reason}') from None


def download_parallel(url, destination: pathlib.Path, expected_size: int,
                      workers=8, chunk_size=4 * 1024 * 1024):
    """Atomically replace destination after all strictly checked chunks arrive.

    Complete chunks survive failed attempts. An existing contiguous partial
    destination is never changed before successful assembly and can seed the
    chunk cache. The caller must verify the complete asset's SHA-256 afterward.
    """
    if not isinstance(expected_size, int) or expected_size < 0:
        raise ValueError('expected_size must be a nonnegative integer')
    if not isinstance(workers, int) or workers < 1:
        raise ValueError('workers must be a positive integer')
    if not isinstance(chunk_size, int) or chunk_size < 1:
        raise ValueError('chunk_size must be a positive integer')
    destination = pathlib.Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_file() and destination.stat().st_size == expected_size:
        return

    cache_root = destination.with_name(destination.name + '.chunks')
    # Do not store URLs (which may include signatures) in cache metadata/names.
    identity = hashlib.sha256(url.encode('utf-8')).hexdigest()
    cache = cache_root / f'{identity}-{expected_size}-{chunk_size}'
    cache.mkdir(mode=0o700, parents=True, exist_ok=True)
    count = (expected_size + chunk_size - 1) // chunk_size

    def chunk(index):
        start = index * chunk_size
        end = min(start + chunk_size, expected_size) - 1
        return cache / f'{index:08d}.part', start, end

    # Only complete blocks from the contiguous prefix are safe to reuse.
    # Oversized destinations are not trusted as partial downloads.
    if destination.is_file() and destination.stat().st_size < expected_size:
        complete = destination.stat().st_size // chunk_size
        with destination.open('rb') as source:
            for index in range(complete):
                path, start, end = chunk(index)
                size = end - start + 1
                if _valid_chunk(path, size):
                    source.seek(size, os.SEEK_CUR)
                    continue
                content = source.read(size)
                if len(content) != size:
                    raise DownloadError('Existing partial asset changed during import')
                _write_chunk(path, content)

    # Keep only workers futures live at once, including for very large assets.
    missing = (index for index in range(count)
               if not _valid_chunk(chunk(index)[0], chunk(index)[2] - chunk(index)[1] + 1))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        pending = set()

        def schedule():
            index = next(missing, None)
            if index is None:
                return False
            path, start, end = chunk(index)
            pending.add(pool.submit(_fetch_chunk, url, path, index, start, end, expected_size))
            return True

        for _ in range(workers):
            if not schedule():
                break
        while pending:
            done, pending = concurrent.futures.wait(pending, return_when=concurrent.futures.FIRST_COMPLETED)
            try:
                for future in done:
                    future.result()
            except Exception:
                for future in pending:
                    future.cancel()
                raise
            for _ in done:
                schedule()

    assembled = None
    try:
        with tempfile.NamedTemporaryFile(prefix='.' + destination.name + '.assembling-',
                                         dir=destination.parent, delete=False) as output:
            assembled = pathlib.Path(output.name)
            for index in range(count):
                path, start, end = chunk(index)
                size = end - start + 1
                with path.open('rb') as source:
                    content = source.read(size + 1)
                if len(content) != size:
                    raise DownloadError(f'Cached chunk {index} has the wrong size')
                output.write(content)
            if output.tell() != expected_size:
                raise DownloadError('Assembled asset has the wrong size')
            output.flush()
            os.fsync(output.fileno())
        os.replace(assembled, destination)
        # A completed asset is checked by the caller. On checksum rejection a
        # subsequent attempt must fetch fresh bytes, not reuse corrupt chunks.
        shutil.rmtree(cache_root, ignore_errors=True)
    finally:
        if assembled is not None:
            assembled.unlink(missing_ok=True)
