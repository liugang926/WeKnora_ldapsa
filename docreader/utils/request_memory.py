"""Reclaim large request buffers after their gRPC worker has returned."""

import ctypes
import gc
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

_LARGE_REQUEST_BYTES = 16 * 1024 * 1024
_request_memory = threading.local()
_malloc_trim = (
    getattr(ctypes.CDLL(None), "malloc_trim", None)
    if sys.platform.startswith("linux")
    else None
)
if _malloc_trim is not None:
    _malloc_trim.argtypes = [ctypes.c_size_t]
    _malloc_trim.restype = ctypes.c_int


def track_request_size(size: int) -> None:
    if size >= _LARGE_REQUEST_BYTES:
        _request_memory.reclaim = True


class RequestThreadPoolExecutor(ThreadPoolExecutor):
    """Collect PDF wrapper cycles and trim idle arenas after large RPC tasks."""

    def submit(self, fn, /, *args, **kwargs):
        def run():
            nonlocal fn, args, kwargs
            try:
                return fn(*args, **kwargs)
            finally:
                # The executor retains this work item until run() returns. Drop
                # its gRPC call/context references before collecting and trimming.
                del fn, args, kwargs
                if getattr(_request_memory, "reclaim", False):
                    _request_memory.reclaim = False
                    gc.collect()
                    if _malloc_trim is not None:
                        _malloc_trim(0)

        return super().submit(run)
