"""Raw bounded frames for one already-owned Windows stdio child.

This is the streaming API over the same DuplexChannel owner/readers/cleanup,
not a protocol decoder, process launcher, reconnect loop or HTTP service.
Notifications and multiple inbound frames remain entirely caller-owned.
"""
from __future__ import annotations

import os
import queue
import threading
import time

from .duplex import DuplexChannel
from .windows import ManagedWindowsProcess, WindowsProcessError


def _pipe_available(fd: int) -> int | None:
    """Nonblocking kernel observation; None means the original writer is gone."""
    import ctypes
    import msvcrt
    from ctypes import wintypes
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    peek = kernel.PeekNamedPipe
    peek.argtypes = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD,
                     ctypes.POINTER(wintypes.DWORD), ctypes.POINTER(wintypes.DWORD), ctypes.POINTER(wintypes.DWORD)]
    peek.restype = wintypes.BOOL
    available = wintypes.DWORD()
    handle = msvcrt.get_osfhandle(fd)
    if not peek(handle, None, 0, None, ctypes.byref(available), None):
        if ctypes.get_last_error() in (109, 233):
            return None
        raise WindowsProcessError('owned stdout readiness cannot be established')
    return int(available.value)


class FramedStdioChannel(DuplexChannel):
    def __init__(self, process: ManagedWindowsProcess, *, frame_bytes: int = 262144,
                 stderr_bytes: int = 65536) -> None:
        # The base constructor starts _stdout immediately; initialize its extra
        # synchronization first. No parser or request identity lives here.
        self._buffer_lock = threading.Lock()
        self._pending_bytes = 0
        super().__init__(process, frame_bytes=frame_bytes, stderr_bytes=stderr_bytes)

    @property
    def buffered_bytes(self) -> int:
        """Nonblocking diagnostic count only; never returns incomplete payloads."""
        with self._buffer_lock:
            return self._pending_bytes

    def _stdout(self) -> None:
        pending = bytearray()
        try:
            while not self._failed.is_set():
                # The only reader peeks and consumes already available bytes under
                # the same short lock as idle. It never blocks inside this lock.
                # This closes the gap where a background reader had taken kernel
                # bytes but had not yet exposed the partial frame to assert_idle.
                with self._buffer_lock:
                    available = _pipe_available(self.process.stdout_fd)
                    if available is None:
                        if pending:
                            self._fail()
                        self._put(None)
                        return
                    if available:
                        chunk = os.read(self.process.stdout_fd, min(4096, available))
                        if not chunk:
                            self._fail()
                            return
                        pending.extend(chunk)
                        while b'\n' in pending:
                            end = pending.index(b'\n') + 1
                            if end > self.frame_bytes:
                                self._pending_bytes = len(pending)
                                self._fail()
                                return
                            self._put(bytes(pending[:end]))
                            del pending[:end]
                        self._pending_bytes = len(pending)
                        if self._pending_bytes > self.frame_bytes:
                            self._fail()
                            return
                if not available:
                    self._failed.wait(0.005)
        except (OSError, ValueError, WindowsProcessError):
            self._fail()

    @staticmethod
    def _deadline(timeout: float) -> float:
        if isinstance(timeout, bool) or not isinstance(timeout, (float, int)) or not 0 < timeout <= 60:
            raise ValueError('frame deadline exceeds bound')
        return time.monotonic() + timeout

    def _acquire(self) -> None:
        # No request may acquire a fresh deadline after silently queueing behind
        # another caller. Rejection does not kill the original active request.
        if not self._io_lock.acquire(blocking=False):
            raise WindowsProcessError('owned channel already has an active operation')

    def _assert_usable(self) -> None:
        if self._closed or self._failed.is_set() or not self.process.process_handle or self.process.poll() is not None:
            raise WindowsProcessError('owned channel is closed or failed')

    def assert_usable(self) -> None:
        self._acquire()
        try:
            self._assert_usable()
        except WindowsProcessError:
            self.close()
            raise
        finally:
            self._io_lock.release()

    def assert_idle(self) -> None:
        self._acquire()
        try:
            self._assert_usable()
            with self._buffer_lock:
                if self._pending_bytes != 0 or not self._frames.empty() or _pipe_available(self.process.stdout_fd) != 0:
                    raise WindowsProcessError('owned channel is not idle')
        except WindowsProcessError:
            self.close()
            raise
        finally:
            self._io_lock.release()

    def write(self, frame: bytes, *, timeout: float) -> None:
        deadline = self._deadline(timeout)
        if not isinstance(frame, bytes) or not frame.endswith(b'\n') or b'\n' in frame[:-1] or b'\r' in frame:
            raise ValueError('one newline-terminated frame is required')
        if len(frame) > self.frame_bytes:
            raise ValueError('frame exceeds byte bound')
        self._acquire()
        try:
            self._assert_usable()
            self._check_deadline(deadline)
            done = threading.Event()
            self._writes.put_nowait((frame, done))
            while not done.wait(min(0.02, max(0, deadline - time.monotonic()))):
                self._check_deadline(deadline)
            self._check_deadline(deadline)
            # A write does not presume a response. MCP notifications intentionally
            # complete here; only the caller chooses whether/when to read.
        except (WindowsProcessError, queue.Full):
            self.close()
            raise
        finally:
            self._io_lock.release()

    def read(self, *, timeout: float) -> bytes:
        deadline = self._deadline(timeout)
        self._acquire()
        try:
            self._assert_usable()
            while True:
                self._check_deadline(deadline)
                try:
                    frame = self._frames.get(timeout=min(0.02, max(0, deadline - time.monotonic())))
                except queue.Empty:
                    continue
                self._check_deadline(deadline)
                if frame is None:
                    raise WindowsProcessError('owned channel closed before a complete frame')
                return frame
        except WindowsProcessError:
            self.close()
            raise
        finally:
            self._io_lock.release()
