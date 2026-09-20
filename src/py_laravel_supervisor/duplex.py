"""Deadline-bounded duplex access to an already-owned Windows Job child.

No JSON-RPC parsing, remote endpoint, retries, subprocess discovery, or logging.
"""
from __future__ import annotations

import os
import queue
import threading
import time

from .windows import ManagedWindowsProcess, WindowsProcessError, job_active_processes


class DuplexChannel:
    def __init__(self, process: ManagedWindowsProcess, *, frame_bytes: int = 262144, stderr_bytes: int = 65536) -> None:
        if not 1024 <= frame_bytes <= 1048576 or not 0 <= stderr_bytes <= 1048576:
            raise ValueError("invalid channel bounds")
        if process.stdin_fd is None or process.stdout_fd is None or process.stderr_fd is None:
            raise WindowsProcessError("owned duplex pipes are required")
        self.process = process
        self.frame_bytes = frame_bytes
        self.stderr_limit = stderr_bytes
        self._frames: queue.Queue[bytes | None] = queue.Queue(maxsize=16)
        self._writes: queue.Queue[tuple[bytes, threading.Event] | None] = queue.Queue(maxsize=1)
        self._failed = threading.Event()
        self._closed = False
        self._io_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._threads = [
            threading.Thread(target=self._stdout, name="owned-stdio-reader", daemon=True),
            threading.Thread(target=self._stderr, name="owned-stdio-stderr", daemon=True),
            threading.Thread(target=self._stdin, name="owned-stdio-writer", daemon=True),
        ]
        for thread in self._threads:
            thread.start()

    def _fail(self) -> None:
        self._failed.set()

    def _put(self, frame: bytes | None) -> None:
        try:
            self._frames.put_nowait(frame)
        except queue.Full:
            self._fail()

    def _stdout(self) -> None:
        pending = bytearray()
        try:
            while not self._failed.is_set():
                chunk = os.read(self.process.stdout_fd, 4096)
                if not chunk:
                    if pending:
                        self._fail()
                    self._put(None)
                    return
                pending.extend(chunk)
                while b"\n" in pending:
                    end = pending.index(b"\n") + 1
                    if end > self.frame_bytes:
                        self._fail()
                        return
                    self._put(bytes(pending[:end]))
                    del pending[:end]
                if len(pending) > self.frame_bytes:
                    self._fail()
                    return
        except OSError:
            self._fail()

    def _stderr(self) -> None:
        total = 0
        try:
            while not self._failed.is_set():
                chunk = os.read(self.process.stderr_fd, 4096)
                if not chunk:
                    return
                total += len(chunk)
                if total > self.stderr_limit:
                    self._fail()
                    return
        except OSError:
            self._fail()

    def _stdin(self) -> None:
        try:
            while True:
                item = self._writes.get()
                if item is None:
                    return
                frame, done = item
                offset = 0
                while offset < len(frame):
                    written = os.write(self.process.stdin_fd, frame[offset:offset + 4096])
                    if written <= 0:
                        raise OSError("closed pipe")
                    offset += written
                done.set()
        except OSError:
            self._fail()

    def exchange(self, frame: bytes, *, timeout: float) -> bytes:
        if not isinstance(frame, bytes) or not frame.endswith(b"\n") or b"\n" in frame[:-1] or b"\r" in frame:
            raise ValueError("one newline-terminated frame is required")
        if len(frame) > self.frame_bytes or not 0 < timeout <= 60:
            raise ValueError("frame or deadline exceeds bound")
        # One original request owns this channel. A competing caller is rejected,
        # not queued with a fresh deadline after the owner's exchange completes.
        if not self._io_lock.acquire(blocking=False):
            raise WindowsProcessError("owned channel already has an active exchange")
        try:
            if self._closed or self._failed.is_set() or not self._frames.empty():
                self.close()
                raise WindowsProcessError("owned channel is not idle")
            deadline = time.monotonic() + timeout
            done = threading.Event()
            self._writes.put_nowait((frame, done))
            try:
                while not done.wait(min(0.02, max(0, deadline - time.monotonic()))):
                    self._check_deadline(deadline)
                while True:
                    self._check_deadline(deadline)
                    try:
                        result = self._frames.get(timeout=min(0.02, deadline - time.monotonic()))
                    except queue.Empty:
                        continue
                    if result is None:
                        raise WindowsProcessError("owned channel closed before response")
                    return result
            except (WindowsProcessError, ValueError):
                self.close()
                raise
        finally:
            self._io_lock.release()

    def _check_deadline(self, deadline: float) -> None:
        if self._failed.is_set():
            raise WindowsProcessError("owned channel failed or exceeded output bounds")
        if time.monotonic() >= deadline:
            raise WindowsProcessError("owned channel deadline exceeded")

    def close(self) -> None:
        # Failed close remains failed/retryable with the same exact Job and
        # handles. Never label the channel closed before OS/I/O quiescence.
        with self._close_lock:
            if self._closed:
                return
            self._failed.set()
            self.process.terminate_tree()
            if self.process.wait(2) is None:
                raise WindowsProcessError("owned child quiescence is unproven")
            try:
                self._writes.put_nowait(None)
            except queue.Full:
                pass
            for thread in self._threads:
                thread.join(timeout=1)
            if any(thread.is_alive() for thread in self._threads):
                raise WindowsProcessError("owned channel cleanup is incomplete")
            if self.process.job_handle is None:
                raise WindowsProcessError("owned Job quiescence is unproven")
            # TerminateJobObject is asynchronous. The root process can signal
            # before Windows has retired all descendants from the original Job.
            # Wait boundedly on that exact handle, never rediscover/adopt a PID.
            deadline = time.monotonic() + 2
            while job_active_processes(self.process.job_handle) != 0:
                if time.monotonic() >= deadline:
                    raise WindowsProcessError("owned Job quiescence is unproven")
                time.sleep(0.01)
            self.process.close()
            self._closed = True
