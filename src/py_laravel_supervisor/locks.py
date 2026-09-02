from __future__ import annotations

import os
import threading
from contextlib import AbstractContextManager


class LockUnavailable(RuntimeError):
    pass


if os.name == "nt":
    import ctypes
    from ctypes import wintypes

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _kernel32.CreateMutexW.argtypes = [ctypes.c_void_p, wintypes.BOOL, wintypes.LPCWSTR]
    _kernel32.CreateMutexW.restype = wintypes.HANDLE
    _kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    _kernel32.WaitForSingleObject.restype = wintypes.DWORD
    _kernel32.ReleaseMutex.argtypes = [wintypes.HANDLE]
    _kernel32.ReleaseMutex.restype = wintypes.BOOL
    _kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    _kernel32.CloseHandle.restype = wintypes.BOOL

    WAIT_OBJECT_0 = 0
    WAIT_ABANDONED = 0x80
    WAIT_TIMEOUT = 258


class WindowsMutex(AbstractContextManager["WindowsMutex"]):
    def __init__(self, name: str, timeout_ms: int = 5000) -> None:
        if os.name != "nt":
            raise LockUnavailable("Windows mutexes are available only on Windows")
        self.name = name
        self.timeout_ms = max(0, min(timeout_ms, 60_000))
        self._handle = None
        self._acquired = False

    def acquire(self) -> "WindowsMutex":
        handle = _kernel32.CreateMutexW(None, False, self.name)
        if not handle:
            raise ctypes.WinError(ctypes.get_last_error())
        self._handle = handle
        result = int(_kernel32.WaitForSingleObject(handle, self.timeout_ms))
        if result not in {WAIT_OBJECT_0, WAIT_ABANDONED}:
            self.close()
            if result == WAIT_TIMEOUT:
                raise LockUnavailable(f"mutex is busy: {self.name}")
            raise ctypes.WinError(ctypes.get_last_error())
        self._acquired = True
        return self

    def close(self) -> None:
        if self._handle is None:
            return
        if self._acquired:
            _kernel32.ReleaseMutex(self._handle)
            self._acquired = False
        _kernel32.CloseHandle(self._handle)
        self._handle = None

    def __enter__(self) -> "WindowsMutex":
        return self.acquire()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def mutex_is_locked(name: str) -> bool:
    result: list[bool] = []
    errors: list[BaseException] = []

    def probe() -> None:
        mutex = WindowsMutex(name, timeout_ms=0)
        try:
            mutex.acquire()
        except LockUnavailable:
            result.append(True)
        except BaseException as error:
            errors.append(error)
        else:
            mutex.close()
            result.append(False)

    thread = threading.Thread(target=probe, name="py-laravel-supervisor-mutex-probe", daemon=True)
    thread.start()
    thread.join(timeout=1.0)
    if thread.is_alive() or errors:
        raise LockUnavailable(f"mutex state is ambiguous: {name}")
    if len(result) != 1:
        raise LockUnavailable(f"mutex probe did not return a state: {name}")
    return result[0]


def mutex_name(installation_id: str, kind: str) -> str:
    safe = "".join(character if character.isalnum() else "-" for character in installation_id)[:80]
    return f"Local\\PyLaravelSupervisor-{safe}-{kind}"