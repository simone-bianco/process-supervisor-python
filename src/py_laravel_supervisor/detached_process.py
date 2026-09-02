from __future__ import annotations

import ctypes
import subprocess
from pathlib import Path
from typing import Callable, Iterable, Mapping

from .windows import (
    CREATE_NEW_PROCESS_GROUP,
    CREATE_NO_WINDOW,
    CREATE_SUSPENDED,
    CREATE_UNICODE_ENVIRONMENT,
    EXTENDED_STARTUPINFO_PRESENT,
    HANDLE,
    PROCESS_INFORMATION,
    PROC_THREAD_ATTRIBUTE_PARENT_PROCESS,
    STARTUPINFOEXW,
    ManagedWindowsProcess,
    WindowsProcessError,
    _environment_block,
    _kernel32,
    _resume_thread,
    _win_error,
    close_handle,
    open_interactive_shell_process,
)


def spawn_detached_resident(
    argv: Iterable[str],
    *,
    cwd: str | Path,
    environment: Mapping[str, str],
    _post_create_hook: Callable[[int], None] | None = None,
) -> ManagedWindowsProcess:
    """Create a hidden resident with Explorer as its process parent.

    The resident receives no inherited handles and is not attached to the short-lived
    web/CLI caller Job. It opens and joins its named Anchor Job during bootstrap.
    """

    command = list(argv)
    if not command:
        raise WindowsProcessError("resident command is required")

    parent_handle = open_interactive_shell_process()
    if parent_handle is None:
        raise WindowsProcessError("interactive Windows shell process is unavailable")

    attribute_buffer = None
    attr_pointer = None
    process_handle = None
    thread_handle = None
    try:
        size = ctypes.c_size_t()
        _kernel32.InitializeProcThreadAttributeList(None, 1, 0, ctypes.byref(size))
        attribute_buffer = ctypes.create_string_buffer(size.value)
        attr_pointer = ctypes.cast(attribute_buffer, ctypes.c_void_p)
        if not _kernel32.InitializeProcThreadAttributeList(
            attr_pointer,
            1,
            0,
            ctypes.byref(size),
        ):
            raise _win_error("unable to initialize detached resident attributes")

        parent_value = HANDLE(parent_handle)
        if not _kernel32.UpdateProcThreadAttribute(
            attr_pointer,
            0,
            PROC_THREAD_ATTRIBUTE_PARENT_PROCESS,
            ctypes.byref(parent_value),
            ctypes.sizeof(parent_value),
            None,
            None,
        ):
            raise _win_error("unable to set detached resident parent")

        startup = STARTUPINFOEXW()
        startup.StartupInfo.cb = ctypes.sizeof(startup)
        startup.lpAttributeList = attr_pointer
        info = PROCESS_INFORMATION()
        command_line = ctypes.create_unicode_buffer(subprocess.list2cmdline(command))
        environment_buffer = ctypes.create_unicode_buffer(_environment_block(environment))
        flags = (
            CREATE_SUSPENDED
            | CREATE_NEW_PROCESS_GROUP
            | CREATE_UNICODE_ENVIRONMENT
            | EXTENDED_STARTUPINFO_PRESENT
            | CREATE_NO_WINDOW
        )
        if not _kernel32.CreateProcessW(
            str(command[0]),
            command_line,
            None,
            None,
            False,
            flags,
            ctypes.cast(environment_buffer, ctypes.c_void_p),
            str(Path(cwd)),
            ctypes.byref(startup.StartupInfo),
            ctypes.byref(info),
        ):
            raise _win_error("unable to create detached resident process")

        process_handle = int(info.hProcess)
        thread_handle = int(info.hThread)
        pid = int(info.dwProcessId)
        if _post_create_hook is not None:
            _post_create_hook(pid)
        _resume_thread(thread_handle)
        close_handle(thread_handle)
        thread_handle = None

        managed = ManagedWindowsProcess(pid, process_handle, None, None, None)
        process_handle = None
        return managed
    except BaseException:
        if process_handle:
            _kernel32.TerminateProcess(HANDLE(process_handle), 1)
            _kernel32.WaitForSingleObject(HANDLE(process_handle), 2_000)
        raise
    finally:
        close_handle(thread_handle)
        close_handle(process_handle)
        if attr_pointer:
            _kernel32.DeleteProcThreadAttributeList(attr_pointer)
        close_handle(parent_handle)


def terminate_detached_process(process: ManagedWindowsProcess, *, timeout: float = 2.0) -> bool:
    """Terminate exactly the process handle returned by detached bootstrap."""

    if process.poll() is not None:
        return True
    if not _kernel32.TerminateProcess(HANDLE(process.process_handle), 1):
        raise _win_error("unable to terminate detached resident process")
    return process.wait(max(0.1, min(timeout, 10.0))) is not None