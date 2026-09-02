from __future__ import annotations

import ctypes
import os
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Mapping

if os.name == "nt":
    import msvcrt
    from ctypes import wintypes
else:  # pragma: no cover - imported for type discovery on non-Windows
    wintypes = None


class WindowsProcessError(RuntimeError):
    pass


if os.name == "nt":
    HANDLE = wintypes.HANDLE
    DWORD = wintypes.DWORD
    SIZE_T = ctypes.c_size_t
    ULONG_PTR = ctypes.c_size_t

    class SECURITY_ATTRIBUTES(ctypes.Structure):
        _fields_ = [
            ("nLength", DWORD),
            ("lpSecurityDescriptor", ctypes.c_void_p),
            ("bInheritHandle", wintypes.BOOL),
        ]

    class STARTUPINFOW(ctypes.Structure):
        _fields_ = [
            ("cb", DWORD),
            ("lpReserved", wintypes.LPWSTR),
            ("lpDesktop", wintypes.LPWSTR),
            ("lpTitle", wintypes.LPWSTR),
            ("dwX", DWORD),
            ("dwY", DWORD),
            ("dwXSize", DWORD),
            ("dwYSize", DWORD),
            ("dwXCountChars", DWORD),
            ("dwYCountChars", DWORD),
            ("dwFillAttribute", DWORD),
            ("dwFlags", DWORD),
            ("wShowWindow", wintypes.WORD),
            ("cbReserved2", wintypes.WORD),
            ("lpReserved2", ctypes.POINTER(ctypes.c_ubyte)),
            ("hStdInput", HANDLE),
            ("hStdOutput", HANDLE),
            ("hStdError", HANDLE),
        ]

    class STARTUPINFOEXW(ctypes.Structure):
        _fields_ = [("StartupInfo", STARTUPINFOW), ("lpAttributeList", ctypes.c_void_p)]

    class PROCESS_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("hProcess", HANDLE),
            ("hThread", HANDLE),
            ("dwProcessId", DWORD),
            ("dwThreadId", DWORD),
        ]

    class LARGE_INTEGER(ctypes.Structure):
        _fields_ = [("QuadPart", ctypes.c_longlong)]

    class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", LARGE_INTEGER),
            ("PerJobUserTimeLimit", LARGE_INTEGER),
            ("LimitFlags", DWORD),
            ("MinimumWorkingSetSize", SIZE_T),
            ("MaximumWorkingSetSize", SIZE_T),
            ("ActiveProcessLimit", DWORD),
            ("Affinity", ULONG_PTR),
            ("PriorityClass", DWORD),
            ("SchedulingClass", DWORD),
        ]

    class IO_COUNTERS(ctypes.Structure):
        _fields_ = [(name, ctypes.c_ulonglong) for name in (
            "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
            "ReadTransferCount", "WriteTransferCount", "OtherTransferCount",
        )]

    class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
            ("IoInfo", IO_COUNTERS),
            ("ProcessMemoryLimit", SIZE_T),
            ("JobMemoryLimit", SIZE_T),
            ("PeakProcessMemoryUsed", SIZE_T),
            ("PeakJobMemoryUsed", SIZE_T),
        ]

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _user32 = ctypes.WinDLL("user32", use_last_error=True)
    _kernel32.CreateJobObjectW.argtypes = [ctypes.POINTER(SECURITY_ATTRIBUTES), wintypes.LPCWSTR]
    _kernel32.CreateJobObjectW.restype = HANDLE
    _kernel32.OpenJobObjectW.argtypes = [DWORD, wintypes.BOOL, wintypes.LPCWSTR]
    _kernel32.OpenJobObjectW.restype = HANDLE
    _kernel32.SetInformationJobObject.argtypes = [HANDLE, ctypes.c_int, ctypes.c_void_p, DWORD]
    _kernel32.SetInformationJobObject.restype = wintypes.BOOL
    _kernel32.AssignProcessToJobObject.argtypes = [HANDLE, HANDLE]
    _kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
    _kernel32.IsProcessInJob.argtypes = [HANDLE, HANDLE, ctypes.POINTER(wintypes.BOOL)]
    _kernel32.IsProcessInJob.restype = wintypes.BOOL
    _kernel32.GetCurrentProcess.argtypes = []
    _kernel32.GetCurrentProcess.restype = HANDLE
    _kernel32.TerminateJobObject.argtypes = [HANDLE, wintypes.UINT]
    _kernel32.TerminateJobObject.restype = wintypes.BOOL
    _kernel32.QueryInformationJobObject.argtypes = [HANDLE, ctypes.c_int, ctypes.c_void_p, DWORD, ctypes.POINTER(DWORD)]
    _kernel32.QueryInformationJobObject.restype = wintypes.BOOL
    _kernel32.OpenProcess.argtypes = [DWORD, wintypes.BOOL, DWORD]
    _kernel32.OpenProcess.restype = HANDLE
    _kernel32.CreatePipe.argtypes = [ctypes.POINTER(HANDLE), ctypes.POINTER(HANDLE), ctypes.POINTER(SECURITY_ATTRIBUTES), DWORD]
    _kernel32.CreatePipe.restype = wintypes.BOOL
    _kernel32.SetHandleInformation.argtypes = [HANDLE, DWORD, DWORD]
    _kernel32.SetHandleInformation.restype = wintypes.BOOL
    _kernel32.CreateFileW.argtypes = [wintypes.LPCWSTR, DWORD, DWORD, ctypes.POINTER(SECURITY_ATTRIBUTES), DWORD, DWORD, HANDLE]
    _kernel32.CreateFileW.restype = HANDLE
    _kernel32.InitializeProcThreadAttributeList.argtypes = [ctypes.c_void_p, DWORD, DWORD, ctypes.POINTER(SIZE_T)]
    _kernel32.InitializeProcThreadAttributeList.restype = wintypes.BOOL
    _kernel32.UpdateProcThreadAttribute.argtypes = [ctypes.c_void_p, DWORD, SIZE_T, ctypes.c_void_p, SIZE_T, ctypes.c_void_p, ctypes.POINTER(SIZE_T)]
    _kernel32.UpdateProcThreadAttribute.restype = wintypes.BOOL
    _kernel32.DeleteProcThreadAttributeList.argtypes = [ctypes.c_void_p]
    _kernel32.CreateProcessW.argtypes = [
        wintypes.LPCWSTR, wintypes.LPWSTR, ctypes.c_void_p, ctypes.c_void_p, wintypes.BOOL,
        DWORD, ctypes.c_void_p, wintypes.LPCWSTR, ctypes.POINTER(STARTUPINFOW), ctypes.POINTER(PROCESS_INFORMATION),
    ]
    _kernel32.CreateProcessW.restype = wintypes.BOOL
    _kernel32.ResumeThread.argtypes = [HANDLE]
    _kernel32.ResumeThread.restype = DWORD
    _kernel32.TerminateProcess.argtypes = [HANDLE, wintypes.UINT]
    _kernel32.TerminateProcess.restype = wintypes.BOOL
    _kernel32.WaitForSingleObject.argtypes = [HANDLE, DWORD]
    _kernel32.WaitForSingleObject.restype = DWORD
    _kernel32.GetExitCodeProcess.argtypes = [HANDLE, ctypes.POINTER(DWORD)]
    _kernel32.GetExitCodeProcess.restype = wintypes.BOOL
    _kernel32.CloseHandle.argtypes = [HANDLE]
    _kernel32.CloseHandle.restype = wintypes.BOOL
    _kernel32.GetLastError.restype = DWORD
    _user32.GetShellWindow.argtypes = []
    _user32.GetShellWindow.restype = wintypes.HWND
    _user32.GetWindowThreadProcessId.argtypes = [wintypes.HWND, ctypes.POINTER(DWORD)]
    _user32.GetWindowThreadProcessId.restype = DWORD

    JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION = 1
    JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9
    JOB_OBJECT_LIMIT_BREAKAWAY_OK = 0x00000800
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
    JOB_OBJECT_ASSIGN_PROCESS = 0x0001
    JOB_OBJECT_QUERY = 0x0004
    JOB_OBJECT_TERMINATE = 0x0008
    PROC_THREAD_ATTRIBUTE_PARENT_PROCESS = 0x00020000
    PROC_THREAD_ATTRIBUTE_HANDLE_LIST = 0x00020002
    PROC_THREAD_ATTRIBUTE_JOB_LIST = 0x0002000D
    CREATE_SUSPENDED = 0x00000004
    CREATE_NEW_PROCESS_GROUP = 0x00000200
    CREATE_UNICODE_ENVIRONMENT = 0x00000400
    CREATE_BREAKAWAY_FROM_JOB = 0x01000000
    EXTENDED_STARTUPINFO_PRESENT = 0x00080000
    CREATE_NO_WINDOW = 0x08000000
    STARTF_USESTDHANDLES = 0x00000100
    HANDLE_FLAG_INHERIT = 0x00000001
    GENERIC_READ = 0x80000000
    PROCESS_CREATE_PROCESS = 0x0080
    SYNCHRONIZE = 0x00100000
    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    FILE_SHARE_READ = 0x00000001
    FILE_SHARE_WRITE = 0x00000002
    OPEN_EXISTING = 3
    FILE_ATTRIBUTE_NORMAL = 0x80
    WAIT_OBJECT_0 = 0
    WAIT_TIMEOUT = 258
    ERROR_ALREADY_EXISTS = 183
    ERROR_FILE_NOT_FOUND = 2
    ERROR_INVALID_HANDLE = 6
    ERROR_INVALID_PARAMETER = 87
    STILL_ACTIVE = 259
    RESUME_FAILED = 0xFFFFFFFF


def _ensure_windows() -> None:
    if os.name != "nt":
        raise WindowsProcessError("py-laravel-supervisor v1 supports Windows only")


def _win_error(message: str) -> WindowsProcessError:
    error = ctypes.get_last_error()
    return WindowsProcessError(f"{message} (Windows error {error})")


def create_job(
    name: str,
    *,
    inheritable: bool = False,
    allow_breakaway: bool = False,
) -> int:
    _ensure_windows()
    security = SECURITY_ATTRIBUTES(ctypes.sizeof(SECURITY_ATTRIBUTES), None, bool(inheritable))
    handle = _kernel32.CreateJobObjectW(ctypes.byref(security), name)
    if not handle:
        raise _win_error("unable to create Job Object")
    if int(_kernel32.GetLastError()) == ERROR_ALREADY_EXISTS:
        _kernel32.CloseHandle(handle)
        raise WindowsProcessError("Job Object already exists")
    limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    if allow_breakaway:
        limits.BasicLimitInformation.LimitFlags |= JOB_OBJECT_LIMIT_BREAKAWAY_OK
    if not _kernel32.SetInformationJobObject(
        handle,
        JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
        ctypes.byref(limits),
        ctypes.sizeof(limits),
    ):
        _kernel32.CloseHandle(handle)
        raise _win_error("unable to configure Job Object")
    if inheritable and not _kernel32.SetHandleInformation(handle, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT):
        _kernel32.CloseHandle(handle)
        raise _win_error("unable to mark Job Object inheritable")
    return int(handle)


def open_job(
    name: str,
    *,
    terminate: bool = False,
    assign: bool = False,
) -> int | None:
    _ensure_windows()
    access = (
        JOB_OBJECT_QUERY
        | (JOB_OBJECT_TERMINATE if terminate else 0)
        | (JOB_OBJECT_ASSIGN_PROCESS if assign else 0)
    )
    handle = _kernel32.OpenJobObjectW(access, False, name)
    if not handle:
        error = ctypes.get_last_error()
        if error in {ERROR_FILE_NOT_FOUND, ERROR_INVALID_HANDLE}:
            return None
        raise _win_error("unable to open Job Object")
    return int(handle)


def assign_current_process_to_job(handle: int) -> None:
    _ensure_windows()
    if not _kernel32.AssignProcessToJobObject(HANDLE(handle), _kernel32.GetCurrentProcess()):
        raise _win_error("unable to assign resident to Anchor Job Object")


def current_process_in_job(handle: int) -> bool:
    _ensure_windows()
    result = wintypes.BOOL()
    if not _kernel32.IsProcessInJob(
        _kernel32.GetCurrentProcess(),
        HANDLE(handle),
        ctypes.byref(result),
    ):
        raise _win_error("unable to verify resident Anchor Job membership")
    return bool(result.value)


def close_handle(handle: int | None) -> None:
    if handle:
        _kernel32.CloseHandle(HANDLE(handle))


def set_handle_inheritable(handle: int, inheritable: bool) -> None:
    _ensure_windows()
    flags = HANDLE_FLAG_INHERIT if inheritable else 0
    if not _kernel32.SetHandleInformation(HANDLE(handle), HANDLE_FLAG_INHERIT, flags):
        raise _win_error("unable to update handle inheritance")


def terminate_job(handle: int, exit_code: int = 1) -> None:
    if not _kernel32.TerminateJobObject(HANDLE(handle), exit_code):
        raise _win_error("unable to terminate Job Object")


def job_exists(name: str) -> bool:
    handle = open_job(name)
    if handle is None:
        return False
    close_handle(handle)
    return True


def job_active_processes(handle: int) -> int:
    _ensure_windows()

    class BasicAccountingInformation(ctypes.Structure):
        _fields_ = [
            ("TotalUserTime", ctypes.c_longlong),
            ("TotalKernelTime", ctypes.c_longlong),
            ("ThisPeriodTotalUserTime", ctypes.c_longlong),
            ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
            ("TotalPageFaultCount", DWORD),
            ("TotalProcesses", DWORD),
            ("ActiveProcesses", DWORD),
            ("TotalTerminatedProcesses", DWORD),
        ]

    information = BasicAccountingInformation()
    returned = DWORD()
    if not _kernel32.QueryInformationJobObject(
        HANDLE(handle),
        JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION,
        ctypes.byref(information),
        ctypes.sizeof(information),
        ctypes.byref(returned),
    ):
        raise _win_error("unable to query Job Object accounting")
    return int(information.ActiveProcesses)


def open_interactive_shell_process() -> int | None:
    _ensure_windows()
    window = _user32.GetShellWindow()
    if not window:
        return None
    process_id = DWORD()
    if not _user32.GetWindowThreadProcessId(window, ctypes.byref(process_id)) or process_id.value <= 0:
        return None
    handle = _kernel32.OpenProcess(
        PROCESS_CREATE_PROCESS | PROCESS_QUERY_LIMITED_INFORMATION,
        False,
        process_id.value,
    )
    if not handle:
        return None
    return int(handle)


def process_exists(process_id: int) -> bool:
    _ensure_windows()
    if process_id <= 0:
        return False
    handle = _kernel32.OpenProcess(SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION, False, process_id)
    if not handle:
        if ctypes.get_last_error() == ERROR_INVALID_PARAMETER:
            return False
        raise _win_error("unable to inspect process")
    try:
        result = int(_kernel32.WaitForSingleObject(handle, 0))
        if result == WAIT_TIMEOUT:
            return True
        if result == WAIT_OBJECT_0:
            return False
        raise _win_error("unable to inspect process state")
    finally:
        _kernel32.CloseHandle(handle)


@dataclass(slots=True)
class ManagedWindowsProcess:
    pid: int
    process_handle: int
    job_handle: int | None
    stdout_fd: int | None
    stderr_fd: int | None

    def poll(self) -> int | None:
        result = int(_kernel32.WaitForSingleObject(HANDLE(self.process_handle), 0))
        if result == WAIT_TIMEOUT:
            return None
        if result != WAIT_OBJECT_0:
            raise _win_error("unable to query process")
        code = DWORD()
        if not _kernel32.GetExitCodeProcess(HANDLE(self.process_handle), ctypes.byref(code)):
            raise _win_error("unable to read process exit code")
        return int(code.value)

    def wait(self, timeout: float) -> int | None:
        milliseconds = max(0, min(int(timeout * 1000), 60_000))
        result = int(_kernel32.WaitForSingleObject(HANDLE(self.process_handle), milliseconds))
        if result == WAIT_TIMEOUT:
            return None
        return self.poll()

    def terminate_tree(self) -> None:
        if self.job_handle is None:
            raise WindowsProcessError("process has no exact Job Object")
        terminate_job(self.job_handle)

    def close(self) -> None:
        if self.stdout_fd is not None:
            try:
                os.close(self.stdout_fd)
            except OSError:
                pass
            self.stdout_fd = None
        if self.stderr_fd is not None:
            try:
                os.close(self.stderr_fd)
            except OSError:
                pass
            self.stderr_fd = None
        close_handle(self.process_handle)
        self.process_handle = 0


def spawn_process(
    argv: Iterable[str],
    *,
    cwd: str | Path,
    environment: Mapping[str, str],
    job_handles: list[int],
    exact_job_handle: int | None,
    cleanup_job_handle: int,
    inherited_handles: list[int] | None = None,
    capture_output: bool = True,
    create_new_process_group: bool = True,
    breakaway_from_parent_job: bool = False,
    _post_create_hook: Callable[[int], None] | None = None,
) -> ManagedWindowsProcess:
    _ensure_windows()
    command = list(argv)
    if not command:
        raise WindowsProcessError("command is required")
    if cleanup_job_handle not in job_handles:
        raise WindowsProcessError("cleanup Job Object must be part of the process Job list")
    stdout_read = stdout_write = stderr_read = stderr_write = stdin_handle = None
    process_handle = thread_handle = None
    attribute_buffer = None
    attr_pointer = None
    try:
        security = SECURITY_ATTRIBUTES(ctypes.sizeof(SECURITY_ATTRIBUTES), None, True)
        stdin_handle = _kernel32.CreateFileW(
            "NUL", GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, ctypes.byref(security), OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, None
        )
        if not stdin_handle:
            raise _win_error("unable to open NUL stdin")
        if capture_output:
            stdout_read, stdout_write = _create_pipe(security)
            stderr_read, stderr_write = _create_pipe(security)
        else:
            stdout_write = _open_nul_write(security)
            stderr_write = _open_nul_write(security)

        inheritable = [int(stdin_handle), int(stdout_write), int(stderr_write), *(inherited_handles or [])]
        attribute_count = 1 + (1 if job_handles else 0)
        size = SIZE_T()
        _kernel32.InitializeProcThreadAttributeList(None, attribute_count, 0, ctypes.byref(size))
        attribute_buffer = ctypes.create_string_buffer(size.value)
        attr_pointer = ctypes.cast(attribute_buffer, ctypes.c_void_p)
        if not _kernel32.InitializeProcThreadAttributeList(attr_pointer, attribute_count, 0, ctypes.byref(size)):
            raise _win_error("unable to initialize process attribute list")

        inherited_array = (HANDLE * len(inheritable))(*[HANDLE(item) for item in inheritable])
        if not _kernel32.UpdateProcThreadAttribute(
            attr_pointer, 0, PROC_THREAD_ATTRIBUTE_HANDLE_LIST, inherited_array, ctypes.sizeof(inherited_array), None, None
        ):
            raise _win_error("unable to restrict inherited handles")
        if job_handles:
            jobs_array = (HANDLE * len(job_handles))(*[HANDLE(item) for item in job_handles])
            if not _kernel32.UpdateProcThreadAttribute(
                attr_pointer, 0, PROC_THREAD_ATTRIBUTE_JOB_LIST, jobs_array, ctypes.sizeof(jobs_array), None, None
            ):
                raise _win_error("unable to assign process Job Object list")

        startup = STARTUPINFOEXW()
        startup.StartupInfo.cb = ctypes.sizeof(startup)
        startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES
        startup.StartupInfo.hStdInput = HANDLE(stdin_handle)
        startup.StartupInfo.hStdOutput = HANDLE(stdout_write)
        startup.StartupInfo.hStdError = HANDLE(stderr_write)
        startup.lpAttributeList = attr_pointer
        info = PROCESS_INFORMATION()
        command_line = ctypes.create_unicode_buffer(subprocess.list2cmdline(command))
        env_block = _environment_block(environment)
        env_buffer = ctypes.create_unicode_buffer(env_block)
        flags = CREATE_SUSPENDED | CREATE_UNICODE_ENVIRONMENT | EXTENDED_STARTUPINFO_PRESENT | CREATE_NO_WINDOW
        if create_new_process_group:
            flags |= CREATE_NEW_PROCESS_GROUP
        if breakaway_from_parent_job:
            flags |= CREATE_BREAKAWAY_FROM_JOB
        application = str(command[0])
        if not _kernel32.CreateProcessW(
            application,
            command_line,
            None,
            None,
            True,
            flags,
            ctypes.cast(env_buffer, ctypes.c_void_p),
            str(Path(cwd)),
            ctypes.byref(startup.StartupInfo),
            ctypes.byref(info),
        ):
            raise _win_error("unable to create managed process")
        process_handle = int(info.hProcess)
        thread_handle = int(info.hThread)
        pid = int(info.dwProcessId)
        if _post_create_hook is not None:
            _post_create_hook(pid)
        _resume_thread(thread_handle)
        close_handle(thread_handle)
        thread_handle = None
        close_handle(int(stdin_handle))
        stdin_handle = None
        close_handle(int(stdout_write))
        stdout_write = None
        close_handle(int(stderr_write))
        stderr_write = None
        stdout_fd = _fd_from_handle(stdout_read) if capture_output and stdout_read else None
        stderr_fd = _fd_from_handle(stderr_read) if capture_output and stderr_read else None
        stdout_read = stderr_read = None
        return ManagedWindowsProcess(pid, process_handle, exact_job_handle, stdout_fd, stderr_fd)
    except BaseException:
        if process_handle:
            _contain_failed_spawn(process_handle, cleanup_job_handle)
        close_handle(thread_handle)
        close_handle(process_handle)
        raise
    finally:
        if attr_pointer:
            _kernel32.DeleteProcThreadAttributeList(attr_pointer)
        for handle in (stdin_handle, stdout_read, stdout_write, stderr_read, stderr_write):
            close_handle(int(handle) if handle else None)


def _resume_thread(thread_handle: int) -> None:
    if int(_kernel32.ResumeThread(HANDLE(thread_handle))) == RESUME_FAILED:
        raise _win_error("unable to resume managed process")


def _contain_failed_spawn(process_handle: int, cleanup_job_handle: int) -> None:
    """Contain a process even when failure occurs after CreateProcessW succeeds."""
    try:
        terminate_job(cleanup_job_handle)
    except Exception:
        _kernel32.TerminateProcess(HANDLE(process_handle), 1)
    result = int(_kernel32.WaitForSingleObject(HANDLE(process_handle), 2_000))
    if result == WAIT_TIMEOUT:
        _kernel32.TerminateProcess(HANDLE(process_handle), 1)
        _kernel32.WaitForSingleObject(HANDLE(process_handle), 2_000)


def start_pipe_reader(
    fd: int,
    on_line: Callable[[str], None],
    on_error: Callable[[str], None],
    *,
    protocol_line_observer: Callable[[str], None] | None = None,
) -> threading.Thread:
    def run() -> None:
        from .redaction import RedactedLineAccumulator

        # The protocol observer is memory-only: callers may parse a raw line into
        # sanitized structured metadata, but must never persist or log the raw text.
        accumulator = RedactedLineAccumulator(protocol_line_observer=protocol_line_observer)
        try:
            while True:
                chunk = os.read(fd, 4096)
                if not chunk:
                    break
                try:
                    lines = accumulator.feed(chunk)
                except Exception:
                    on_error("pipe_protocol_failed")
                    continue
                for line in lines:
                    try:
                        on_line(line)
                    except Exception:
                        on_error("pipe_consumer_failed")
            try:
                final_lines = accumulator.finish()
            except Exception:
                on_error("pipe_protocol_failed")
                final_lines = []
            for line in final_lines:
                try:
                    on_line(line)
                except Exception:
                    on_error("pipe_consumer_failed")
        except OSError:
            on_error("pipe_read_failed")
        finally:
            try:
                os.close(fd)
            except OSError:
                pass

    thread = threading.Thread(target=run, name=f"py-laravel-supervisor-pipe-{fd}", daemon=True)
    thread.start()
    return thread


def _create_pipe(security: SECURITY_ATTRIBUTES) -> tuple[int, int]:
    read_handle = HANDLE()
    write_handle = HANDLE()
    if not _kernel32.CreatePipe(ctypes.byref(read_handle), ctypes.byref(write_handle), ctypes.byref(security), 0):
        raise _win_error("unable to create child pipe")
    if not _kernel32.SetHandleInformation(read_handle, HANDLE_FLAG_INHERIT, 0):
        close_handle(read_handle.value)
        close_handle(write_handle.value)
        raise _win_error("unable to make parent pipe handle non-inheritable")
    if read_handle.value is None or write_handle.value is None:
        raise WindowsProcessError("Windows pipe returned an invalid handle")
    return int(read_handle.value), int(write_handle.value)


def _open_nul_write(security: SECURITY_ATTRIBUTES) -> int:
    GENERIC_WRITE = 0x40000000
    handle = _kernel32.CreateFileW(
        "NUL", GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, ctypes.byref(security), OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, None
    )
    if not handle:
        raise _win_error("unable to open NUL output")
    return int(handle)


def _fd_from_handle(handle: int) -> int:
    return msvcrt.open_osfhandle(handle, os.O_RDONLY | os.O_BINARY)


def _environment_block(environment: Mapping[str, str]) -> str:
    values = dict(environment)
    for key in (
        "SystemRoot",
        "WINDIR",
        "TEMP",
        "TMP",
        "PATH",
        "PATHEXT",
        "USERPROFILE",
        "HOME",
    ):
        if key not in values and key in os.environ:
            values[key] = os.environ[key]
    ordered = sorted(values.items(), key=lambda pair: pair[0].lower())
    return "\0".join(f"{key}={value}" for key, value in ordered) + "\0\0"


def wait_for_job_absent(name: str, timeout: float) -> bool:
    deadline = time.monotonic() + max(0.0, timeout)
    while time.monotonic() < deadline:
        if not job_exists(name):
            return True
        time.sleep(0.05)
    return not job_exists(name)