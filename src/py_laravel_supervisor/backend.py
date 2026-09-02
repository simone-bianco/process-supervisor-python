from __future__ import annotations

from pathlib import Path
from typing import Mapping, Protocol

from .windows import (
    ManagedWindowsProcess,
    close_handle,
    create_job,
    job_active_processes,
    job_exists,
    open_job,
    spawn_process,
    terminate_job,
    wait_for_job_absent,
)


class ManagedProcess(Protocol):
    pid: int
    stdout_fd: int | None
    stderr_fd: int | None

    def poll(self) -> int | None: ...

    def wait(self, timeout: float) -> int | None: ...

    def terminate_tree(self) -> None: ...

    def close(self) -> None: ...


class ProcessBackend(Protocol):
    def create_job(self, name: str, *, inheritable: bool = False) -> int: ...

    def open_job(self, name: str, *, terminate: bool = False) -> int | None: ...

    def close_handle(self, handle: int | None) -> None: ...

    def terminate_job(self, handle: int, exit_code: int = 1) -> None: ...

    def job_exists(self, name: str) -> bool: ...

    def job_active_processes(self, handle: int) -> int: ...

    def wait_for_job_absent(self, name: str, timeout: float) -> bool: ...

    def spawn(
        self,
        argv: tuple[str, ...],
        *,
        cwd: Path,
        environment: Mapping[str, str],
        job_handles: list[int],
        exact_job_handle: int | None,
        cleanup_job_handle: int,
        inherited_handles: list[int] | None = None,
        capture_output: bool = True,
    ) -> ManagedProcess: ...


class WindowsProcessBackend:
    def create_job(self, name: str, *, inheritable: bool = False) -> int:
        return create_job(name, inheritable=inheritable)

    def open_job(self, name: str, *, terminate: bool = False) -> int | None:
        return open_job(name, terminate=terminate)

    def close_handle(self, handle: int | None) -> None:
        close_handle(handle)

    def terminate_job(self, handle: int, exit_code: int = 1) -> None:
        terminate_job(handle, exit_code)

    def job_exists(self, name: str) -> bool:
        return job_exists(name)

    def job_active_processes(self, handle: int) -> int:
        return job_active_processes(handle)

    def wait_for_job_absent(self, name: str, timeout: float) -> bool:
        return wait_for_job_absent(name, timeout)

    def spawn(
        self,
        argv: tuple[str, ...],
        *,
        cwd: Path,
        environment: Mapping[str, str],
        job_handles: list[int],
        exact_job_handle: int | None,
        cleanup_job_handle: int,
        inherited_handles: list[int] | None = None,
        capture_output: bool = True,
    ) -> ManagedWindowsProcess:
        return spawn_process(
            argv,
            cwd=cwd,
            environment=environment,
            job_handles=job_handles,
            exact_job_handle=exact_job_handle,
            cleanup_job_handle=cleanup_job_handle,
            inherited_handles=inherited_handles,
            capture_output=capture_output,
        )