"""Finite isolated utilities on the existing Windows process owner; no daemon or RPC parser."""
from __future__ import annotations

from dataclasses import dataclass
import os
import re
from pathlib import Path
import threading
import time
from typing import Mapping
import uuid

from .appcontainer import AppContainerProfile
from .windows import WindowsProcessError, close_handle, create_job, job_active_processes, spawn_process


@dataclass(frozen=True)
class IsolatedUtilityResult:
    exit_code: int
    stdout: bytes
    stderr_bytes: int


def run_isolated_utility(
    command: list[str], *, cwd: Path, read_directories: list[Path], write_directories: list[Path],
    environment: Mapping[str, str], timeout: float = 120, stdout_limit: int = 262144,
    stderr_limit: int = 65536, memory_bytes: int = 1024 * 1024 * 1024, owner_id: str,
) -> IsolatedUtilityResult:
    """Caller owns install admission and the directories. No inherited secrets, network or child launch."""
    if not command or not Path(command[0]).is_absolute() or not 0 < timeout <= 240:
        raise WindowsProcessError('invalid isolated utility admission')
    if not 1024 <= stdout_limit <= 8 * 1024 * 1024 or not 0 <= stderr_limit <= 1024 * 1024:
        raise WindowsProcessError('invalid isolated utility output bounds')
    if cwd not in write_directories or set(read_directories) & set(write_directories):
        raise WindowsProcessError('isolated utility directories must have explicit disjoint roles')
    allowed_environment = {'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE', 'APPDATA', 'LOCALAPPDATA', 'PATH'}
    if set(environment) - allowed_environment or (allowed_environment - {'PATH'}) - set(environment):
        raise WindowsProcessError('isolated utility requires a complete explicit environment')
    if not isinstance(owner_id, str) or re.fullmatch(r'[a-f0-9]{32}', owner_id) is None:
        raise WindowsProcessError('invalid utility owner identity')
    profile = AppContainerProfile('localgpt.' + owner_id)
    job = None
    process = None
    readers: list[threading.Thread] = []
    stdout = bytearray()
    stderr_bytes = 0
    failed = threading.Event()
    try:
        for directory in read_directories:
            profile.grant_directory(directory)
        for directory in write_directories:
            profile.grant_directory(directory, writable=True)
        job = create_job('Local\\McpUtility-' + owner_id, process_limit=1, memory_bytes=memory_bytes)
        process = spawn_process(
            command, cwd=cwd, environment=environment, job_handles=[job], exact_job_handle=job,
            cleanup_job_handle=job, sandbox_sid=profile.sid, exact_environment=True,
        )

        def drain(fd: int, output: bool):
            nonlocal stderr_bytes
            try:
                while True:
                    chunk = os.read(fd, 4096)
                    if not chunk:
                        return
                    if output:
                        if len(stdout) + len(chunk) > stdout_limit:
                            failed.set()
                            return
                        stdout.extend(chunk)
                    else:
                        stderr_bytes += len(chunk)
                        if stderr_bytes > stderr_limit:
                            failed.set()
                            return
            except OSError:
                failed.set()

        readers = [threading.Thread(target=drain, args=(process.stdout_fd, True), daemon=True),
                   threading.Thread(target=drain, args=(process.stderr_fd, False), daemon=True)]
        for reader in readers:
            reader.start()
        deadline = time.monotonic() + timeout
        while process.poll() is None:
            if failed.wait(0.02):
                raise WindowsProcessError('isolated utility exceeded its output budget')
            if time.monotonic() >= deadline:
                raise WindowsProcessError('isolated utility deadline exceeded')
        for reader in readers:
            reader.join(timeout=1)
        if failed.is_set() or any(reader.is_alive() for reader in readers) or job_active_processes(job) != 0:
            raise WindowsProcessError('isolated utility quiescence or output integrity is unproven')
        return IsolatedUtilityResult(process.poll(), bytes(stdout), stderr_bytes)
    finally:
        if process is not None:
            process.terminate_tree()
            process.wait(2)
        if job is not None and job_active_processes(job) != 0:
            # Keep evidence/profile alive rather than declaring safe cleanup.
            close_handle(job)
            raise WindowsProcessError('isolated utility tree did not terminate')
        for reader in readers:
            reader.join(timeout=1)
        if process is not None:
            process.close()
        close_handle(job)
        profile.close()
