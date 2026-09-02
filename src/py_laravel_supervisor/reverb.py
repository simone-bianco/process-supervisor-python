from __future__ import annotations

import secrets
from typing import Callable, Mapping

from .backend import ProcessBackend, WindowsProcessBackend
from .commands import build_reverb_restart_command
from .contracts import DesiredManifest, ProcessGroupSpec
from .names import signal_job_name

ReverbRestartBuilder = Callable[[DesiredManifest], tuple[str, ...]]


class ReverbGracefulSignaler:
    """Issue a bounded `reverb:restart` signal without treating it as target exit proof."""

    def __init__(
        self,
        *,
        installation_id: str,
        supervisor_incarnation: str,
        anchor_job_handle: int,
        backend: ProcessBackend | None = None,
        command_builder: ReverbRestartBuilder = build_reverb_restart_command,
    ) -> None:
        self.installation_id = installation_id
        self.supervisor_incarnation = supervisor_incarnation
        self.anchor_job_handle = anchor_job_handle
        self.backend = backend or WindowsProcessBackend()
        self.command_builder = command_builder

    def signal(
        self,
        manifest: DesiredManifest,
        group: ProcessGroupSpec,
        target_job_handle: int,
        environment: Mapping[str, str],
    ) -> bool:
        if group.kind != "reverb":
            raise ValueError("Reverb graceful signaling requires a reverb process group")
        try:
            if self.backend.job_active_processes(target_job_handle) <= 0:
                return True
        except Exception:
            return False

        attempt_id = secrets.token_hex(16)
        job_name = signal_job_name(
            self.installation_id,
            self.supervisor_incarnation,
            group.id,
            attempt_id,
        )
        try:
            signal_job = self.backend.create_job(job_name)
        except Exception:
            return False

        process = None
        succeeded = False
        try:
            process = self.backend.spawn(
                self.command_builder(manifest),
                cwd=manifest.runtime.project_root,
                environment=environment,
                job_handles=[self.anchor_job_handle, signal_job],
                exact_job_handle=signal_job,
                cleanup_job_handle=signal_job,
                capture_output=False,
            )
            timeout = min(5.0, max(0.1, group.stop_grace_seconds / 2.0))
            exit_code = process.wait(timeout)
            if exit_code is None:
                self.backend.terminate_job(signal_job)
                exit_code = process.wait(1.0)
            succeeded = exit_code == 0
        except Exception:
            try:
                self.backend.terminate_job(signal_job)
            except Exception:
                pass
        finally:
            if process is not None:
                process.close()
            try:
                if self.backend.job_active_processes(signal_job) > 0:
                    self.backend.terminate_job(signal_job)
            except Exception:
                succeeded = False
            self.backend.close_handle(signal_job)

        return succeeded and self.backend.wait_for_job_absent(job_name, 1.0)