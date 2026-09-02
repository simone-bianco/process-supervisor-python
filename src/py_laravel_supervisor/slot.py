from __future__ import annotations


import secrets
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Callable

from .backend import ProcessBackend, WindowsProcessBackend
from .commands import build_group_command
from .contracts import DesiredManifest, ProcessGroupSpec
from .events import EventStore
from .lifecycle import LifecycleLedger, LifecycleState
from .locks import WindowsMutex, mutex_name
from .names import child_job_name
from .queue_protocol import QueueProtocolClassifier
from .runtime_files import RuntimeStore
from .windows import ManagedWindowsProcess, start_pipe_reader

CommandBuilder = Callable[[DesiredManifest, ProcessGroupSpec], tuple[str, ...]]
DesiredValidator = Callable[[str, int, int, int], bool]


@dataclass(frozen=True, slots=True)
class SlotTick:
    healthy_completion: bool = False
    process_failure: bool = False
    recovery_required: bool = False


class ManagedSlot:
    """One desired process slot with exact Job ownership and bounded restart accounting."""

    def __init__(
        self,
        *,
        store: RuntimeStore,
        events: EventStore,
        installation_id: str,
        supervisor_incarnation: str,
        anchor_job_handle: int,
        group: ProcessGroupSpec,
        slot: int,
        command_builder: CommandBuilder = build_group_command,
        desired_validator: DesiredValidator | None = None,
        backend: ProcessBackend | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.store = store
        self.events = events
        self.installation_id = installation_id
        self.supervisor_incarnation = supervisor_incarnation
        self.anchor_job_handle = anchor_job_handle
        self.group = group
        self.slot = slot
        self.command_builder = command_builder
        self.desired_validator = desired_validator
        self.backend = backend or WindowsProcessBackend()
        self.clock = clock
        self.ledger = LifecycleLedger(
            store,
            store.paths.instance_ledger(group.id, slot),
            installation_id=installation_id,
            role="child",
        )
        self.ledger.initialize_clean()
        self.process: ManagedWindowsProcess | None = None
        self.job_handle: int | None = None
        self.job_name: str | None = None
        self.attempt_id: str | None = None
        self.classifier: QueueProtocolClassifier | None = None
        self.reader_threads: list[threading.Thread] = []
        self.runtime_error = False
        self.protocol_error = False
        self.process_started_at: float | None = None
        self.stop_requested_at: float | None = None
        self.stop_deadline: float | None = None
        self.next_spawn_at = 0.0
        self.crashes: deque[float] = deque()
        self.fatal = False
        self.last_outcome = "unknown"
        self.last_exit_code: int | None = None
        self.spawn_count = 0
        self.restart_count = 0
        self.healthy_recycle_pending = False
        self._observer_lock = threading.Lock()

    @property
    def active(self) -> bool:
        return self.process is not None

    @property
    def stopping(self) -> bool:
        return self.stop_requested_at is not None

    def replace_group(self, group: ProcessGroupSpec) -> None:
        if self.active:
            raise RuntimeError("cannot replace an active slot generation")
        self.group = group
        self.stop_requested_at = None
        self.stop_deadline = None
        self.fatal = False
        self.next_spawn_at = 0.0
        self.crashes.clear()
        self.spawn_count = 0
        self.restart_count = 0
        self.healthy_recycle_pending = False

    def spawn(self, manifest: DesiredManifest) -> None:
        if self.process is not None or self.fatal or self.clock() < self.next_spawn_at:
            return
        attempt_id = secrets.token_hex(16)
        job_name = child_job_name(
            self.installation_id,
            self.supervisor_incarnation,
            self.group.id,
            self.slot,
            attempt_id,
        )
        child_job: int | None = None
        process: ManagedWindowsProcess | None = None
        armed = False
        try:
            child_job = self.backend.create_job(job_name)
            self.classifier = QueueProtocolClassifier() if self.group.kind == "queue_once" else None
            self.healthy_recycle_pending = False
            self.runtime_error = False
            self.protocol_error = False
            self.process_started_at = None
            with self._transition_lock():
                if self.desired_validator is not None and not self.desired_validator(
                    self.group.id,
                    self.group.generation,
                    self.slot,
                    manifest.revision,
                ):
                    return
                self.ledger.arm(
                    attempt_id=attempt_id,
                    desired_revision=manifest.revision,
                    ownership_id=job_name,
                    supervisor_incarnation=self.supervisor_incarnation,
                    group_id=self.group.id,
                    slot=self.slot,
                )
                armed = True

            process = self.backend.spawn(
                self.command_builder(manifest, self.group),
                cwd=manifest.runtime.project_root,
                environment=manifest.runtime.child_environment_mapping(),
                job_handles=[self.anchor_job_handle, child_job],
                exact_job_handle=child_job,
                cleanup_job_handle=child_job,
                capture_output=True,
            )

            stale_after_spawn = False
            with self._transition_lock():
                if self.desired_validator is not None and not self.desired_validator(
                    self.group.id,
                    self.group.generation,
                    self.slot,
                    manifest.revision,
                ):
                    self.ledger.mark_stopping(attempt_id)
                    stale_after_spawn = True
                else:
                    self.ledger.mark_active(attempt_id, pid=process.pid)
            self.process = process
            self.job_handle = child_job
            self.job_name = job_name
            self.attempt_id = attempt_id
            self.process_started_at = self.clock()
            if stale_after_spawn:
                self.stop_requested_at = self.process_started_at
                self.stop_deadline = self.process_started_at + 0.1
            else:
                self.stop_requested_at = None
                self.stop_deadline = None
            self.spawn_count += 1
            self._start_readers(process)
            self.events.publish_lifecycle("process_started", group_id=self.group.id, slot=self.slot)
        except BaseException:
            if process is not None:
                process.close()
            quiescent = True
            if child_job is not None:
                quiescent = self._ensure_job_quiescent(child_job, timeout=2.0)
                self.backend.close_handle(child_job)
                child_job = None
            if armed:
                try:
                    with self._transition_lock():
                        current = self.ledger.read()
                        if current["attempt_id"] == attempt_id:
                            if quiescent:
                                self.ledger.mark_clean(attempt_id)
                            else:
                                self.ledger.mark_uncertain(attempt_id)
                except Exception:
                    pass
            raise
        finally:
            if child_job is not None and self.process is None:
                self.backend.close_handle(child_job)

    def request_stop(self, *, grace_seconds: float | None = None) -> None:
        if self.process is None or self.attempt_id is None:
            return
        if self.stop_requested_at is not None:
            return
        now = self.clock()
        self.stop_requested_at = now
        self.stop_deadline = now + (self.group.stop_grace_seconds if grace_seconds is None else max(0.1, grace_seconds))
        try:
            with self._transition_lock():
                current = self.ledger.read()
                if current["state"] not in {LifecycleState.STOPPING.value, LifecycleState.CLEAN.value}:
                    self.ledger.mark_stopping(self.attempt_id)
        except Exception:
            self.runtime_error = True

    def tick(self) -> SlotTick:
        process = self.process
        if process is None:
            return SlotTick()
        now = self.clock()
        watchdog_seconds = 0
        if self.group.kind == "queue_once" and self.group.queue is not None:
            watchdog_seconds = self.group.queue.watchdog_seconds
        elif self.group.kind == "scheduler" and self.group.scheduler is not None:
            watchdog_seconds = self.group.scheduler.watchdog_seconds
        if (
            watchdog_seconds > 0
            and self.process_started_at is not None
            and now - self.process_started_at >= watchdog_seconds
        ):
            self.runtime_error = True
            self.events.publish_lifecycle(
                "process_watchdog_timeout",
                group_id=self.group.id,
                slot=self.slot,
                reason_code="process_watchdog_timeout",
            )
            self._terminate_exact_job()
        if self.stop_deadline is not None and now >= self.stop_deadline and process.poll() is None:
            self._terminate_exact_job()
        exit_code = process.poll()
        if exit_code is None:
            return SlotTick()
        return self._finish(exit_code, now)

    def record_spawn_failure(self) -> None:
        self._record_crash(self.clock())

    def force_stop(self) -> SlotTick:
        if self.process is None:
            return SlotTick()
        self.runtime_error = self.runtime_error or not self.stopping
        self._terminate_exact_job()
        exit_code = self.process.wait(2.0)
        if exit_code is None:
            return SlotTick(recovery_required=True)
        return self._finish(exit_code, self.clock())

    def status(self) -> dict[str, object]:
        backoff_remaining = max(0.0, self.next_spawn_at - self.clock())
        state = (
            "fatal"
            if self.fatal
            else "stopping"
            if self.stopping
            else "running"
            if self.active
            else "idle"
            if backoff_remaining > 0 and self.healthy_recycle_pending
            else "backoff"
            if backoff_remaining > 0
            else "stopped"
        )
        return {
            "group_id": self.group.id,
            "slot": self.slot,
            "kind": self.group.kind,
            "generation": self.group.generation,
            "state": state,
            "pid": self.process.pid if self.process is not None else None,
            "last_exit_code": self.last_exit_code,
            "last_job_outcome": self.last_outcome,
            "crash_count": len(self.crashes),
            "restart_count": self.restart_count,
            "backoff_remaining_seconds": backoff_remaining,
        }

    def close(self) -> None:
        if self.process is not None:
            self.force_stop()
        if self.job_handle is not None and not self.ledger.requires_recovery():
            self.backend.close_handle(self.job_handle)
            self.job_handle = None

    def _start_readers(self, process: ManagedWindowsProcess) -> None:
        stdout_fd, stderr_fd = process.stdout_fd, process.stderr_fd
        process.stdout_fd = None
        process.stderr_fd = None
        if stdout_fd is None and stderr_fd is None:
            self.reader_threads = []
            return
        if stdout_fd is None or stderr_fd is None:
            raise RuntimeError("managed process output handles are incomplete")
        self.reader_threads = [
            start_pipe_reader(
                stdout_fd,
                lambda _line: None,
                self._on_pipe_error,
                protocol_line_observer=self._observe_protocol if self.classifier is not None else None,
            ),
            start_pipe_reader(
                stderr_fd,
                lambda _line: None,
                self._on_pipe_error,
            ),
        ]

    def _observe_protocol(self, raw_line: str) -> None:
        classifier = self.classifier
        if classifier is None:
            return
        with self._observer_lock:
            try:
                consumed = classifier.consume_line(raw_line)
            except Exception:
                self.protocol_error = True
                raise
            if not consumed:
                return
            frame = classifier.frames[-1]
            self.events.publish_queue(group_id=self.group.id, slot=self.slot, frame=frame)

    def _on_pipe_error(self, _reason: str) -> None:
        self.runtime_error = True

    def _terminate_exact_job(self) -> None:
        if self.job_handle is None or self.job_name is None or self.attempt_id is None:
            self.runtime_error = True
            return
        try:
            with self._transition_lock():
                current = self.ledger.read()
                if (
                    current["attempt_id"] != self.attempt_id
                    or current["ownership_id"] != self.job_name
                    or current["state"] == LifecycleState.CLEAN.value
                ):
                    raise RuntimeError("exact child ownership changed before termination")
                self.backend.terminate_job(self.job_handle)
        except Exception:
            self.runtime_error = True

    def _finish(self, exit_code: int, now: float) -> SlotTick:
        process = self.process
        attempt_id = self.attempt_id
        job_handle = self.job_handle
        job_name = self.job_name
        if process is None or attempt_id is None or job_handle is None or job_name is None:
            return SlotTick(recovery_required=True)

        quiescent = self._ensure_job_quiescent(job_handle, timeout=2.0)
        if not quiescent:
            self._terminate_exact_job()
            quiescent = self._ensure_job_quiescent(job_handle, timeout=1.0)

        for thread in self.reader_threads:
            thread.join(timeout=2.0)
            if thread.is_alive():
                self.runtime_error = True
        self.reader_threads = []

        if not quiescent:
            self._mark_uncertain_if_current(attempt_id)
            process.close()
            self.process = None
            return SlotTick(recovery_required=True)

        was_stopping = self.stopping
        self.last_exit_code = exit_code
        healthy, outcome = self._classify_completion(exit_code)
        self.last_outcome = outcome

        process.close()
        self.process = None
        self.backend.close_handle(job_handle)
        self.job_handle = None
        if not self.backend.wait_for_job_absent(job_name, 1.0):
            self._mark_uncertain_if_current(attempt_id)
            return SlotTick(recovery_required=True)

        try:
            with self._transition_lock():
                current = self.ledger.read()
                if current["attempt_id"] != attempt_id or current["ownership_id"] != job_name:
                    raise RuntimeError("child ownership changed before clean transition")
                self.ledger.mark_clean(attempt_id)
        except Exception:
            return SlotTick(recovery_required=True)

        self._clear_runtime_handles(close_job=False)
        self.events.publish_lifecycle("process_stopped", group_id=self.group.id, slot=self.slot)
        finished_at = self.clock()
        if healthy or was_stopping:
            if healthy and not was_stopping and self.group.kind == "queue_once" and self.group.queue is not None:
                delay = max(0.25, float(self.group.queue.sleep_seconds)) if outcome == "empty" else 0.05
                self.next_spawn_at = finished_at + delay
                self.healthy_recycle_pending = True
            else:
                self.next_spawn_at = finished_at + 0.05
                self.healthy_recycle_pending = False
            return SlotTick(healthy_completion=True)
        self._record_crash(finished_at)
        return SlotTick(process_failure=True)

    def _classify_completion(self, exit_code: int) -> tuple[bool, str]:
        if self.group.kind == "queue_once" and self.classifier is not None:
            with self._observer_lock:
                return self.classifier.completion(
                    exit_code,
                    runtime_error=self.runtime_error or self.protocol_error,
                )
        if self.group.kind == "scheduler":
            healthy = exit_code == 0 and not self.runtime_error and not self.protocol_error
            return healthy, "scheduler_completed" if healthy else "scheduler_failed"
        return (False, "unknown")

    def _record_crash(self, now: float) -> None:
        policy = self.group.restart
        self.healthy_recycle_pending = False
        self.restart_count += 1
        self.crashes.append(now)
        while self.crashes and now - self.crashes[0] > policy.crash_window_seconds:
            self.crashes.popleft()
        if not policy.enabled or len(self.crashes) >= policy.max_crashes:
            self.fatal = True
            return
        exponent = max(0, len(self.crashes) - 1)
        delay = min(policy.max_delay_seconds, policy.base_delay_seconds * (2**exponent))
        self.next_spawn_at = now + delay
        self.events.publish_lifecycle(
            "process_backoff",
            group_id=self.group.id,
            slot=self.slot,
            reason_code="process_failure",
        )

    def _ensure_job_quiescent(self, job_handle: int, *, timeout: float) -> bool:
        deadline = self.clock() + timeout
        while self.clock() < deadline:
            try:
                if self.backend.job_active_processes(job_handle) == 0:
                    return True
            except Exception:
                return False
            time.sleep(0.02)
        try:
            return self.backend.job_active_processes(job_handle) == 0
        except Exception:
            return False

    def _mark_uncertain_if_current(self, attempt_id: str) -> None:
        try:
            with self._transition_lock():
                current = self.ledger.read()
                if current["attempt_id"] == attempt_id and current["state"] != LifecycleState.CLEAN.value:
                    self.ledger.mark_uncertain(attempt_id)
        except Exception:
            pass

    def _clear_runtime_handles(self, *, close_job: bool) -> None:
        if self.process is not None:
            self.process.close()
        if close_job and self.job_handle is not None:
            self.backend.close_handle(self.job_handle)
        self.process = None
        self.job_handle = None
        self.job_name = None
        self.attempt_id = None
        self.classifier = None
        self.process_started_at = None
        self.stop_requested_at = None
        self.stop_deadline = None
        self.runtime_error = False
        self.protocol_error = False

    def _transition_lock(self) -> WindowsMutex:
        return WindowsMutex(
            mutex_name(self.installation_id, "transition"),
            timeout_ms=2_000,
        )