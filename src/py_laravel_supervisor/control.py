from __future__ import annotations

import os
import secrets
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .contracts import ContractError, DesiredManifest, SCHEMA_VERSION
from .lifecycle import LifecycleLedger, LifecycleState
from .locks import WindowsMutex, mutex_is_locked, mutex_name
from .names import anchor_job_name
from .recovery import RuntimeRecovery
from .runtime_files import RuntimeStore
from .windows import (
    close_handle,
    create_job,
    job_active_processes,
    job_exists,
    open_job,
    spawn_process,
    terminate_job,
    wait_for_job_absent,
)


class ControlError(RuntimeError):
    pass


ResidentArgvBuilder = Callable[[Path, str, str, str, str, str], tuple[str, ...]]


class SupervisorControl:
    """Short-lived control surface for resident bootstrap, shutdown and recovery."""

    def __init__(
        self,
        runtime_root: str | Path,
        installation_id: str,
        *,
        resident_argv_builder: ResidentArgvBuilder | None = None,
    ) -> None:
        self.store = RuntimeStore(runtime_root)
        self.installation_id = installation_id
        self.resident_argv_builder = resident_argv_builder or self._default_resident_argv
        self.ledger = LifecycleLedger(
            self.store,
            self.store.paths.supervisor_ledger,
            installation_id=installation_id,
            role="resident",
        )

    def apply_desired(self, payload: dict[str, object]) -> dict[str, object]:
        """Atomically linearize Laravel-owned desired state with the spawn gate."""
        self.store.initialize(self.installation_id)
        desired = DesiredManifest.from_mapping(payload)
        if desired.installation_id != self.installation_id:
            raise ControlError("desired installation identity mismatch")

        with self._transition_lock():
            current_raw = self.store.read_json(self.store.paths.desired, required=False)
            current = DesiredManifest.from_mapping(current_raw) if current_raw is not None else None
            gate = self.store.gate(self.installation_id)
            if current is not None:
                if desired.revision < current.revision:
                    raise ControlError("desired revision cannot move backwards")
                if desired.revision == current.revision and current_raw != payload:
                    raise ControlError("desired revision cannot be reused for different state")
            recovery_constrained = False
            recovery_reduction = False
            if desired.enabled and gate["state"] == "recovery_required":
                recovery_constrained = True
                if current is None:
                    raise ControlError("recovery is required before enabling the supervisor")
                if current_raw != payload:
                    if not self._is_recovery_safe_reduction(current, desired):
                        raise ControlError("recovery is required before increasing or changing supervisor desired state")
                    recovery_reduction = True

            if desired.enabled:
                self.store.write_json(self.store.paths.desired, payload)
                if not recovery_constrained:
                    self.store.enable_gate(self.installation_id, desired.revision)
                    self.store.unlink(self.store.paths.shutdown)
            else:
                # Disable the spawn fence before publishing the disabled desired state.
                self.store.disable_gate(self.installation_id, desired.revision)
                self.store.write_json(self.store.paths.desired, payload)
            final_gate = self.store.gate(self.installation_id)

        return {
            "status": "applied",
            "enabled": desired.enabled,
            "revision": desired.revision,
            "spawn_gate": final_gate["state"],
            "recovery_reduction": recovery_reduction,
        }

    def disable_gate(self) -> dict[str, object]:
        """Close the spawn fence without reading or rewriting desired state."""
        try:
            self.store.validate_runtime_owner(self.installation_id)
        except ContractError as error:
            raise ControlError(str(error)) from error

        with self._transition_lock():
            try:
                self.store.validate_runtime_owner(self.installation_id)
                gate = self.store.gate_with_backup(self.installation_id)
            except ContractError as error:
                raise ControlError(str(error)) from error
            revision = int(gate["revision"])
            self.store.disable_gate(self.installation_id, revision)
            final_gate = self.store.gate(self.installation_id)

        return {
            "status": "disabled",
            "spawn_gate": final_gate["state"],
            "revision": final_gate["revision"],
        }

    @staticmethod
    def _is_recovery_safe_reduction(current: DesiredManifest, desired: DesiredManifest) -> bool:
        if not current.enabled or not desired.enabled or desired.revision <= current.revision:
            return False
        if current.installation_id != desired.installation_id or current.runtime != desired.runtime:
            return False
        if len(current.groups) != len(desired.groups):
            return False

        current_by_id = {group.id: group for group in current.groups}
        reduced = False
        for target in desired.groups:
            existing = current_by_id.get(target.id)
            if existing is None:
                return False
            if (
                existing.id != target.id
                or existing.kind != target.kind
                or existing.generation != target.generation
                or existing.stop_grace_seconds != target.stop_grace_seconds
                or existing.restart != target.restart
                or existing.queue != target.queue
                or existing.scheduler != target.scheduler
            ):
                return False
            if target.desired_processes > existing.desired_processes:
                return False
            reduced = reduced or target.desired_processes < existing.desired_processes
        return reduced

    def ensure_running(self, *, ready_timeout_seconds: float = 5.0) -> dict[str, object]:
        self.store.initialize(self.installation_id)
        self.ledger.initialize_clean()
        desired, gate = self._desired_gate_snapshot()
        if desired.installation_id != self.installation_id:
            raise ControlError("desired installation identity mismatch")
        if not desired.enabled or gate["state"] != "enabled":
            return {"status": "disabled"}
        if gate["revision"] != desired.revision:
            raise ControlError("spawn gate revision does not match desired state")

        resident_lock_free = self._resident_lock_free()
        state = self.ledger.read()
        if state["state"] != LifecycleState.CLEAN.value:
            return self._wait_for_existing_resident(
                timeout=max(0.1, min(ready_timeout_seconds, 30.0)),
            )
        if not resident_lock_free:
            self._mark_recovery_required(desired.revision, "resident_lock_ambiguous")
            raise ControlError("resident singleton lock is held while lifecycle is clean")

        anchor_handle: int | None = None
        process = None
        armed = False
        attempt_id = secrets.token_hex(16)
        incarnation = secrets.token_hex(16)
        ready_nonce = secrets.token_hex(16)
        anchor_name = anchor_job_name(self.installation_id, incarnation, attempt_id)
        stale_after_spawn = False
        try:
            with self._transition_lock():
                desired = self.store.desired_manifest()
                gate = self.store.require_gate_enabled(self.installation_id)
                if not desired.enabled:
                    return {"status": "disabled"}
                if gate["revision"] != desired.revision:
                    raise ControlError("spawn gate revision changed while acquiring transition lock")
                state = self.ledger.read()
                if state["state"] == LifecycleState.CLEAN.value:
                    anchor_handle = create_job(anchor_name)
                    self.ledger.arm(
                        attempt_id=attempt_id,
                        desired_revision=desired.revision,
                        ownership_id=anchor_name,
                        supervisor_incarnation=incarnation,
                        ready_nonce=ready_nonce,
                    )
                    armed = True

            if not armed:
                return self._wait_for_existing_resident(
                    timeout=max(0.1, min(ready_timeout_seconds, 30.0)),
                )
            if anchor_handle is None:
                raise ControlError("resident Anchor handle is unavailable after arm")

            package_src = str(Path(__file__).resolve().parents[1])
            python_path_entries = [package_src]
            venv_site_packages = Path(sys.prefix) / "Lib" / "site-packages"
            if venv_site_packages.is_dir():
                python_path_entries.append(str(venv_site_packages))
            process = spawn_process(
                self.resident_argv_builder(
                    self.store.paths.root,
                    self.installation_id,
                    incarnation,
                    attempt_id,
                    ready_nonce,
                    anchor_name,
                ),
                cwd=desired.runtime.project_root,
                environment={
                    "PYTHONPATH": os.pathsep.join(python_path_entries),
                    "PYTHONUTF8": "1",
                    "PYTHONIOENCODING": "utf-8",
                },
                job_handles=[anchor_handle],
                exact_job_handle=None,
                cleanup_job_handle=anchor_handle,
                capture_output=False,
                breakaway_from_parent_job=True,
            )

            with self._transition_lock():
                current_desired = self.store.desired_manifest()
                current_gate = self.store.gate(self.installation_id)
                current = self.ledger.read()
                same_attempt = (
                    current.get("attempt_id") == attempt_id
                    and current.get("supervisor_incarnation") == incarnation
                    and current.get("ownership_id") == anchor_name
                )
                if not same_attempt:
                    raise ControlError("resident bootstrap identity changed after process creation")
                stale_after_spawn = (
                    not current_desired.enabled
                    or current_gate["state"] != "enabled"
                    or current_gate["revision"] != current_desired.revision
                    or current_desired.revision != desired.revision
                )
                if stale_after_spawn and current["state"] not in {
                    LifecycleState.CLEAN.value,
                    LifecycleState.STOPPING.value,
                }:
                    self.ledger.mark_stopping(attempt_id)
        except BaseException:
            quiescent = True
            if anchor_handle is not None:
                try:
                    quiescent = job_active_processes(anchor_handle) == 0
                except Exception:
                    quiescent = False
                close_handle(anchor_handle)
                anchor_handle = None
            if armed:
                try:
                    with self._transition_lock():
                        current = self.ledger.read()
                        if current["attempt_id"] == attempt_id:
                            if quiescent and not job_exists(anchor_name):
                                self.ledger.mark_clean(attempt_id)
                            else:
                                self.ledger.mark_uncertain(attempt_id)
                                self.store.mark_recovery_required(
                                    self.installation_id,
                                    desired.revision,
                                    "resident_spawn_uncertain",
                                )
                except Exception:
                    pass
            raise
        finally:
            if process is not None:
                process.close()
        if stale_after_spawn:
            if anchor_handle is None:
                raise ControlError("stale resident Anchor handle is unavailable")
            return self._contain_stale_bootstrap(
                anchor_handle=anchor_handle,
                anchor_name=anchor_name,
                attempt_id=attempt_id,
                incarnation=incarnation,
            )
        ready = self._wait_for_ready(
            incarnation,
            attempt_id,
            ready_nonce,
            timeout=max(0.1, min(ready_timeout_seconds, 30.0)),
        )
        if ready is None:
            if anchor_handle is None:
                raise ControlError("resident Anchor handle is unavailable after readiness timeout")
            contained = False
            try:
                terminate_job(anchor_handle)
            finally:
                close_handle(anchor_handle)
                anchor_handle = None
            contained = wait_for_job_absent(anchor_name, 3.0)
            with self._transition_lock():
                current = self.ledger.read()
                if current.get("attempt_id") == attempt_id:
                    if contained:
                        self.ledger.mark_clean(attempt_id)
                    else:
                        self.ledger.mark_uncertain(attempt_id)
                self.store.mark_recovery_required(
                    self.installation_id,
                    desired.revision,
                    "resident_ready_ambiguous",
                )
            return {
                "status": "recovery_required",
                "incarnation": incarnation,
                "attempt_id": attempt_id,
            }
        if anchor_handle is not None:
            close_handle(anchor_handle)
            anchor_handle = None
        return {
            "status": "started",
            "incarnation": incarnation,
            "attempt_id": attempt_id,
            "pid": ready.get("pid"),
        }

    def shutdown(
        self,
        *,
        graceful_timeout_seconds: float = 10.0,
        hard_timeout_seconds: float = 5.0,
    ) -> dict[str, object]:
        self.store.initialize(self.installation_id)
        self.ledger.initialize_clean()
        gate = self.store.gate(self.installation_id)
        if gate["state"] == "enabled":
            raise ControlError("disable the spawn gate before shutdown")
        state = self.ledger.read()
        if state["state"] == LifecycleState.CLEAN.value:
            self.store.unlink(self.store.paths.ready)
            return {"status": "stopped"}
        attempt_id = state.get("attempt_id")
        incarnation = state.get("supervisor_incarnation")
        anchor_name = state.get("ownership_id")
        if not all(isinstance(value, str) and value for value in (attempt_id, incarnation, anchor_name)):
            raise ControlError("resident shutdown identity is unavailable")
        with self._transition_lock():
            gate = self.store.gate(self.installation_id)
            current = self.ledger.read()
            if gate["state"] == "enabled":
                raise ControlError("spawn gate changed during shutdown")
            if current.get("attempt_id") != attempt_id or current.get("supervisor_incarnation") != incarnation:
                raise ControlError("resident identity changed during shutdown")
            self.store.write_json(
                self.store.paths.shutdown,
                {
                    "schema_version": SCHEMA_VERSION,
                    "installation_id": self.installation_id,
                    "incarnation": incarnation,
                    "attempt_id": attempt_id,
                    "requested_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        if wait_for_job_absent(anchor_name, max(0.1, min(graceful_timeout_seconds, 60.0))):
            return {"status": RuntimeRecovery(self.store.paths.root, self.installation_id).recover()}
        with self._transition_lock():
            gate = self.store.gate(self.installation_id)
            current = self.ledger.read()
            if gate["state"] == "enabled":
                raise ControlError("spawn gate changed before hard stop")
            if (
                current.get("attempt_id") != attempt_id
                or current.get("supervisor_incarnation") != incarnation
                or current.get("ownership_id") != anchor_name
            ):
                raise ControlError("resident identity changed before hard stop")
            handle = open_job(anchor_name, terminate=True)
            if handle is not None:
                try:
                    terminate_job(handle)
                finally:
                    close_handle(handle)
        if not wait_for_job_absent(anchor_name, max(0.1, min(hard_timeout_seconds, 30.0))):
            self.store.mark_recovery_required(
                self.installation_id,
                int(gate.get("revision", 0)),
                "anchor_termination_unconfirmed",
            )
            return {"status": "recovery_required"}
        return {"status": RuntimeRecovery(self.store.paths.root, self.installation_id).recover()}

    def recover(self) -> dict[str, object]:
        return {"status": RuntimeRecovery(self.store.paths.root, self.installation_id).recover()}

    def status(self) -> dict[str, object]:
        self.store.initialize(self.installation_id)
        gate = self.store.gate(self.installation_id)
        payload = self.store.read_json(self.store.paths.status, required=False) or {
            "schema_version": SCHEMA_VERSION,
            "installation_id": self.installation_id,
            "state": "stopped",
        }
        payload = dict(payload)
        payload["spawn_gate"] = gate["state"]
        return payload

    def _contain_stale_bootstrap(
        self,
        *,
        anchor_handle: int,
        anchor_name: str,
        attempt_id: str,
        incarnation: str,
    ) -> dict[str, object]:
        target_enabled = False
        revision = 0
        try:
            with self._transition_lock():
                desired = self.store.desired_manifest()
                gate = self.store.gate(self.installation_id)
                current = self.ledger.read()
                if (
                    current.get("attempt_id") != attempt_id
                    or current.get("supervisor_incarnation") != incarnation
                    or current.get("ownership_id") != anchor_name
                ):
                    raise ControlError("resident identity changed before stale bootstrap containment")
                target_enabled = desired.enabled and gate["state"] == "enabled"
                revision = desired.revision
                if current["state"] not in {LifecycleState.CLEAN.value, LifecycleState.STOPPING.value}:
                    self.ledger.mark_stopping(attempt_id)
                terminate_job(anchor_handle)
        except BaseException:
            try:
                close_handle(anchor_handle)
            finally:
                self._mark_recovery_required(revision, "stale_anchor_termination_failed", attempt_id=attempt_id)
            return {
                "status": "recovery_required",
                "incarnation": incarnation,
                "attempt_id": attempt_id,
            }

        close_handle(anchor_handle)
        if not wait_for_job_absent(anchor_name, 3.0):
            self._mark_recovery_required(revision, "stale_anchor_termination_unconfirmed", attempt_id=attempt_id)
            return {
                "status": "recovery_required",
                "incarnation": incarnation,
                "attempt_id": attempt_id,
            }

        with self._transition_lock():
            current = self.ledger.read()
            if (
                current.get("attempt_id") == attempt_id
                and current.get("supervisor_incarnation") == incarnation
                and current["state"] != LifecycleState.CLEAN.value
            ):
                self.ledger.mark_clean(attempt_id)
        self.store.unlink(self.store.paths.ready)
        return {
            "status": "state_changed" if target_enabled else "disabled",
            "incarnation": incarnation,
            "attempt_id": attempt_id,
        }

    def _wait_for_existing_resident(self, *, timeout: float) -> dict[str, object]:
        deadline = time.monotonic() + timeout
        last_state = self.ledger.read()
        while time.monotonic() < deadline:
            state = self.ledger.read()
            last_state = state
            if state["state"] == LifecycleState.UNCERTAIN.value:
                break
            if state["state"] == LifecycleState.CLEAN.value:
                raise ControlError("resident bootstrap ended without an active resident")
            if state["state"] == LifecycleState.RESIDENT_ACTIVE.value:
                ownership = state.get("ownership_id")
                incarnation = state.get("supervisor_incarnation")
                attempt = state.get("attempt_id")
                ready_nonce = state.get("ready_nonce")
                handle = open_job(ownership) if isinstance(ownership, str) else None
                try:
                    anchor_has_member = handle is not None and job_active_processes(handle) > 0
                finally:
                    close_handle(handle)
                ready = self.store.read_json(self.store.paths.ready, required=False)
                if (
                    not self._resident_lock_free()
                    and anchor_has_member
                    and isinstance(ready, dict)
                    and ready.get("incarnation") == incarnation
                    and ready.get("attempt_id") == attempt
                    and ready.get("ready_nonce") == ready_nonce
                    and self._ready_is_fresh(ready)
                ):
                    return {
                        "status": "already_running",
                        "incarnation": incarnation,
                        "attempt_id": attempt,
                        "pid": ready.get("pid"),
                    }
            time.sleep(0.05)
        revision = int(last_state.get("desired_revision") or 0)
        attempt = last_state.get("attempt_id")
        self._mark_recovery_required(
            revision,
            "resident_readiness_ambiguous",
            attempt_id=attempt if isinstance(attempt, str) else None,
        )
        return {
            "status": "recovery_required",
            "incarnation": last_state.get("supervisor_incarnation"),
            "attempt_id": last_state.get("attempt_id"),
        }

    def _wait_for_ready(
        self,
        incarnation: str,
        attempt_id: str,
        ready_nonce: str,
        *,
        timeout: float,
    ) -> dict[str, object] | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            ready = self.store.read_json(self.store.paths.ready, required=False)
            if (
                isinstance(ready, dict)
                and ready.get("installation_id") == self.installation_id
                and ready.get("incarnation") == incarnation
                and ready.get("attempt_id") == attempt_id
                and ready.get("ready_nonce") == ready_nonce
                and self._ready_is_fresh(ready)
            ):
                return ready
            state = self.ledger.read()
            if state["state"] in {LifecycleState.CLEAN.value, LifecycleState.UNCERTAIN.value}:
                return None
            time.sleep(0.05)
        return None

    def _ready_is_fresh(self, ready: dict[str, object], *, max_age_seconds: float = 5.0) -> bool:
        heartbeat = ready.get("heartbeat_at")
        if not isinstance(heartbeat, str):
            return False
        try:
            observed = datetime.fromisoformat(heartbeat)
        except ValueError:
            return False
        if observed.tzinfo is None:
            return False
        age = (datetime.now(timezone.utc) - observed.astimezone(timezone.utc)).total_seconds()
        return -1.0 <= age <= max_age_seconds

    def _mark_recovery_required(
        self,
        revision: int,
        reason_code: str,
        *,
        attempt_id: str | None = None,
    ) -> None:
        with self._transition_lock():
            state = self.ledger.read()
            if attempt_id is not None and state.get("attempt_id") not in {attempt_id, None}:
                raise ControlError("resident attempt changed before recovery fence")
            self.store.mark_recovery_required(self.installation_id, revision, reason_code)

    def _desired_gate_snapshot(self) -> tuple[DesiredManifest, dict[str, object]]:
        with self._transition_lock():
            return self.store.desired_manifest(), self.store.gate(self.installation_id)

    def _resident_lock_free(self) -> bool:
        return not mutex_is_locked(mutex_name(self.installation_id, "resident"))

    def _transition_lock(self) -> WindowsMutex:
        return WindowsMutex(
            mutex_name(self.installation_id, "transition"),
            timeout_ms=5000,
        )

    @staticmethod
    def _default_resident_argv(
        runtime_root: Path,
        installation_id: str,
        incarnation: str,
        attempt_id: str,
        ready_nonce: str,
        anchor_name: str,
    ) -> tuple[str, ...]:
        bootstrap = (
            "import sys; "
            "from py_laravel_supervisor.cli import main; "
            "raise SystemExit(main(sys.argv[1:]))"
        )
        return (
            getattr(sys, "_base_executable", sys.executable),
            "-c",
            bootstrap,
            "resident",
            "--runtime-root",
            str(runtime_root),
            "--installation-id",
            installation_id,
            "--incarnation",
            incarnation,
            "--attempt-id",
            attempt_id,
            "--ready-nonce",
            ready_nonce,
            "--anchor-job-name",
            anchor_name,
        )