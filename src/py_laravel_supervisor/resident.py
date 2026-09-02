from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .commands import build_group_command
from .contracts import ContractError, DesiredManifest, ProcessGroupSpec, SCHEMA_VERSION
from .events import EventStore
from .lifecycle import LifecycleLedger, LifecycleState
from .locks import LockUnavailable, WindowsMutex, mutex_name
from .names import anchor_job_name
from .reverb import ReverbGracefulSignaler
from .runtime_files import RuntimeStore
from .scheduler import SchedulerTrigger
from .slot import CommandBuilder, ManagedSlot
from .status import StatusRegistry
from .windows import (
    close_handle,
    current_process_in_job,
    job_active_processes,
    open_job,
)


class RecoveryRequired(RuntimeError):
    pass


class SupervisorResident:
    """Resident desired-state reconciler for one installation and Anchor incarnation."""

    def __init__(
        self,
        *,
        runtime_root: str | Path,
        installation_id: str,
        incarnation: str,
        attempt_id: str,
        ready_nonce: str,
        anchor_name: str | None = None,
        anchor_handle: int | None = None,
        command_builder: CommandBuilder = build_group_command,
        reverb_signaler: ReverbGracefulSignaler | None = None,
        poll_interval_seconds: float = 0.1,
        heartbeat_interval_seconds: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.store = RuntimeStore(runtime_root)
        self.installation_id = installation_id
        self.incarnation = incarnation
        self.attempt_id = attempt_id
        self.ready_nonce = ready_nonce
        expected_anchor_name = anchor_job_name(installation_id, incarnation, attempt_id)
        if anchor_name is not None and anchor_name != expected_anchor_name:
            raise ValueError("resident Anchor name does not match runtime identity")
        self.anchor_name = expected_anchor_name
        self.anchor_handle = anchor_handle
        self.command_builder = command_builder
        self.reverb_signaler = reverb_signaler
        self.poll_interval_seconds = max(0.02, min(poll_interval_seconds, 1.0))
        self.heartbeat_interval_seconds = max(
            self.poll_interval_seconds,
            min(heartbeat_interval_seconds, 4.0),
        )
        self.clock = clock
        self.ledger = LifecycleLedger(
            self.store,
            self.store.paths.supervisor_ledger,
            installation_id=installation_id,
            role="resident",
        )
        self.events = EventStore(self.store, installation_id)
        self.status = StatusRegistry(self.store, installation_id, incarnation)
        self.scheduler_trigger = SchedulerTrigger(self.store)
        self.slots: dict[tuple[str, int], ManagedSlot] = {}
        self.shutting_down = False
        self.shutdown_reason = "requested"
        self._resident_lock: WindowsMutex | None = None
        self._next_heartbeat_at = 0.0
        self._last_published_revision: int | None = None
        self._last_published_summary: str | None = None

    def run(self) -> int:
        self.store.initialize(self.installation_id)
        self._attach_anchor()
        self.ledger.initialize_clean()
        try:
            self._resident_lock = WindowsMutex(
                mutex_name(self.installation_id, "resident"),
                timeout_ms=0,
            ).acquire()
        except LockUnavailable as error:
            raise RecoveryRequired("another resident owns the installation") from error
        try:
            self._activate()
            self.events.publish_lifecycle("resident_started")
            while True:
                desired, gate = self._state_snapshot()
                if gate["state"] == "enabled" and desired.enabled and gate["revision"] != desired.revision:
                    raise RecoveryRequired("desired and spawn-gate revisions diverged")
                if gate["state"] != "enabled" or not desired.enabled or self._shutdown_requested():
                    if not self.shutting_down:
                        self._begin_shutdown(desired, reason="desired_disabled")
                if self.shutting_down:
                    self._tick_slots()
                    self._request_all_stops(desired)
                    self._remove_quiescent_undesired({})
                    self._publish_runtime_snapshot(desired, "stopping")
                    if not any(slot.active for slot in self.slots.values()):
                        self._finish_clean_shutdown(desired)
                        return 0
                else:
                    self._tick_slots()
                    self._reconcile(desired)
                    self._publish_runtime_snapshot(desired, self._runtime_summary())
                time.sleep(self.poll_interval_seconds)
        except BaseException as error:
            self._fail_closed(error)
            raise
        finally:
            self.store.unlink(self.store.paths.ready)
            if self._resident_lock is not None:
                self._resident_lock.close()
                self._resident_lock = None
            close_handle(self.anchor_handle)
            self.anchor_handle = None

    def _attach_anchor(self) -> None:
        if self.anchor_handle is None:
            handle = open_job(self.anchor_name, assign=True)
            if handle is None:
                raise RecoveryRequired("resident Anchor Job Object is unavailable")
            try:
                if not current_process_in_job(handle):
                    raise RecoveryRequired("resident is not a member of its exact Anchor Job Object")
            except BaseException:
                close_handle(handle)
                raise
            self.anchor_handle = handle
        if self.reverb_signaler is None:
            self.reverb_signaler = ReverbGracefulSignaler(
                installation_id=self.installation_id,
                supervisor_incarnation=self.incarnation,
                anchor_job_handle=self.anchor_handle,
            )

    def _activate(self) -> None:
        desired = self._load_desired()
        with self._transition_lock():
            gate = self.store.require_gate_enabled(self.installation_id)
            if not desired.enabled or gate["revision"] != desired.revision:
                raise RecoveryRequired("resident activation revision is not current")
            ledger = self.ledger.read()
            if ledger["state"] != LifecycleState.SPAWN_ARMED.value:
                raise RecoveryRequired("resident ledger is not spawn_armed")
            if (
                ledger["attempt_id"] != self.attempt_id
                or ledger["supervisor_incarnation"] != self.incarnation
                or ledger.get("ready_nonce") != self.ready_nonce
                or ledger.get("ownership_id") != self.anchor_name
            ):
                raise RecoveryRequired("resident spawn identity changed")
            if self.anchor_handle is None or job_active_processes(self.anchor_handle) < 1:
                raise RecoveryRequired("resident is not an active Anchor member")
            self.ledger.mark_active(self.attempt_id, pid=os.getpid())
            self._publish_ready(desired)

    def _load_desired(self) -> DesiredManifest:
        desired = self.store.desired_manifest()
        if desired.installation_id != self.installation_id:
            raise RecoveryRequired("desired installation identity mismatch")
        return desired

    def _state_snapshot(self) -> tuple[DesiredManifest, dict[str, object]]:
        with self._transition_lock():
            desired = self._load_desired()
            gate = self.store.gate(self.installation_id)
            return desired, gate

    def _tick_slots(self) -> None:
        for key, slot in list(self.slots.items()):
            result = slot.tick()
            if result.recovery_required:
                raise RecoveryRequired(f"slot ownership became uncertain: {key[0]}:{key[1]}")

    def _reconcile(self, desired: DesiredManifest) -> None:
        targets = self._desired_slots(desired)
        self._request_all_stops(desired, targets=targets)
        self._remove_quiescent_undesired(targets)
        for key, group in targets.items():
            slot = self.slots.get(key)
            if slot is None:
                if self.anchor_handle is None:
                    raise RecoveryRequired("resident Anchor handle is unavailable")
                slot = ManagedSlot(
                    store=self.store,
                    events=self.events,
                    installation_id=self.installation_id,
                    supervisor_incarnation=self.incarnation,
                    anchor_job_handle=self.anchor_handle,
                    group=group,
                    slot=key[1],
                    command_builder=self.command_builder,
                    desired_validator=self._slot_is_current,
                    clock=self.clock,
                )
                if slot.ledger.requires_recovery():
                    raise RecoveryRequired(f"unresolved slot ledger: {key[0]}:{key[1]}")
                self.slots[key] = slot
            elif slot.group.generation != group.generation:
                if slot.active:
                    self._request_slot_stop(desired, slot)
                    continue
                slot.replace_group(group)
            if slot.active or slot.stopping or slot.fatal or self.clock() < slot.next_spawn_at:
                continue
            if group.kind == "scheduler" and not self.scheduler_trigger.claim_if_due(group):
                continue
            self._spawn_slot_if_current(desired, key, slot)

    def _spawn_slot_if_current(
        self,
        desired: DesiredManifest,
        key: tuple[str, int],
        slot: ManagedSlot,
    ) -> None:
        try:
            slot.spawn(desired)
            if slot.active and slot.stopping and slot.group.kind == "reverb":
                self._signal_reverb(desired, slot)
        except RecoveryRequired:
            raise
        except BaseException:
            if slot.ledger.requires_recovery():
                raise RecoveryRequired(f"slot spawn became uncertain: {key[0]}:{key[1]}")
            slot.record_spawn_failure()

    def _request_all_stops(
        self,
        desired: DesiredManifest,
        *,
        targets: dict[tuple[str, int], ProcessGroupSpec] | None = None,
    ) -> None:
        active_targets = self._desired_slots(desired) if targets is None and not self.shutting_down else (targets or {})
        for key, slot in self.slots.items():
            target = active_targets.get(key)
            generation_changed = target is not None and target.generation != slot.group.generation
            if self.shutting_down or target is None or generation_changed:
                if slot.active:
                    self._request_slot_stop(desired, slot)

    def _request_slot_stop(self, desired: DesiredManifest, slot: ManagedSlot) -> None:
        was_stopping = slot.stopping
        slot.request_stop()
        if not was_stopping and slot.group.kind == "reverb":
            self._signal_reverb(desired, slot)

    def _signal_reverb(self, desired: DesiredManifest, slot: ManagedSlot) -> None:
        target_job = slot.job_handle
        if target_job is None:
            slot.runtime_error = True
            return
        if not self.reverb_signaler.signal(
            desired,
            slot.group,
            target_job,
            desired.runtime.child_environment_mapping(),
        ):
            slot.runtime_error = True

    def _remove_quiescent_undesired(
        self,
        targets: dict[tuple[str, int], ProcessGroupSpec],
    ) -> None:
        for key, slot in list(self.slots.items()):
            target = targets.get(key)
            if slot.active:
                continue
            if target is None:
                slot.close()
                self.status.remove_instance(key[0], key[1])
                del self.slots[key]
            elif target.generation != slot.group.generation:
                slot.replace_group(target)

    def _begin_shutdown(self, desired: DesiredManifest, *, reason: str) -> None:
        self.shutting_down = True
        self.shutdown_reason = reason
        self.events.publish_lifecycle("resident_stopping", reason_code=reason)
        with self._transition_lock():
            current = self.ledger.read()
            if current["state"] == LifecycleState.RESIDENT_ACTIVE.value:
                self.ledger.mark_stopping(self.attempt_id)
        self._request_all_stops(desired)

    def _finish_clean_shutdown(self, desired: DesiredManifest) -> None:
        if any(slot.ledger.requires_recovery() for slot in self.slots.values()):
            raise RecoveryRequired("cannot finish resident shutdown while child ledger is unresolved")
        with self._transition_lock():
            current = self.ledger.read()
            if current["attempt_id"] != self.attempt_id or current["state"] != LifecycleState.STOPPING.value:
                raise RecoveryRequired("resident stop identity changed during shutdown")
        # The resident cannot mark itself clean while it still owns the Anchor.
        # The control/recovery path cleans the ledger only after Anchor absence.
        self._publish_status(desired, "stopping")
        self.store.unlink(self.store.paths.ready)

    def _fail_closed(self, error: BaseException) -> None:
        try:
            for slot in self.slots.values():
                if slot.active:
                    slot.force_stop()
        except Exception:
            pass
        try:
            desired = self.store.desired_manifest()
            revision = desired.revision
        except Exception:
            revision = 0
        try:
            with self._transition_lock():
                current = self.ledger.read()
                if current["state"] != LifecycleState.CLEAN.value and current["attempt_id"] == self.attempt_id:
                    try:
                        self.ledger.mark_uncertain(self.attempt_id)
                    except Exception:
                        pass
                self.store.mark_recovery_required(
                    self.installation_id,
                    revision,
                    self._failure_reason(error),
                )
        except Exception:
            pass
        reason_code = self._failure_reason(error)
        try:
            self.status.set_summary("recovery_required", reason_code=reason_code)
            desired = self.store.desired_manifest()
            self.status.publish(desired.groups, desired.revision)
        except Exception:
            pass

    @staticmethod
    def _failure_reason(error: BaseException) -> str:
        if isinstance(error, RecoveryRequired):
            return "resident_contract_failure"
        if isinstance(error, (OSError, IOError)) or isinstance(error.__cause__, OSError):
            return "resident_io_failure"
        if isinstance(error, ContractError):
            return "resident_state_failure"
        return "resident_internal_failure"

    def _shutdown_requested(self) -> bool:
        request = self.store.read_json(self.store.paths.shutdown, required=False)
        if request is None:
            return False
        return (
            request.get("schema_version") == SCHEMA_VERSION
            and request.get("installation_id") == self.installation_id
            and request.get("incarnation") == self.incarnation
            and request.get("attempt_id") == self.attempt_id
        )

    def _slot_is_current(self, group_id: str, generation: int, slot: int, revision: int) -> bool:
        desired = self._load_desired()
        gate = self.store.gate(self.installation_id)
        if (
            not desired.enabled
            or gate["state"] != "enabled"
            or desired.revision != revision
            or gate["revision"] != revision
        ):
            return False
        target = self._desired_slots(desired).get((group_id, slot))
        return target is not None and target.generation == generation

    def _desired_slots(self, desired: DesiredManifest) -> dict[tuple[str, int], ProcessGroupSpec]:
        targets: dict[tuple[str, int], ProcessGroupSpec] = {}
        if not desired.enabled:
            return targets
        for group in desired.groups:
            for slot in range(group.desired_processes):
                targets[(group.id, slot)] = group
        return targets

    def _publish_runtime_snapshot(
        self,
        desired: DesiredManifest,
        summary: str,
        *,
        force: bool = False,
    ) -> None:
        now = self.clock()
        changed = (
            desired.revision != self._last_published_revision
            or summary != self._last_published_summary
        )
        if not force and not changed and now < self._next_heartbeat_at:
            return
        self._publish_status(desired, summary)
        self._publish_ready(desired)
        self._last_published_revision = desired.revision
        self._last_published_summary = summary
        self._next_heartbeat_at = now + self.heartbeat_interval_seconds

    def _publish_ready(self, desired: DesiredManifest) -> None:
        self.store.write_json(
            self.store.paths.ready,
            {
                "schema_version": SCHEMA_VERSION,
                "installation_id": self.installation_id,
                "incarnation": self.incarnation,
                "attempt_id": self.attempt_id,
                "ready_nonce": self.ready_nonce,
                "anchor_job_name": self.anchor_name,
                "pid": os.getpid(),
                "desired_revision": desired.revision,
                "heartbeat_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    def _publish_status(self, desired: DesiredManifest, summary: str) -> None:
        self.status.set_summary(summary)
        active_keys = set(self.slots)
        for key, slot in self.slots.items():
            snapshot = slot.status()
            state = str(snapshot["state"])
            self.status.update_instance(
                slot.group,
                key[1],
                state=state,
                pid=snapshot["pid"] if isinstance(snapshot["pid"], int) else None,
                restart_count=int(snapshot["restart_count"]),
                last_exit_code=snapshot["last_exit_code"] if isinstance(snapshot["last_exit_code"], int) else None,
                last_error_code="process_failure" if state in {"backoff", "fatal", "uncertain"} else None,
                job_outcome=str(snapshot["last_job_outcome"]),
            )
        for group in desired.groups:
            for slot_index in range(group.desired_processes):
                if (group.id, slot_index) not in active_keys:
                    self.status.remove_instance(group.id, slot_index)
        self.status.publish(desired.groups, desired.revision)

    def _runtime_summary(self) -> str:
        states = {str(slot.status()["state"]) for slot in self.slots.values()}
        if states & {"fatal", "uncertain", "backoff"}:
            return "degraded"
        return "ready"

    def _transition_lock(self) -> WindowsMutex:
        return WindowsMutex(
            mutex_name(self.installation_id, "transition"),
            timeout_ms=5000,
        )