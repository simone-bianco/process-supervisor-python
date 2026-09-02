from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from .contracts import ContractError, SCHEMA_VERSION
from .lifecycle import LifecycleLedger, LifecycleState
from .locks import LockUnavailable, WindowsMutex, mutex_name
from .recovery_authority import AuthorityRepairError, RecoveryAuthority, RecoveryOwnership
from .runtime_files import RuntimeStore
from .windows import close_handle, job_exists, open_job, terminate_job, wait_for_job_absent


class RecoveryError(RuntimeError):
    pass


class RuntimeRecovery:
    """Recover exact-owned runtime authority through serialized Windows fences."""

    def __init__(
        self,
        runtime_root: str | Path,
        installation_id: str,
        *,
        transition_timeout_ms: int = 5000,
        recovery_timeout_ms: int = 5000,
    ) -> None:
        self.store = RuntimeStore(runtime_root)
        self.installation_id = installation_id
        self.transition_timeout_ms = max(0, min(transition_timeout_ms, 60_000))
        self.recovery_timeout_ms = max(0, min(recovery_timeout_ms, 60_000))
        self.resident = LifecycleLedger(
            self.store,
            self.store.paths.supervisor_ledger,
            installation_id=installation_id,
            role="resident",
        )
        self.authority = RecoveryAuthority(
            self.store,
            installation_id,
            self.resident,
        )

    def recover(self) -> str:
        try:
            self.store.validate_runtime_owner(self.installation_id)
        except ContractError as error:
            raise RecoveryError(str(error)) from error
        try:
            with WindowsMutex(
                mutex_name(self.installation_id, "recovery"),
                timeout_ms=self.recovery_timeout_ms,
            ):
                return self._recover_serialized()
        except LockUnavailable as error:
            raise RecoveryError("recovery or transition mutex is busy") from error

    def _recover_serialized(self) -> str:
        resident_guard: WindowsMutex | None = None
        ownership_snapshots: list[RecoveryOwnership] = []
        try:
            with self._transition_lock():
                resident_guard = self._acquire_resident_guard()
                try:
                    ownership_snapshots = self.authority.lifecycle_ownership_locked()
                    self.authority.assert_recovery_gate_locked()
                except AuthorityRepairError as error:
                    raise RecoveryError(str(error)) from error

                unresolved = [
                    snapshot
                    for snapshot in ownership_snapshots
                    if snapshot.state["state"] != LifecycleState.CLEAN.value
                ]
                if not unresolved:
                    self.store.ensure_runtime_owner(self.installation_id)
                    try:
                        repaired_state = self.authority.prepare_locked()
                    except AuthorityRepairError as error:
                        raise RecoveryError(str(error)) from error
                    gate = self.store.gate(self.installation_id)
                    current = self._lifecycle_snapshots()
                    if any(
                        state["state"] != LifecycleState.CLEAN.value
                        for _, state in current
                    ):
                        raise RecoveryError(
                            "recovery repair introduced unresolved lifecycle ownership"
                        )
                    outcome = "recovered" if repaired_state else "already_clean"
                    self._commit_stopped(int(gate.get("revision", 0)), outcome)
                    return outcome

                for snapshot in unresolved:
                    ownership = self._ownership(snapshot.state)
                    handle = open_job(ownership, terminate=True)
                    if handle is None:
                        continue
                    try:
                        terminate_job(handle)
                    finally:
                        close_handle(handle)

            for snapshot in unresolved:
                if not wait_for_job_absent(self._ownership(snapshot.state), 3.0):
                    raise RecoveryError(
                        "recorded owned Job Object did not become quiescent"
                    )

            with self._transition_lock():
                try:
                    current_ownership = self.authority.lifecycle_ownership_locked()
                    self.authority.assert_recovery_gate_locked()
                except AuthorityRepairError as error:
                    raise RecoveryError(str(error)) from error
                self._assert_same_ownership(ownership_snapshots, current_ownership)
                for snapshot in unresolved:
                    self._assert_job_absent(snapshot.state)

                self.store.ensure_runtime_owner(self.installation_id)
                try:
                    self.authority.prepare_locked()
                    self.authority.validate_current_locked()
                except AuthorityRepairError as error:
                    raise RecoveryError(str(error)) from error

                gate = self.store.gate(self.installation_id)
                if gate["state"] == "enabled":
                    raise RecoveryError("spawn gate changed while recovery was waiting")

                current = self._lifecycle_snapshots()
                for _, state in current:
                    self._assert_job_absent(state)
                for ledger, state in current:
                    if state["state"] == LifecycleState.CLEAN.value:
                        continue
                    attempt = state.get("attempt_id")
                    if not isinstance(attempt, str):
                        raise RecoveryError(
                            "recovery attempt identity is unavailable"
                        )
                    ledger.mark_clean(attempt)

                self._commit_stopped(int(gate.get("revision", 0)), "recovered")
                return "recovered"
        finally:
            if resident_guard is not None:
                resident_guard.close()

    def _acquire_resident_guard(self) -> WindowsMutex:
        try:
            return WindowsMutex(
                mutex_name(self.installation_id, "resident"),
                timeout_ms=0,
            ).acquire()
        except LockUnavailable as error:
            raise RecoveryError("resident singleton is still active") from error

    def _transition_lock(self) -> WindowsMutex:
        return WindowsMutex(
            mutex_name(self.installation_id, "transition"),
            timeout_ms=self.transition_timeout_ms,
        )

    def _commit_stopped(self, revision: int, reason: str) -> None:
        self.store.disable_gate(self.installation_id, revision)
        self.store.unlink(self.store.paths.ready)
        self.store.unlink(self.store.paths.shutdown)
        self._publish_stopped(reason)

    def _lifecycle_snapshots(
        self,
    ) -> list[tuple[LifecycleLedger, dict[str, object]]]:
        return [(self.resident, self.resident.read()), *self._child_states()]

    def _child_states(self) -> list[tuple[LifecycleLedger, dict[str, object]]]:
        states: list[tuple[LifecycleLedger, dict[str, object]]] = []
        for path in self.store.instance_ledgers():
            raw = self.store.read_json(path)
            if (
                raw.get("installation_id") != self.installation_id
                or raw.get("role") != "child"
            ):
                raise RecoveryError("child lifecycle ledger identity mismatch")
            ledger = LifecycleLedger(
                self.store,
                path,
                installation_id=self.installation_id,
                role="child",
            )
            states.append((ledger, ledger.read()))
        return states

    @staticmethod
    def _ownership(state: dict[str, object]) -> str:
        ownership = state.get("ownership_id")
        if not isinstance(ownership, str) or ownership == "":
            raise RecoveryError(
                "unresolved lifecycle ownership identity is unavailable"
            )
        return ownership

    @classmethod
    def _assert_job_absent(cls, state: dict[str, object]) -> None:
        if state.get("state") == LifecycleState.CLEAN.value:
            return
        if job_exists(cls._ownership(state)):
            raise RecoveryError("recorded owned Job Object still exists")

    @staticmethod
    def _assert_same_ownership(
        before: list[RecoveryOwnership],
        after: list[RecoveryOwnership],
    ) -> None:
        def identities(values: list[RecoveryOwnership]) -> dict[str, tuple[object, object, object]]:
            return {
                str(snapshot.path): (
                    snapshot.state.get("attempt_id"),
                    snapshot.state.get("ownership_id"),
                    snapshot.state.get("state"),
                )
                for snapshot in values
            }

        if identities(before) != identities(after):
            raise RecoveryError("lifecycle ownership changed during recovery")

    def _publish_stopped(self, reason: str) -> None:
        self.store.write_json(
            self.store.paths.status,
            {
                "schema_version": SCHEMA_VERSION,
                "installation_id": self.installation_id,
                "state": "stopped",
                "reason_code": reason,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )