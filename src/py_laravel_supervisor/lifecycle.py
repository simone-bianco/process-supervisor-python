from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterable

from .contracts import ContractError, SCHEMA_VERSION
from .runtime_files import RuntimeStore


class LifecycleState(str, Enum):
    CLEAN = "clean"
    SPAWN_ARMED = "spawn_armed"
    RESIDENT_ACTIVE = "resident_active"
    CHILD_ACTIVE = "child_active"
    STOPPING = "stopping"
    UNCERTAIN = "uncertain"


_ACTIVE = {LifecycleState.RESIDENT_ACTIVE, LifecycleState.CHILD_ACTIVE}


class LifecycleLedger:
    """Durable write-ahead ownership state for one resident or managed child."""

    def __init__(
        self,
        store: RuntimeStore,
        path: Path,
        *,
        installation_id: str,
        role: str,
    ) -> None:
        if role not in {"resident", "child"}:
            raise ContractError("lifecycle role must be resident or child")
        self.store = store
        self.path = path
        self.installation_id = installation_id
        self.role = role

    def initialize_clean(self) -> dict[str, Any]:
        current = self.store.read_json(self.path, required=False)
        if current is None:
            try:
                self.store.create_json_exclusive(self.path, self._clean_document(0))
            except FileExistsError:
                pass
        return self.read()

    def read(self) -> dict[str, Any]:
        value = self.store.read_json(self.path)
        self._validate(value)
        return value

    def arm(
        self,
        *,
        attempt_id: str,
        desired_revision: int,
        ownership_id: str,
        supervisor_incarnation: str,
        ready_nonce: str | None = None,
        group_id: str | None = None,
        slot: int | None = None,
    ) -> dict[str, Any]:
        current = self._require_state({LifecycleState.CLEAN})
        self._require_hex32(attempt_id, "attempt_id")
        self._require_hex32(supervisor_incarnation, "supervisor_incarnation")
        if isinstance(desired_revision, bool) or not isinstance(desired_revision, int) or desired_revision < 0:
            raise ContractError("desired_revision must be a non-negative integer")
        if not isinstance(ownership_id, str) or not ownership_id:
            raise ContractError("ownership_id is required")
        if self.role == "child":
            if ready_nonce is not None:
                raise ContractError("child lifecycle may not include a ready nonce")
            if not isinstance(group_id, str) or not group_id:
                raise ContractError("child lifecycle requires group_id")
            if isinstance(slot, bool) or not isinstance(slot, int) or slot < 0:
                raise ContractError("child lifecycle requires a non-negative slot")
        else:
            self._require_hex32(ready_nonce, "ready_nonce")
            if group_id is not None or slot is not None:
                raise ContractError("resident lifecycle may not include child identity")
        return self._transition(current, LifecycleState.SPAWN_ARMED, {
            "attempt_id": attempt_id,
            "desired_revision": desired_revision,
            "ownership_id": ownership_id,
            "supervisor_incarnation": supervisor_incarnation,
            "ready_nonce": ready_nonce,
            "group_id": group_id,
            "slot": slot,
            "pid": None,
        })

    def mark_active(self, attempt_id: str, *, pid: int) -> dict[str, Any]:
        current = self._require_attempt(attempt_id, {LifecycleState.SPAWN_ARMED, LifecycleState.UNCERTAIN})
        if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
            raise ContractError("pid must be a positive informational integer")
        state = LifecycleState.RESIDENT_ACTIVE if self.role == "resident" else LifecycleState.CHILD_ACTIVE
        return self._transition(current, state, {"pid": pid})

    def mark_stopping(self, attempt_id: str) -> dict[str, Any]:
        current = self._require_attempt(
            attempt_id,
            {LifecycleState.SPAWN_ARMED, LifecycleState.UNCERTAIN, *_ACTIVE},
        )
        return self._transition(current, LifecycleState.STOPPING, {})

    def mark_uncertain(self, attempt_id: str) -> dict[str, Any]:
        current = self._require_attempt(
            attempt_id,
            {LifecycleState.SPAWN_ARMED, LifecycleState.STOPPING, *_ACTIVE},
        )
        return self._transition(current, LifecycleState.UNCERTAIN, {})

    def mark_clean(self, attempt_id: str) -> dict[str, Any]:
        current = self._require_attempt(
            attempt_id,
            {LifecycleState.SPAWN_ARMED, LifecycleState.STOPPING, LifecycleState.UNCERTAIN, *_ACTIVE},
        )
        clean = self._clean_document(current["state_sequence"] + 1)
        self.store.write_json(self.path, clean)
        return clean

    def requires_recovery(self) -> bool:
        return LifecycleState(self.read()["state"]) is not LifecycleState.CLEAN

    def validate_document(self, value: dict[str, Any]) -> None:
        self._validate(value)

    def _transition(
        self,
        current: dict[str, Any],
        state: LifecycleState,
        changes: dict[str, Any],
    ) -> dict[str, Any]:
        next_value = {
            **current,
            **changes,
            "state": state.value,
            "state_sequence": current["state_sequence"] + 1,
            "written_at": self._timestamp(),
        }
        self._validate(next_value)
        self.store.write_json(self.path, next_value)
        return next_value

    def _require_state(self, allowed: Iterable[LifecycleState]) -> dict[str, Any]:
        value = self.read()
        if LifecycleState(value["state"]) not in set(allowed):
            raise ContractError(f"lifecycle state [{value['state']}] is not allowed for this transition")
        return value

    def _require_attempt(self, attempt_id: str, allowed: Iterable[LifecycleState]) -> dict[str, Any]:
        self._require_hex32(attempt_id, "attempt_id")
        value = self._require_state(allowed)
        if value["attempt_id"] != attempt_id:
            raise ContractError("lifecycle attempt identity mismatch")
        return value

    def _clean_document(self, sequence: int) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "installation_id": self.installation_id,
            "role": self.role,
            "state": LifecycleState.CLEAN.value,
            "state_sequence": sequence,
            "attempt_id": None,
            "desired_revision": None,
            "ownership_id": None,
            "supervisor_incarnation": None,
            "ready_nonce": None,
            "group_id": None,
            "slot": None,
            "pid": None,
            "written_at": self._timestamp(),
        }

    def _validate(self, value: dict[str, Any]) -> None:
        if value.get("schema_version") != SCHEMA_VERSION or value.get("installation_id") != self.installation_id:
            raise ContractError("lifecycle ledger identity or schema mismatch")
        if value.get("role") != self.role:
            raise ContractError("lifecycle ledger role mismatch")
        try:
            state = LifecycleState(value.get("state"))
        except ValueError as error:
            raise ContractError("invalid lifecycle state") from error
        sequence = value.get("state_sequence")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
            raise ContractError("invalid lifecycle state sequence")
        if not isinstance(value.get("written_at"), str):
            raise ContractError("invalid lifecycle timestamp")
        if state is LifecycleState.CLEAN:
            for field in ["attempt_id", "desired_revision", "ownership_id", "supervisor_incarnation", "ready_nonce", "group_id", "slot", "pid"]:
                if value.get(field) is not None:
                    raise ContractError(f"clean lifecycle ledger retains {field}")
            return
        self._require_hex32(value.get("attempt_id"), "attempt_id")
        self._require_hex32(value.get("supervisor_incarnation"), "supervisor_incarnation")
        revision = value.get("desired_revision")
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
            raise ContractError("invalid lifecycle desired revision")
        if not isinstance(value.get("ownership_id"), str) or not value["ownership_id"]:
            raise ContractError("invalid lifecycle ownership identity")
        if self.role == "child":
            if value.get("ready_nonce") is not None:
                raise ContractError("child lifecycle contains a ready nonce")
            if not isinstance(value.get("group_id"), str) or not value["group_id"]:
                raise ContractError("child lifecycle group id is invalid")
            slot = value.get("slot")
            if isinstance(slot, bool) or not isinstance(slot, int) or slot < 0:
                raise ContractError("child lifecycle slot is invalid")
        else:
            self._require_hex32(value.get("ready_nonce"), "ready_nonce")
            if value.get("group_id") is not None or value.get("slot") is not None:
                raise ContractError("resident lifecycle contains child identity")
        pid = value.get("pid")
        if state in _ACTIVE and (isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0):
            raise ContractError("active lifecycle requires a positive informational pid")
        if state not in _ACTIVE and pid is not None and (isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0):
            raise ContractError("invalid lifecycle pid")

    @staticmethod
    def _require_hex32(value: Any, label: str) -> str:
        if not isinstance(value, str) or len(value) != 32 or any(character not in "0123456789abcdef" for character in value):
            raise ContractError(f"{label} must be a 32-character lowercase hexadecimal id")
        return value

    @staticmethod
    def _timestamp() -> str:
        return datetime.now(timezone.utc).isoformat()