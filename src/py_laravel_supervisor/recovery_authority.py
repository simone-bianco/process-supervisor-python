from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .contracts import ContractError, DesiredManifest, SCHEMA_VERSION
from .lifecycle import LifecycleLedger, LifecycleState
from .runtime_files import RuntimeStore


class AuthorityRepairError(RuntimeError):
    pass


class ForeignAuthorityError(AuthorityRepairError):
    pass


@dataclass(frozen=True, slots=True)
class RecoveryOwnership:
    path: Path
    role: str
    state: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _AuthoritySpec:
    primary: Path
    kind: str
    required: bool
    backed: bool = True
    group_id: str | None = None
    slot: int | None = None


@dataclass(slots=True)
class _AuthorityPlan:
    restore: list[tuple[_AuthoritySpec, dict[str, Any]]]
    refresh_backups: list[tuple[_AuthoritySpec, dict[str, Any]]]
    primary_repair: bool
    salvage: bool
    gate: dict[str, Any] | None


class RecoveryAuthority:
    """Validate and repair runtime authority while caller owns transition and resident fences."""

    def __init__(
        self,
        store: RuntimeStore,
        installation_id: str,
        resident: LifecycleLedger,
    ) -> None:
        self.store = store
        self.installation_id = installation_id
        self.resident = resident

    def lifecycle_ownership_locked(self) -> list[RecoveryOwnership]:
        specs, _invalid_inventory = self._specs()
        snapshots: list[RecoveryOwnership] = []
        for spec in specs:
            if spec.kind not in {"resident", "child"}:
                continue
            primary_state, primary = self._document_state(spec, spec.primary)
            backup_path = self.store.paths.backup(spec.primary)
            backup_state, backup = self._document_state(spec, backup_path)
            if primary_state == "foreign" or backup_state == "foreign":
                raise ForeignAuthorityError("runtime lifecycle authority belongs to another installation")
            selected = primary if primary_state == "valid" else backup if backup_state == "valid" else None
            if selected is None:
                if primary_state == "corrupt" or backup_state == "corrupt":
                    raise AuthorityRepairError(
                        f"runtime lifecycle ownership is corrupt without a valid backup: {spec.primary.name}"
                    )
                continue
            snapshots.append(
                RecoveryOwnership(
                    path=spec.primary,
                    role="resident" if spec.kind == "resident" else "child",
                    state=selected,
                )
            )
        return snapshots

    def assert_recovery_gate_locked(self) -> None:
        if not self.store.authority_evidence_exists():
            return
        plan = self._plan()
        if plan.gate is not None and plan.gate.get("state") == "enabled":
            raise AuthorityRepairError("disable the spawn gate before recovery")

    def prepare_locked(self) -> bool:
        if not self.store.authority_evidence_exists():
            self._create_clean_authority()
            self.validate_current_locked()
            return True

        plan = self._plan()
        if plan.gate is not None and plan.gate.get("state") == "enabled":
            raise AuthorityRepairError("disable the spawn gate before recovery")
        if plan.salvage:
            self._salvage_locked()
            self.validate_current_locked()
            return True

        for spec, value in plan.restore:
            self.store.write_json(spec.primary, value)
        for spec, value in plan.refresh_backups:
            self.store.write_backup(spec.primary, value)

        self.validate_current_locked()
        if plan.primary_repair:
            gate = self.store.gate(self.installation_id)
            if gate["state"] == "enabled":
                self.store.mark_recovery_required(
                    self.installation_id,
                    int(gate["revision"]),
                    "last_known_good_restored",
                )
        return plan.primary_repair

    def validate_current_locked(self) -> None:
        specs, invalid_inventory = self._specs()
        if invalid_inventory:
            raise AuthorityRepairError("runtime instance authority layout is invalid")
        for spec in specs:
            state, _ = self._document_state(spec, spec.primary)
            if state == "missing" and not spec.required:
                continue
            if state != "valid":
                raise AuthorityRepairError(
                    f"runtime authority remains invalid after repair: {spec.primary.name}"
                )

    def _plan(self) -> _AuthorityPlan:
        specs, invalid_inventory = self._specs()
        restore: list[tuple[_AuthoritySpec, dict[str, Any]]] = []
        refresh: list[tuple[_AuthoritySpec, dict[str, Any]]] = []
        primary_repair = False
        salvage = invalid_inventory
        gate: dict[str, Any] | None = None

        for spec in specs:
            primary_state, primary = self._document_state(spec, spec.primary)
            if spec.backed:
                backup_path = self.store.paths.backup(spec.primary)
                backup_state, backup = self._document_state(spec, backup_path)
            else:
                backup_state, backup = "missing", None
            if primary_state == "foreign" or backup_state == "foreign":
                raise ForeignAuthorityError("runtime authority belongs to another installation")
            if spec.kind == "gate":
                if primary_state == "valid":
                    gate = primary
                elif backup_state == "valid":
                    gate = backup

            if primary_state == "valid" and primary is not None:
                if spec.backed and (backup_state != "valid" or backup != primary):
                    refresh.append((spec, primary))
                continue

            if backup_state == "valid" and backup is not None:
                restore.append((spec, backup))
                primary_repair = True
                continue

            if not spec.required and primary_state == "missing" and backup_state == "missing":
                continue

            primary_repair = True
            salvage = True

        return _AuthorityPlan(
            restore=restore,
            refresh_backups=refresh,
            primary_repair=primary_repair,
            salvage=salvage,
            gate=gate,
        )

    def _specs(self) -> tuple[list[_AuthoritySpec], bool]:
        specs = [
            _AuthoritySpec(self.store.paths.manifest, "manifest", True),
            _AuthoritySpec(self.store.paths.spawn_gate, "gate", True),
            _AuthoritySpec(self.store.paths.desired, "desired", False),
            _AuthoritySpec(self.store.paths.supervisor_ledger, "resident", True),
        ]
        invalid_inventory = False
        relative_instances: set[Path] = set()
        if self.store.paths.instances.exists():
            relative_instances.update(
                path.relative_to(self.store.paths.instances)
                for path in self.store.paths.instances.rglob("*.json")
                if path.is_file()
            )
        backed_instances = self.store.paths.backups / "instances"
        if backed_instances.exists():
            relative_instances.update(
                path.relative_to(backed_instances)
                for path in backed_instances.rglob("*.json")
                if path.is_file()
            )

        for relative in sorted(relative_instances):
            if len(relative.parts) != 2 or relative.suffix.lower() != ".json":
                invalid_inventory = True
                continue
            group_id = relative.parts[0]
            try:
                slot = int(relative.stem)
            except ValueError:
                invalid_inventory = True
                continue
            if group_id == "" or slot < 0 or str(slot) != relative.stem:
                invalid_inventory = True
                continue
            specs.append(
                _AuthoritySpec(
                    self.store.paths.instances / relative,
                    "child",
                    True,
                    group_id=group_id,
                    slot=slot,
                )
            )

        if self.store.paths.schedulers.exists():
            for path in sorted(self.store.paths.schedulers.rglob("*")):
                if not path.is_file():
                    continue
                relative = path.relative_to(self.store.paths.schedulers)
                group_id = relative.stem
                if (
                    len(relative.parts) != 1
                    or relative.suffix.lower() != ".json"
                    or re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,63}", group_id) is None
                ):
                    invalid_inventory = True
                    continue
                specs.append(
                    _AuthoritySpec(
                        path,
                        "scheduler",
                        True,
                        backed=False,
                        group_id=group_id,
                    )
                )
        return specs, invalid_inventory

    def _document_state(
        self,
        spec: _AuthoritySpec,
        path: Path,
    ) -> tuple[str, dict[str, Any] | None]:
        try:
            value = self.store.read_json(path, required=False)
        except ContractError:
            return "corrupt", None
        if value is None:
            return "missing", None
        try:
            self._validate_document(spec, path, value)
        except ForeignAuthorityError:
            return "foreign", value
        except (AuthorityRepairError, ContractError, ValueError):
            return "corrupt", None
        return "valid", value

    def _validate_document(
        self,
        spec: _AuthoritySpec,
        path: Path,
        value: dict[str, Any],
    ) -> None:
        if spec.kind == "manifest":
            self._validate_identity(value)
            return
        if spec.kind == "gate":
            self._validate_identity(value)
            if value.get("state") not in {"enabled", "disabled", "recovery_required"}:
                raise AuthorityRepairError("spawn gate state is invalid")
            revision = value.get("revision")
            if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
                raise AuthorityRepairError("spawn gate revision is invalid")
            return
        if spec.kind == "desired":
            desired = DesiredManifest.from_mapping(value)
            if desired.installation_id != self.installation_id:
                raise ForeignAuthorityError("desired installation identity mismatch")
            return
        if spec.kind == "scheduler":
            if set(value) != {"schema_version", "group_id", "generation", "minute"}:
                raise AuthorityRepairError("scheduler claim has unsupported or missing fields")
            if value.get("schema_version") != SCHEMA_VERSION or value.get("group_id") != spec.group_id:
                raise AuthorityRepairError("scheduler claim identity mismatch")
            generation = value.get("generation")
            if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
                raise AuthorityRepairError("scheduler claim generation is invalid")
            minute = value.get("minute")
            if not isinstance(minute, str):
                raise AuthorityRepairError("scheduler claim minute is invalid")
            observed = datetime.fromisoformat(minute)
            if (
                observed.tzinfo is None
                or observed.second != 0
                or observed.microsecond != 0
                or observed.utcoffset() != timezone.utc.utcoffset(observed)
            ):
                raise AuthorityRepairError("scheduler claim minute is invalid")
            return

        role = "resident" if spec.kind == "resident" else "child"
        if value.get("schema_version") == SCHEMA_VERSION and value.get("installation_id") != self.installation_id:
            raise ForeignAuthorityError("lifecycle installation identity mismatch")
        ledger = LifecycleLedger(
            self.store,
            path,
            installation_id=self.installation_id,
            role=role,
        )
        ledger.validate_document(value)
        if role == "child" and value.get("state") != LifecycleState.CLEAN.value:
            if value.get("group_id") != spec.group_id or value.get("slot") != spec.slot:
                raise AuthorityRepairError("child lifecycle path identity mismatch")

    def _validate_identity(self, value: dict[str, Any]) -> None:
        if (
            value.get("schema_version") != SCHEMA_VERSION
            or value.get("installation_id") != self.installation_id
        ):
            raise ForeignAuthorityError("runtime authority identity mismatch")

    def _salvage_locked(self) -> None:
        self.store.archive_authority_evidence()
        self.store.clear_authority_for_quiescent_salvage()
        self._create_clean_authority()

    def _create_clean_authority(self) -> None:
        self.store.initialize(self.installation_id)
        self.resident.initialize_clean()
        self.store.write_json(
            self.store.paths.events,
            {
                "schema_version": SCHEMA_VERSION,
                "installation_id": self.installation_id,
                "events": [],
            },
        )
        self.store.write_json(
            self.store.paths.status,
            {
                "schema_version": SCHEMA_VERSION,
                "installation_id": self.installation_id,
                "state": "stopped",
                "reason_code": "corrupt_state_salvaged",
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )