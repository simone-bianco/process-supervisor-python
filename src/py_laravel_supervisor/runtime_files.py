from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .contracts import ContractError, DesiredManifest, SCHEMA_VERSION


@dataclass(frozen=True, slots=True)
class RuntimePaths:
    root: Path

    @property
    def backups(self) -> Path:
        return self.root / "backups"

    @property
    def owner(self) -> Path:
        return self.root / "runtime-owner.json"

    def backup(self, path: Path) -> Path:
        try:
            relative = path.resolve().relative_to(self.root.resolve())
        except ValueError as error:
            raise ContractError("runtime backup target is outside the runtime root") from error
        return self.backups / relative

    @property
    def manifest(self) -> Path:
        return self.root / "manifest.json"

    @property
    def spawn_gate(self) -> Path:
        return self.root / "spawn-gate.json"

    @property
    def desired(self) -> Path:
        return self.root / "desired.json"

    @property
    def supervisor_ledger(self) -> Path:
        return self.root / "supervisor-ledger.json"

    @property
    def ready(self) -> Path:
        return self.root / "ready.json"

    @property
    def status(self) -> Path:
        return self.root / "status.json"

    @property
    def events(self) -> Path:
        return self.root / "events.json"

    @property
    def shutdown(self) -> Path:
        return self.root / "control" / "shutdown.json"

    @property
    def instances(self) -> Path:
        return self.root / "instances"

    @property
    def schedulers(self) -> Path:
        return self.root / "schedulers"

    def scheduler_state(self, group_id: str) -> Path:
        return self.schedulers / f"{group_id}.json"

    def instance_ledger(self, group_id: str, slot: int) -> Path:
        if slot < 0:
            raise ContractError("slot must be non-negative")
        return self.instances / group_id / f"{slot}.json"


class RuntimeStore:
    _BACKED_TOP_LEVEL = {
        "manifest.json",
        "spawn-gate.json",
        "desired.json",
        "supervisor-ledger.json",
    }
    _WINDOWS_IO_RETRY_SECONDS = 2.0
    _WINDOWS_IO_RETRY_INTERVAL_SECONDS = 0.005

    def __init__(self, root: str | Path) -> None:
        self.paths = RuntimePaths(Path(root).expanduser().resolve())
        self.paths.root.mkdir(parents=True, exist_ok=True)

    def initialize(self, installation_id: str) -> None:
        self.ensure_runtime_owner(installation_id)
        current = self.read_json(self.paths.manifest, required=False)
        if current is None:
            try:
                self.create_json_exclusive(
                    self.paths.manifest,
                    {"schema_version": SCHEMA_VERSION, "installation_id": installation_id},
                )
            except FileExistsError:
                pass
            current = self.read_json(self.paths.manifest)
        if current.get("schema_version") != SCHEMA_VERSION or current.get("installation_id") != installation_id:
            raise ContractError("runtime manifest identity mismatch")
        if self.read_json(self.paths.spawn_gate, required=False) is None:
            self.write_json(
                self.paths.spawn_gate,
                {"schema_version": SCHEMA_VERSION, "installation_id": installation_id, "state": "disabled", "revision": 0},
            )

    def validate_runtime_owner(self, installation_id: str) -> bool:
        """Validate root identity without mutating runtime state.

        Returns True when the immutable owner file already exists, False when
        identity can be established safely but the owner still needs creation.
        """
        try:
            owner = self.read_json(self.paths.owner, required=False)
        except ContractError as error:
            raise ContractError("runtime owner identity is unreadable") from error
        if owner is not None:
            self._validate_owner_document(owner, installation_id)
            return True

        identities: set[str] = set()
        for candidate in [self.paths.manifest, self.paths.backup(self.paths.manifest)]:
            try:
                value = self.read_json(candidate, required=False)
            except ContractError:
                continue
            if value is None or value.get("schema_version") != SCHEMA_VERSION:
                continue
            observed = value.get("installation_id")
            if isinstance(observed, str) and observed:
                identities.add(observed)
        if identities:
            if identities != {installation_id}:
                raise ContractError("runtime owner identity mismatch")
            return False
        if self.authority_evidence_exists():
            raise ContractError("runtime owner identity cannot be established from corrupt authority")
        return False

    def ensure_runtime_owner(self, installation_id: str) -> None:
        if self.validate_runtime_owner(installation_id):
            return
        payload = {
            "schema_version": SCHEMA_VERSION,
            "installation_id": installation_id,
        }
        try:
            self.create_json_exclusive(self.paths.owner, payload)
        except FileExistsError:
            pass
        owner = self.read_json(self.paths.owner)
        self._validate_owner_document(owner, installation_id)

    @staticmethod
    def _validate_owner_document(value: dict[str, Any], installation_id: str) -> None:
        if set(value) != {"schema_version", "installation_id"}:
            raise ContractError("runtime owner has unsupported or missing fields")
        if value.get("schema_version") != SCHEMA_VERSION or value.get("installation_id") != installation_id:
            raise ContractError("runtime owner identity mismatch")

    def desired_manifest(self) -> DesiredManifest:
        return DesiredManifest.from_mapping(self.read_json(self.paths.desired))

    def read_json(self, path: Path, *, required: bool = True) -> dict[str, Any] | None:
        value: Any = None
        deadline = time.monotonic() + self._WINDOWS_IO_RETRY_SECONDS
        while True:
            try:
                with path.open("r", encoding="utf-8") as handle:
                    value = json.load(handle)
                break
            except FileNotFoundError:
                if required:
                    raise ContractError(f"required runtime file is missing: {path.name}")
                return None
            except OSError as error:
                if (
                    os.name == "nt"
                    and self._is_transient_windows_share_error(error)
                    and time.monotonic() < deadline
                ):
                    time.sleep(self._WINDOWS_IO_RETRY_INTERVAL_SECONDS)
                    continue
                raise ContractError(f"runtime file is unreadable or corrupt: {path.name}") from error
            except json.JSONDecodeError as error:
                raise ContractError(f"runtime file is unreadable or corrupt: {path.name}") from error
        if not isinstance(value, dict):
            raise ContractError(f"runtime file must contain an object: {path.name}")
        return value

    @staticmethod
    def _is_transient_windows_share_error(error: OSError) -> bool:
        return isinstance(error, PermissionError) or getattr(error, "winerror", None) in {5, 32, 33}

    def write_json(self, path: Path, value: dict[str, Any]) -> None:
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
        self._replace_text(path, encoded)
        if self._is_backed(path):
            self._replace_text(self.paths.backup(path), encoded)

    def write_backup(self, path: Path, value: dict[str, Any]) -> None:
        if not self._is_backed(path):
            raise ContractError(f"runtime file is not backup-authoritative: {path.name}")
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
        self._replace_text(self.paths.backup(path), encoded)

    def _replace_text(self, path: Path, encoded: str) -> None:
        if not encoded:
            raise ContractError("runtime JSON payload must not be empty")
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            self._replace_with_retry(Path(temporary), path)
            observed = self._read_text_with_retry(path)
            if observed != encoded:
                raise ContractError(f"runtime file verification failed after replace: {path.name}")
        except BaseException:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise

    def _replace_with_retry(self, source: Path, target: Path) -> None:
        deadline = time.monotonic() + self._WINDOWS_IO_RETRY_SECONDS
        while True:
            try:
                os.replace(source, target)
                return
            except OSError as error:
                if (
                    os.name != "nt"
                    or not self._is_transient_windows_share_error(error)
                    or time.monotonic() >= deadline
                ):
                    raise ContractError(f"runtime file replace failed: {target.name}") from error
                time.sleep(self._WINDOWS_IO_RETRY_INTERVAL_SECONDS)

    def _read_text_with_retry(self, path: Path) -> str:
        deadline = time.monotonic() + self._WINDOWS_IO_RETRY_SECONDS
        while True:
            try:
                return path.read_text(encoding="utf-8")
            except OSError as error:
                if (
                    os.name != "nt"
                    or not self._is_transient_windows_share_error(error)
                    or time.monotonic() >= deadline
                ):
                    raise ContractError(f"runtime file verification read failed: {path.name}") from error
                time.sleep(self._WINDOWS_IO_RETRY_INTERVAL_SECONDS)

    def _is_backed(self, path: Path) -> bool:
        try:
            relative = path.resolve().relative_to(self.paths.root.resolve())
        except ValueError:
            return False
        return (
            len(relative.parts) == 1 and relative.name in self._BACKED_TOP_LEVEL
        ) or (len(relative.parts) >= 2 and relative.parts[0] == "instances")

    def create_json_exclusive(self, path: Path, value: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
        fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                raise
            except OSError:
                if os.name != "nt":
                    raise
                os.rename(temporary, path)
                temporary = ""
        finally:
            if temporary:
                try:
                    os.unlink(temporary)
                except FileNotFoundError:
                    pass
        if self._read_text_with_retry(path) != encoded:
            raise ContractError(f"runtime file verification failed after exclusive create: {path.name}")
        if self._is_backed(path):
            self._replace_text(self.paths.backup(path), encoded)

    def restore_backed_files(self, installation_id: str) -> list[str]:
        restored: list[str] = []
        # Only authoritative lifecycle/desired documents are backed up. Status and
        # event journals are high-churn derived projections and are regenerated.
        candidates = {
            self.paths.manifest,
            self.paths.spawn_gate,
            self.paths.desired,
            self.paths.supervisor_ledger,
        }
        if self.paths.instances.exists():
            candidates.update(path for path in self.paths.instances.rglob("*.json") if path.is_file())
        if self.paths.backups.joinpath("instances").exists():
            candidates.update(
                self.paths.root / path.relative_to(self.paths.backups)
                for path in self.paths.backups.joinpath("instances").rglob("*.json")
            )
        for path in sorted(candidates):
            try:
                current = self.read_json(path, required=False)
            except ContractError:
                current = None
            if current is not None:
                continue
            backup = self.paths.backup(path)
            try:
                recovered = self.read_json(backup, required=False)
            except ContractError:
                recovered = None
            if recovered is None:
                continue
            if recovered.get("schema_version") != SCHEMA_VERSION or recovered.get("installation_id") != installation_id:
                continue
            self._replace_text(
                path,
                json.dumps(recovered, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n",
            )
            restored.append(path.relative_to(self.paths.root).as_posix())
        return restored

    def archive_authority_evidence(self) -> Path:
        archive = self.paths.root / "corrupt" / f"{time.time_ns()}-{os.getpid()}"
        archive.mkdir(parents=True, exist_ok=False)
        for path in [
            self.paths.owner,
            self.paths.manifest,
            self.paths.spawn_gate,
            self.paths.desired,
            self.paths.supervisor_ledger,
            self.paths.status,
            self.paths.events,
            self.paths.ready,
            self.paths.shutdown,
        ]:
            if not path.exists() or not path.is_file():
                continue
            target = archive / path.relative_to(self.paths.root)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
        for directory in [
            self.paths.instances,
            self.paths.backups,
            self.paths.schedulers,
        ]:
            if directory.exists():
                shutil.copytree(
                    directory,
                    archive / directory.relative_to(self.paths.root),
                    dirs_exist_ok=True,
                )
        return archive

    def clear_authority_for_quiescent_salvage(self) -> None:
        for path in [
            self.paths.manifest,
            self.paths.spawn_gate,
            self.paths.desired,
            self.paths.supervisor_ledger,
            self.paths.status,
            self.paths.events,
            self.paths.ready,
            self.paths.shutdown,
        ]:
            self.unlink(path)
        for directory in [
            self.paths.instances,
            self.paths.backups,
            self.paths.schedulers,
        ]:
            self._remove_tree(directory)

    def authority_evidence_exists(self) -> bool:
        if any(
            path.exists()
            for path in [
                self.paths.manifest,
                self.paths.spawn_gate,
                self.paths.desired,
                self.paths.supervisor_ledger,
                self.paths.status,
                self.paths.events,
                self.paths.ready,
                self.paths.shutdown,
            ]
        ):
            return True
        return any(
            directory.exists() and any(directory.rglob("*"))
            for directory in [
                self.paths.instances,
                self.paths.backups,
                self.paths.schedulers,
            ]
        )

    def _remove_tree(self, path: Path) -> None:
        if not path.exists():
            return
        deadline = time.monotonic() + self._WINDOWS_IO_RETRY_SECONDS
        while True:
            try:
                shutil.rmtree(path)
                return
            except FileNotFoundError:
                return
            except OSError as error:
                if (
                    os.name != "nt"
                    or not self._is_transient_windows_share_error(error)
                    or time.monotonic() >= deadline
                ):
                    raise ContractError(f"runtime directory deletion failed: {path.name}") from error
                time.sleep(self._WINDOWS_IO_RETRY_INTERVAL_SECONDS)

    def gate(self, installation_id: str) -> dict[str, Any]:
        return self._validate_gate(self.read_json(self.paths.spawn_gate), installation_id)

    def gate_with_backup(self, installation_id: str) -> dict[str, Any]:
        try:
            return self.gate(installation_id)
        except ContractError as primary_error:
            backup = self.read_json(self.paths.backup(self.paths.spawn_gate), required=False)
            if backup is None:
                raise primary_error
            return self._validate_gate(backup, installation_id)

    @staticmethod
    def _validate_gate(gate: dict[str, Any], installation_id: str) -> dict[str, Any]:
        if gate.get("schema_version") != SCHEMA_VERSION or gate.get("installation_id") != installation_id:
            raise ContractError("spawn gate identity mismatch")
        if gate.get("state") not in {"enabled", "disabled", "recovery_required"}:
            raise ContractError("spawn gate state is invalid")
        revision = gate.get("revision")
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
            raise ContractError("spawn gate revision is invalid")
        return gate

    def require_gate_enabled(self, installation_id: str) -> dict[str, Any]:
        gate = self.gate(installation_id)
        if gate.get("state") != "enabled":
            raise ContractError("spawn gate is not enabled")
        return gate

    def enable_gate(self, installation_id: str, revision: int) -> None:
        self.write_json(
            self.paths.spawn_gate,
            {"schema_version": SCHEMA_VERSION, "installation_id": installation_id, "state": "enabled", "revision": revision},
        )

    def disable_gate(self, installation_id: str, revision: int) -> None:
        self.write_json(
            self.paths.spawn_gate,
            {"schema_version": SCHEMA_VERSION, "installation_id": installation_id, "state": "disabled", "revision": revision},
        )

    def instance_ledgers(self) -> list[Path]:
        if not self.paths.instances.exists():
            return []
        return sorted(path for path in self.paths.instances.rglob("*.json") if path.is_file())

    def unlink(self, path: Path) -> None:
        deadline = time.monotonic() + self._WINDOWS_IO_RETRY_SECONDS
        while True:
            try:
                path.unlink()
                return
            except FileNotFoundError:
                return
            except OSError as error:
                if (
                    os.name != "nt"
                    or not self._is_transient_windows_share_error(error)
                    or time.monotonic() >= deadline
                ):
                    raise ContractError(f"runtime file deletion failed: {path.name}") from error
                time.sleep(self._WINDOWS_IO_RETRY_INTERVAL_SECONDS)

    def mark_recovery_required(self, installation_id: str, revision: int, reason_code: str) -> None:
        self.write_json(
            self.paths.spawn_gate,
            {
                "schema_version": SCHEMA_VERSION,
                "installation_id": installation_id,
                "state": "recovery_required",
                "revision": revision,
                "reason_code": reason_code,
            },
        )