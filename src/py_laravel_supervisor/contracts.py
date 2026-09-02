from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

SCHEMA_VERSION = 1
ProcessKind = Literal["queue_once", "reverb", "scheduler"]
_GROUP_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_QUEUE_NAME = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_CONNECTION = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_SAFE_CHILD_ENV_KEYS = {
    "APP_DEBUG",
    "APP_ENV",
    "APP_URL",
    "BROADCAST_CONNECTION",
    "CACHE_PREFIX",
    "CACHE_STORE",
    "DB_CONNECTION",
    "LOG_CHANNEL",
    "LOG_LEVEL",
    "QUEUE_CONNECTION",
    "REDIS_CLIENT",
    "REDIS_DB",
    "REDIS_HOST",
    "REDIS_PORT",
    "REVERB_HOST",
    "REVERB_PORT",
    "REVERB_SCHEME",
    "REVERB_SERVER_HOST",
    "REVERB_SERVER_PORT",
}
_MAX_CHILD_ENV_ITEMS = 24
_MAX_CHILD_ENV_VALUE_CHARS = 2048


class ContractError(ValueError):
    """Raised when Laravel-owned supervisor state violates the package contract."""


@dataclass(frozen=True, slots=True)
class RestartPolicy:
    enabled: bool = True
    base_delay_seconds: float = 0.25
    max_delay_seconds: float = 5.0
    crash_window_seconds: float = 30.0
    max_crashes: int = 5

    @classmethod
    def from_mapping(cls, value: Any) -> "RestartPolicy":
        if value is None:
            return cls()
        data = _mapping(value, "restart_policy")
        _exact_keys(data, {"enabled", "base_delay_seconds", "max_delay_seconds", "crash_window_seconds", "max_crashes"}, "restart_policy")
        policy = cls(
            enabled=_boolean(data["enabled"], "restart_policy.enabled"),
            base_delay_seconds=_number(data["base_delay_seconds"], 0.05, 60.0, "restart_policy.base_delay_seconds"),
            max_delay_seconds=_number(data["max_delay_seconds"], 0.05, 300.0, "restart_policy.max_delay_seconds"),
            crash_window_seconds=_number(data["crash_window_seconds"], 1.0, 3600.0, "restart_policy.crash_window_seconds"),
            max_crashes=_integer(data["max_crashes"], 1, 100, "restart_policy.max_crashes"),
        )
        if policy.base_delay_seconds > policy.max_delay_seconds:
            raise ContractError("restart_policy base delay exceeds max delay")
        return policy


@dataclass(frozen=True, slots=True)
class RuntimeSpec:
    project_root: Path
    php_executable: Path
    child_environment: tuple[tuple[str, str], ...]

    @classmethod
    def from_mapping(cls, value: Any) -> "RuntimeSpec":
        data = _mapping(value, "runtime")
        _exact_keys(data, {"project_root", "php_executable", "child_environment"}, "runtime")
        project_root = Path(_string(data["project_root"], "runtime.project_root"))
        php_executable = Path(_string(data["php_executable"], "runtime.php_executable"))
        if not project_root.is_absolute() or not php_executable.is_absolute():
            raise ContractError("runtime paths must be absolute")
        environment = _child_environment(data["child_environment"])
        return cls(
            project_root=project_root,
            php_executable=php_executable,
            child_environment=environment,
        )

    def child_environment_mapping(self) -> dict[str, str]:
        return dict(self.child_environment)


@dataclass(frozen=True, slots=True)
class QueueSpec:
    connection: str
    queues: tuple[str, ...]
    backoff: tuple[int, ...]
    tries: int
    sleep_seconds: float
    watchdog_seconds: int

    @classmethod
    def from_mapping(cls, value: Any) -> "QueueSpec":
        data = _mapping(value, "queue")
        _exact_keys(data, {"connection", "queues", "backoff", "tries", "sleep_seconds", "watchdog_seconds"}, "queue")
        connection = _string(data["connection"], "queue.connection")
        if _CONNECTION.fullmatch(connection) is None:
            raise ContractError("invalid queue connection")
        queues_raw = data["queues"]
        if not isinstance(queues_raw, list) or not queues_raw or len(queues_raw) > 32:
            raise ContractError("queue.queues must be a bounded non-empty list")
        queues: list[str] = []
        for item in queues_raw:
            queue = _string(item, "queue.queues[]")
            if _QUEUE_NAME.fullmatch(queue) is None:
                raise ContractError("invalid queue name")
            queues.append(queue)
        backoff_raw = data["backoff"]
        if not isinstance(backoff_raw, list) or not backoff_raw or len(backoff_raw) > 16:
            raise ContractError("queue.backoff must be a bounded non-empty list")
        backoff = tuple(_integer(item, 0, 86400, "queue.backoff[]") for item in backoff_raw)
        return cls(
            connection=connection,
            queues=tuple(queues),
            backoff=backoff,
            tries=_integer(data["tries"], 1, 100, "queue.tries"),
            sleep_seconds=_number(data["sleep_seconds"], 0.0, 3600.0, "queue.sleep_seconds"),
            watchdog_seconds=_integer(data["watchdog_seconds"], 0, 86400, "queue.watchdog_seconds"),
        )


@dataclass(frozen=True, slots=True)
class SchedulerSpec:
    cron: str
    timezone: str
    watchdog_seconds: int

    @classmethod
    def from_mapping(cls, value: Any) -> "SchedulerSpec":
        from .scheduler import validate_cron, validate_timezone

        data = _mapping(value, "scheduler")
        _exact_keys(data, {"cron", "timezone", "watchdog_seconds"}, "scheduler")
        return cls(
            cron=validate_cron(_string(data["cron"], "scheduler.cron")),
            timezone=validate_timezone(_string(data["timezone"], "scheduler.timezone")),
            watchdog_seconds=_integer(data["watchdog_seconds"], 0, 86400, "scheduler.watchdog_seconds"),
        )


@dataclass(frozen=True, slots=True)
class ProcessGroupSpec:
    id: str
    kind: ProcessKind
    generation: int
    desired_processes: int
    stop_grace_seconds: float
    restart: RestartPolicy
    queue: QueueSpec | None
    scheduler: SchedulerSpec | None

    @classmethod
    def from_mapping(cls, value: Any, *, max_processes: int = 32) -> "ProcessGroupSpec":
        data = _mapping(value, "group")
        kind = data.get("kind")
        common = {"id", "kind", "generation", "desired_processes", "stop_grace_seconds", "restart_policy", "queue"}
        if kind == "scheduler":
            _exact_keys(data, common | {"scheduler"}, "group")
        else:
            _exact_keys(data, common, "group")
        group_id = _string(data["id"], "group.id")
        if _GROUP_ID.fullmatch(group_id) is None:
            raise ContractError("invalid group id")
        if kind not in {"queue_once", "reverb", "scheduler"}:
            raise ContractError(f"unsupported process kind for {group_id}")
        desired = _integer(data["desired_processes"], 0, max_processes, "group.desired_processes")
        queue = QueueSpec.from_mapping(data["queue"]) if kind == "queue_once" else None
        scheduler = SchedulerSpec.from_mapping(data["scheduler"]) if kind == "scheduler" else None
        if kind in {"reverb", "scheduler"}:
            if data["queue"] is not None:
                raise ContractError(f"{kind} group cannot define queue options")
            if desired > 1:
                raise ContractError(f"{kind} supports at most one desired process")
        return cls(
            id=group_id,
            kind=kind,
            generation=_integer(data["generation"], 0, 2**63 - 1, "group.generation"),
            desired_processes=desired,
            stop_grace_seconds=_number(data["stop_grace_seconds"], 0.1, 300.0, "group.stop_grace_seconds"),
            restart=RestartPolicy.from_mapping(data["restart_policy"]),
            queue=queue,
            scheduler=scheduler,
        )


@dataclass(frozen=True, slots=True)
class DesiredManifest:
    installation_id: str
    revision: int
    enabled: bool
    generated_at: str
    runtime: RuntimeSpec
    groups: tuple[ProcessGroupSpec, ...]

    @classmethod
    def from_mapping(cls, value: Any, *, max_groups: int = 32, max_processes: int = 32) -> "DesiredManifest":
        data = _mapping(value, "desired manifest")
        _exact_keys(data, {"schema_version", "installation_id", "revision", "enabled", "generated_at", "runtime", "groups"}, "desired manifest")
        if data["schema_version"] != SCHEMA_VERSION:
            raise ContractError("unsupported desired manifest schema_version")
        installation_id = _string(data["installation_id"], "installation_id")
        if not re.fullmatch(r"[A-Za-z0-9_-]{16,128}", installation_id):
            raise ContractError("invalid installation_id")
        groups_raw = data["groups"]
        if not isinstance(groups_raw, list) or len(groups_raw) > max_groups:
            raise ContractError("desired groups must be a bounded list")
        groups = tuple(ProcessGroupSpec.from_mapping(item, max_processes=max_processes) for item in groups_raw)
        ids = [group.id for group in groups]
        if len(ids) != len(set(ids)):
            raise ContractError("desired group ids must be unique")
        return cls(
            installation_id=installation_id,
            revision=_integer(data["revision"], 0, 2**63 - 1, "revision"),
            enabled=_boolean(data["enabled"], "enabled"),
            generated_at=_string(data["generated_at"], "generated_at"),
            runtime=RuntimeSpec.from_mapping(data["runtime"]),
            groups=groups,
        )


def _child_environment(value: Any) -> tuple[tuple[str, str], ...]:
    data = _mapping(value, "runtime.child_environment")
    if len(data) > _MAX_CHILD_ENV_ITEMS:
        raise ContractError("runtime.child_environment exceeds the bounded allowlist size")
    normalized: list[tuple[str, str]] = []
    for key in sorted(data):
        if key not in _SAFE_CHILD_ENV_KEYS:
            raise ContractError(f"runtime.child_environment key is not allowlisted: {key}")
        raw = data[key]
        if not isinstance(raw, str) or "\x00" in raw or len(raw) > _MAX_CHILD_ENV_VALUE_CHARS:
            raise ContractError(f"runtime.child_environment value is invalid: {key}")
        normalized.append((key, raw))
    return tuple(normalized)


def _mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ContractError(f"{label} must be an object with string keys")
    return value


def _exact_keys(data: dict[str, Any], expected: set[str], label: str) -> None:
    if set(data) != expected:
        raise ContractError(f"{label} has unsupported or missing fields")


def _string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value or len(value) > 4096:
        raise ContractError(f"{label} must be a non-empty bounded string")
    return value


def _boolean(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise ContractError(f"{label} must be boolean")
    return value


def _integer(value: Any, minimum: int, maximum: int, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum or value > maximum:
        raise ContractError(f"{label} must be an integer between {minimum} and {maximum}")
    return value


def _number(value: Any, minimum: float, maximum: float, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractError(f"{label} must be numeric")
    number = float(value)
    if number < minimum or number > maximum:
        raise ContractError(f"{label} must be between {minimum} and {maximum}")
    return number