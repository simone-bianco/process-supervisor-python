from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any

from .contracts import ProcessGroupSpec, SCHEMA_VERSION
from .runtime_files import RuntimeStore


class StatusRegistry:
    """Thread-safe sanitized status projection. It never accepts raw child output."""

    def __init__(self, store: RuntimeStore, installation_id: str, incarnation: str) -> None:
        self.store = store
        self.installation_id = installation_id
        self.incarnation = incarnation
        self._lock = threading.Lock()
        self._instances: dict[tuple[str, int], dict[str, Any]] = {}
        self._summary = "starting"
        self._reason_code: str | None = None

    def set_summary(self, summary: str, *, reason_code: str | None = None) -> None:
        if summary not in {"starting", "ready", "degraded", "stopping", "stopped", "recovery_required"}:
            raise ValueError("invalid resident summary")
        with self._lock:
            self._summary = summary
            self._reason_code = reason_code

    def update_instance(
        self,
        group: ProcessGroupSpec,
        slot: int,
        *,
        state: str,
        pid: int | None = None,
        restart_count: int = 0,
        last_exit_code: int | None = None,
        last_error_code: str | None = None,
        job_outcome: str | None = None,
    ) -> None:
        if state not in {"stopped", "starting", "running", "idle", "stopping", "backoff", "fatal", "uncertain"}:
            raise ValueError("invalid instance state")
        with self._lock:
            self._instances[(group.id, slot)] = {
                "group_id": group.id,
                "kind": group.kind,
                "generation": group.generation,
                "slot": slot,
                "state": state,
                "pid": pid,
                "restart_count": max(0, restart_count),
                "last_exit_code": last_exit_code,
                "last_error_code": last_error_code,
                "job_outcome": job_outcome,
            }

    def remove_instance(self, group_id: str, slot: int) -> None:
        with self._lock:
            self._instances.pop((group_id, slot), None)

    def publish(self, groups: tuple[ProcessGroupSpec, ...], revision: int) -> None:
        with self._lock:
            configured = {group.id: group for group in groups}
            grouped: list[dict[str, Any]] = []
            for group in groups:
                instances = [
                    dict(value)
                    for (group_id, _), value in sorted(self._instances.items())
                    if group_id == group.id
                ]
                grouped.append(
                    {
                        "id": group.id,
                        "kind": group.kind,
                        "generation": group.generation,
                        "desired_processes": group.desired_processes,
                        "instances": instances,
                    }
                )
            stale = [key for key in self._instances if key[0] not in configured]
            for key in stale:
                self._instances.pop(key, None)
            document = {
                "schema_version": SCHEMA_VERSION,
                "installation_id": self.installation_id,
                "incarnation": self.incarnation,
                "revision": revision,
                "summary": self._summary,
                "reason_code": self._reason_code,
                "heartbeat_at": datetime.now(timezone.utc).isoformat(),
                "groups": grouped,
            }
        self.store.write_json(self.store.paths.status, document)