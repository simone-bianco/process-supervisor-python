from __future__ import annotations

import threading
from collections import deque
from datetime import datetime, timezone
from typing import Any

from .contracts import SCHEMA_VERSION
from .queue_protocol import QueueFrame
from .runtime_files import RuntimeStore


class EventStore:
    """Persist bounded allow-listed operational events, never raw child output."""

    def __init__(self, runtime: RuntimeStore, installation_id: str, *, max_events: int = 200) -> None:
        self.runtime = runtime
        self.installation_id = installation_id
        self.max_events = max(1, min(max_events, 1000))
        self._events: deque[dict[str, Any]] = deque()
        self._lock = threading.Lock()

    def publish_queue(self, *, group_id: str, slot: int, frame: QueueFrame) -> None:
        self._append({
            "event": {
                "starting": "job_starting",
                "success": "job_succeeded",
                "failed": "job_failed",
                "released_after_exception": "job_released",
            }[frame.status],
            "at": datetime.now(timezone.utc).isoformat(),
            "group_id": group_id[:64],
            "slot": max(0, min(slot, 1024)),
            "connection": frame.connection[:64],
            "queue": frame.queue[:128],
            "job": frame.job[:256],
            "attempts": max(0, min(frame.attempts, 1000)),
        })

    def publish_lifecycle(
        self,
        event: str,
        *,
        group_id: str | None = None,
        slot: int | None = None,
        reason_code: str | None = None,
    ) -> None:
        if event not in {
            "resident_started",
            "resident_stopping",
            "process_started",
            "process_stopped",
            "process_backoff",
            "process_watchdog_timeout",
        }:
            raise ValueError("unsupported lifecycle event")
        payload: dict[str, Any] = {
            "event": event,
            "at": datetime.now(timezone.utc).isoformat(),
        }
        if group_id is not None:
            payload["group_id"] = group_id[:64]
        if slot is not None:
            payload["slot"] = max(0, min(slot, 1024))
        if reason_code is not None:
            payload["reason_code"] = reason_code[:64]
        self._append(payload)

    def _append(self, payload: dict[str, Any]) -> None:
        with self._lock:
            self._events.append(payload)
            while len(self._events) > self.max_events:
                self._events.popleft()
            self.runtime.write_json(
                self.runtime.paths.events,
                {
                    "schema_version": SCHEMA_VERSION,
                    "installation_id": self.installation_id,
                    "events": list(self._events),
                },
            )